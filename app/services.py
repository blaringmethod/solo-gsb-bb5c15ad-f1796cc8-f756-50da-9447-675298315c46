"""核心业务逻辑：创建 / 取件（原子消费）/ 撤回 / 状态 / 审计。

取件并发正确性的两道保险：
1. Redis ``SET NX`` 串行锁：同一 token 的并发取件只有一个进入临界区，
   其他请求立即得到 409 in_progress（可重试）。
2. PostgreSQL 单条条件 UPDATE（行级原子）：只有
   ``consumed_at IS NULL AND withdrawn_at IS NULL AND now() <= expires_at``
   的行才能被标记 consumed。即使 Redis 被清空、多个请求同时穿透到数据库，
   也只有一个 UPDATE 命中一行并拿到内容，其余得到 consumed。
"""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import asyncpg

from . import redis_store
from .config import settings
from . import db
from .errors import DomainError

# 稳定的终态 reason 码
REASON_NOT_FOUND = "not_found"
REASON_EXPIRED = "expired"
REASON_CONSUMED = "consumed"
REASON_WITHDRAWN = "withdrawn"
REASON_IN_PROGRESS = "in_progress"
REASON_PAYLOAD_TOO_LARGE = "payload_too_large"
REASON_INVALID_TTL = "invalid_ttl"
REASON_WRONG_SECRET = "wrong_secret"

_TERMINAL_REASONS = {
    REASON_NOT_FOUND,
    REASON_EXPIRED,
    REASON_CONSUMED,
    REASON_WITHDRAWN,
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _new_token() -> str:
    """URL-safe 的随机取件令牌（约 43 字符）。"""
    return secrets.token_urlsafe(32)


def _new_secret() -> str:
    return secrets.token_urlsafe(24)


async def _add_audit(
    conn: asyncpg.Connection,
    token: str,
    action: str,
    actor: str,
    detail: dict | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO audit_events (token, action, actor, detail)
        VALUES ($1, $2, $3, $4)
        """,
        token,
        action,
        actor,
        detail or {},
    )


def _derive_state(row: asyncpg.Record) -> str:
    """以数据库字段推导当前状态。"""
    if row["consumed_at"] is not None:
        return REASON_CONSUMED
    if row["withdrawn_at"] is not None:
        return REASON_WITHDRAWN
    if row["expired_at"] is not None or _now() > row["expires_at"]:
        return REASON_EXPIRED
    return "active"


# ---------------------------------------------------------------------------
# 创建投递
# ---------------------------------------------------------------------------


async def _create(
    *,
    kind: str,
    content: bytes,
    ttl_seconds: int | None,
    sender: str | None,
    filename: str | None = None,
    content_type: str | None = None,
) -> dict:
    if ttl_seconds is None:
        ttl_seconds = settings.default_ttl_seconds
    if ttl_seconds < 1 or ttl_seconds > settings.max_ttl_seconds:
        raise DomainError(
            REASON_INVALID_TTL,
            f"ttl_seconds 必须在 1 到 {settings.max_ttl_seconds} 之间",
            status_code=422,
        )

    token = _new_token()
    withdraw_secret = _new_secret()
    now = _now()
    expires_at = now + timedelta(seconds=ttl_seconds)

    assert db.pool is not None
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO deliveries
                    (token, withdraw_secret, kind, content, filename,
                     content_type, expires_at, sender)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                token,
                withdraw_secret,
                kind,
                content,
                filename,
                content_type,
                expires_at,
                sender,
            )
            await _add_audit(
                conn,
                token,
                "created",
                sender or "sender",
                {
                    "kind": kind,
                    "size_bytes": len(content),
                    "ttl_seconds": ttl_seconds,
                    "filename": filename,
                },
            )

    return {
        "token": token,
        "kind": kind,
        "withdraw_secret": withdraw_secret,
        "expires_at": expires_at,
        "ttl_seconds": ttl_seconds,
        "size_bytes": len(content),
        "filename": filename,
    }


async def create_text(
    content: str, ttl_seconds: int | None, sender: str | None
) -> dict:
    raw = content.encode("utf-8")
    if len(raw) > settings.max_text_bytes:
        raise DomainError(
            REASON_PAYLOAD_TOO_LARGE,
            f"文本大小 {len(raw)} 字节超过上限 {settings.max_text_bytes} 字节",
            status_code=413,
        )
    return await _create(
        kind="text",
        content=raw,
        ttl_seconds=ttl_seconds,
        sender=sender,
    )


async def create_file(
    content: bytes,
    filename: str,
    content_type: str | None,
    ttl_seconds: int | None,
    sender: str | None,
) -> dict:
    if len(content) > settings.max_file_bytes:
        raise DomainError(
            REASON_PAYLOAD_TOO_LARGE,
            f"文件大小 {len(content)} 字节超过上限 {settings.max_file_bytes} 字节",
            status_code=413,
        )
    return await _create(
        kind="file",
        content=content,
        ttl_seconds=ttl_seconds,
        sender=sender,
        filename=filename,
        content_type=content_type,
    )


# ---------------------------------------------------------------------------
# 取件（核心：原子消费）
# ---------------------------------------------------------------------------


@dataclass
class PickupOutcome:
    reason: str  # consumed / consumed_race / expired / withdrawn / not_found / in_progress
    row: dict | None = None


async def pickup(token: str, request_id: str) -> PickupOutcome:
    """尝试一次性取件。

    返回 ``reason == "consumed"`` 且带 row 时，调用方投递内容；
    其余 reason 均为明确的失败/终态结果。
    """
    # 1) 终态缓存：已消费/撤回/过期/不存在的 token 直接短路
    cached = await redis_store.get_terminal(token)
    if cached in _TERMINAL_REASONS:
        return PickupOutcome(reason=cached)

    # 2) Redis 串行锁：同一时刻只允许一个请求消费该 token
    if not await redis_store.acquire_pickup_lock(token, request_id):
        return PickupOutcome(reason=REASON_IN_PROGRESS)

    try:
        assert db.pool is not None
        async with db.pool.acquire() as conn:
            tx = conn.transaction()
            await tx.start()
            try:
                row = await conn.fetchrow(
                    "SELECT * FROM deliveries WHERE token = $1", token
                )
                if row is None:
                    await _add_audit(conn, token, "pickup_denied", "recipient",
                                     {"reason": REASON_NOT_FOUND})
                    await tx.commit()
                    await redis_store.cache_terminal(token, REASON_NOT_FOUND)
                    return PickupOutcome(reason=REASON_NOT_FOUND)

                if row["withdrawn_at"] is not None:
                    await _add_audit(conn, token, "pickup_denied", "recipient",
                                     {"reason": REASON_WITHDRAWN})
                    await tx.commit()
                    await redis_store.cache_terminal(token, REASON_WITHDRAWN)
                    return PickupOutcome(reason=REASON_WITHDRAWN)

                # 3) 单条条件 UPDATE，行级原子：并发穿透时只会有一条命中
                consumed = await conn.fetchrow(
                    """
                    UPDATE deliveries
                    SET consumed_at = now()
                    WHERE token = $1
                      AND consumed_at IS NULL
                      AND withdrawn_at IS NULL
                      AND expired_at IS NULL
                      AND now() <= expires_at
                    RETURNING *
                    """,
                    token,
                )
                if consumed is not None:
                    await _add_audit(
                        conn,
                        token,
                        "consumed",
                        "recipient",
                        {
                            "size_bytes": len(consumed["content"]),
                            "request_id": request_id,
                        },
                    )
                    await tx.commit()
                    await redis_store.cache_terminal(token, REASON_CONSUMED)
                    return PickupOutcome(reason=REASON_CONSUMED, row=dict(consumed))

                # UPDATE 未命中：重读最新行，判定是被并发消费、撤回还是已过期
                latest = await conn.fetchrow(
                    "SELECT * FROM deliveries WHERE token = $1", token
                )
                assert latest is not None  # 前面已确认存在
                if latest["withdrawn_at"] is not None:
                    reason = REASON_WITHDRAWN
                elif latest["consumed_at"] is not None or _now() <= latest["expires_at"]:
                    # 未过期却没抢到 → 并发竞争中已被别的取件消费
                    reason = REASON_CONSUMED
                else:
                    reason = REASON_EXPIRED
                await _add_audit(
                    conn, token, "pickup_denied", "recipient",
                    {"reason": reason, "request_id": request_id},
                )
                await tx.commit()
                await redis_store.cache_terminal(token, reason)
                return PickupOutcome(reason=reason)
            except Exception:
                await tx.rollback()
                raise
    finally:
        await redis_store.release_pickup_lock(token, request_id)


# ---------------------------------------------------------------------------
# 撤回（发送者，需要 withdraw_secret）
# ---------------------------------------------------------------------------


async def _get_for_sender(conn: asyncpg.Connection, token: str) -> asyncpg.Record:
    row = await conn.fetchrow("SELECT * FROM deliveries WHERE token = $1", token)
    if row is None:
        raise DomainError(
            REASON_NOT_FOUND, "投递不存在或令牌无效", status_code=404
        )
    return row


async def withdraw(
    token: str, secret: str | None, request_id: str
) -> dict:
    if not secret:
        raise DomainError(
            REASON_WRONG_SECRET,
            "缺少 X-Withdraw-Secret（或 withdraw_secret 查询参数）",
            status_code=401,
        )

    assert db.pool is not None
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT * FROM deliveries WHERE token = $1", token
            )
            if row is None:
                raise DomainError(
                    REASON_NOT_FOUND, "投递不存在或令牌无效", status_code=404
                )
            if not secrets.compare_digest(row["withdraw_secret"], secret):
                await _add_audit(
                    conn, token, "withdraw_denied", "sender",
                    {"reason": REASON_WRONG_SECRET, "request_id": request_id},
                )
                raise DomainError(
                    REASON_WRONG_SECRET, "撤回密钥不正确", status_code=403
                )

            if row["consumed_at"] is not None:
                await _add_audit(
                    conn, token, "withdraw_denied", "sender",
                    {"reason": REASON_CONSUMED, "request_id": request_id},
                )
                raise DomainError(
                    REASON_CONSUMED, "投递已被取件消费，无法撤回", status_code=409
                )

            if row["withdrawn_at"] is not None:
                # 幂等：重复撤回返回成功
                await _add_audit(
                    conn, token, "withdraw_idempotent", "sender",
                    {"request_id": request_id},
                )
                return {"token": token, "state": REASON_WITHDRAWN,
                        "withdrawn_at": row["withdrawn_at"]}

            if row["expired_at"] is not None or _now() > row["expires_at"]:
                await _add_audit(
                    conn, token, "withdraw_denied", "sender",
                    {"reason": REASON_EXPIRED, "request_id": request_id},
                )
                raise DomainError(
                    REASON_EXPIRED, "投递已过期，无需撤回", status_code=409
                )

            updated = await conn.fetchrow(
                """
                UPDATE deliveries
                SET withdrawn_at = now()
                WHERE token = $1
                  AND consumed_at IS NULL
                  AND withdrawn_at IS NULL
                  AND expired_at IS NULL
                  AND now() <= expires_at
                RETURNING *
                """,
                token,
            )
            if updated is None:
                # 与并发取件/过期标记竞争失败
                raise DomainError(
                    REASON_CONSUMED,
                    "投递状态在撤回时发生变化，请查询当前状态",
                    status_code=409,
                )

            await _add_audit(
                conn, token, "withdrawn", "sender", {"request_id": request_id}
            )
            withdrawn_at = updated["withdrawn_at"]

    await redis_store.invalidate_token(token)
    await redis_store.cache_terminal(token, REASON_WITHDRAWN)
    return {"token": token, "state": REASON_WITHDRAWN, "withdrawn_at": withdrawn_at}


# ---------------------------------------------------------------------------
# 状态查询（发送者）
# ---------------------------------------------------------------------------


async def get_status(token: str, secret: str | None) -> dict:
    if not secret:
        raise DomainError(
            REASON_WRONG_SECRET,
            "缺少 X-Withdraw-Secret（或 withdraw_secret 查询参数）",
            status_code=401,
        )

    assert db.pool is not None
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM deliveries WHERE token = $1", token
        )
        if row is None:
            raise DomainError(
                REASON_NOT_FOUND, "投递不存在或令牌无效", status_code=404
            )
        if not secrets.compare_digest(row["withdraw_secret"], secret):
            raise DomainError(
                REASON_WRONG_SECRET, "撤回密钥不正确", status_code=403
            )

    state = _derive_state(row)
    return {
        "token": token,
        "kind": row["kind"],
        "state": state,
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "consumed_at": row["consumed_at"],
        "withdrawn_at": row["withdrawn_at"],
        "expired_at": row["expired_at"],
        "sender": row["sender"],
        "expired": state == REASON_EXPIRED,
        "filename": row["filename"],
    }


# ---------------------------------------------------------------------------
# 审计记录
# ---------------------------------------------------------------------------


async def list_audit(token: str, secret: str | None, limit: int) -> list[dict]:
    if not secret:
        raise DomainError(
            REASON_WRONG_SECRET,
            "缺少 X-Withdraw-Secret（或 withdraw_secret 查询参数）",
            status_code=401,
        )

    assert db.pool is not None
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT withdraw_secret FROM deliveries WHERE token = $1", token
        )
        if row is None:
            raise DomainError(
                REASON_NOT_FOUND, "投递不存在或令牌无效", status_code=404
            )
        if not secrets.compare_digest(row["withdraw_secret"], secret):
            raise DomainError(
                REASON_WRONG_SECRET, "撤回密钥不正确", status_code=403
            )

        events = await conn.fetch(
            """
            SELECT event_time, token, action, actor, detail
            FROM audit_events
            WHERE token = $1
            ORDER BY event_time DESC, id DESC
            LIMIT $2
            """,
            token,
            limit,
        )
    return [dict(e) for e in events]


# ---------------------------------------------------------------------------
# 过期清理（后台 reaper 复用）
# ---------------------------------------------------------------------------


async def mark_due_expirations(limit: int = 200) -> int:
    """把数据库中已到期但仍处于 active 的投递原子标记为过期。

    通过条件 UPDATE 与取件竞争（两条 UPDATE 的 WHERE 互斥）：
    - 若取件先提交（consumed_at 已写入），这里 0 行命中；
    - 若这里先提交（expired_at 写入），取件的条件 UPDATE 0 行命中并返回 expired。
    返回本次标记的行数。
    """
    assert db.pool is not None
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """
                UPDATE deliveries AS d
                SET expired_at = now()
                WHERE d.id IN (
                    SELECT id
                    FROM deliveries
                    WHERE consumed_at IS NULL
                      AND withdrawn_at IS NULL
                      AND expired_at IS NULL
                      AND now() > expires_at
                    ORDER BY expires_at
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING d.token
                """,
                limit,
            )
            marked = [r["token"] for r in rows]
            for token in marked:
                await _add_audit(conn, token, "expired", "system", {})

    for token in marked:
        await redis_store.invalidate_token(token)
        await redis_store.cache_terminal(token, REASON_EXPIRED)
    return len(marked)
