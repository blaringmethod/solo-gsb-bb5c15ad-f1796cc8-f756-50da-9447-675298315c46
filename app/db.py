"""PostgreSQL 连接池、建表与 JSONB 编解码。

PostgreSQL 是权威数据存储：投递内容、状态流转与审计记录全部落盘，
即使 Redis 被清空也不会破坏“只能成功读取一次”的语义。
"""

from __future__ import annotations

import asyncio
import json
import logging

import asyncpg

from .config import settings

logger = logging.getLogger("dropbox.db")

pool: asyncpg.pool.Pool | None = None


CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS deliveries (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    token           TEXT NOT NULL UNIQUE,
    withdraw_secret TEXT NOT NULL,
    kind            TEXT NOT NULL CHECK (kind IN ('text', 'file')),
    content         BYTEA NOT NULL,
    filename        TEXT,
    content_type    TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL,
    consumed_at     TIMESTAMPTZ,
    withdrawn_at    TIMESTAMPTZ,
    expired_at      TIMESTAMPTZ,
    sender          TEXT
);

CREATE INDEX IF NOT EXISTS idx_deliveries_expires_active
    ON deliveries (expires_at)
    WHERE consumed_at IS NULL AND withdrawn_at IS NULL AND expired_at IS NULL;

CREATE TABLE IF NOT EXISTS audit_events (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_time   TIMESTAMPTZ NOT NULL DEFAULT now(),
    token        TEXT NOT NULL,
    action       TEXT NOT NULL,
    actor        TEXT NOT NULL,
    detail       JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_audit_token_time
    ON audit_events (token, event_time);
"""


async def _set_jsonb_codec(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=lambda value: json.dumps(value, ensure_ascii=False, default=str),
        decoder=json.loads,
        schema="pg_catalog",
    )


async def init_db(retries: int = 30, delay: float = 1.0) -> None:
    """建立连接池（等待数据库就绪）并幂等建表。"""
    global pool

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            pool = await asyncpg.create_pool(
                settings.database_url,
                min_size=1,
                max_size=10,
                init=_set_jsonb_codec,
            )
            break
        except (OSError, asyncpg.PostgresError) as exc:
            last_error = exc
            logger.warning("等待 PostgreSQL 就绪（%s/%s）: %s", attempt, retries, exc)
            await asyncio.sleep(delay)
    else:
        raise RuntimeError(f"无法连接 PostgreSQL: {last_error}")

    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLES)
    logger.info("数据库就绪，schema 已确认")


async def close_db() -> None:
    global pool
    if pool is not None:
        await pool.close()
        pool = None
