"""FastAPI 应用：路由、请求 ID 中间件与统一错误处理。"""

from __future__ import annotations

import asyncio
import logging
import urllib.parse
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Form, Header, Query, Request, Response, UploadFile
from fastapi.responses import JSONResponse

from . import db, redis_store, services
from .config import settings
from .errors import DomainError
from .reaper import reaper_loop
from .schemas import (
    AuditEvent,
    CreateTextRequest,
    DeliveryCreated,
    HealthResponse,
    StatusResponse,
    WithdrawResponse,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("dropbox.api")

# 终态 reason → HTTP 状态码
_REASON_STATUS = {
    services.REASON_NOT_FOUND: 404,
    services.REASON_EXPIRED: 410,
    services.REASON_CONSUMED: 410,
    services.REASON_WITHDRAWN: 410,
}

# 终态 reason → 人类可读说明
_REASON_MESSAGES = {
    services.REASON_NOT_FOUND: "投递不存在或令牌无效",
    services.REASON_EXPIRED: "投递已过期，内容不可用",
    services.REASON_CONSUMED: "投递已被消费，内容只能成功读取一次",
    services.REASON_WITHDRAWN: "投递已被发送者撤回",
    services.REASON_IN_PROGRESS: "已有取件请求正在处理，请稍后重试",
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    stop_event = asyncio.Event()
    await db.init_db()
    await redis_store.init_redis()
    reaper_task = asyncio.create_task(reaper_loop(stop_event))
    logger.info("一次性内容投递箱 API 已启动")
    try:
        yield
    finally:
        stop_event.set()
        reaper_task.cancel()
        try:
            await reaper_task
        except asyncio.CancelledError:
            pass
        await redis_store.close_redis()
        await db.close_db()
        logger.info("API 已停止")


app = FastAPI(
    title="一次性内容投递箱 API",
    description=(
        "发送者创建带过期时间的文本或小文件投递，生成只能成功读取一次的取件令牌。"
        "并发取件通过 Redis 串行锁 + PostgreSQL 条件 UPDATE 实现原子消费。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


@app.exception_handler(DomainError)
async def domain_error_handler(request: Request, exc: DomainError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "reason": exc.reason,
            "message": exc.message,
            "request_id": getattr(request.state, "request_id", None),
        },
    )


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------


def _secret(
    x_withdraw_secret: str | None, withdraw_secret_query: str | None
) -> str | None:
    return x_withdraw_secret or withdraw_secret_query


def _base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def _created_response(request: Request, info: dict) -> dict:
    base = _base_url(request)
    token = info["token"]
    return {
        "token": token,
        "kind": info["kind"],
        "pickup_url": f"{base}/api/deliveries/{token}/pickup",
        "withdraw_url": f"{base}/api/deliveries/{token}/withdraw",
        "status_url": f"{base}/api/deliveries/{token}/status",
        "withdraw_secret": info["withdraw_secret"],
        "expires_at": info["expires_at"],
        "ttl_seconds": info["ttl_seconds"],
        "size_bytes": info["size_bytes"],
        "filename": info.get("filename"),
    }


def _safe_filename(filename: str | None) -> str:
    """去掉路径部分，防止 Content-Disposition 注入。"""
    if not filename:
        return "attachment.bin"
    name = filename.replace("\\", "/").split("/")[-1].strip()
    return name or "attachment.bin"


# ---------------------------------------------------------------------------
# 根 / 健康检查
# ---------------------------------------------------------------------------


@app.get("/", include_in_schema=False)
async def root() -> dict:
    return {
        "service": "one-time-dropbox",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health", response_model=HealthResponse, tags=["meta"])
async def health() -> HealthResponse:
    db_ok = "ok"
    redis_ok = "ok"
    try:
        assert db.pool is not None
        async with db.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
    except Exception:
        db_ok = "error"
    try:
        assert redis_store.client is not None
        await redis_store.client.ping()
    except Exception:
        redis_ok = "error"
    overall = "ok" if db_ok == "ok" and redis_ok == "ok" else "degraded"
    return HealthResponse(status=overall, database=db_ok, redis=redis_ok)


# ---------------------------------------------------------------------------
# 创建投递
# ---------------------------------------------------------------------------


@app.post(
    "/api/deliveries/text",
    response_model=DeliveryCreated,
    status_code=201,
    tags=["deliveries"],
    summary="创建一次性文本投递",
)
async def create_text_delivery(
    body: CreateTextRequest, request: Request
) -> DeliveryCreated:
    info = await services.create_text(
        content=body.content,
        ttl_seconds=body.ttl_seconds,
        sender=body.sender,
    )
    return DeliveryCreated(**_created_response(request, info))


@app.post(
    "/api/deliveries/files",
    response_model=DeliveryCreated,
    status_code=201,
    tags=["deliveries"],
    summary="创建一次性文件投递",
)
async def create_file_delivery(
    request: Request,
    file: UploadFile = File(..., description="一次性投递的文件（默认最大 1 MiB）"),
    ttl_seconds: int | None = Form(None, ge=1),
    sender: str | None = Form(None, max_length=200),
) -> DeliveryCreated:
    content = await file.read()
    if len(content) > settings.max_file_bytes:
        raise DomainError(
            services.REASON_PAYLOAD_TOO_LARGE,
            f"文件大小 {len(content)} 字节超过上限 {settings.max_file_bytes} 字节",
            status_code=413,
        )
    info = await services.create_file(
        content=content,
        filename=_safe_filename(file.filename),
        content_type=file.content_type,
        ttl_seconds=ttl_seconds,
        sender=sender,
    )
    return DeliveryCreated(**_created_response(request, info))


# ---------------------------------------------------------------------------
# 取件（核心接口）
# ---------------------------------------------------------------------------


@app.post(
    "/api/deliveries/{token}/pickup",
    tags=["deliveries"],
    summary="一次性取件（成功读取后立即销毁）",
    responses={
        200: {"description": "取件成功：文本返回 JSON，文件返回二进制附件。"},
        404: {"description": "投递不存在或令牌无效。"},
        409: {"description": "已有取件请求正在处理（in_progress）。"},
        410: {"description": "终态：已消费 / 已撤回 / 已过期。"},
    },
)
async def pickup_delivery(token: str, request: Request):
    request_id = request.state.request_id
    outcome = await services.pickup(token, request_id)

    if outcome.reason == services.REASON_CONSUMED and outcome.row is not None:
        row = outcome.row
        if row["kind"] == "text":
            return JSONResponse(
                status_code=200,
                content={
                    "token": token,
                    "kind": "text",
                    "consumed_at": row["consumed_at"].isoformat(),
                    "expires_at": row["expires_at"].isoformat(),
                    "content": row["content"].decode("utf-8"),
                },
                headers={"X-Delivery-Kind": "text"},
            )

        filename = _safe_filename(row["filename"])
        quoted = urllib.parse.quote(filename)
        return Response(
            content=bytes(row["content"]),
            status_code=200,
            media_type=row["content_type"] or "application/octet-stream",
            headers={
                "Content-Disposition": (
                    f"attachment; filename=\"{quoted}\"; filename*=UTF-8''{quoted}"
                ),
                "X-Delivery-Kind": "file",
                "X-Consumed-At": row["consumed_at"].isoformat(),
            },
        )

    if outcome.reason == services.REASON_IN_PROGRESS:
        return JSONResponse(
            status_code=409,
            content={
                "error": True,
                "reason": services.REASON_IN_PROGRESS,
                "message": _REASON_MESSAGES[services.REASON_IN_PROGRESS],
                "request_id": request_id,
            },
            headers={"Retry-After": "1"},
        )

    status_code = _REASON_STATUS.get(outcome.reason, 410)
    return JSONResponse(
        status_code=status_code,
        content={
            "error": True,
            "reason": outcome.reason,
            "message": _REASON_MESSAGES.get(outcome.reason, "取件失败"),
            "request_id": request_id,
        },
    )


# ---------------------------------------------------------------------------
# 撤回 / 状态 / 审计
# ---------------------------------------------------------------------------


@app.post(
    "/api/deliveries/{token}/withdraw",
    response_model=WithdrawResponse,
    tags=["deliveries"],
    summary="发送者撤回投递（需要 withdraw_secret）",
)
async def withdraw_delivery(
    token: str,
    request: Request,
    x_withdraw_secret: str | None = Header(default=None),
    withdraw_secret: str | None = Query(default=None),
) -> WithdrawResponse:
    secret = _secret(x_withdraw_secret, withdraw_secret)
    result = await services.withdraw(token, secret, request.state.request_id)
    return WithdrawResponse(**result)


@app.get(
    "/api/deliveries/{token}/status",
    response_model=StatusResponse,
    tags=["deliveries"],
    summary="查询投递状态（需要 withdraw_secret）",
)
async def delivery_status(
    token: str,
    x_withdraw_secret: str | None = Header(default=None),
    withdraw_secret: str | None = None,
) -> StatusResponse:
    result = await services.get_status(
        token, _secret(x_withdraw_secret, withdraw_secret)
    )
    return StatusResponse(**result)


@app.get(
    "/api/deliveries/{token}/audit",
    response_model=list[AuditEvent],
    tags=["deliveries"],
    summary="查询投递的审计记录（需要 withdraw_secret）",
)
async def delivery_audit(
    token: str,
    limit: int = 50,
    x_withdraw_secret: str | None = Header(default=None),
    withdraw_secret: str | None = None,
) -> list[AuditEvent]:
    limit = max(1, min(limit, 200))
    events = await services.list_audit(
        token, _secret(x_withdraw_secret, withdraw_secret), limit
    )
    return [AuditEvent(**e) for e in events]
