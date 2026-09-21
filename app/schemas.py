"""Pydantic 请求 / 响应模型。"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from .config import settings


class CreateTextRequest(BaseModel):
    content: str = Field(..., min_length=1, description="一次性文本内容")
    ttl_seconds: int | None = Field(
        None, ge=1, le=settings.max_ttl_seconds, description="过期时间（秒）"
    )
    sender: str | None = Field(None, max_length=200, description="发送方标识（可选）")


class DeliveryCreated(BaseModel):
    token: str
    kind: str
    pickup_url: str
    withdraw_url: str
    status_url: str
    withdraw_secret: str
    expires_at: datetime
    ttl_seconds: int
    size_bytes: int
    filename: str | None = None


class StatusResponse(BaseModel):
    token: str
    kind: str
    state: str
    created_at: datetime
    expires_at: datetime
    consumed_at: datetime | None = None
    withdrawn_at: datetime | None = None
    expired_at: datetime | None = None
    sender: str | None = None
    expired: bool
    filename: str | None = None


class WithdrawResponse(BaseModel):
    token: str
    state: str
    withdrawn_at: datetime | None = None


class AuditEvent(BaseModel):
    event_time: datetime
    token: str
    action: str
    actor: str
    detail: dict


class TextPickupResponse(BaseModel):
    token: str
    kind: str = "text"
    consumed_at: datetime
    expires_at: datetime
    content: str


class HealthResponse(BaseModel):
    status: str
    database: str
    redis: str
