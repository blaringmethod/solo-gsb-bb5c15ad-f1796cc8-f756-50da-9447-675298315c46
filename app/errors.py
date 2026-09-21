"""业务错误类型，由全局异常处理器统一转成 JSON 响应。"""

from __future__ import annotations


class DomainError(Exception):
    """携带稳定 reason 码与 HTTP 状态码的业务错误。"""

    def __init__(self, reason: str, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.status_code = status_code
