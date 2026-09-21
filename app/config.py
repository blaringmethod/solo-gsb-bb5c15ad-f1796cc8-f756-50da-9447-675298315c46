"""运行期配置，全部可通过环境变量覆盖（Compose 中已注入默认值）。"""

from __future__ import annotations

import os


class Settings:
    def __init__(self) -> None:
        self.database_url: str = os.getenv(
            "DATABASE_URL", "postgresql://dropbox:dropbox@localhost:5432/dropbox"
        )
        self.redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

        # 内容大小上限
        self.max_text_bytes: int = int(os.getenv("MAX_TEXT_BYTES", 64 * 1024))
        self.max_file_bytes: int = int(os.getenv("MAX_FILE_BYTES", 1024 * 1024))

        # 投递 TTL 上限：7 天
        self.max_ttl_seconds: int = int(os.getenv("MAX_TTL_SECONDS", 7 * 24 * 3600))
        self.default_ttl_seconds: int = int(
            os.getenv("DEFAULT_TTL_SECONDS", 24 * 3600)
        )

        # 后台过期扫描间隔
        self.reaper_interval_seconds: float = float(
            os.getenv("REAPER_INTERVAL_SECONDS", 30)
        )

        # Redis：消费串行锁 TTL（持锁进程崩溃时的兜底）
        self.lock_ttl_seconds: int = int(os.getenv("LOCK_TTL_SECONDS", 15))
        # Redis：终态结果缓存 TTL（已消费 / 已撤回 / 已过期 / 不存在）
        self.terminal_cache_ttl_seconds: int = int(
            os.getenv("TERMINAL_CACHE_TTL_SECONDS", 3600)
        )


settings = Settings()
