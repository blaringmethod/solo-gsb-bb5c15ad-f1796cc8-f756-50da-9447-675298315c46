"""后台任务：周期性把已到期投递标记为 expired，保证过期结果及时明确。"""

from __future__ import annotations

import asyncio
import logging

from .config import settings
from .services import mark_due_expirations

logger = logging.getLogger("dropbox.reaper")


async def reaper_loop(stop_event: asyncio.Event) -> None:
    """到期投递扫描循环，收到 stop_event 后退出。"""
    interval = settings.reaper_interval_seconds
    logger.info("过期扫描器已启动，间隔 %.0f 秒", interval)
    while not stop_event.is_set():
        try:
            count = await mark_due_expirations()
            if count:
                logger.info("标记了 %s 个到期投递", count)
        except Exception:
            logger.exception("过期扫描一轮失败")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
    logger.info("过期扫描器已停止")
