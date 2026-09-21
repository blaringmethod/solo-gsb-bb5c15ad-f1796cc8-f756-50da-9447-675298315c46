"""Redis 客户端：消费串行锁 + 终态结果缓存。

- 串行锁（pickup:lock:<token>）：SET NX，保证同一投递的并发取件
  只有一个请求进入临界区执行 UPDATE，其余立刻收到 409。
- 终态缓存（pickup:dead:<token>）：记录 consumed / withdrawn /
  expired / not_found，命中后直接返回明确结果，不必再访问数据库。
  缓存只做加速，PostgreSQL 始终是权威来源。
"""

from __future__ import annotations

import redis.asyncio as redis

from .config import settings

client: redis.Redis | None = None

_LOCK_PREFIX = "pickup:lock:"
_DEAD_PREFIX = "pickup:dead:"

# 仅在 value 仍等于请求 ID 时删除，避免误删别人持有的锁
_RELEASE_LOCK_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""


async def init_redis() -> None:
    global client
    client = redis.from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=True,
    )
    await client.ping()


async def close_redis() -> None:
    global client
    if client is not None:
        await client.aclose()
        client = None


def _lock_key(token: str) -> str:
    return f"{_LOCK_PREFIX}{token}"


def _dead_key(token: str) -> str:
    return f"{_DEAD_PREFIX}{token}"


async def acquire_pickup_lock(token: str, request_id: str) -> bool:
    """尝试立即获取取件锁；拿不到返回 False（不阻塞等待）。"""
    assert client is not None
    result = await client.set(
        _lock_key(token),
        request_id,
        nx=True,
        ex=settings.lock_ttl_seconds,
    )
    return bool(result)


async def release_pickup_lock(token: str, request_id: str) -> None:
    assert client is not None
    await client.eval(_RELEASE_LOCK_LUA, 1, _lock_key(token), request_id)


async def cache_terminal(token: str, reason: str) -> None:
    """写入终态结果缓存。"""
    assert client is not None
    await client.set(
        _dead_key(token),
        reason,
        ex=settings.terminal_cache_ttl_seconds,
    )


async def get_terminal(token: str) -> str | None:
    assert client is not None
    return await client.get(_dead_key(token))


async def invalidate_token(token: str) -> None:
    """撤回/过期等状态流转后，刷新终态缓存。"""
    assert client is not None
    await client.delete(_dead_key(token))
