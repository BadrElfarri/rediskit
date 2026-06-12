from typing import Any, Awaitable, cast

from redis import asyncio as redis_async

from rediskit.redis.a_client.connection import get_async_redis_connection


async def rpush(
    key: str,
    *values: str | bytes | int | float,
    connection: redis_async.Redis | None = None,
) -> int:
    conn = connection if connection is not None else get_async_redis_connection()
    return await cast(Awaitable[int], conn.rpush(key, *values))


async def lpush(
    key: str,
    *values: str | bytes | int | float,
    connection: redis_async.Redis | None = None,
) -> int:
    conn = connection if connection is not None else get_async_redis_connection()
    return await cast(Awaitable[int], conn.lpush(key, *values))


async def lpop(
    key: str,
    count: int | None = None,
    connection: redis_async.Redis | None = None,
) -> Any:
    conn = connection if connection is not None else get_async_redis_connection()
    if count is None:
        return await cast(Awaitable[Any], conn.lpop(key))
    return await cast(Awaitable[Any], conn.lpop(key, count))


async def rpop(
    key: str,
    count: int | None = None,
    connection: redis_async.Redis | None = None,
) -> Any:
    conn = connection if connection is not None else get_async_redis_connection()
    if count is None:
        return await cast(Awaitable[Any], conn.rpop(key))
    return await cast(Awaitable[Any], conn.rpop(key, count))


async def llen(key: str, connection: redis_async.Redis | None = None) -> int:
    conn = connection if connection is not None else get_async_redis_connection()
    return await cast(Awaitable[int], conn.llen(key))


async def lrange(
    key: str,
    start: int = 0,
    end: int = -1,
    connection: redis_async.Redis | None = None,
) -> list:
    conn = connection if connection is not None else get_async_redis_connection()
    return await cast(Awaitable[list], conn.lrange(key, start, end))


_DRAIN_LUA = """
local items = redis.call('LRANGE', KEYS[1], 0, -1)
redis.call('DEL', KEYS[1])
return items
"""


async def drain_list(
    key: str,
    connection: redis_async.Redis | None = None,
) -> list:
    """Atomically pop every element from the list, in arrival order.

    Useful for inbox/queue patterns where a consumer wants to consume all
    pending messages in a single pass. Runs as a single Lua script, so
    concurrent drains cannot interleave and each element is delivered
    exactly once.
    """
    conn = connection if connection is not None else get_async_redis_connection()
    return await cast(Awaitable[list], conn.eval(_DRAIN_LUA, 1, key))
