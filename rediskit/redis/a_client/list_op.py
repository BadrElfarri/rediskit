from typing import Any

from redis import asyncio as redis_async

from rediskit.redis.a_client.connection import get_async_redis_connection


async def rpush(
    key: str,
    *values: str | bytes | int | float,
    connection: redis_async.Redis | None = None,
) -> int:
    conn = connection if connection is not None else get_async_redis_connection()
    return await conn.rpush(key, *values)


async def lpush(
    key: str,
    *values: str | bytes | int | float,
    connection: redis_async.Redis | None = None,
) -> int:
    conn = connection if connection is not None else get_async_redis_connection()
    return await conn.lpush(key, *values)


async def lpop(
    key: str,
    count: int | None = None,
    connection: redis_async.Redis | None = None,
) -> Any:
    conn = connection if connection is not None else get_async_redis_connection()
    if count is None:
        return await conn.lpop(key)
    return await conn.lpop(key, count)


async def rpop(
    key: str,
    count: int | None = None,
    connection: redis_async.Redis | None = None,
) -> Any:
    conn = connection if connection is not None else get_async_redis_connection()
    if count is None:
        return await conn.rpop(key)
    return await conn.rpop(key, count)


async def llen(key: str, connection: redis_async.Redis | None = None) -> int:
    conn = connection if connection is not None else get_async_redis_connection()
    return await conn.llen(key)


async def lrange(
    key: str,
    start: int = 0,
    end: int = -1,
    connection: redis_async.Redis | None = None,
) -> list:
    conn = connection if connection is not None else get_async_redis_connection()
    return await conn.lrange(key, start, end)


async def drain_list(
    key: str,
    connection: redis_async.Redis | None = None,
) -> list:
    """Atomically pop every element from the head of the list, in arrival order.

    Useful for inbox/queue patterns where a consumer wants to consume all
    pending messages in a single pass without risk of double-delivery from
    concurrent drains.
    """
    conn = connection if connection is not None else get_async_redis_connection()
    out: list = []
    while True:
        raw = await conn.lpop(key)
        if raw is None:
            break
        out.append(raw)
    return out
