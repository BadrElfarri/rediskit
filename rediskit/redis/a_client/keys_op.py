from typing import AsyncIterator, Callable

from redis import asyncio as redis_async

from rediskit.redis.a_client.connection import get_async_redis_connection
from rediskit.redis.node import get_redis_top_node


async def list_keys(
    tenant_id: str | None,
    math_key: str,
    count: int = 1_000,
    top_node: Callable = get_redis_top_node,
    connection: redis_async.Redis | None = None,
) -> AsyncIterator[str]:
    pattern = top_node(tenant_id, math_key)
    conn = connection if connection is not None else get_async_redis_connection()
    i = 0
    async for key in conn.scan_iter(match=pattern, count=count):
        if i >= 10_000:
            raise ValueError("Redis keys exceeded 10_000 matches")
        i += 1
        yield key


async def set_redis_cache_expiry(
    tenant_id: str | None,
    key: str,
    expiry: int,
    connection: redis_async.Redis | None = None,
    top_node: Callable[..., str] = get_redis_top_node,
) -> None:
    node_key = top_node(tenant_id, key)
    conn = connection if connection is not None else get_async_redis_connection()
    await conn.expire(node_key, expiry)


async def get_keys(
    tenant_id: str | None,
    key: str | None,
    top_node: Callable[..., str] = get_redis_top_node,
    connection: redis_async.Redis | None = None,
    only_last_key: bool = True,
) -> list[str]:
    node_key = top_node(tenant_id, key)
    conn = connection if connection is not None else get_async_redis_connection()
    keys = await conn.keys(node_key)
    if only_last_key:
        keys = [k.split(":")[-1] for k in keys]
    return keys


async def set_ttl_for_key(
    tenant_id: str | None,
    key: str | None,
    ttl: int,
    connection: redis_async.Redis | None = None,
    top_node: Callable[..., str] = get_redis_top_node,
) -> None:
    node_key = top_node(tenant_id, key)
    conn = connection if connection is not None else get_async_redis_connection()
    await conn.expire(node_key, ttl)
