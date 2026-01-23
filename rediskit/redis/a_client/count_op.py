from typing import Awaitable, Callable, cast

from redis import asyncio as redis_async

from rediskit.redis.a_client import get_async_redis_connection
from rediskit.redis.node import get_redis_top_node


async def counter(
    tenant_id: str,
    field: str,
    delta: int,
    key: str,
    top_node: Callable[..., str] = get_redis_top_node,
    connection: redis_async.Redis | None = None,
    min_value: int = 0,
) -> int:
    connection = connection if connection is not None else get_async_redis_connection()
    node_key = top_node(tenant_id, key)
    new_count = await cast(Awaitable[int], connection.hincrby(node_key, field, delta))
    if new_count <= min_value:
        await cast(Awaitable[int], connection.hdel(node_key, field))
    return new_count


async def counter_value(
    tenant_id: str, field: str, key: str, top_node: Callable[..., str] = get_redis_top_node, connection: redis_async.Redis | None = None
) -> int | None:
    connection = connection if connection is not None else get_async_redis_connection()
    node_key = top_node(tenant_id, key)

    count = await cast(Awaitable[str], connection.hget(node_key, field))
    return int(count) if count is not None else None
