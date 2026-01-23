from typing import Callable, cast

import redis

from rediskit.redis.client import get_redis_connection
from rediskit.redis.node import get_redis_top_node


def counter(
    tenant_id: str,
    field: str,
    delta: int,
    key: str,
    top_node: Callable[..., str] = get_redis_top_node,
    connection: redis.Redis | None = None,
    min_value: int = 0,
) -> int:
    connection = connection if connection is not None else get_redis_connection()
    node_key = top_node(tenant_id, key)
    new_count = cast(int, connection.hincrby(node_key, field, delta))
    if new_count <= min_value:
        connection.hdel(node_key, field)
    return new_count


def counter_value(tenant_id: str, field: str, key: str, top_node: Callable[..., str] = get_redis_top_node, connection: redis.Redis | None = None) -> int | None:
    connection = connection if connection is not None else get_redis_connection()
    node_key = top_node(tenant_id, key)
    count = cast(str, connection.hget(node_key, field))
    return int(count) if count is not None else None
