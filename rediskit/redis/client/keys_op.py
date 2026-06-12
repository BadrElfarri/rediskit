from typing import Callable, Iterator

from redis import Redis

from rediskit import config
from rediskit.redis.client.connection import get_redis_connection
from rediskit.redis.node import get_redis_top_node


def set_redis_cache_expiry(
    tenant_id: str | None, key: str, expiry: int, connection: Redis | None = None, top_node: Callable[..., str] = get_redis_top_node
) -> None:
    node_key = top_node(tenant_id, key)
    connection = connection if connection is not None else get_redis_connection()
    connection.expire(node_key, expiry)


def get_keys(
    tenant_id: str | None, key: str | None, top_node: Callable[..., str] = get_redis_top_node, connection: Redis | None = None, only_last_key: bool = True
) -> list[str]:
    node_key = top_node(tenant_id, key)
    connection = connection if connection is not None else get_redis_connection()
    # SCAN instead of KEYS: same result, but does not block Redis on large keyspaces.
    # SCAN may return duplicates, so dedupe while preserving order.
    keys: list[str] = list(dict.fromkeys(connection.scan_iter(match=node_key, count=config.REDIS_SCAN_COUNT)))
    if only_last_key:
        keys = [k.split(":")[-1] for k in keys]
    return keys


def set_ttl_for_key(
    tenant_id: str | None, key: str | None, ttl: int, connection: Redis | None = None, top_node: Callable[..., str] = get_redis_top_node
) -> None:
    nodeKey = top_node(tenant_id, key)
    connection = connection if connection is not None else get_redis_connection()
    connection.expire(nodeKey, ttl)


def expire(
    key: str,
    seconds: int,
    connection: Redis | None = None,
) -> bool:
    connection = connection if connection is not None else get_redis_connection()
    return connection.expire(key, seconds)  # type: ignore[return-value]


def delete(
    *keys: str,
    connection: Redis | None = None,
) -> int:
    connection = connection if connection is not None else get_redis_connection()
    return connection.delete(*keys)  # type: ignore[return-value]


def list_keys(
    tenant_id: str | None,
    match_key: str,
    count: int = 1_000,
    top_node: Callable = get_redis_top_node,
    connection: Redis | None = None,
) -> Iterator[str]:
    pattern = top_node(tenant_id, match_key)
    conn = connection if connection is not None else get_redis_connection()
    for i, key in enumerate(conn.scan_iter(match=pattern, count=count)):
        if i >= 10_000:
            raise ValueError("Redis keys exceeded 10_000 matches")
        yield key
