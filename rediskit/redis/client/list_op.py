from typing import Any

from redis import Redis

from rediskit.redis.client.connection import get_redis_connection


def rpush(
    key: str,
    *values: str | bytes | int | float,
    connection: Redis | None = None,
) -> int:
    conn = connection if connection is not None else get_redis_connection()
    return conn.rpush(key, *values)  # type: ignore[return-value]


def lpush(
    key: str,
    *values: str | bytes | int | float,
    connection: Redis | None = None,
) -> int:
    conn = connection if connection is not None else get_redis_connection()
    return conn.lpush(key, *values)  # type: ignore[return-value]


def lpop(
    key: str,
    count: int | None = None,
    connection: Redis | None = None,
) -> Any:
    conn = connection if connection is not None else get_redis_connection()
    if count is None:
        return conn.lpop(key)
    return conn.lpop(key, count)


def rpop(
    key: str,
    count: int | None = None,
    connection: Redis | None = None,
) -> Any:
    conn = connection if connection is not None else get_redis_connection()
    if count is None:
        return conn.rpop(key)
    return conn.rpop(key, count)


def llen(key: str, connection: Redis | None = None) -> int:
    conn = connection if connection is not None else get_redis_connection()
    return conn.llen(key)  # type: ignore[return-value]


def lrange(
    key: str,
    start: int = 0,
    end: int = -1,
    connection: Redis | None = None,
) -> list:
    conn = connection if connection is not None else get_redis_connection()
    return conn.lrange(key, start, end)  # type: ignore[return-value]


def drain_list(
    key: str,
    connection: Redis | None = None,
) -> list:
    """Atomically pop every element from the head of the list, in arrival order."""
    conn = connection if connection is not None else get_redis_connection()
    out: list = []
    while True:
        raw = conn.lpop(key)
        if raw is None:
            break
        out.append(raw)
    return out
