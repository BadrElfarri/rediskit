import asyncio

from redis import asyncio as redis_async

from rediskit.redis.a_client.connection import get_async_redis_connection


async def readiness_ping(
    connection: redis_async.Redis | None = None,
    timeout: float = 2.0,
) -> bool:
    try:
        conn = connection if connection is not None else get_async_redis_connection()
        return bool(await asyncio.wait_for(conn.ping(), timeout=timeout))
    except Exception:  # noqa: BLE001 - a readiness probe reports failure, it never raises
        return False
