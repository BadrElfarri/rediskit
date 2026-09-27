from redis import Redis

from rediskit.redis.client.connection import get_redis_connection


def readiness_ping(connection: Redis | None = None) -> bool:
    try:
        conn = connection if connection is not None else get_redis_connection()
        return bool(conn.ping())
    except Exception:  # noqa: BLE001 - a readiness probe reports failure, it never raises
        return False
