import logging

from redis import ConnectionPool, Redis

from rediskit import config
from rediskit.redis.client.sentinel import build_sentinel_master_pool, close_sentinel_monitor_clients

log = logging.getLogger(__name__)
redis_connection_pool: ConnectionPool | None = None


def init_redis_connection_pool() -> None:
    global redis_connection_pool
    if config.REDIS_SENTINEL_ENABLED:
        # Sentinel-managed pool: every connection resolves the current master
        # via Sentinel, so the sync client (mutex lock, semaphore, all ops)
        # follows failovers just like the async client.
        log.info("Initializing Sentinel-managed redis connection pool")
        redis_connection_pool = build_sentinel_master_pool(decode_responses=True)
        return
    log.info("Initializing redis connection pool")
    redis_connection_pool = ConnectionPool(host=config.REDIS_HOST, port=config.REDIS_PORT, password=config.REDIS_PASSWORD, decode_responses=True)


def get_redis_connection() -> Redis:
    if redis_connection_pool is None:
        raise RuntimeError("Redis connection pool is not initialized! Call init_redis_connection_pool() first.")
    return Redis(connection_pool=redis_connection_pool)


def close() -> None:
    global redis_connection_pool
    if redis_connection_pool is not None:
        try:
            redis_connection_pool.disconnect(inuse_connections=True)
            close_sentinel_monitor_clients(redis_connection_pool)
            log.info("Closed Redis connection pool")
        finally:
            redis_connection_pool = None
