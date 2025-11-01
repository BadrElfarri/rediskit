import logging

from redis import ConnectionPool, Redis

from rediskit import config

log = logging.getLogger(__name__)
redis_connection_pool: ConnectionPool | None = None


def init_redis_connection_pool() -> None:
    global redis_connection_pool
    log.info("Initializing redis connection pool")
    redis_connection_pool = ConnectionPool(host=config.REDIS_HOST, port=config.REDIS_PORT, password=config.REDIS_PASSWORD, decode_responses=True)


def get_redis_connection() -> Redis:
    if redis_connection_pool is None:
        raise Exception("Redis connection pool is not initialized!")
    return Redis(connection_pool=redis_connection_pool)


def close() -> None:
    global redis_connection_pool
    if redis_connection_pool is not None:
        try:
            redis_connection_pool.disconnect(inuse_connections=True)
            log.info("Closed Redis connection pool")
        finally:
            redis_connection_pool = None
