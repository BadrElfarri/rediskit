from contextlib import asynccontextmanager

from redis import asyncio as redis_async

from rediskit import config
from rediskit.redis.a_client.redis_in_eventloop import close_loop_redis, get_async_client_for_current_loop, get_async_redis_connection_in_eventloop


async def init_async_redis_connection_pool() -> None:
    await get_async_redis_connection_in_eventloop()


@asynccontextmanager
async def redis_single_connection_context():
    pool = redis_async.ConnectionPool(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        password=config.REDIS_PASSWORD,
        decode_responses=True,
        max_connections=1,
    )
    client = redis_async.Redis(connection_pool=pool)
    try:
        yield client
    finally:
        await client.aclose()
        await pool.disconnect()


def get_async_redis_connection() -> redis_async.Redis:
    return get_async_client_for_current_loop()


async def async_connection_close() -> None:
    await close_loop_redis()
