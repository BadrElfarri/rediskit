from contextlib import asynccontextmanager

from redis import asyncio as redis_async

from rediskit import config
from rediskit.redis.a_client.redis_in_eventloop import close_loop_redis, get_async_client_for_current_loop, get_async_redis_connection_in_eventloop


async def init_async_redis_connection_pool(
    host: str = config.REDIS_HOST,
    port: int = config.REDIS_PORT,
    password: str = config.REDIS_PASSWORD,
    retry_on_timeout: bool = True,
    decode_responses: bool = True,
    socket_timeout: int = 10,
    socket_connect_timeout: int = 5,
    socket_keepalive: bool = True,
    health_check_interval: int = 30,
    max_connections: int = 10,
    timeout: int = 5,
) -> None:
    await get_async_redis_connection_in_eventloop(
        host=host,
        port=port,
        password=password,
        retry_on_timeout=retry_on_timeout,
        decode_responses=decode_responses,
        socket_timeout=socket_timeout,
        socket_connect_timeout=socket_connect_timeout,
        socket_keepalive=socket_keepalive,
        health_check_interval=health_check_interval,
        max_connections=max_connections,
        timeout=timeout,
    )


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
