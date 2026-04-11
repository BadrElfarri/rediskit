import pytest
import pytest_asyncio

from rediskit.redis import get_redis_top_node
from rediskit.redis.a_client import (
    delete,
    drain_list,
    expire,
    get_async_redis_connection,
    llen,
    lpop,
    lpush,
    lrange,
    rpop,
    rpush,
)
from rediskit.redis.a_client.connection import init_async_redis_connection_pool

TEST_TENANT_ID = "PYTEST_REDISKIT_TENANT_LIST_ASYNC"


@pytest_asyncio.fixture
async def connection():
    await init_async_redis_connection_pool()
    return get_async_redis_connection()


@pytest_asyncio.fixture(autouse=True)
async def cleanup_redis(connection):
    prefix = get_redis_top_node(TEST_TENANT_ID, "")
    async for key in connection.scan_iter(match=f"{prefix}*"):
        await connection.delete(key)
    yield
    async for key in connection.scan_iter(match=f"{prefix}*"):
        await connection.delete(key)


def _k(name: str) -> str:
    return get_redis_top_node(TEST_TENANT_ID, name)


@pytest.mark.asyncio
async def test_rpush_lpop_preserves_arrival_order(connection):
    key = _k("inbox")
    await rpush(key, "a", connection=connection)
    await rpush(key, "b", connection=connection)
    await rpush(key, "c", connection=connection)

    assert await llen(key, connection=connection) == 3
    assert await lpop(key, connection=connection) == "a"
    assert await lpop(key, connection=connection) == "b"
    assert await lpop(key, connection=connection) == "c"
    assert await lpop(key, connection=connection) is None


@pytest.mark.asyncio
async def test_lpush_and_rpop(connection):
    key = _k("stack")
    await lpush(key, "a", connection=connection)
    await lpush(key, "b", connection=connection)
    assert await rpop(key, connection=connection) == "a"
    assert await rpop(key, connection=connection) == "b"


@pytest.mark.asyncio
async def test_lrange(connection):
    key = _k("range")
    await rpush(key, "x", "y", "z", connection=connection)
    assert await lrange(key, 0, -1, connection=connection) == ["x", "y", "z"]
    assert await lrange(key, 0, 0, connection=connection) == ["x"]


@pytest.mark.asyncio
async def test_drain_list_returns_all_in_order_and_empties(connection):
    key = _k("drain")
    await rpush(key, "1", connection=connection)
    await rpush(key, "2", connection=connection)
    await rpush(key, "3", connection=connection)

    drained = await drain_list(key, connection=connection)
    assert drained == ["1", "2", "3"]
    assert await llen(key, connection=connection) == 0
    assert await drain_list(key, connection=connection) == []


@pytest.mark.asyncio
async def test_expire_and_delete_raw_key(connection):
    key = _k("expiring")
    await rpush(key, "only", connection=connection)
    assert await expire(key, 60, connection=connection) is True
    assert await connection.ttl(key) > 0

    assert await delete(key, connection=connection) == 1
    assert await llen(key, connection=connection) == 0
