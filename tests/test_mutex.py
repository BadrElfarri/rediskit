# test_mutex_real_redis.py
# test_mutex_real_redis_async.py
import asyncio

import pytest
import pytest_asyncio

from rediskit.redis import get_redis_top_node
from rediskit.redis.a_client.connection import get_async_redis_connection, init_async_redis_connection_pool
from rediskit.redis_lock import get_async_redis_mutex_lock, nonblocking_mutex

TEST_TENANT_ID = "test"


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


@pytest.mark.asyncio
async def test_nonblocking_skips_when_already_running():
    lock_name = "job-nonblocking-skip"

    holder = get_async_redis_mutex_lock(lock_name, expire=10)
    assert await holder.acquire(blocking=True)

    try:
        contender = get_async_redis_mutex_lock(lock_name)
        got = await contender.acquire(blocking=False)
        assert got is False
    finally:
        await holder.release()


@pytest.mark.asyncio
async def test_nonblocking_helper_yields_true_and_releases():
    lock_name = "job-helper-acquire"

    async with nonblocking_mutex(lock_name, expire=5) as acquired:
        assert acquired is True
        await asyncio.sleep(0.05)

    again = get_async_redis_mutex_lock(lock_name, expire=5)
    assert await again.acquire(blocking=False)
    await again.release()


@pytest.mark.asyncio
async def test_nonblocking_helper_yields_false_when_held():
    lock_name = "job-helper-skip"

    l1 = get_async_redis_mutex_lock(lock_name, expire=5)
    assert await l1.acquire(blocking=True)
    try:
        async with nonblocking_mutex(lock_name) as acquired:
            assert acquired is False
    finally:
        await l1.release()


@pytest.mark.asyncio
async def test_lock_expires_then_can_be_reacquired():
    lock_name = "job-expire"

    l1 = get_async_redis_mutex_lock(lock_name, expire=1)
    assert await l1.acquire(blocking=True)
    await asyncio.sleep(1.2)  # Let TTL expire

    l2 = get_async_redis_mutex_lock(lock_name, expire=5)
    assert await l2.acquire(blocking=False)
    await l2.release()
