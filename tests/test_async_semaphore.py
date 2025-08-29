import asyncio
import time
import uuid

import pytest
import pytest_asyncio

from rediskit import redis_client
from rediskit.async_semaphore import AsyncSemaphore
from rediskit.redis_client import get_async_redis_connection, get_redis_top_node

TEST_TENANT_ID = "TEST_SEMAPHORE_TENANT_REDIS"


@pytest_asyncio.fixture(autouse=True)
async def CleanupRedis():
    await redis_client.init_async_redis_connection_pool()
    async_redis_conn = get_async_redis_connection()
    prefix = get_redis_top_node(TEST_TENANT_ID, "")
    keys = [k async for k in async_redis_conn.scan_iter(match=f"{prefix}*")]
    for key in keys:
        await async_redis_conn.delete(key)
    yield
    keys = [k async for k in async_redis_conn.scan_iter(match=f"{prefix}*")]
    for key in keys:
        await async_redis_conn.delete(key)


async def semaphore(namespace, limit=2, acquire_timeout=2, lock_ttl=3, process_unique_id=None, ttl_auto_renewal=True):
    await redis_client.init_async_redis_connection_pool()
    conn = get_async_redis_connection()
    return AsyncSemaphore(
        redis_conn=conn,
        key=namespace,
        limit=limit,
        acquire_timeout=acquire_timeout,
        lock_ttl=lock_ttl,
        process_unique_id=process_unique_id,
        ttl_auto_renewal=ttl_auto_renewal,
    )


@pytest.mark.asyncio
async def test_basic_acquire_and_release():
    key = f"testsem:{uuid.uuid4()}"
    sem = await semaphore(key, limit=2)
    t1 = await sem.acquire_blocking()
    assert t1 is not None
    assert await sem.get_active_count() == 1
    await sem.release_lock()
    assert await sem.get_active_count() == 0


@pytest.mark.asyncio
async def test_block_when_full():
    key = f"testsem:{uuid.uuid4()}"
    sem1 = await semaphore(key, limit=1)
    sem2 = await semaphore(key, limit=1)
    await sem1.acquire_blocking()
    start = asyncio.get_event_loop().time()
    with pytest.raises(RuntimeError):
        await sem2.acquire_blocking()
    elapsed = asyncio.get_event_loop().time() - start
    await sem1.release_lock()
    assert elapsed < 10


@pytest.mark.asyncio
async def test_multiple_parallel():
    key = f"testsem:{uuid.uuid4()}"
    max_count = 20
    sems = [await semaphore(key, limit=max_count, lock_ttl=20) for _ in range(10)]
    results = []
    errors = []

    async def worker(i):
        try:
            await sems[i].acquire_blocking()
            results.append(i)
            await asyncio.sleep(1)
            await sems[i].release_lock()
        except Exception as e:
            errors.append(e)

    await asyncio.gather(*(worker(i) for i in range(10)))
    assert len(results) == 10
    assert not errors


@pytest.mark.asyncio
async def test_semaphore_expires_on_crash():
    key = f"testsem:{uuid.uuid4()}"
    sem1 = await semaphore(key, limit=1, lock_ttl=2)
    sem2 = await semaphore(key, limit=1, lock_ttl=2)
    await sem1.acquire_blocking()
    await sem1.stop_ttl_renewal()
    del sem1
    await asyncio.sleep(3)
    assert await sem2.acquire_blocking()
    await sem2.release_lock()


@pytest.mark.asyncio
async def test_context_manager():
    key = f"testsem:{uuid.uuid4()}"
    async with await semaphore(key, limit=1) as sem:
        assert await sem.get_active_count() == 1
    sem2 = await semaphore(key, limit=1)
    assert await sem2.acquire_blocking()
    await sem2.release_lock()


@pytest.mark.asyncio
async def test_different_process_unique_ids():
    await redis_client.init_async_redis_connection_pool()
    key = f"testsem:{uuid.uuid4()}"
    process_unique_id1 = str(uuid.uuid4())
    process_unique_id2 = str(uuid.uuid4())
    sem1 = await semaphore(key, limit=2, process_unique_id=process_unique_id1)
    sem2 = await semaphore(key, limit=2, process_unique_id=process_unique_id2)
    await sem1.acquire_blocking()
    await sem2.acquire_blocking()
    holder_keys = await sem2.get_active_process_unique_ids()
    assert {process_unique_id1, process_unique_id2} == holder_keys
    await sem1.release_lock()
    await sem2.release_lock()


@pytest.mark.asyncio
async def test_release_without_acquire():
    key = f"testsem:{uuid.uuid4()}"
    sem = await semaphore(key)
    await sem.release_lock()


@pytest.mark.asyncio
async def test_semaphore_ttl_isolated():
    key = f"testsem:{uuid.uuid4()}"
    sem = await semaphore(key, limit=1, lock_ttl=1, ttl_auto_renewal=False)
    await sem.acquire_blocking()
    await asyncio.sleep(1.5)
    assert await sem.acquire_blocking()
    await sem.release_lock()


@pytest.mark.asyncio
async def test_ttl_none():
    key = f"testsem:{uuid.uuid4()}"
    sem = await semaphore(key, limit=1, lock_ttl=None)
    await sem.acquire_blocking()
    await asyncio.sleep(1.5)
    assert await sem.is_acquired_by_process()
    await sem.release_lock()
    assert not await sem.is_acquired_by_process()


@pytest.mark.asyncio
async def test_invalid_limit():
    key = f"testsem:{uuid.uuid4()}"
    with pytest.raises(ValueError):
        await semaphore(key, limit=0)
    with pytest.raises(ValueError):
        await semaphore(key, limit=-5)


@pytest.mark.asyncio
async def test_invalid_timeout():
    key = f"testsem:{uuid.uuid4()}"
    with pytest.raises(ValueError):
        await semaphore(key, acquire_timeout=0)
    with pytest.raises(ValueError):
        await semaphore(key, acquire_timeout=-10)


@pytest.mark.asyncio
async def test_invalid_ttl():
    key = f"testsem:{uuid.uuid4()}"
    with pytest.raises(ValueError):
        await semaphore(key, lock_ttl=0)
    with pytest.raises(ValueError):
        await semaphore(key, lock_ttl=-1)


@pytest.mark.asyncio
async def test_re_acquire_same_process_unique_id():
    key = f"testsem:{uuid.uuid4()}"
    process_unique_id = str(uuid.uuid4())
    sem = await semaphore(key, process_unique_id=process_unique_id)
    await sem.acquire_blocking()
    with pytest.raises(RuntimeError):
        await sem.acquire_blocking()
    await sem.release_lock()


@pytest.mark.asyncio
async def test_multiple_release():
    key = f"testsem:{uuid.uuid4()}"
    sem = await semaphore(key)
    await sem.acquire_blocking()
    await sem.release_lock()
    await sem.release_lock()


@pytest.mark.asyncio
async def test_semaphore_parallel_contention():
    key = f"testsem:{uuid.uuid4()}"
    max_count = 2
    acquired = []
    errors = []

    async def contender(i):
        process_unique_id = f"process_unique_id-{i}"
        sem = await semaphore(key, limit=max_count, lock_ttl=2, process_unique_id=process_unique_id)
        try:
            await sem.acquire_blocking()
            acquired.append(process_unique_id)
            await asyncio.sleep(0.5)
        except Exception as e:
            errors.append(e)
        finally:
            await sem.release_lock()

    await asyncio.gather(*(contender(i) for i in range(4)))
    assert len(acquired) == 4
    assert not errors


@pytest.mark.asyncio
async def test_ttl_per_holder_is_isolated():
    key = f"testsem:{uuid.uuid4()}"
    process_unique_id1 = str(uuid.uuid4())
    process_unique_id2 = str(uuid.uuid4())
    sem1 = await semaphore(key, limit=2, lock_ttl=1, process_unique_id=process_unique_id1, ttl_auto_renewal=False)
    sem2 = await semaphore(key, limit=2, lock_ttl=5, process_unique_id=process_unique_id2, ttl_auto_renewal=False)
    await sem1.acquire_blocking()
    await sem2.acquire_blocking()
    await asyncio.sleep(1.5)
    assert not await sem1.is_acquired_by_process()
    assert await sem2.is_acquired_by_process()
    await sem2.release_lock()


@pytest.mark.asyncio
async def test_acquire_after_release():
    key = f"testsem:{uuid.uuid4()}"
    sem1 = await semaphore(key, limit=1)
    sem2 = await semaphore(key, limit=1)
    await sem1.acquire_blocking()
    await sem1.release_lock()
    assert await sem2.acquire_blocking()
    await sem2.release_lock()


@pytest.mark.asyncio
async def test_acquire_with_zero_ttl():
    key = f"testsem:{uuid.uuid4()}"
    with pytest.raises(ValueError):
        await semaphore(key, limit=1, lock_ttl=0)


@pytest.mark.asyncio
async def test_manual_expiry_behavior():
    await redis_client.init_async_redis_connection_pool()
    key = f"testsem:{uuid.uuid4()}"
    sem = await semaphore(key, limit=1, lock_ttl=1)
    await sem.acquire_blocking()
    await get_async_redis_connection().delete(sem.hashKey)
    assert await sem.acquire_blocking()
    await sem.release_lock()


@pytest.mark.asyncio
async def test_custom_process_unique_id():
    key = f"testsem:{uuid.uuid4()}"
    process_unique_id = "my-process-id"
    sem = await semaphore(key, process_unique_id=process_unique_id)
    assert sem.process_unique_id == process_unique_id
    await sem.acquire_blocking()
    await sem.release_lock()


@pytest.mark.asyncio
async def test_semaphore_ttl_renewal():
    key = f"testsem:{uuid.uuid4()}"
    ttl = 2  # seconds
    sem = await semaphore(key, limit=1, lock_ttl=ttl)
    await sem.acquire_blocking()
    await asyncio.sleep(ttl + 2)
    assert await sem.is_acquired_by_process()
    await sem.stop_ttl_renewal()
    await asyncio.sleep(ttl + 1)
    assert not await sem.is_acquired_by_process()


@pytest.mark.asyncio
async def test_semaphore_parallel_blocking_batches():
    await redis_client.init_async_redis_connection_pool()
    key = f"testsem:{uuid.uuid4()}"
    limit = 10
    total = 30
    hold_time = 1  # seconds

    semaphores = [AsyncSemaphore(key=key, limit=limit, acquire_timeout=5, lock_ttl=5) for _ in range(total)]

    start_time = time.perf_counter()
    results = []

    async def worker(i):
        async with semaphores[i]:
            results.append(i)
            await asyncio.sleep(hold_time)

    await asyncio.gather(*(worker(i) for i in range(total)))
    elapsed = time.perf_counter() - start_time

    assert len(results) == total
    # Should take a bit more than 3s (3 batches of 10 with 1s hold each)
    assert elapsed >= 3.0 and elapsed < 5.5, f"Expected ~3s, got {elapsed:.2f}s"


@pytest.mark.asyncio
async def test_semaphore_large_parallel():
    await redis_client.init_async_redis_connection_pool()
    key = f"testsem:{uuid.uuid4()}"
    limit = 100
    total = 200
    hold_time = 1  # seconds

    semaphores = [AsyncSemaphore(key=key, limit=limit, acquire_timeout=5, lock_ttl=5) for _ in range(total)]

    start_time = time.perf_counter()
    results = []

    async def worker(i):
        async with semaphores[i]:
            results.append(i)
            await asyncio.sleep(hold_time)

    await asyncio.gather(*(worker(i) for i in range(total)))
    elapsed = time.perf_counter() - start_time

    assert len(results) == total
    # Should take a bit more than 2s (2 batches of 100 with 1s hold each)
    assert elapsed >= 2.0 and elapsed < 4.0, f"Expected ~2s, got {elapsed:.2f}s"
