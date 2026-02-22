import asyncio
import sys
import textwrap
import time
import uuid

import pytest
import pytest_asyncio

from rediskit import config
from rediskit.async_semaphore import AsyncSemaphore
from rediskit.redis import get_redis_top_node
from rediskit.redis.a_client import get_async_redis_connection
from rediskit.redis.a_client.connection import init_async_redis_connection_pool

TEST_TENANT_ID = "TEST_SEMAPHORE_TENANT_REDIS"


@pytest_asyncio.fixture(autouse=True)
async def CleanupRedis():
    await init_async_redis_connection_pool()
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
    await init_async_redis_connection_pool()
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
    await init_async_redis_connection_pool()
    key = f"testsem:{uuid.uuid4()}"
    process_unique_id1 = str(uuid.uuid4())
    process_unique_id2 = str(uuid.uuid4())
    sem1 = await semaphore(key, limit=2, process_unique_id=process_unique_id1)
    sem2 = await semaphore(key, limit=2, process_unique_id=process_unique_id2)
    await sem1.acquire_blocking()
    await sem2.acquire_blocking()
    holder_keys = await sem2.get_active_process_unique_ids()
    prefixes = {k.split(":", 1)[0] for k in holder_keys}
    assert {process_unique_id1, process_unique_id2} == prefixes
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
    await init_async_redis_connection_pool()
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
    await init_async_redis_connection_pool()
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
    await init_async_redis_connection_pool()
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


@pytest.mark.asyncio
async def test_same_instance_parallel_acquire_should_fill_limit():
    key = f"testsem:{uuid.uuid4()}"
    sem = await semaphore(key, limit=3, lock_ttl=10)

    active_counts = []
    errors = []

    async def worker():
        try:
            async with sem:
                active_counts.append(await sem.get_active_count())
                await asyncio.sleep(0.2)
        except Exception as e:
            errors.append(e)

    await asyncio.gather(*(worker() for _ in range(3)))

    # EXPECT: all 3 can hold concurrently
    assert not errors
    assert max(active_counts) == 3


@pytest.mark.asyncio
async def test_pool_not_leaking_in_use_connections_under_contention():
    max_conn = 10
    await init_async_redis_connection_pool(max_connections=max_conn)

    redis = get_async_redis_connection()
    pool = redis.connection_pool

    key = f"testsem:{uuid.uuid4()}"
    limit = 5
    total = 50

    semaphores = [AsyncSemaphore(key=key, limit=limit, acquire_timeout=5, lock_ttl=5, redis_conn=redis) for _ in range(total)]

    async def worker(i: int):
        async with semaphores[i]:
            await asyncio.sleep(0.01)

    await asyncio.gather(*(worker(i) for i in range(total)))
    await asyncio.sleep(0)

    in_use = len(getattr(pool, "_in_use_connections", []))
    available = len(getattr(pool, "_available_connections", []))
    total_known = in_use + available

    assert in_use == 0
    assert total_known <= max_conn


@pytest.mark.asyncio
async def test_no_pool_timeout_errors_under_burst():
    await init_async_redis_connection_pool(max_connections=10, timeout=2)

    redis = get_async_redis_connection()
    key = f"testsem:{uuid.uuid4()}"

    limit = 5
    total = 100
    semaphores = [AsyncSemaphore(key=key, limit=limit, acquire_timeout=10, lock_ttl=10, redis_conn=redis) for _ in range(total)]

    errors: list[Exception] = []

    async def worker(i: int):
        try:
            async with semaphores[i]:
                # do a couple redis operations to create more pressure than just lock ops
                await redis.ping()
                await asyncio.sleep(0.02)
        except Exception as e:
            errors.append(e)

    await asyncio.gather(*(worker(i) for i in range(total)))

    # If pool is overloaded, you'll typically see TimeoutError / ConnectionError / "Too many connections"
    assert not errors, f"Errors seen under burst: {[type(e).__name__ + ':' + str(e) for e in errors[:5]]}"


@pytest.mark.asyncio
async def test_pool_not_exhausted_under_realistic_pressure():
    max_conn = 10
    await init_async_redis_connection_pool(max_connections=max_conn, timeout=1)

    redis = get_async_redis_connection()
    pool = redis.connection_pool

    key = f"testsem:{uuid.uuid4()}"
    limit = 10
    total = 200

    semaphores = [AsyncSemaphore(key=key, limit=limit, acquire_timeout=10, lock_ttl=5, redis_conn=redis) for _ in range(total)]

    async def worker(i: int):
        async with semaphores[i]:
            # force some redis usage while held
            await redis.ping()
            await asyncio.sleep(0.01)

    await asyncio.gather(*(worker(i) for i in range(total)))
    await asyncio.sleep(0)

    in_use = len(getattr(pool, "_in_use_connections", []))
    assert in_use == 0, f"Connections stuck checked-out: {in_use}"


@pytest.mark.asyncio
async def test_no_leftover_slot_keys_after_release():
    key = f"testsem:{uuid.uuid4()}"
    sem = await semaphore(key, limit=3, lock_ttl=5)

    await sem.acquire_blocking()
    await sem.release_lock()

    redis = get_async_redis_connection()
    prefix = f"{config.REDIS_TOP_NODE}:{key}:slot:"
    leftovers = [k async for k in redis.scan_iter(match=f"{prefix}*")]
    assert leftovers == [], f"Leftover slot keys: {leftovers}"


@pytest.mark.asyncio
async def test_release_does_not_delete_other_holders_slot():
    redis = get_async_redis_connection()
    key = f"testsem:{uuid.uuid4()}"

    sem1 = AsyncSemaphore(key=key, limit=1, acquire_timeout=5, lock_ttl=5, redis_conn=redis)
    sem2 = AsyncSemaphore(key=key, limit=1, acquire_timeout=5, lock_ttl=5, redis_conn=redis)

    await sem1.acquire_blocking()
    slot_key = sem1.hashKey

    # simulate sem1 losing lease (expiry/crash/manual delete)
    await redis.delete(slot_key)

    # sem2 should acquire the (same) slot key now
    await sem2.acquire_blocking()
    assert sem2.hashKey == slot_key

    # sem1 releasing now MUST NOT delete sem2's lease
    await sem1.release_lock()

    assert await sem2.is_acquired_by_process(), "sem1 release deleted sem2 lease (bug)"
    await sem2.release_lock()


@pytest.mark.asyncio
async def test_sudden_crash_does_not_keep_slot():
    redis = get_async_redis_connection()
    key = f"testsem:{uuid.uuid4()}"

    ttl = 1  # seconds
    sem1 = AsyncSemaphore(key=key, limit=1, acquire_timeout=2, lock_ttl=ttl, redis_conn=redis, ttl_auto_renewal=True)
    sem2 = AsyncSemaphore(key=key, limit=1, acquire_timeout=5, lock_ttl=ttl, redis_conn=redis, ttl_auto_renewal=True)

    await sem1.acquire_blocking()

    # simulate crash: stop renewal and drop reference (no release)
    await sem1.stop_ttl_renewal()
    crash_key = sem1.hashKey
    del sem1

    # slot should still exist briefly
    assert await redis.exists(crash_key) == 1

    # after ttl, slot must disappear (auto cleanup)
    await asyncio.sleep(ttl + 0.5)
    assert await redis.exists(crash_key) == 0

    # sem2 should now be able to acquire
    assert await sem2.acquire_blocking()
    await sem2.release_lock()


@pytest.mark.asyncio
async def test_hard_crash_process_exits_does_not_keep_slot():
    ttl = 2
    key = f"testsem:{uuid.uuid4()}"

    await init_async_redis_connection_pool()
    redis = get_async_redis_connection()

    # Child script: acquire then hard-exit
    child_code = textwrap.dedent(f"""
        import os, asyncio
        from rediskit.redis.a_client.connection import init_async_redis_connection_pool
        from rediskit.redis.a_client import get_async_redis_connection
        from rediskit.async_semaphore import AsyncSemaphore

        async def main():
            await init_async_redis_connection_pool()
            r = get_async_redis_connection()
            sem = AsyncSemaphore(key="{key}", limit=1, acquire_timeout=5, lock_ttl={ttl}, redis_conn=r, ttl_auto_renewal=False)
            await sem.acquire_blocking()
            # hard crash: no cleanup, no finally
            os._exit(1)

        asyncio.run(main())
    """)

    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        child_code,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.wait()

    await asyncio.sleep(0.1)

    await asyncio.sleep(ttl + 0.5)
    sem2 = AsyncSemaphore(key=key, limit=1, acquire_timeout=5, lock_ttl=ttl, redis_conn=redis, ttl_auto_renewal=False)
    assert await sem2.acquire_blocking()
    await sem2.release_lock()

    keys = [k async for k in redis.scan_iter(match=f"{config.REDIS_TOP_NODE}:{key}:slot:*")]
    assert keys == []
