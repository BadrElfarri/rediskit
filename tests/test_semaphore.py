import pytest
import threading
import time
import uuid
from rediskit.redisClient import GetRedisConnection, GetRedisTopNode

from rediskit.semaphore import Semaphore

TEST_TENANT_ID = "TEST_SEMAPHORE_TENANT_REDIS"


@pytest.fixture
def redis_conn():
    return GetRedisConnection()


@pytest.fixture(autouse=True)
def CleanupRedis(redis_conn):
    prefix = GetRedisTopNode(TEST_TENANT_ID, "")
    for key in redis_conn.scan_iter(match=f"{prefix}*"):
        redis_conn.delete(key)
    yield
    for key in redis_conn.scan_iter(match=f"{prefix}*"):
        redis_conn.delete(key)


def create_semaphore(redis_conn, namespace, count=2, acquireTimeOut=2, lockTimeToLive=3, token=None):
    return Semaphore(
        redis_conn=redis_conn, namespace=namespace, limit=count, acquire_time_out=acquireTimeOut, lock_time_to_live=lockTimeToLive, process_unique_id=token
    )


def test_basic_acquire_and_release(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    sem = create_semaphore(redis_conn, key, count=2)
    t1 = sem.acquire_blocking()
    assert t1 is not None
    assert sem.get_active_count() == 1
    sem.release_lock()
    assert sem.get_active_count() == 0


def test_block_when_full(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    sem1 = create_semaphore(redis_conn, key, count=1)
    sem2 = create_semaphore(redis_conn, key, count=1)
    sem1.acquire_blocking()
    start = time.time()
    with pytest.raises(RuntimeError):
        sem2.acquire_blocking()
    elapsed = time.time() - start
    sem1.release_lock()
    assert elapsed < 10  # It should fail quickly due to acquireTimeOut=2


def test_multiple_parallel(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    max_count = 20
    sems = [create_semaphore(redis_conn, key, count=max_count, lockTimeToLive=20) for _ in range(10)]
    results = []
    errors = []

    def worker(i):
        try:
            sems[i].acquire_blocking()
            results.append(i)
            time.sleep(1)
            sems[i].release_lock()
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    [t.start() for t in threads]
    [t.join() for t in threads]

    # At no time should more than max_count have been in results at once
    # But with parallel execution, it's tricky to assert this *exactly* unless we track times
    assert len(results) == 10
    assert not errors


def test_semaphore_expires_on_crash(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    sem1 = create_semaphore(redis_conn, key, count=1, lockTimeToLive=2)
    sem2 = create_semaphore(redis_conn, key, count=1, lockTimeToLive=2)
    assert sem1.acquire_blocking()
    # Simulate crash: no ReleaseLock, just delete sem1 ref
    del sem1
    # Wait for TTL to expire in Redis (plus a little slack)
    time.sleep(3)
    # Now sem2 should be able to acquire
    assert sem2.acquire_blocking()
    sem2.release_lock()


def test_context_manager(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    with create_semaphore(redis_conn, key, count=1) as sem:
        assert sem.get_active_count() == 1
    # After context, should be released
    sem2 = create_semaphore(redis_conn, key, count=1)
    assert sem2.acquire_blocking()
    sem2.release_lock()


def test_different_tokens(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    token1 = str(uuid.uuid4())
    token2 = str(uuid.uuid4())
    sem1 = create_semaphore(redis_conn, key, count=2, token=token1)
    sem2 = create_semaphore(redis_conn, key, count=2, token=token2)
    assert sem1.acquire_blocking()
    assert sem2.acquire_blocking()
    assert set(redis_conn.hkeys(f"{key}:holders")) == {token1, token2}
    sem1.release_lock()
    sem2.release_lock()


def test_release_without_acquire(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    sem = create_semaphore(redis_conn, key)
    # Should not raise
    sem.release_lock()


def test_semaphore_ttl_isolated(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    sem = create_semaphore(redis_conn, key, count=1, lockTimeToLive=1)
    assert sem.acquire_blocking()
    time.sleep(1.5)
    # Should be auto-expired, so re-acquire is possible
    assert sem.acquire_blocking()
    sem.release_lock()
