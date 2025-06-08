import threading
import time
import uuid

import pytest

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


def semaphore(redis_conn, namespace, limit=2, acquire_timeout=2, lock_ttl=3, process_unique_id=None, ttl_auto_renewal=True):
    return Semaphore(
        redis_conn=redis_conn,
        key=namespace,
        limit=limit,
        acquire_timeout=acquire_timeout,
        lock_ttl=lock_ttl,
        process_unique_id=process_unique_id,
        ttl_auto_renewal=ttl_auto_renewal,
    )


def test_basic_acquire_and_release(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    sem = semaphore(redis_conn, key, limit=2)
    t1 = sem.acquire_blocking()
    assert t1 is not None
    assert sem.get_active_count() == 1
    sem.release_lock()
    assert sem.get_active_count() == 0


def test_block_when_full(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    sem1 = semaphore(redis_conn, key, limit=1)
    sem2 = semaphore(redis_conn, key, limit=1)
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
    sems = [semaphore(redis_conn, key, limit=max_count, lock_ttl=20) for _ in range(10)]
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
    sem1 = semaphore(redis_conn, key, limit=1, lock_ttl=2)
    sem2 = semaphore(redis_conn, key, limit=1, lock_ttl=2)
    assert sem1.acquire_blocking()
    # Simulate crash: no ReleaseLock, just delete sem1 ref
    sem1._stop_ttl_renewal()
    del sem1
    # Wait for TTL to expire in Redis (plus a little slack)
    time.sleep(3)
    # Now sem2 should be able to acquire
    assert sem2.acquire_blocking()
    sem2.release_lock()


def test_context_manager(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    with semaphore(redis_conn, key, limit=1) as sem:
        assert sem.get_active_count() == 1
    # After context, should be released
    sem2 = semaphore(redis_conn, key, limit=1)
    assert sem2.acquire_blocking()
    sem2.release_lock()


def test_different_process_unique_ids(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    process_unique_id1 = str(uuid.uuid4())
    process_unique_id2 = str(uuid.uuid4())
    sem1 = semaphore(redis_conn, key, limit=2, process_unique_id=process_unique_id1)
    sem2 = semaphore(redis_conn, key, limit=2, process_unique_id=process_unique_id2)
    assert sem1.acquire_blocking()
    assert sem2.acquire_blocking()
    assert sem2.get_active_process_unique_ids() == {process_unique_id1, process_unique_id2}
    sem1.release_lock()
    sem2.release_lock()


def test_release_without_acquire(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    sem = semaphore(redis_conn, key)
    # Should not raise
    sem.release_lock()


def test_semaphore_ttl_isolated(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    sem = semaphore(redis_conn, key, limit=1, lock_ttl=1, ttl_auto_renewal=False)
    assert sem.acquire_blocking()
    time.sleep(1.5)
    # Should be auto-expired, so re-acquire is possible
    assert sem.acquire_blocking()
    sem.release_lock()


def test_ttl_none(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    sem = semaphore(redis_conn, key, limit=1, lock_ttl=None)
    assert sem.acquire_blocking()
    # Should not expire by itself, so after some time, still there
    time.sleep(1.5)
    assert sem.is_acquired_by_process()
    sem.release_lock()
    assert not sem.is_acquired_by_process()


def test_invalid_limit(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    with pytest.raises(ValueError):
        semaphore(redis_conn, key, limit=0)
    with pytest.raises(ValueError):
        semaphore(redis_conn, key, limit=-5)


def test_invalid_timeout(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    with pytest.raises(ValueError):
        semaphore(redis_conn, key, acquire_timeout=0)
    with pytest.raises(ValueError):
        semaphore(redis_conn, key, acquire_timeout=-10)


def test_invalid_ttl(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    with pytest.raises(ValueError):
        semaphore(redis_conn, key, lock_ttl=0)
    with pytest.raises(ValueError):
        semaphore(redis_conn, key, lock_ttl=-1)


def test_re_acquire_same_process_unique_id(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    process_unique_id = str(uuid.uuid4())
    sem = semaphore(redis_conn, key, process_unique_id=process_unique_id)
    assert sem.acquire_blocking()
    # Should raise, as same process tries to re-acquire
    with pytest.raises(RuntimeError):
        sem.acquire_blocking()
    sem.release_lock()


def test_multiple_release(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    sem = semaphore(redis_conn, key)
    assert sem.acquire_blocking()
    sem.release_lock()
    # Releasing again should not fail
    sem.release_lock()


def test_semaphore_parallel_contention(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    max_count = 2
    acquired = []
    errors = []

    def contender(i):
        process_unique_id = f"process_unique_id-{i}"
        sem = semaphore(redis_conn, key, limit=max_count, lock_ttl=2, process_unique_id=process_unique_id)
        try:
            sem.acquire_blocking()
            acquired.append(process_unique_id)
            time.sleep(0.5)
        except Exception as e:
            errors.append(e)
        finally:
            sem.release_lock()

    threads = [threading.Thread(target=contender, args=(i,)) for i in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    # Only max_count can hold at once, but all should succeed eventually due to release
    assert len(acquired) == 4
    assert not errors


def test_ttl_per_holder_is_isolated(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    process_unique_id1 = str(uuid.uuid4())
    process_unique_id2 = str(uuid.uuid4())
    sem1 = semaphore(redis_conn, key, limit=2, lock_ttl=1, process_unique_id=process_unique_id1, ttl_auto_renewal=False)
    sem2 = semaphore(redis_conn, key, limit=2, lock_ttl=5, process_unique_id=process_unique_id2, ttl_auto_renewal=False)
    assert sem1.acquire_blocking()
    assert sem2.acquire_blocking()
    # Wait for the short TTL to expire for sem1 only
    time.sleep(1.5)
    assert not sem1.is_acquired_by_process()  # Should be expired
    assert sem2.is_acquired_by_process()  # Should still be there
    sem2.release_lock()


def test_acquire_after_release(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    sem1 = semaphore(redis_conn, key, limit=1)
    sem2 = semaphore(redis_conn, key, limit=1)
    assert sem1.acquire_blocking()
    sem1.release_lock()
    assert sem2.acquire_blocking()
    sem2.release_lock()


def test_acquire_with_zero_ttl(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    # lockTimeToLive=0 should raise (per __init__)
    with pytest.raises(ValueError):
        semaphore(redis_conn, key, limit=1, lock_ttl=0)


def test_manual_expiry_behavior(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    sem = semaphore(redis_conn, key, limit=1, lock_ttl=1)
    assert sem.acquire_blocking()
    # Simulate Redis deletion (like a crash, force unlock)
    redis_conn.delete(sem.hashKey)
    # Should be able to acquire again
    assert sem.acquire_blocking()
    sem.release_lock()


def test_custom_process_unique_id(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    process_unique_id = "my-process-id"
    sem = semaphore(redis_conn, key, process_unique_id=process_unique_id)
    assert sem.process_unique_id == process_unique_id
    assert sem.acquire_blocking()
    sem.release_lock()


def test_semaphore_ttl_renewal(redis_conn):
    key = f"testsem:{uuid.uuid4()}"
    ttl = 2  # seconds
    sem = semaphore(redis_conn, key, limit=1, lock_ttl=ttl)
    assert sem.acquire_blocking()

    # Wait longer than the initial TTL (to test renewal)
    time.sleep(ttl + 2)
    # Lock should still be held by this process, due to renewal thread
    assert sem.is_acquired_by_process()

    # Now, stop renewal and wait for the lock to expire
    sem._stop_ttl_renewal()
    time.sleep(ttl + 1)
    # Should no longer be held
    assert not sem.is_acquired_by_process()
