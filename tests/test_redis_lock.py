import asyncio
import time
import uuid

import pytest
import pytest_asyncio

from rediskit.redis import get_redis_top_node
from rediskit.redis.a_client import get_async_redis_connection
from rediskit.redis.a_client.connection import init_async_redis_connection_pool

TEST_TENANT_ID = "TEST_LOCK_POOL_TIMEOUT"


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


def _lock_key() -> str:
    # use your tenant prefixing convention so cleanup works
    return get_redis_top_node(TEST_TENANT_ID, f"lock:{uuid.uuid4()}")


@pytest.mark.asyncio
async def test_lock_pool_exhaustion_waits_then_succeeds():
    """
    Proves the pool waits instead of raising immediately when max_connections is exhausted.

    Strategy:
    - Use BLPOP to hold the ONLY pooled connection for ~2s.
    - In parallel, attempt to acquire a redis lock.
    - If the pool is blocking, lock.acquire() will wait until BLPOP releases the connection.
    - Assert it takes ~>=2s and succeeds without "Too many connections".
    """
    await init_async_redis_connection_pool()
    r = get_async_redis_connection()

    key = _lock_key()

    list_key = get_redis_top_node(TEST_TENANT_ID, "pool-hold-list")
    await r.delete(list_key)

    async def hold_single_connection_for_2s():
        # This command will occupy one pooled connection while waiting
        await r.blpop(list_key, timeout=2)

    async def acquire_lock():
        lock = r.lock(
            key,
            timeout=5,  # lock TTL
            blocking=True,
            blocking_timeout=5,  # lock wait max (separate from pool)
            sleep=0.1,
        )
        acquired = await lock.acquire()
        try:
            return acquired
        finally:
            if acquired:
                try:
                    await lock.release()
                except Exception:
                    pass

    t0 = time.perf_counter()

    t_hold = asyncio.create_task(hold_single_connection_for_2s())
    await asyncio.sleep(0.05)  # give BLPOP time to checkout the connection

    t_lock = asyncio.create_task(acquire_lock())

    await t_hold
    acquired = await t_lock

    elapsed = time.perf_counter() - t0

    assert acquired is True
    # Should have waited for the held connection (~2s), not failed immediately.
    assert elapsed >= 1.8, f"Expected ~2s wait, got {elapsed:.2f}s"
    # Should not exceed your pool timeout (5s) in normal conditions.
    assert elapsed < 5.0, f"Unexpectedly waited too long ({elapsed:.2f}s)."


@pytest.mark.asyncio
async def test_pool_timeout_5s_is_enforced_when_connection_never_frees():
    """
    Proves the pool timeout (~5s) is enforced: if no connection becomes available,
    an operation that needs a connection will fail after ~5 seconds (not immediately).

    Strategy:
    - Occupy all pool connections with long BLPOPs (longer than 5s).
    - Then attempt to acquire a lock which needs a connection from the pool.
    - Expect failure around 5s (pool timeout), not instant 'Too many connections'.
    """
    await init_async_redis_connection_pool()
    r = get_async_redis_connection()

    key = _lock_key()
    list_key = get_redis_top_node(TEST_TENANT_ID, "pool-hold-list-2")
    await r.delete(list_key)

    # You said max_connections=10 in your pool.
    # We try to occupy all 10 connections with BLPOP timeout=10.
    # If your actual pool size differs, adjust this number OR derive it if accessible.
    CONNS = 10

    async def hold_conn():
        await r.blpop(list_key, timeout=10)

    holders = [asyncio.create_task(hold_conn()) for _ in range(CONNS)]
    await asyncio.sleep(0.2)  # allow them to grab pool connections

    lock = r.lock(
        key,
        timeout=5,
        blocking=False,  # fail immediately at lock-level, but pool should still WAIT for a conn
    )

    t0 = time.perf_counter()
    with pytest.raises(Exception) as exc:
        await lock.acquire()  # should block at POOL level, then fail at ~5s pool timeout
    elapsed = time.perf_counter() - t0

    # Cancel holders
    for t in holders:
        t.cancel()
    await asyncio.gather(*holders, return_exceptions=True)

    # The exact exception type can vary by redis-py version.
    # What we care about: it should NOT fail instantly; it should take ~5s.
    assert elapsed >= 4.5, f"Expected ~5s wait, got {elapsed:.2f}s"
    assert elapsed < 7.0, f"Expected failure around 5s, got {elapsed:.2f}s"

    msg = str(exc.value).lower()
    assert "too many connections" not in msg, "Non-blocking pool likely still in use"
