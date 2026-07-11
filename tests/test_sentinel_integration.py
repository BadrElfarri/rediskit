"""Live integration test: every capability through the Sentinel client.

Unlike ``test_sentinel_async.py`` / ``test_sentinel_sync.py`` (which assert
wiring only, no connection), this suite runs the *real* operations a service
makes — cache, hash, list/queue, counters, key TTLs, distributed mutex locks,
semaphores, memoize, and the pub/sub broadcast (publish + subscribe_channel +
FanoutBroker) — against a live Sentinel-managed master, for BOTH the async and
the sync client. It first asserts each client really is Sentinel-managed, so
every subsequent op is proven to flow through Sentinel.

RUN IT against a live topology (e.g. the revvue-services redis stack) with:

    REDIS_SENTINEL_ENABLED=true \
    REDIS_SENTINEL_HOSTS=redis-sentinel:26379 \
    REDIS_SENTINEL_MASTER_NAME=myMaster \
    pytest tests/test_sentinel_integration.py -v

The module skips entirely unless REDIS_SENTINEL_ENABLED=true, so the normal
single-redis CI suite is unaffected. It must run where the Sentinel-announced
master/replica hostnames resolve (i.e. inside the same docker network).
"""

import asyncio
import contextlib
import os
import time

import pytest
import pytest_asyncio
from redis.asyncio.sentinel import SentinelConnectionPool
from redis.sentinel import SentinelConnectionPool as SyncSentinelConnectionPool

from rediskit import config
from rediskit.async_semaphore import AsyncSemaphore
from rediskit.memoize import a_redis_memoize
from rediskit.redis import get_redis_top_node
from rediskit.redis.a_client import (
    FanoutBroker,
    counter,
    counter_value,
    delete,
    delete_cache_from_redis,
    dump_blob_to_redis,
    dump_cache_to_redis,
    get_async_redis_connection,
    h_get_cache_from_redis,
    h_set_cache_to_redis,
    init_async_redis_connection_pool,
    llen,
    load_blob_from_redis,
    load_cache_from_redis,
    lpop,
    lrange,
    publish,
    readiness_ping,
    rpush,
    set_ttl_for_key,
    subscribe_channel,
)
from rediskit.redis.client import get_redis_connection, init_redis_connection_pool
from rediskit.redis_lock import get_async_redis_mutex_lock, get_redis_mutex_lock, nonblocking_mutex
from rediskit.semaphore import Semaphore

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not config.REDIS_SENTINEL_ENABLED,
        reason="Sentinel integration test: set REDIS_SENTINEL_ENABLED=true + REDIS_SENTINEL_HOSTS to run against a live topology",
    ),
]

TAG = "ITSENT"  # every key/lock/channel embeds this so cleanup is a single scan
TENANT = "ITSENT_TENANT"


@pytest_asyncio.fixture(autouse=True)
async def sentinel_pool():
    await init_async_redis_connection_pool()
    conn = get_async_redis_connection()
    async for k in conn.scan_iter(match=f"*{TAG}*"):
        await conn.delete(k)
    yield conn
    async for k in conn.scan_iter(match=f"*{TAG}*"):
        await conn.delete(k)


async def test_client_is_sentinel_managed():
    """Prove the shared client is actually Sentinel-managed — everything below rides it."""
    conn = get_async_redis_connection()
    pool = conn.connection_pool
    assert isinstance(pool, SentinelConnectionPool), f"expected SentinelConnectionPool, got {type(pool).__name__}"
    assert pool.is_master is True
    assert pool.service_name == config.REDIS_SENTINEL_MASTER_NAME
    # and it can actually reach the master
    assert await conn.ping() is True


async def test_string_blob_roundtrip():
    await dump_blob_to_redis(TENANT, f"{TAG}-blob", "hello-blob", ttl=60)
    assert await load_blob_from_redis(TENANT, f"{TAG}-blob") == "hello-blob"


async def test_json_cache_roundtrip():
    await dump_cache_to_redis(TENANT, f"{TAG}-doc", {"a": 1, "b": [1, 2, 3]}, ttl=60)
    loaded = await load_cache_from_redis(TENANT, f"{TAG}-doc")
    assert loaded and loaded[0]["a"] == 1
    await delete_cache_from_redis(TENANT, f"{TAG}-doc")
    assert not await load_cache_from_redis(TENANT, f"{TAG}-doc")


async def test_hash_roundtrip():
    await h_set_cache_to_redis(TENANT, f"{TAG}-hash", {"field1": "v1", "field2": "v2"})
    got = await h_get_cache_from_redis(TENANT, f"{TAG}-hash", ["field1", "field2"])
    assert got["field1"] == "v1" and got["field2"] == "v2"


async def test_list_queue_ops():
    key = get_redis_top_node(TENANT, f"{TAG}-list")
    await rpush(key, "a", "b", "c")
    assert await llen(key) == 3
    assert await lrange(key, 0, -1) == ["a", "b", "c"]
    assert await lpop(key) == "a"


async def test_counter_incr_decr():
    key = f"{TAG}-cnt"
    assert await counter(TENANT, "hits", 1, key=key) == 1
    await counter(TENANT, "hits", 4, key=key)
    assert await counter(TENANT, "hits", -2, key=key) == 3
    assert int(await counter_value(TENANT, "hits", key=key)) == 3


async def test_key_ttl_and_delete():
    conn = get_async_redis_connection()
    node = get_redis_top_node(TENANT, f"{TAG}-ttlkey")
    await conn.set(node, "x")
    await set_ttl_for_key(TENANT, f"{TAG}-ttlkey", ttl=30)
    assert 0 < await conn.ttl(node) <= 30
    assert await delete(node) == 1


async def test_mutex_lock_write():
    """The exact op that threw READONLY in prod: lock acquire runs SET NX PX."""
    conn = get_async_redis_connection()
    lock = get_async_redis_mutex_lock(f"{TAG}-lock", expire=10, blocking_timeout=5)
    assert await lock.acquire() is True
    try:
        await conn.set(get_redis_top_node(TENANT, f"{TAG}-guarded"), "written-under-lock")
    finally:
        await lock.release()


async def test_nonblocking_mutex_mutual_exclusion():
    async with nonblocking_mutex(f"{TAG}-nb", expire=10) as first:
        assert first is True
        async with nonblocking_mutex(f"{TAG}-nb", expire=10) as second:
            assert second is False  # already held


async def test_semaphore_limit():
    a = AsyncSemaphore(f"{TAG}-sem", limit=2, acquire_timeout=3, lock_ttl=15)
    b = AsyncSemaphore(f"{TAG}-sem", limit=2, acquire_timeout=3, lock_ttl=15)
    ha, hb = await a.acquire_blocking(), await b.acquire_blocking()
    try:
        assert ha and hb  # both slots acquired through the Sentinel master
    finally:
        await a.release_lock()
        await b.release_lock()


async def test_memoize_caches():
    calls = {"n": 0}

    @a_redis_memoize(memoize_key=f"{TAG}-memo", ttl=30, cache_type="zipJson")
    async def compute(tenantId: str, x: int):
        calls["n"] += 1
        return {"result": x * 2}

    r1 = await compute(TENANT, 21)
    r2 = await compute(TENANT, 21)
    assert r1 == r2 == {"result": 42}
    assert calls["n"] == 1  # second call served from the Sentinel-backed cache


async def test_broadcast_subscribe_channel():
    channel = f"{TAG}-chan"
    sub = await subscribe_channel(channel)
    try:
        await asyncio.sleep(0.3)  # let SUBSCRIBE register
        await publish(channel, {"msg": "broadcast"})
        received = await asyncio.wait_for(sub.__anext__(), timeout=5)
        assert received == {"msg": "broadcast"}
    finally:
        await sub.aclose()


async def test_broadcast_fanout_broker():
    """The broadcast fan-out path: one Sentinel pub/sub connection, many local consumers."""
    channel = f"{TAG}-fanout"
    broker = FanoutBroker()
    await broker.start(channels=[channel])
    try:
        h1 = await broker.subscribe(channel)
        h2 = await broker.subscribe(channel)
        await asyncio.sleep(0.3)
        await publish(channel, {"n": 99})
        m1 = await asyncio.wait_for(h1.__anext__(), timeout=5)
        m2 = await asyncio.wait_for(h2.__anext__(), timeout=5)
        assert m1 == {"n": 99} and m2 == {"n": 99}  # fanned out to both consumers
    finally:
        await broker.stop()


async def test_readiness_ping():
    assert await readiness_ping() is True


# ---------------------------------------------------------------------------
# Sync client through Sentinel — the sync mutex lock is the exact op class that
# threw READONLY in prod, so it gets the same live proof as the async side.
# ---------------------------------------------------------------------------


async def test_sync_client_is_sentinel_managed():
    """Prove the sync shared pool is Sentinel-managed — the sync ops below ride it."""
    init_redis_connection_pool()
    conn = get_redis_connection()
    pool = conn.connection_pool
    assert isinstance(pool, SyncSentinelConnectionPool), f"expected SentinelConnectionPool, got {type(pool).__name__}"
    assert pool.is_master is True
    assert pool.service_name == config.REDIS_SENTINEL_MASTER_NAME
    # and it can actually reach the master
    assert conn.ping() is True


async def test_sync_mutex_lock_write():
    init_redis_connection_pool()
    lock = get_redis_mutex_lock(f"{TAG}-sync-lock", expire=10, auto_renewal=False)
    assert lock.acquire(blocking=True) is True
    try:
        get_redis_connection().set(get_redis_top_node(TENANT, f"{TAG}-sync-guarded"), "written-under-sync-lock")
    finally:
        lock.release()


async def test_sync_semaphore_limit():
    init_redis_connection_pool()
    a = Semaphore(f"{TAG}-sync-sem", limit=2, acquire_timeout=3, lock_ttl=15)
    b = Semaphore(f"{TAG}-sync-sem", limit=2, acquire_timeout=3, lock_ttl=15)
    ha, hb = a.acquire_blocking(), b.acquire_blocking()
    try:
        assert ha and hb  # both slots acquired through the Sentinel master
    finally:
        a.release_lock()
        b.release_lock()


@pytest.mark.skipif(
    os.environ.get("REDISKIT_SENTINEL_FAILOVER") not in ("1", "true", "TRUE"),
    reason="opt-in: set REDISKIT_SENTINEL_FAILOVER=1 and kill the master mid-run to verify transparent recovery",
)
async def test_failover_transparent_recovery():
    """Continuous real writes for ~45s. Kill the master mid-run (docker stop) and
    assert the client re-resolves the promoted replica and fully recovers — for
    writes AND for a pub/sub subscriber (whose connection must re-attach to the
    new master and keep receiving)."""
    conn = get_async_redis_connection()
    channel = f"{TAG}-fo-chan"

    sub = await subscribe_channel(channel)
    received: list = []

    async def _collect():
        async for message in sub:
            received.append(message)

    collector = asyncio.create_task(_collect())
    await asyncio.sleep(0.3)  # let SUBSCRIBE register

    results: list[bool] = []
    start = time.monotonic()
    n = 0
    while time.monotonic() - start < 45:
        n += 1
        try:
            lock = get_async_redis_mutex_lock(f"{TAG}-fo", expire=5, blocking_timeout=3)
            async with lock:
                await conn.set(get_redis_top_node(TENANT, f"{TAG}-fo-key"), str(n))
            await counter(TENANT, "fo", 1, key=f"{TAG}-fo-cnt")
            await publish(channel, {"n": n})
            results.append(True)
        except Exception:
            results.append(False)
        await asyncio.sleep(0.4)

    tail = results[-10:]
    assert len(tail) == 10 and all(tail), f"did not recover: last 10 ops = {tail}"

    # Messages published while no master was reachable are lost by design
    # (pub/sub is fire-and-forget); recovery means FRESH publishes arrive again.
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        await publish(channel, {"final": True})
        await asyncio.sleep(0.5)
        if any(isinstance(m, dict) and m.get("final") for m in received):
            break
    assert any(isinstance(m, dict) and m.get("final") for m in received), "pub/sub subscriber did not recover after failover"

    collector.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await collector
    await sub.aclose()
