"""Tests for async Redis Sentinel support.

These are wiring tests: they assert the Sentinel-managed client is constructed
with the right pool, master name, and — critically — a per-command retry whose
error set includes ``ReadOnlyError`` (so a failover is transparent). They do not
require a live Sentinel because redis-py builds the client lazily; no connection
is made until a command runs.
"""

import pytest
import redis.asyncio as redis_async
from redis.asyncio.connection import BlockingConnectionPool
from redis.asyncio.sentinel import SentinelConnectionPool
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ReadOnlyError
from redis.exceptions import TimeoutError as RedisTimeoutError

from rediskit import config
from rediskit.redis.a_client import redis_in_eventloop as loop_redis
from rediskit.redis.a_client.sentinel import (
    SENTINEL_RETRY_ERRORS,
    SentinelBlockingConnectionPool,
    aclose_sentinel_master_client,
    build_sentinel_master_client,
    parse_sentinel_hosts,
)


def test_parse_sentinel_hosts_variants():
    assert parse_sentinel_hosts("a:26379,b:5000", 26379) == [("a", 26379), ("b", 5000)]
    # bare host falls back to the default port
    assert parse_sentinel_hosts("host-only", 26379) == [("host-only", 26379)]
    # a trailing colon also falls back (and never leaks ":" into the hostname)
    assert parse_sentinel_hosts("host:", 26379) == [("host", 26379)]
    # whitespace and empty entries are ignored
    assert parse_sentinel_hosts("  a:1 , , b ", 26379) == [("a", 1), ("b", 26379)]
    # a single k8s Service DNS name is a valid one-entry list
    assert parse_sentinel_hosts("redis-sentinel-sentinel.redis.svc.cluster.local:26379", 26379) == [("redis-sentinel-sentinel.redis.svc.cluster.local", 26379)]
    assert parse_sentinel_hosts("", 26379) == []


def test_parse_sentinel_hosts_rejects_malformed_entries():
    # A misconfigured endpoint must fail loudly at build time, not produce a
    # hostname that can never resolve.
    with pytest.raises(ValueError, match="port"):
        parse_sentinel_hosts("host:notaport", 26379)
    with pytest.raises(ValueError, match="port"):
        parse_sentinel_hosts("host:0", 26379)
    with pytest.raises(ValueError, match="port"):
        parse_sentinel_hosts("host:70000", 26379)
    with pytest.raises(ValueError, match="host"):
        parse_sentinel_hosts(":26379", 26379)


def test_retry_error_set_includes_readonly():
    # ReadOnlyError is the whole point: it forces re-resolution of the master.
    assert ReadOnlyError in SENTINEL_RETRY_ERRORS
    assert RedisConnectionError in SENTINEL_RETRY_ERRORS
    assert RedisTimeoutError in SENTINEL_RETRY_ERRORS


def test_blocking_pool_mro_is_pinned():
    # isinstance checks below can't catch this: if a future redis-py adds
    # get_connection/release overrides to SentinelConnectionPool, the MRO would
    # silently pick the NON-blocking implementations and an exhausted pool
    # would raise "Too many connections" again. Pin the resolution.
    assert SentinelBlockingConnectionPool.get_connection is BlockingConnectionPool.get_connection
    assert SentinelBlockingConnectionPool.release is BlockingConnectionPool.release


def test_build_sentinel_master_client_wiring(monkeypatch):
    monkeypatch.setattr(config, "REDIS_SENTINEL_HOSTS", "s1:26379,s2:26379,s3:26379", raising=False)
    monkeypatch.setattr(config, "REDIS_SENTINEL_MASTER_NAME", "myMaster", raising=False)
    monkeypatch.setattr(config, "REDIS_SENTINEL_PASSWORD", "", raising=False)
    monkeypatch.setattr(config, "REDIS_KIT_RETRY_ATTEMPTS", 7, raising=False)

    client = build_sentinel_master_client(password="datapw", decode_responses=True, client_name="rediskit")

    pool = client.connection_pool
    assert isinstance(client, redis_async.Redis)
    assert isinstance(pool, SentinelConnectionPool)
    # Must also be a BLOCKING pool: under contention (locks/semaphores) it waits
    # for a free connection up to `timeout` instead of raising "Too many
    # connections" — parity with the non-Sentinel client. Regression guard.
    assert isinstance(pool, BlockingConnectionPool)
    # Pool wait must exceed the ~16.6s failover retry budget: exhaustion is
    # raised before the per-command retry starts, so it is never retried.
    assert pool.timeout == 20
    assert pool.is_master is True
    assert pool.service_name == "myMaster"
    assert len(pool.sentinel_manager.sentinels) == 3

    # A managed connection must carry the retry + ReadOnlyError set and the data
    # password / decode settings.
    conn = pool.connection_class(**pool.connection_kwargs)
    assert ReadOnlyError in conn.retry_on_error
    assert conn.retry.get_retries() == 7
    assert conn.encoder.decode_responses is True
    assert conn.client_name == "rediskit"
    assert bool(conn.password) is True
    # the module-level error set must never be handed out by reference
    assert pool.connection_kwargs["retry_on_error"] is not SENTINEL_RETRY_ERRORS
    assert pool.connection_kwargs["retry_on_error"] == list(SENTINEL_RETRY_ERRORS)


def test_sentinel_monitor_clients_fail_fast(monkeypatch):
    # The monitor clients must NOT inherit redis-py's default retry (10
    # attempts with backoff): discover_master() tries sentinels in order, and a
    # dead/black-holed sentinel would stall discovery for ~60s mid-failover.
    # Fast-fail here; failover speed comes from trying the NEXT sentinel.
    monkeypatch.setattr(config, "REDIS_SENTINEL_HOSTS", "s1:26379,s2:26379", raising=False)
    monkeypatch.setattr(config, "REDIS_SENTINEL_PASSWORD", "", raising=False)

    client = build_sentinel_master_client()

    monitors = client.connection_pool.sentinel_manager.sentinels
    assert len(monitors) == 2
    for monitor in monitors:
        assert monitor.get_retry().get_retries() <= 1
        monitor_kwargs = monitor.connection_pool.connection_kwargs
        assert monitor_kwargs["socket_connect_timeout"] == 2
        assert monitor_kwargs["socket_timeout"] == 2


def test_build_sentinel_master_client_forwards_sentinel_password(monkeypatch):
    monkeypatch.setattr(config, "REDIS_SENTINEL_HOSTS", "s1:26379", raising=False)
    monkeypatch.setattr(config, "REDIS_SENTINEL_PASSWORD", "sentinel-pw", raising=False)

    client = build_sentinel_master_client(password="datapw")
    # sentinel-auth is kept separate from the data-node password
    sentinel_conn_kwargs = client.connection_pool.sentinel_manager.sentinel_kwargs
    assert sentinel_conn_kwargs.get("password") == "sentinel-pw"


def test_build_sentinel_master_client_requires_hosts(monkeypatch):
    monkeypatch.setattr(config, "REDIS_SENTINEL_HOSTS", "", raising=False)
    with pytest.raises(ValueError, match="REDIS_SENTINEL_HOSTS"):
        build_sentinel_master_client()


@pytest.mark.asyncio
async def test_aclose_closes_sentinel_monitor_clients(monkeypatch):
    # client.aclose() alone leaks the per-sentinel monitor connections; the
    # helper must close every one of them.
    monkeypatch.setattr(config, "REDIS_SENTINEL_HOSTS", "s1:26379,s2:26379", raising=False)
    monkeypatch.setattr(config, "REDIS_SENTINEL_PASSWORD", "", raising=False)

    client = build_sentinel_master_client()
    closed: list = []
    for monitor in client.connection_pool.sentinel_manager.sentinels:

        async def _record_aclose(monitor=monitor):
            closed.append(monitor)

        monkeypatch.setattr(monitor, "aclose", _record_aclose, raising=True)

    await aclose_sentinel_master_client(client)
    assert len(closed) == 2


@pytest.mark.asyncio
async def test_aclose_is_safe_on_plain_client():
    pool = redis_async.ConnectionPool(host="localhost", port=1, max_connections=1)
    client = redis_async.Redis(connection_pool=pool)
    # no sentinel_manager on the pool: must behave like a plain aclose()
    await aclose_sentinel_master_client(client)
    await pool.disconnect()


@pytest.mark.asyncio
async def test_make_client_uses_sentinel_when_enabled(monkeypatch):
    sentinel_marker = object()
    called: dict = {}

    def fake_builder(**kwargs):
        called.update(kwargs)
        return sentinel_marker

    monkeypatch.setattr(config, "REDIS_SENTINEL_ENABLED", True, raising=False)
    monkeypatch.setattr(loop_redis, "build_sentinel_master_client", fake_builder, raising=True)

    result = loop_redis._make_client(password="pw", decode_responses=True)
    assert result is sentinel_marker
    # settings are forwarded to the builder
    assert called["password"] == "pw"
    assert called["decode_responses"] is True


@pytest.mark.asyncio
async def test_make_client_uses_direct_pool_when_disabled(monkeypatch):
    def boom(**kwargs):
        raise AssertionError("sentinel builder must not be called when disabled")

    monkeypatch.setattr(config, "REDIS_SENTINEL_ENABLED", False, raising=False)
    monkeypatch.setattr(loop_redis, "build_sentinel_master_client", boom, raising=True)

    result = loop_redis._make_client()
    assert isinstance(result, redis_async.Redis)
    assert not isinstance(result.connection_pool, SentinelConnectionPool)
