"""Tests for sync Redis Sentinel support.

Wiring tests, mirroring ``test_sentinel_async.py``: they assert the
Sentinel-managed pool is constructed with the right master name and a
per-command retry whose error set includes ``ReadOnlyError``, and that
``init_redis_connection_pool()`` routes through it when Sentinel is enabled.
No live Sentinel is needed — nothing connects until a command runs.
"""

import pytest
from redis import ConnectionPool, Redis
from redis.exceptions import ReadOnlyError
from redis.sentinel import SentinelConnectionPool

from rediskit import config
from rediskit.redis.client import connection as sync_connection
from rediskit.redis.client.sentinel import (
    SENTINEL_RETRY_ERRORS,
    build_sentinel_master_pool,
    close_sentinel_monitor_clients,
)


@pytest.fixture(autouse=True)
def _reset_global_pool():
    saved = sync_connection.redis_connection_pool
    yield
    sync_connection.redis_connection_pool = saved


def test_build_sentinel_master_pool_wiring(monkeypatch):
    monkeypatch.setattr(config, "REDIS_SENTINEL_HOSTS", "s1:26379,s2:26379,s3:26379", raising=False)
    monkeypatch.setattr(config, "REDIS_SENTINEL_MASTER_NAME", "myMaster", raising=False)
    monkeypatch.setattr(config, "REDIS_SENTINEL_PASSWORD", "", raising=False)
    monkeypatch.setattr(config, "REDIS_KIT_RETRY_ATTEMPTS", 7, raising=False)

    pool = build_sentinel_master_pool(password="datapw", decode_responses=True, client_name="rediskit")

    assert isinstance(pool, SentinelConnectionPool)
    assert pool.is_master is True
    assert pool.service_name == "myMaster"
    assert len(pool.sentinel_manager.sentinels) == 3

    # A managed connection must carry the retry + ReadOnlyError set and the data
    # password / decode settings — the exact wiring that makes a failover
    # transparent to the sync mutex lock and semaphore.
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
    # Same rationale as the async twin: a dead sentinel must not stall
    # discover_master() with redis-py's default 10-retry policy mid-failover.
    monkeypatch.setattr(config, "REDIS_SENTINEL_HOSTS", "s1:26379,s2:26379", raising=False)
    monkeypatch.setattr(config, "REDIS_SENTINEL_PASSWORD", "", raising=False)

    pool = build_sentinel_master_pool()

    monitors = pool.sentinel_manager.sentinels
    assert len(monitors) == 2
    for monitor in monitors:
        monitor_kwargs = monitor.connection_pool.connection_kwargs
        assert monitor_kwargs["retry"].get_retries() <= 1
        assert monitor_kwargs["socket_connect_timeout"] == 2
        assert monitor_kwargs["socket_timeout"] == 2


def test_build_sentinel_master_pool_forwards_sentinel_password(monkeypatch):
    monkeypatch.setattr(config, "REDIS_SENTINEL_HOSTS", "s1:26379", raising=False)
    monkeypatch.setattr(config, "REDIS_SENTINEL_PASSWORD", "sentinel-pw", raising=False)

    pool = build_sentinel_master_pool(password="datapw")
    # sentinel-auth is kept separate from the data-node password
    assert pool.sentinel_manager.sentinel_kwargs.get("password") == "sentinel-pw"


def test_build_sentinel_master_pool_requires_hosts(monkeypatch):
    monkeypatch.setattr(config, "REDIS_SENTINEL_HOSTS", "", raising=False)
    with pytest.raises(ValueError, match="REDIS_SENTINEL_HOSTS"):
        build_sentinel_master_pool()


def test_init_redis_connection_pool_uses_sentinel_when_enabled(monkeypatch):
    marker_pool = ConnectionPool(host="marker-host", port=1)
    called: dict = {}

    def fake_builder(**kwargs):
        called.update(kwargs)
        return marker_pool

    monkeypatch.setattr(config, "REDIS_SENTINEL_ENABLED", True, raising=False)
    monkeypatch.setattr(sync_connection, "build_sentinel_master_pool", fake_builder, raising=True)

    sync_connection.init_redis_connection_pool()
    assert sync_connection.redis_connection_pool is marker_pool
    assert called["decode_responses"] is True

    conn = sync_connection.get_redis_connection()
    assert isinstance(conn, Redis)
    assert conn.connection_pool is marker_pool


def test_init_redis_connection_pool_direct_when_disabled(monkeypatch):
    def boom(**kwargs):
        raise AssertionError("sentinel builder must not be called when disabled")

    monkeypatch.setattr(config, "REDIS_SENTINEL_ENABLED", False, raising=False)
    monkeypatch.setattr(sync_connection, "build_sentinel_master_pool", boom, raising=True)

    sync_connection.init_redis_connection_pool()
    assert isinstance(sync_connection.redis_connection_pool, ConnectionPool)
    assert not isinstance(sync_connection.redis_connection_pool, SentinelConnectionPool)


def test_close_tears_down_sentinel_monitor_clients(monkeypatch):
    # close() must also close the per-sentinel monitor clients — disconnecting
    # the data-node pool alone leaks their sockets.
    monkeypatch.setattr(config, "REDIS_SENTINEL_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "REDIS_SENTINEL_HOSTS", "s1:26379,s2:26379", raising=False)
    monkeypatch.setattr(config, "REDIS_SENTINEL_PASSWORD", "", raising=False)

    sync_connection.init_redis_connection_pool()
    pool = sync_connection.redis_connection_pool
    assert isinstance(pool, SentinelConnectionPool)

    closed: list = []
    for monitor in pool.sentinel_manager.sentinels:
        monkeypatch.setattr(monitor, "close", lambda monitor=monitor: closed.append(monitor), raising=True)

    sync_connection.close()
    assert len(closed) == 2
    assert sync_connection.redis_connection_pool is None


def test_close_sentinel_monitor_clients_noop_on_plain_pool():
    pool = ConnectionPool(host="localhost", port=1)
    close_sentinel_monitor_clients(pool)  # must not raise
