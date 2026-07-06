"""Tests for async Redis Sentinel support.

These are wiring tests: they assert the Sentinel-managed client is constructed
with the right pool, master name, and — critically — a per-command retry whose
error set includes ``ReadOnlyError`` (so a failover is transparent). They do not
require a live Sentinel because redis-py builds the client lazily; no connection
is made until a command runs.
"""

import pytest
import redis.asyncio as redis_async
from redis.asyncio.sentinel import SentinelConnectionPool
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ReadOnlyError
from redis.exceptions import TimeoutError as RedisTimeoutError

from rediskit import config
from rediskit.redis.a_client import redis_in_eventloop as loop_redis
from rediskit.redis.a_client.sentinel import (
    SENTINEL_RETRY_ERRORS,
    build_sentinel_master_client,
    parse_sentinel_hosts,
)


def test_parse_sentinel_hosts_variants():
    assert parse_sentinel_hosts("a:26379,b:5000", 26379) == [("a", 26379), ("b", 5000)]
    # bare host falls back to the default port
    assert parse_sentinel_hosts("host-only", 26379) == [("host-only", 26379)]
    # whitespace and empty entries are ignored
    assert parse_sentinel_hosts("  a:1 , , b ", 26379) == [("a", 1), ("b", 26379)]
    # a single k8s Service DNS name is a valid one-entry list
    assert parse_sentinel_hosts("redis-sentinel-sentinel.redis.svc.cluster.local:26379", 26379) == [("redis-sentinel-sentinel.redis.svc.cluster.local", 26379)]
    assert parse_sentinel_hosts("", 26379) == []


def test_retry_error_set_includes_readonly():
    # ReadOnlyError is the whole point: it forces re-resolution of the master.
    assert ReadOnlyError in SENTINEL_RETRY_ERRORS
    assert RedisConnectionError in SENTINEL_RETRY_ERRORS
    assert RedisTimeoutError in SENTINEL_RETRY_ERRORS


def test_build_sentinel_master_client_wiring(monkeypatch):
    monkeypatch.setattr(config, "REDIS_SENTINEL_HOSTS", "s1:26379,s2:26379,s3:26379", raising=False)
    monkeypatch.setattr(config, "REDIS_SENTINEL_MASTER_NAME", "myMaster", raising=False)
    monkeypatch.setattr(config, "REDIS_SENTINEL_PASSWORD", "", raising=False)
    monkeypatch.setattr(config, "REDIS_KIT_RETRY_ATTEMPTS", 7, raising=False)

    client = build_sentinel_master_client(password="datapw", decode_responses=True, client_name="rediskit")

    pool = client.connection_pool
    assert isinstance(client, redis_async.Redis)
    assert isinstance(pool, SentinelConnectionPool)
    assert pool.is_master is True
    assert pool.service_name == "myMaster"
    assert len(pool.sentinel_manager.sentinels) == 3

    # A managed connection must carry the retry + ReadOnlyError set and the data
    # password / decode settings.
    conn = pool.connection_class(**pool.connection_kwargs)
    assert ReadOnlyError in conn.retry_on_error
    assert conn.retry._retries == 7
    assert conn.encoder.decode_responses is True
    assert conn.client_name == "rediskit"
    assert bool(conn.password) is True


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
