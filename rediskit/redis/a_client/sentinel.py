"""Async Redis Sentinel support.

Builds a Sentinel-managed *master* client that automatically follows failovers.

Why this exists
---------------
A plain ``redis.asyncio.Redis`` connects to a fixed host and pools TCP
connections. When Redis Sentinel promotes a new master, Redis does **not** drop
the existing client connections to the demoted node — it just starts rejecting
writes on them with ``ReadOnlyError: You can't write against a read only
replica``. A non-Sentinel client never learns the master moved and keeps writing
to the now read-only node until it is restarted.

A Sentinel-managed client instead asks Sentinel "who is the master?" on every
(re)connect. Combined with a per-command retry whose error set includes
``ReadOnlyError``, a command that lands on a stale/demoted node disconnects and
retries, re-resolving the *current* master — so a failover is transparent to the
caller (a slightly slower write, not a failed one).

The returned object is an ordinary ``redis.asyncio.Redis`` instance, so every
rediskit capability (locks, pub/sub, memoize, semaphores, counters, all ops)
works through it unchanged.
"""

from __future__ import annotations

import logging

from redis import asyncio as redis_async
from redis.asyncio.connection import BlockingConnectionPool
from redis.asyncio.retry import Retry
from redis.asyncio.sentinel import Sentinel, SentinelConnectionPool
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ReadOnlyError
from redis.exceptions import TimeoutError as RedisTimeoutError

from rediskit import config

log = logging.getLogger(__name__)


class SentinelBlockingConnectionPool(SentinelConnectionPool, BlockingConnectionPool):
    """Sentinel-managed pool that BLOCKS (up to ``timeout``) when exhausted.

    ``Sentinel.master_for`` defaults to a plain ``SentinelConnectionPool`` which
    raises ``ConnectionError("Too many connections")`` the moment the pool is
    saturated. The non-Sentinel client rediskit builds uses a
    ``BlockingConnectionPool`` that instead waits for a connection to free up —
    behaviour the mutex-lock and semaphore primitives depend on under contention.

    In redis-py 8 ``BlockingConnectionPool`` shares ``ConnectionPool``'s internal
    structures (it only adds a condition-variable wait around
    ``get_available_connection``), so mixing it with ``SentinelConnectionPool``
    (which adds master discovery via ``SentinelManagedConnection``) composes
    cleanly: MRO takes ``get_connection``/``release`` from the blocking pool and
    ``get_master_address``/``rotate_slaves`` from the sentinel pool.
    """


# Errors that trigger a transparent retry of a single command.
#
# ReadOnlyError is the important one here: it means we are talking to a node that
# used to be the master but was demoted to a read-only replica during a failover.
# redis-py's SentinelManagedConnection disconnects on ReadOnlyError, so retrying
# forces a fresh connection whose address is re-resolved from Sentinel — i.e. the
# new master. ConnectionError/TimeoutError cover the promotion window where no
# master is reachable yet.
SENTINEL_RETRY_ERRORS: list[type[Exception]] = [RedisConnectionError, RedisTimeoutError, ReadOnlyError]


def parse_sentinel_hosts(raw: str, default_port: int) -> list[tuple[str, int]]:
    """Parse ``"host-a:26379, host-b, host-c:5000"`` into ``[(host, port), ...]``.

    A bare host (no ``:port``) uses ``default_port``. Blank entries are ignored.
    """
    hosts: list[tuple[str, int]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        host, sep, port = item.rpartition(":")
        if sep and host and port.isdigit():
            hosts.append((host, int(port)))
        else:
            hosts.append((item, default_port))
    return hosts


def build_sentinel_master_client(
    *,
    password: str = config.REDIS_PASSWORD,
    decode_responses: bool = True,
    socket_timeout: float = 10,
    socket_connect_timeout: float = 5,
    socket_keepalive: bool = True,
    health_check_interval: float = 30,
    max_connections: int = 15,
    timeout: float = 5,
    retry_attempts: int | None = None,
    client_name: str = "rediskit",
) -> redis_async.Redis:
    """Return a ``redis.asyncio.Redis`` bound to the current Sentinel master.

    Reads sentinel endpoints / master name / sentinel auth from ``config``. The
    ``password`` argument authenticates the *data nodes* (master/replica) and is
    kept separate from the sentinel password so the data password is never sent
    to the (typically auth-less) sentinels.
    """
    sentinel_hosts = parse_sentinel_hosts(config.REDIS_SENTINEL_HOSTS, config.REDIS_SENTINEL_PORT)
    if not sentinel_hosts:
        raise ValueError(
            "REDIS_SENTINEL_ENABLED is true but REDIS_SENTINEL_HOSTS is empty. "
            "Set REDIS_SENTINEL_HOSTS, e.g. 'redis-sentinel-sentinel.redis.svc.cluster.local:26379'."
        )

    if retry_attempts is None:
        retry_attempts = config.REDIS_KIT_RETRY_ATTEMPTS

    # Connection args used to reach the sentinels themselves. Only send a
    # password when one is configured — sentinels usually have none, and sending
    # AUTH to a no-auth server is an error.
    sentinel_kwargs: dict = {
        "socket_timeout": socket_timeout,
        "socket_connect_timeout": socket_connect_timeout,
        "socket_keepalive": socket_keepalive,
    }
    if config.REDIS_SENTINEL_PASSWORD:
        sentinel_kwargs["password"] = config.REDIS_SENTINEL_PASSWORD

    sentinel = Sentinel(sentinel_hosts, sentinel_kwargs=sentinel_kwargs)

    # Per-command retry. call_with_retry runs at the connection level; on failure
    # the connection is disconnected and re-established, and a
    # SentinelManagedConnection re-resolves the master on reconnect.
    retry = Retry(ExponentialBackoff(cap=1.0, base=0.05), retries=retry_attempts)

    log.info(
        "Building Sentinel-managed Redis master client: master=%s sentinels=%s retries=%s",
        config.REDIS_SENTINEL_MASTER_NAME,
        sentinel_hosts,
        retry_attempts,
    )

    # master_for() merges these into the pool's connection kwargs, so retry /
    # retry_on_error land on every managed connection. connection_pool_class picks
    # the blocking pool so an exhausted pool waits up to ``timeout`` for a free
    # connection instead of raising "Too many connections" (parity with the
    # non-Sentinel BlockingConnectionPool the direct client uses).
    return sentinel.master_for(
        config.REDIS_SENTINEL_MASTER_NAME,
        connection_pool_class=SentinelBlockingConnectionPool,
        password=password,
        decode_responses=decode_responses,
        socket_timeout=socket_timeout,
        socket_connect_timeout=socket_connect_timeout,
        socket_keepalive=socket_keepalive,
        health_check_interval=health_check_interval,
        max_connections=max_connections,
        timeout=timeout,
        client_name=client_name,
        retry=retry,
        retry_on_error=SENTINEL_RETRY_ERRORS,
    )
