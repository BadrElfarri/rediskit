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
(re)connect. Combined with a per-command retry, a command that lands on a
stale/demoted node disconnects and retries against the re-resolved *current*
master — so a failover is transparent to the caller (a slightly slower write,
not a failed one). Mechanically: the demoted node answers ``ReadOnlyError``,
redis-py's ``SentinelManagedConnection`` catches it, disconnects, and re-raises
``ConnectionError("The previous master is now a slave")`` — that retryable
error is what the per-command retry actually consumes.

The returned object is an ordinary ``redis.asyncio.Redis`` instance, so every
rediskit capability (locks, pub/sub, memoize, semaphores, counters, all ops)
works through it unchanged.

The sync twin lives in ``rediskit.redis.client.sentinel``.
"""

from __future__ import annotations

import contextlib
import logging

from redis import asyncio as redis_async
from redis.asyncio.connection import BlockingConnectionPool
from redis.asyncio.retry import Retry
from redis.asyncio.sentinel import Sentinel, SentinelConnectionPool
from redis.backoff import ExponentialBackoff

from rediskit import config
from rediskit.redis.sentinel_common import SENTINEL_RETRY_ERRORS, parse_sentinel_hosts, resolve_sentinel_hosts

__all__ = (
    "SENTINEL_RETRY_ERRORS",
    "SentinelBlockingConnectionPool",
    "aclose_sentinel_master_client",
    "build_sentinel_master_client",
    "parse_sentinel_hosts",
)

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
    ``tests/test_sentinel_async.py`` pins that MRO resolution against future
    redis-py versions.
    """


def build_sentinel_master_client(
    *,
    password: str = config.REDIS_PASSWORD,
    decode_responses: bool = True,
    socket_timeout: float = 10,
    socket_connect_timeout: float = 5,
    socket_keepalive: bool = True,
    health_check_interval: float = 30,
    max_connections: int = 15,
    timeout: float = 20,
    retry_attempts: int | None = None,
    client_name: str = "rediskit",
    sentinel_socket_timeout: float = 2,
    sentinel_socket_connect_timeout: float = 2,
) -> redis_async.Redis:
    """Return a ``redis.asyncio.Redis`` bound to the current Sentinel master.

    Reads sentinel endpoints / master name / sentinel auth from ``config``. The
    ``password`` argument authenticates the *data nodes* (master/replica) and is
    kept separate from the sentinel password so the data password is never sent
    to the (typically auth-less) sentinels.

    ``timeout`` is the blocking-pool wait for a free connection. It defaults to
    20s, deliberately *longer* than the ~16.6s failover retry budget: pool
    exhaustion is raised before the per-command retry starts and is therefore
    NOT retried, and during a failover in-flight commands hold their connections
    for the whole retry budget — a shorter wait would surface un-retried
    ConnectionErrors to concurrent callers mid-failover.

    Close the returned client with :func:`aclose_sentinel_master_client`;
    ``client.aclose()`` alone leaks the sentinel monitor connections.
    """
    sentinel_hosts = resolve_sentinel_hosts()

    if retry_attempts is None:
        retry_attempts = config.REDIS_KIT_RETRY_ATTEMPTS

    # Connection args used to reach the sentinels themselves. Only send a
    # password when one is configured — sentinels usually have none, and sending
    # AUTH to a no-auth server is an error.
    #
    # The monitor clients get a deliberately fast-fail retry and short timeouts:
    # redis-py's default (10 retries with backoff) would let one dead or
    # black-holed sentinel stall master discovery for ~60s — during a failover,
    # exactly when the dead node may also have hosted a sentinel. Failover speed
    # comes from moving on to the NEXT sentinel (discover_master tries them in
    # order), not from hammering an unreachable one.
    sentinel_kwargs: dict = {
        "socket_timeout": sentinel_socket_timeout,
        "socket_connect_timeout": sentinel_socket_connect_timeout,
        "socket_keepalive": socket_keepalive,
        "retry": Retry(ExponentialBackoff(cap=0.2, base=0.05), retries=1),
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
        retry_on_error=list(SENTINEL_RETRY_ERRORS),
    )


async def aclose_sentinel_master_client(client: redis_async.Redis) -> None:
    """Close a Sentinel-managed client *and* its sentinel monitor clients.

    ``client.aclose()`` closes the managed connection pool (the client owns it
    via ``from_pool``) but not the ``Sentinel`` manager's own per-endpoint Redis
    clients, each of which holds an open connection after the first master
    discovery — closing only the client leaks those sockets. Safe to call on a
    non-Sentinel client too, where it is equivalent to ``aclose()``.
    """
    pool = client.connection_pool
    try:
        await client.aclose()
    finally:
        manager = getattr(pool, "sentinel_manager", None)
        if manager is not None:
            for sentinel_client in manager.sentinels:
                with contextlib.suppress(Exception):
                    await sentinel_client.aclose()
