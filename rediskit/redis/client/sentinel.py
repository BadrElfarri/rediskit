"""Sync Redis Sentinel support — twin of ``rediskit.redis.a_client.sentinel``.

Builds a Sentinel-managed connection pool for the synchronous client so the
sync mutex lock, semaphore, and every ``rediskit.redis.client`` operation
follow a failover instead of staying pinned to a demoted, read-only node.

The mechanics are identical to the async side (see that module's docstring):
each connection asks Sentinel for the current master when it (re)connects, and
a per-command retry turns the post-failover ``ReadOnlyError`` (surfaced by
redis-py as ``ConnectionError("The previous master is now a slave")``) into a
transparent reconnect against the newly promoted master.

The sync pool mirrors the existing non-Sentinel sync setup: a plain
(non-blocking) pool shared by cheap ``Redis`` wrapper instances, unbounded
``max_connections`` — only the connection class and address resolution change.
"""

from __future__ import annotations

import contextlib
import logging

from redis import ConnectionPool
from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from redis.sentinel import Sentinel, SentinelConnectionPool

from rediskit import config
from rediskit.redis.sentinel_common import SENTINEL_RETRY_ERRORS, parse_sentinel_hosts, resolve_sentinel_hosts

__all__ = (
    "SENTINEL_RETRY_ERRORS",
    "build_sentinel_master_pool",
    "close_sentinel_monitor_clients",
    "parse_sentinel_hosts",
)

log = logging.getLogger(__name__)


def build_sentinel_master_pool(
    *,
    password: str = config.REDIS_PASSWORD,
    decode_responses: bool = True,
    socket_timeout: float = 10,
    socket_connect_timeout: float = 5,
    socket_keepalive: bool = True,
    health_check_interval: float = 30,
    retry_attempts: int | None = None,
    client_name: str = "rediskit",
    sentinel_socket_timeout: float = 2,
    sentinel_socket_connect_timeout: float = 2,
) -> SentinelConnectionPool:
    """Return a sync ``SentinelConnectionPool`` bound to the current master.

    Reads sentinel endpoints / master name / sentinel auth from ``config``. The
    ``password`` argument authenticates the *data nodes* (master/replica) and is
    kept separate from the sentinel password so the data password is never sent
    to the (typically auth-less) sentinels.

    Wrap it with ``Redis(connection_pool=pool)`` — exactly what
    ``rediskit.redis.client.connection.get_redis_connection`` does. When tearing
    the pool down, also call :func:`close_sentinel_monitor_clients`.
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

    # Per-command retry, carried by every managed connection (redis-py runs
    # call_with_retry at the connection level). On failure the connection is
    # disconnected and re-established, and a SentinelManagedConnection
    # re-resolves the master on reconnect.
    retry = Retry(ExponentialBackoff(cap=1.0, base=0.05), retries=retry_attempts)

    log.info(
        "Building Sentinel-managed Redis master pool (sync): master=%s sentinels=%s retries=%s",
        config.REDIS_SENTINEL_MASTER_NAME,
        sentinel_hosts,
        retry_attempts,
    )

    # Constructing the pool directly is exactly what Sentinel.master_for() does
    # internally; building it here keeps the sync module's global-pool
    # architecture (one shared pool, throwaway Redis wrappers) unchanged.
    return SentinelConnectionPool(
        config.REDIS_SENTINEL_MASTER_NAME,
        sentinel,
        is_master=True,
        password=password,
        decode_responses=decode_responses,
        socket_timeout=socket_timeout,
        socket_connect_timeout=socket_connect_timeout,
        socket_keepalive=socket_keepalive,
        health_check_interval=health_check_interval,
        client_name=client_name,
        retry=retry,
        retry_on_error=list(SENTINEL_RETRY_ERRORS),
    )


def close_sentinel_monitor_clients(pool: ConnectionPool) -> None:
    """Close the per-sentinel monitor clients behind a Sentinel-managed pool.

    ``pool.disconnect()`` only tears down the data-node connections; the
    ``Sentinel`` manager's own Redis clients (one per sentinel endpoint) keep
    their sockets open after the first master discovery. No-op for plain pools.
    """
    manager = getattr(pool, "sentinel_manager", None)
    if manager is not None:
        for sentinel_client in manager.sentinels:
            with contextlib.suppress(Exception):
                sentinel_client.close()
