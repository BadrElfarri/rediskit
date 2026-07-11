"""Shared Redis Sentinel helpers used by both the sync and async clients.

Only transport-agnostic pieces live here: endpoint parsing, the retryable
error set, and config resolution. The actual builders live in
``rediskit.redis.client.sentinel`` (sync) and
``rediskit.redis.a_client.sentinel`` (async); they differ only in the redis-py
classes they instantiate.
"""

from __future__ import annotations

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ReadOnlyError
from redis.exceptions import TimeoutError as RedisTimeoutError

from rediskit import config

# Errors that trigger a transparent retry of a single command.
#
# During a failover a command fails one of two ways: the demoted node answers
# ReadOnlyError — which redis-py's SentinelManagedConnection catches itself,
# disconnects on, and re-raises as ConnectionError("The previous master is now
# a slave") — or no master is reachable while promotion is in flight
# (ConnectionError / TimeoutError). Retrying either forces a fresh connection
# whose address is re-resolved from Sentinel, i.e. the new master.
# ReadOnlyError stays in the set as defence in depth even though the
# translation above means the retry usually consumes a ConnectionError.
# MasterNotFoundError and BusyLoadingError are ConnectionError subclasses, so
# failed discovery and a promoted-but-still-loading master are covered too.
#
# Kept immutable on purpose; builders pass ``list(SENTINEL_RETRY_ERRORS)``
# because redis-py mutates the retry_on_error list it is handed under some
# settings (e.g. retry_on_timeout).
SENTINEL_RETRY_ERRORS: tuple[type[Exception], ...] = (RedisConnectionError, RedisTimeoutError, ReadOnlyError)


def parse_sentinel_hosts(raw: str, default_port: int) -> list[tuple[str, int]]:
    """Parse ``"host-a:26379, host-b, host-c:5000"`` into ``[(host, port), ...]``.

    A bare host (no ``:port``) or a trailing colon (``"host:"``) falls back to
    ``default_port``; blank entries are ignored. A malformed entry — empty host
    or a non-numeric / out-of-range port — raises ``ValueError`` instead of
    silently producing an endpoint that can never resolve. IPv6 literals are
    not supported; use a resolvable hostname.
    """
    hosts: list[tuple[str, int]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        host, sep, port = item.rpartition(":")
        if not sep:
            hosts.append((item, default_port))
            continue
        if not host:
            raise ValueError(f"Invalid sentinel endpoint {item!r}: missing host")
        if not port:
            hosts.append((host, default_port))
            continue
        if not port.isdigit() or not 0 < int(port) < 65536:
            raise ValueError(f"Invalid sentinel endpoint {item!r}: port must be 1-65535")
        hosts.append((host, int(port)))
    return hosts


def resolve_sentinel_hosts() -> list[tuple[str, int]]:
    """Read and validate the sentinel endpoints from config.

    Raises ``ValueError`` with setup guidance when none are configured, so a
    half-enabled deployment fails loudly at client build time instead of
    connecting somewhere unintended.
    """
    hosts = parse_sentinel_hosts(config.REDIS_SENTINEL_HOSTS, config.REDIS_SENTINEL_PORT)
    if not hosts:
        raise ValueError(
            "REDIS_SENTINEL_ENABLED is true but REDIS_SENTINEL_HOSTS is empty. "
            "Set REDIS_SENTINEL_HOSTS, e.g. 'redis-sentinel-sentinel.redis.svc.cluster.local:26379'."
        )
    return hosts
