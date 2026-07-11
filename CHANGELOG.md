# Changelog

## 0.0.43

### Added

- **Redis Sentinel support (sync + async)** for high-availability master
  discovery. When `REDIS_SENTINEL_ENABLED=true`, both clients are built through
  Redis Sentinel so they re-resolve the current master on every reconnect and
  follow failovers automatically, instead of pinning to a node that was demoted
  to a read-only replica (the
  `ReadOnlyError: You can't write against a read only replica` failure after a
  Sentinel failover).
  - Per-command retry (`REDIS_KIT_RETRY_ATTEMPTS`, default 20 ≈ 16.6s budget)
    whose error set includes `ReadOnlyError`, `ConnectionError`, and
    `TimeoutError`, so a command that lands on a stale/demoted node disconnects
    and retries against the newly resolved master — a failover is transparent to
    callers. The budget MUST exceed the Sentinel failover window
    (`down-after-milliseconds` + promotion, up to `failover-timeout` worst case)
    or writes raise `The previous master is now a slave` mid-failover; raise the
    setting for a larger window or lower `down-after-milliseconds` to shrink it.
  - The async client uses a blocking connection pool
    (`SentinelBlockingConnectionPool`) so an exhausted pool waits up to `timeout`
    for a free connection instead of raising `Too many connections` — parity with
    the non-Sentinel `BlockingConnectionPool`, which the mutex-lock and semaphore
    primitives rely on under contention. The sync client mirrors its existing
    non-blocking pool setup through a `SentinelConnectionPool`, so the sync mutex
    lock (`get_redis_mutex_lock`), sync semaphore, and all sync ops follow
    failovers too.
  - The Sentinel *monitor* clients (used for master discovery) are configured to
    fail fast (1 retry, 2s timeouts) instead of redis-py's default 10-retry
    policy — a dead or black-holed sentinel would otherwise stall master
    discovery for ~60s in the middle of a failover. Discovery speed comes from
    trying the next sentinel, not re-hammering an unreachable one.
  - Opt-in and backwards compatible: with `REDIS_SENTINEL_ENABLED` unset/false,
    behaviour is unchanged. No code change is needed in consumers — every
    capability built on the shared clients (locks, pub/sub, memoize, semaphores,
    counters, all ops) routes through the Sentinel-managed master.
  - New settings: `REDIS_SENTINEL_ENABLED`, `REDIS_SENTINEL_HOSTS`,
    `REDIS_SENTINEL_PORT`, `REDIS_SENTINEL_MASTER_NAME`,
    `REDIS_SENTINEL_PASSWORD`, `REDIS_KIT_RETRY_ATTEMPTS`.
    `REDIS_SENTINEL_HOSTS` has no default: enabling Sentinel without setting it
    raises at client build time instead of silently targeting a guessed
    hostname. Malformed endpoint entries (empty host, non-numeric or
    out-of-range port) also raise instead of producing unresolvable addresses.
  - New public helpers: `rediskit.redis.a_client.build_sentinel_master_client`,
    `rediskit.redis.a_client.aclose_sentinel_master_client` (closes the client
    *and* the sentinel monitor connections, which a bare `aclose()` leaks),
    `rediskit.redis.client.build_sentinel_master_pool`,
    `rediskit.redis.client.close_sentinel_monitor_clients`, and
    `parse_sentinel_hosts`.
  - `docker-compose.sentinel.yml`: a real 1-master + 1-replica + 1-sentinel
    topology with an in-network `test` runner for the live integration suite,
    including an opt-in transparent-failover drill (writes and a pub/sub
    subscriber must recover after the master is killed).

### Changed

- The default connection-pool wait (`timeout`) for the shared async client is
  now **20s** (was 5s), matching redis-py's own `BlockingConnectionPool`
  default. In Sentinel mode this is load-bearing: pool exhaustion is raised
  *before* the per-command retry starts (so it is never retried), and during a
  failover in-flight commands hold their connections for the whole ~16.6s retry
  budget — a 5s wait would surface un-retried `ConnectionError`s to concurrent
  callers mid-failover. Pass `timeout=` explicitly to restore fail-fast
  behaviour.
