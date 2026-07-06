# Changelog

## 0.0.43

### Added

- **Async Redis Sentinel support** for high-availability master discovery. When
  `REDIS_SENTINEL_ENABLED=true`, the async client is built through Redis Sentinel
  (`sentinel.master_for(...)`) so it re-resolves the current master on every
  reconnect and follows failovers automatically, instead of pinning to a node
  that was demoted to a read-only replica (the
  `ReadOnlyError: You can't write against a read only replica` failure after a
  Sentinel failover).
  - Per-command retry (`REDIS_KIT_RETRY_ATTEMPTS`, default 10) whose error set
    includes `ReadOnlyError`, `ConnectionError`, and `TimeoutError`, so a command
    that lands on a stale/demoted node disconnects and retries against the newly
    resolved master — a failover is transparent to callers.
  - Opt-in and backwards compatible: with `REDIS_SENTINEL_ENABLED` unset/false,
    behaviour is unchanged. No code change is needed in consumers — every
    capability built on the shared async client (locks, pub/sub, memoize,
    semaphores, counters, all ops) routes through the Sentinel-managed master.
  - New settings: `REDIS_SENTINEL_ENABLED`, `REDIS_SENTINEL_HOSTS`,
    `REDIS_SENTINEL_PORT`, `REDIS_SENTINEL_MASTER_NAME`,
    `REDIS_SENTINEL_PASSWORD`, `REDIS_KIT_RETRY_ATTEMPTS`.
  - New public helpers: `rediskit.a_client.build_sentinel_master_client` and
    `rediskit.a_client.parse_sentinel_hosts`.

The synchronous client is intentionally unaffected by these settings.
