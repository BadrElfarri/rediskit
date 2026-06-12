from __future__ import annotations

import logging
import random
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Optional, cast

from redis import RedisError

from rediskit import config
from rediskit.redis.client.connection import get_redis_connection

log = logging.getLogger(__name__)

_RENEW_LUA = """
local v = redis.call('GET', KEYS[1])
if v == ARGV[1] then
  return redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
end
return 0
"""

_RELEASE_LUA = """
local v = redis.call('GET', KEYS[1])
if v == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


@dataclass
class _Lease:
    holder_id: str
    slot_key: str
    stop_event: threading.Event
    renew_thread: Optional[threading.Thread]


class Semaphore:
    def __init__(
        self,
        key: str,
        limit: int,
        acquire_timeout: int,
        lock_ttl: int | None,
        redis_conn=None,
        process_unique_id: str | None = None,
        ttl_auto_renewal: bool = True,
        backoff_initial: float = 0.5,
        backoff_max: float = 2.0,
        backoff_multiplier: float = 1.5,
    ):
        if limit <= 0:
            raise ValueError("Limit must be positive")
        if acquire_timeout <= 0:
            raise ValueError("Acquire timeout must be positive")
        if lock_ttl is not None and lock_ttl <= 0:
            raise ValueError("Lock TTL must be positive or None")

        self.redisConn = redis_conn if redis_conn else get_redis_connection()
        self.namespace = f"{config.REDIS_TOP_NODE}:{key}"
        self.limit = limit
        self.acquireTimeOut = acquire_timeout
        self.ttl: int | None = lock_ttl
        self.process_unique_id = str(uuid.uuid4()) if not process_unique_id else process_unique_id

        self.ttl_auto_renewal = bool(ttl_auto_renewal) and (self.ttl is not None)
        self._local = threading.local()

        self.backoff_initial = backoff_initial
        self.backoff_max = backoff_max
        self.backoff_multiplier = backoff_multiplier

    # ---- internal helpers ----

    def _slot_key(self, i: int) -> str:
        return f"{self.namespace}:slot:{i}"

    def _get_lease(self) -> Optional[_Lease]:
        return getattr(self._local, "lease", None)

    def _set_lease(self, lease: Optional[_Lease]) -> None:
        self._local.lease = lease

    @property
    def hashKey(self) -> str:
        lease = self._get_lease()
        return lease.slot_key if lease else f"{self.namespace}:slots"

    # ---- public API ----

    def get_active_count(self) -> int:
        try:
            keys = [self._slot_key(i) for i in range(self.limit)]
            return int(cast(int, self.redisConn.exists(*keys)))
        except RedisError as e:
            raise RuntimeError(f"Failed to get active count: {e}") from e

    def lock_limit_reached(self) -> bool:
        return self.get_active_count() >= self.limit

    def _find_slot_by_process_unique_id(self) -> tuple[str, str] | None:
        """Scan slots and return (slot_key, holder_id) for this process_unique_id, if any."""
        try:
            keys = [self._slot_key(i) for i in range(self.limit)]
            vals = cast(list, self.redisConn.mget(keys))
        except RedisError as e:
            raise RuntimeError(f"Failed to scan semaphore slots: {e}") from e
        prefix = f"{self.process_unique_id}:"
        for k, v in zip(keys, vals):
            if v is None:
                continue
            if v == self.process_unique_id or v.startswith(prefix):
                return k, v
        return None

    def is_acquired_by_process(self) -> bool:
        lease = self._get_lease()
        if lease:
            try:
                v = self.redisConn.get(lease.slot_key)
                return v == lease.holder_id
            except RedisError as e:
                raise RuntimeError(f"Failed to check semaphore ownership: {e}") from e
        # Lease-less fallback: look up slot by process_unique_id.
        # Supports cross-instance patterns where a process_unique_id is
        # sent over the wire and the receiver wants to check ownership.
        return self._find_slot_by_process_unique_id() is not None

    def get_active_process_unique_ids(self) -> set[str]:
        try:
            keys = [self._slot_key(i) for i in range(self.limit)]
            vals = cast(list, self.redisConn.mget(keys))
        except RedisError as e:
            raise RuntimeError(f"Failed to get active process ids: {e}") from e
        out: set[str] = set()
        for v in vals:
            if v is None:
                continue
            # Stored value is f"{process_unique_id}:{acquire_uuid}" — strip suffix.
            out.add(v.rsplit(":", 1)[0])
        return out

    def _renew_loop(self, lease: _Lease):
        # renew around ~80% TTL with jitter to avoid herd
        assert self.ttl is not None
        base = max(1.0, self.ttl * 0.8)
        while not lease.stop_event.is_set():
            # Use stop_event.wait so a release can interrupt us promptly
            # instead of blocking join() for up to a full sleep interval.
            if lease.stop_event.wait(base * (0.85 + random.random() * 0.3)):
                return
            try:
                ok = self.redisConn.eval(_RENEW_LUA, 1, lease.slot_key, lease.holder_id, self.ttl)
                if ok != 1:
                    log.warning("Semaphore lease lost for %s", lease.slot_key)
                    return
            except Exception as e:
                log.warning("Semaphore TTL renewal failed: %s", e)
                return

    def _start_ttl_renewal(self, lease: _Lease):
        if self.ttl is None:
            raise ValueError("TTL must be set to start TTL renewal")

        lease.stop_event.clear()
        t = threading.Thread(target=self._renew_loop, args=(lease,), daemon=True)
        lease.renew_thread = t
        t.start()

    def _stop_ttl_renewal(self, lease: _Lease):
        if self.ttl is None:
            raise ValueError("TTL must be set to stop TTL renewal")

        lease.stop_event.set()
        if lease.renew_thread and lease.renew_thread.is_alive():
            lease.renew_thread.join(timeout=2)
        lease.renew_thread = None

    def acquire_lock(self) -> bool:
        holder_id = f"{self.process_unique_id}:{uuid.uuid4()}"  # unique per acquire
        indices = list(range(self.limit))
        random.shuffle(indices)

        try:
            for i in indices:
                k = self._slot_key(i)
                if self.ttl is None:
                    ok = self.redisConn.set(k, holder_id, nx=True)
                else:
                    ok = self.redisConn.set(k, holder_id, nx=True, ex=self.ttl)

                if ok:
                    lease = _Lease(holder_id=holder_id, slot_key=k, stop_event=threading.Event(), renew_thread=None)
                    self._set_lease(lease)

                    log.debug("Acquired semaphore slot: %s", k)

                    if self.ttl_auto_renewal:
                        self._start_ttl_renewal(lease)
                    return True

            return False
        except RedisError as e:
            raise RuntimeError(f"Failed to acquire semaphore: {e}") from e

    def acquire_blocking(self) -> str:
        if self.is_acquired_by_process():
            raise RuntimeError("Semaphore already acquired")

        end_time = time.monotonic() + self.acquireTimeOut
        backoff = self.backoff_initial

        while time.monotonic() < end_time:
            if self.acquire_lock():
                lease = self._get_lease()
                assert lease is not None
                return lease.holder_id

            sleep_time = backoff * (0.8 + random.random() * 0.4)
            time.sleep(sleep_time)

            backoff = min(backoff * self.backoff_multiplier, self.backoff_max)

        raise RuntimeError(f"Timeout: Unable to acquire semaphore '{self.namespace}', total locks holding {self.get_active_count()} out of {self.limit}")

    def release_lock(self):
        lease = self._get_lease()
        if not lease:
            # Lease-less fallback: try to release a slot owned by this
            # process_unique_id. Supports cross-instance release patterns
            # where the process_unique_id is sent over the wire.
            found = self._find_slot_by_process_unique_id()
            if found is None:
                return
            slot_key, holder_id = found
            try:
                deleted = self.redisConn.eval(_RELEASE_LUA, 1, slot_key, holder_id)
                if deleted == 1:
                    log.debug("Released semaphore slot (by process_unique_id): %s", slot_key)
                else:
                    log.warning("Semaphore slot already lost/expired: %s", slot_key)
            except RedisError as e:
                raise RuntimeError(f"Failed to release semaphore: {e}") from e
            return

        try:
            if self.ttl_auto_renewal:
                self._stop_ttl_renewal(lease)

            deleted = self.redisConn.eval(_RELEASE_LUA, 1, lease.slot_key, lease.holder_id)
            if deleted == 1:
                log.debug("Released semaphore slot: %s", lease.slot_key)
            else:
                log.warning("Semaphore slot already lost/expired: %s", lease.slot_key)
        except RedisError as e:
            raise RuntimeError(f"Failed to release semaphore: {e}") from e
        finally:
            self._set_lease(None)

    def __enter__(self):
        self.acquire_blocking()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release_lock()
