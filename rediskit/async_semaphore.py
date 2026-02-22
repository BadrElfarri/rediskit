import asyncio
import contextvars
import logging
import random
import uuid
from dataclasses import dataclass
from typing import Awaitable, Optional, Tuple, cast

import redis.asyncio as redis_async
from redis import RedisError

from rediskit import config
from rediskit.redis.a_client import get_async_redis_connection

log = logging.getLogger(__name__)

_RENEW_LUA = """
local v = redis.call('GET', KEYS[1])
if v == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
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
    renew_task: Optional[asyncio.Task]
    stop_event: asyncio.Event


class AsyncSemaphore:
    def __init__(
        self,
        key: str,
        limit: int,
        acquire_timeout: int,
        lock_ttl: int | None,
        redis_conn: redis_async.Redis | None = None,
        process_unique_id: str | None = None,
        ttl_auto_renewal: bool = True,
        backoff_initial: float = 0.5,
        backoff_max: float = 2.0,
        backoff_multiplier: float = 1.5,
        backoff_jitter: Tuple[float, float] = (0.8, 1.2),
        renew_ratio: float = 0.7,  # renew every ttl * renew_ratio
        renew_jitter: Tuple[float, float] = (0.85, 1.15),
    ):
        if limit <= 0:
            raise ValueError("Limit must be positive")
        if acquire_timeout <= 0:
            raise ValueError("Acquire timeout must be positive")
        if lock_ttl is not None and lock_ttl <= 0:
            raise ValueError("Lock TTL must be positive or None")

        self.redisConn = redis_conn if redis_conn else get_async_redis_connection()
        self.namespace = f"{config.REDIS_TOP_NODE}:{key}"
        self.limit = limit
        self.acquireTimeOut = acquire_timeout
        self.ttl = lock_ttl
        self.process_unique_id = str(uuid.uuid4()) if not process_unique_id else process_unique_id
        self.ttl_auto_renewal = bool(ttl_auto_renewal) and (self.ttl is not None)

        self.backoff_initial = float(backoff_initial)
        self.backoff_max = float(backoff_max)
        self.backoff_multiplier = float(backoff_multiplier)
        self.backoff_jitter = backoff_jitter

        self.renew_ratio = float(renew_ratio)
        self.renew_jitter = renew_jitter

        self._lease_var: contextvars.ContextVar[Optional[_Lease]] = contextvars.ContextVar(
            f"AsyncSemaphoreLease:{self.namespace}",
            default=None,
        )

    @property
    def holder_id(self) -> Optional[str]:
        lease = self._lease_var.get()
        return lease.holder_id if lease else None

    @property
    def hashKey(self) -> str:
        lease = self._lease_var.get()
        return lease.slot_key if lease else f"{self.namespace}:slots"

    def _slot_key(self, i: int) -> str:
        return f"{self.namespace}:slot:{i}"

    async def _start_ttl_renewal(self, lease: _Lease) -> None:
        if self.ttl is None:
            return
        lease.stop_event.clear()
        lease.renew_task = asyncio.create_task(self._renew_loop(lease))

    async def _stop_ttl_renewal(self, lease: _Lease) -> None:
        lease.stop_event.set()
        if lease.renew_task:
            lease.renew_task.cancel()
            try:
                await lease.renew_task
            except asyncio.CancelledError:
                pass
            lease.renew_task = None

    async def _renew_loop(self, lease: _Lease) -> None:
        assert self.ttl is not None
        ttl_ms = int(self.ttl * 1000)

        # renew before expiry; ensure interval < ttl
        base = max(0.2, self.ttl * self.renew_ratio)
        base = min(base, max(0.2, self.ttl * 0.9))  # never schedule after expiry

        try:
            while not lease.stop_event.is_set():
                await asyncio.sleep(base * random.uniform(*self.renew_jitter))

                ok = await self.redisConn.eval(_RENEW_LUA, 1, lease.slot_key, lease.holder_id, ttl_ms)
                if ok != 1:
                    log.warning("Semaphore lease lost for %s", lease.slot_key)
                    return
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.warning("Semaphore TTL renewal failed: %s", e)
            return

    async def get_active_count(self) -> int:
        try:
            keys = [self._slot_key(i) for i in range(self.limit)]
            return int(await self.redisConn.exists(*keys))
        except RedisError as e:
            raise RuntimeError(f"Failed to get active count: {e}")

    async def lock_limit_reached(self) -> bool:
        return (await self.get_active_count()) >= self.limit

    async def is_acquired_by_process(self) -> bool:
        lease = self._lease_var.get()
        if not lease:
            return False
        try:
            v = await self.redisConn.get(lease.slot_key)
            return v == lease.holder_id
        except RedisError as e:
            raise RuntimeError(f"Failed to check semaphore ownership: {e}")

    async def get_active_process_unique_ids(self) -> set[str]:
        keys = [self._slot_key(i) for i in range(self.limit)]
        try:
            vals = await cast(Awaitable[list], self.redisConn.mget(*keys))
            return {v for v in vals if v is not None}
        except RedisError as e:
            raise RuntimeError(f"Failed to get active process ids: {e}")

    async def start_ttl_renewal(self) -> None:
        if self.ttl is None:
            raise ValueError("TTL must be set to start TTL renewal")
        lease = self._lease_var.get()
        if lease:
            await self._start_ttl_renewal(lease)

    async def stop_ttl_renewal(self) -> None:
        if self.ttl is None:
            raise ValueError("TTL must be set to stop TTL renewal")
        lease = self._lease_var.get()
        if lease:
            await self._stop_ttl_renewal(lease)

    async def acquire_lock(self, holder_id: str) -> bool:
        indices = list(range(self.limit))
        random.shuffle(indices)

        try:
            for i in indices:
                k = self._slot_key(i)
                if self.ttl is None:
                    ok = await self.redisConn.set(k, holder_id, nx=True)
                else:
                    ok = await self.redisConn.set(k, holder_id, nx=True, px=int(self.ttl * 1000))

                if ok:
                    lease = _Lease(holder_id=holder_id, slot_key=k, renew_task=None, stop_event=asyncio.Event())
                    self._lease_var.set(lease)

                    if self.ttl_auto_renewal:
                        await self._start_ttl_renewal(lease)
                    return True

            return False
        except RedisError as e:
            raise RuntimeError(f"Failed to acquire semaphore: {e}")

    async def acquire_blocking(self) -> str:
        if await self.is_acquired_by_process():
            raise RuntimeError("Semaphore already acquired")

        holder_id = f"{self.process_unique_id}:{uuid.uuid4()}"

        loop = asyncio.get_running_loop()
        deadline = loop.time() + float(self.acquireTimeOut)

        backoff = self.backoff_initial
        while loop.time() < deadline:
            if await self.acquire_lock(holder_id):
                return holder_id

            await asyncio.sleep(backoff * random.uniform(*self.backoff_jitter))
            backoff = min(backoff * self.backoff_multiplier, self.backoff_max)

        raise RuntimeError(
            f"Timeout: Unable to acquire the semaphore lock {self.namespace}, total locks holding {await self.get_active_count()} out of {self.limit}"
        )

    async def release_lock(self) -> None:
        lease = self._lease_var.get()
        if not lease:
            return

        try:
            if self.ttl_auto_renewal:
                await self._stop_ttl_renewal(lease)

            await self.redisConn.eval(_RELEASE_LUA, 1, lease.slot_key, lease.holder_id)
        except RedisError as e:
            raise RuntimeError(f"Failed to release semaphore: {e}")
        finally:
            self._lease_var.set(None)

    async def __aenter__(self):
        await self.acquire_blocking()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release_lock()
