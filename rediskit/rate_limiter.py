import asyncio
import uuid
from collections.abc import Awaitable
from typing import cast

import redis.asyncio as redis_async
from redis import RedisError

from rediskit import config
from rediskit.redis.a_client import get_async_redis_connection

_TRY_ACQUIRE_LUA = """
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local window = tonumber(ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - window)
if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[2]) then
  redis.call('ZADD', KEYS[1], now, ARGV[4])
  redis.call('PEXPIRE', KEYS[1], window)
  return 0
end
local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
local wait = math.max(tonumber(oldest[2]) + window - now, tonumber(ARGV[3]))
if #KEYS == 1 then
  return wait
end
local step = tonumber(ARGV[5])
local turn = math.max(now + wait, tonumber(redis.call('GET', KEYS[2]) or 0) + step)
redis.call('SET', KEYS[2], turn, 'PX', turn - now + step)
return turn - now
"""

_COUNT_LUA = """
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - tonumber(ARGV[1]))
return redis.call('ZCARD', KEYS[1])
"""


class AsyncRateLimiter:
    """At most `limit` acquisitions per sliding `window_ms` on one key, atomic across processes.

    The window is a sorted set of acquisition times kept on the Redis server clock, so every
    process sees the same count and a slot frees the moment the oldest acquisition ages out.
    Two limiters on the same key with different limits share one window: the lower limit leaves
    room for the higher one. The key expires on its own once the window is empty.

    When the window is full, `try_acquire()` tells every caller the same wait. `take_turn()` instead
    gives each caller its own turn, one `window_ms / limit` after the turn handed out before it, so
    waiters come back one at a time and in order. A caller back at its turn calls `try_acquire()`.
    """

    def __init__(self, key: str, limit: int, window_ms: int = 60_000, redis_conn: redis_async.Redis | None = None, min_wait_ms: int = 200):
        if limit <= 0:
            raise ValueError("Limit must be positive")
        if window_ms <= 0:
            raise ValueError("Window must be positive")
        if min_wait_ms <= 0:
            raise ValueError("Minimum wait must be positive")
        self.key = f"{config.REDIS_TOP_NODE}:{key}"
        self.limit = int(limit)
        self.window_ms = int(window_ms)
        self.min_wait_ms = int(min_wait_ms)
        self._redis_conn = redis_conn

    @property
    def redis_conn(self) -> redis_async.Redis:
        return self._redis_conn if self._redis_conn is not None else get_async_redis_connection()

    @property
    def line_key(self) -> str:
        return f"{self.key}:line"

    async def try_acquire(self) -> int:
        """Take a slot and return 0, or return the milliseconds until the oldest acquisition leaves the window."""
        return await self._try_acquire(self.key)

    async def take_turn(self) -> int:
        """Take a slot and return 0, or take the next place in line and return the milliseconds until that turn."""
        return await self._try_acquire(self.key, self.line_key)

    async def _try_acquire(self, *keys: str) -> int:
        try:
            return int(
                await cast(
                    Awaitable[int],
                    self.redis_conn.eval(
                        _TRY_ACQUIRE_LUA, len(keys), *keys, self.window_ms, self.limit, self.min_wait_ms, str(uuid.uuid4()), self.window_ms // self.limit
                    ),
                )
            )
        except RedisError as e:
            raise RuntimeError(f"Failed to acquire rate limit slot on {self.key}: {e}") from e

    async def acquire(self, timeout: float | None = None) -> None:
        """Wait until a slot is taken; raise TimeoutError if `timeout` seconds pass first."""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            wait_ms = await self.try_acquire()
            if wait_ms == 0:
                return
            if deadline is not None and loop.time() + wait_ms / 1000 > deadline:
                raise TimeoutError(f"No rate limit slot on {self.key} within {timeout}s ({self.limit} per {self.window_ms}ms)")
            await asyncio.sleep(wait_ms / 1000)

    async def count(self) -> int:
        """Acquisitions currently inside the window."""
        try:
            return int(await cast(Awaitable[int], self.redis_conn.eval(_COUNT_LUA, 1, self.key, self.window_ms)))
        except RedisError as e:
            raise RuntimeError(f"Failed to count rate limit slots on {self.key}: {e}") from e
