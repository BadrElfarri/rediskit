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
local fullest = redis.call('ZCARD', KEYS[1])
local newest = redis.call('ZRANGE', KEYS[1], -1, -1, 'WITHSCORES')
if newest[2] and tonumber(newest[2]) > now then
  local raw = redis.call('ZRANGEBYSCORE', KEYS[1], '(' .. (now - window), '(' .. (now + window), 'WITHSCORES')
  local s = {}
  for i = 2, #raw, 2 do
    s[#s + 1] = tonumber(raw[i])
  end
  fullest = 0
  local lo, hi = 1, 0
  for i = 0, #s do
    local edge = now
    if i > 0 then
      edge = s[i]
    end
    if edge >= now then
      while hi < #s and s[hi + 1] <= edge do
        hi = hi + 1
      end
      while lo <= hi and s[lo] <= edge - window do
        lo = lo + 1
      end
      fullest = math.max(fullest, hi - lo + 1)
    end
  end
end
if fullest < tonumber(ARGV[2]) then
  redis.call('ZADD', KEYS[1], now, ARGV[4])
  if redis.call('PTTL', KEYS[1]) < window then
    redis.call('PEXPIRE', KEYS[1], window)
  end
  return 0
end
local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
return math.max(tonumber(oldest[2]) + window - now, tonumber(ARGV[3]))
"""

_TAKE_TURN_LUA = """
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - window)
local booked = redis.call('ZSCORE', KEYS[1], ARGV[4])
if booked then
  booked = tonumber(booked)
  if booked > now then
    return booked - now
  end
  redis.call('ZREM', KEYS[1], ARGV[4])
  redis.call('ZADD', KEYS[1], booked, ARGV[5])
  return 0
end
local at = now
local newest = redis.call('ZRANGE', KEYS[1], -1, -1, 'WITHSCORES')
if newest[2] and tonumber(newest[2]) > now then
  at = tonumber(newest[2]) + tonumber(ARGV[3])
end
local nth = redis.call('ZRANGE', KEYS[1], -limit, -limit, 'WITHSCORES')
if nth[2] then
  at = math.max(at, tonumber(nth[2]) + window)
end
if at == now then
  redis.call('ZADD', KEYS[1], now, ARGV[5])
else
  redis.call('ZADD', KEYS[1], at, ARGV[4])
end
if redis.call('PTTL', KEYS[1]) < at - now + window then
  redis.call('PEXPIRE', KEYS[1], at - now + window)
end
return at - now
"""

_COUNT_LUA = """
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - tonumber(ARGV[1]))
return redis.call('ZCOUNT', KEYS[1], '-inf', now)
"""


class AsyncRateLimiter:
    """At most `limit` acquisitions per sliding `window_ms` on one key, atomic across processes.

    The window is a sorted set of acquisition times kept on the Redis server clock, so every
    process sees the same count and a slot frees the moment the oldest acquisition ages out.
    Two limiters on the same key with different limits share one window: the lower limit leaves
    room for the higher one. The key expires on its own once the window is empty.

    When the window is full, `try_acquire()` tells every caller the same wait. `take_turn()` instead
    books the caller a slot in the window: the moment the slot it needs frees, and at least
    `window_ms / limit` after the booking before it. A booked caller sends at its turn without asking
    again, and `try_acquire()` never hands out a slot that a booking needs.
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

    async def try_acquire(self) -> int:
        """Take a slot and return 0, or return the milliseconds until the oldest acquisition leaves the window."""
        return await self._eval(_TRY_ACQUIRE_LUA, self.window_ms, self.limit, self.min_wait_ms, str(uuid.uuid4()))

    async def take_turn(self, member: str | None = None) -> int:
        """Take a slot and return 0, or book the next slot in line and return the milliseconds until it.

        With a `member`, calling again returns the time left on its booking, then 0 once the turn is due.
        """
        booking = member or str(uuid.uuid4())
        return await self._eval(_TAKE_TURN_LUA, self.window_ms, self.limit, self.window_ms // self.limit, booking, str(uuid.uuid4()))

    async def release(self, member: str) -> bool:
        """Give back the booking of a `member` that will not send; False if it has none."""
        try:
            return bool(await self.redis_conn.zrem(self.key, member))
        except RedisError as e:
            raise RuntimeError(f"Failed to release rate limit booking on {self.key}: {e}") from e

    async def _eval(self, script: str, *args: str | int) -> int:
        try:
            return int(await cast(Awaitable[int], self.redis_conn.eval(script, 1, self.key, *args)))
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
        """Acquisitions currently inside the window, not counting bookings still to come."""
        try:
            return int(await cast(Awaitable[int], self.redis_conn.eval(_COUNT_LUA, 1, self.key, self.window_ms)))
        except RedisError as e:
            raise RuntimeError(f"Failed to count rate limit slots on {self.key}: {e}") from e
