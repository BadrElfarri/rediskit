import asyncio
import time
import uuid

import pytest
import pytest_asyncio

from rediskit import config
from rediskit.rate_limiter import AsyncRateLimiter
from rediskit.redis.a_client import get_async_redis_connection
from rediskit.redis.a_client.connection import init_async_redis_connection_pool

PREFIX = "TEST_RATE_LIMITER"


@pytest_asyncio.fixture(autouse=True)
async def clean_redis():
    await init_async_redis_connection_pool()
    conn = get_async_redis_connection()
    pattern = f"{config.REDIS_TOP_NODE}:{PREFIX}:*"
    yield
    keys = [k async for k in conn.scan_iter(match=pattern)]
    if keys:
        await conn.delete(*keys)


def limiter(limit: int, window_ms: int, key: str | None = None, **kwargs) -> AsyncRateLimiter:
    return AsyncRateLimiter(key=key or f"{PREFIX}:{uuid.uuid4()}", limit=limit, window_ms=window_ms, **kwargs)


@pytest.mark.asyncio
async def test_under_the_limit_every_call_takes_a_slot():
    lim = limiter(limit=5, window_ms=60_000)
    assert [await lim.try_acquire() for _ in range(5)] == [0, 0, 0, 0, 0]
    assert await lim.count() == 5


@pytest.mark.asyncio
async def test_at_the_limit_callers_are_told_how_long_until_the_oldest_slot_ages_out():
    lim = limiter(limit=3, window_ms=60_000)
    for _ in range(3):
        await lim.try_acquire()

    waits = [await lim.try_acquire() for _ in range(10)]

    assert all(50 <= w <= 60_000 for w in waits)
    assert waits == sorted(waits, reverse=True)
    assert await lim.count() == 3


@pytest.mark.asyncio
async def test_a_slot_frees_the_moment_the_oldest_acquisition_leaves_the_window():
    lim = limiter(limit=3, window_ms=600)

    assert await lim.try_acquire() == 0
    await asyncio.sleep(0.3)
    assert [await lim.try_acquire() for _ in range(2)] == [0, 0]

    wait_ms = await lim.try_acquire()
    assert 50 <= wait_ms <= 300
    await asyncio.sleep(wait_ms / 1000 + 0.05)

    assert await lim.try_acquire() == 0
    assert await lim.try_acquire() > 0
    assert await lim.count() == 3


@pytest.mark.asyncio
async def test_limiters_on_the_same_key_share_one_window_each_with_its_own_cap():
    key = f"{PREFIX}:{uuid.uuid4()}"
    low = limiter(limit=2, window_ms=60_000, key=key)
    high = limiter(limit=3, window_ms=60_000, key=key)

    assert [await low.try_acquire() for _ in range(2)] == [0, 0]
    assert await low.try_acquire() > 0
    assert await high.try_acquire() == 0
    assert await high.try_acquire() > 0
    assert await low.count() == await high.count() == 3


@pytest.mark.asyncio
async def test_different_keys_do_not_see_each_other():
    a, b = limiter(limit=1, window_ms=60_000), limiter(limit=1, window_ms=60_000)
    assert await a.try_acquire() == 0
    assert await b.try_acquire() == 0
    assert await a.try_acquire() > 0
    assert await b.try_acquire() > 0


@pytest.mark.asyncio
async def test_concurrent_callers_never_take_more_than_the_limit():
    key = f"{PREFIX}:{uuid.uuid4()}"
    limiters = [limiter(limit=10, window_ms=60_000, key=key) for _ in range(5)]

    results = await asyncio.gather(*(limiters[i % 5].try_acquire() for i in range(200)))

    assert results.count(0) == 10
    assert all(r >= 50 for r in results if r != 0)
    assert await limiters[0].count() == 10


@pytest.mark.asyncio
async def test_sustained_load_is_held_to_the_limit_per_window():
    lim = limiter(limit=5, window_ms=500)
    taken, refused = 0, 0
    started = time.monotonic()
    while time.monotonic() - started < 1.6:
        if await lim.try_acquire() == 0:
            taken += 1
        else:
            refused += 1
        await asyncio.sleep(0.01)

    assert refused > taken
    assert 10 <= taken <= 20


@pytest.mark.asyncio
async def test_forty_emails_at_two_per_ten_seconds_each_get_their_own_turn():
    plain, in_line = limiter(limit=2, window_ms=10_000), limiter(limit=2, window_ms=10_000)

    same_wait = [await plain.try_acquire() for _ in range(40)]
    own_turn = [await in_line.take_turn() for _ in range(40)]

    print("\nemail  try_acquire  take_turn")
    for email, (a, b) in enumerate(zip(same_wait, own_turn), start=1):
        print(f"{email:>5}  {a / 1000:>10.1f}s  {b / 1000:>8.1f}s")

    assert [round(w / 1000) for w in same_wait] == [0, 0] + [10] * 38
    assert [round(w / 1000) for w in own_turn] == [0, 0] + [10 + 5 * i for i in range(38)]
    assert await in_line.count() == 2


@pytest.mark.asyncio
async def test_callers_who_wait_for_their_turn_go_out_in_line_at_the_limit():
    lim = limiter(limit=2, window_ms=600, min_wait_ms=50)
    started = time.monotonic()

    async def send() -> tuple[int, float]:
        turn_ms = wait_ms = await lim.take_turn()
        while wait_ms:
            await asyncio.sleep(wait_ms / 1000)
            wait_ms = await lim.try_acquire()
        return turn_ms, time.monotonic() - started

    sends = sorted(await asyncio.gather(*(send() for _ in range(6))))

    assert [round(turn_ms / 100) for turn_ms, _ in sends] == [0, 0, 6, 9, 12, 15]
    assert all(turn_ms / 1000 - 0.01 <= sent_at <= turn_ms / 1000 + 0.15 for turn_ms, sent_at in sends)


@pytest.mark.asyncio
async def test_the_line_clears_itself_after_the_last_turn():
    lim = limiter(limit=1, window_ms=200)
    conn = get_async_redis_connection()

    assert await lim.take_turn() == 0
    assert await conn.exists(lim.line_key) == 0
    assert [round(await lim.take_turn() / 100) for _ in range(2)] == [2, 4]

    assert 0 < await conn.pttl(lim.line_key) <= 600
    await asyncio.sleep(0.7)
    assert await conn.exists(lim.line_key) == 0


@pytest.mark.asyncio
async def test_acquire_waits_until_a_slot_frees():
    lim = limiter(limit=1, window_ms=400)
    await lim.acquire()

    started = time.monotonic()
    await lim.acquire()

    assert 0.3 <= time.monotonic() - started <= 0.7
    assert await lim.count() == 1


@pytest.mark.asyncio
async def test_acquire_gives_up_when_the_slot_would_come_after_the_timeout():
    lim = limiter(limit=1, window_ms=60_000)
    await lim.acquire()

    started = time.monotonic()
    with pytest.raises(TimeoutError):
        await lim.acquire(timeout=0.2)

    assert time.monotonic() - started < 0.2
    assert await lim.count() == 1


@pytest.mark.asyncio
async def test_the_key_expires_on_its_own_once_the_window_has_passed():
    lim = limiter(limit=1, window_ms=300)
    await lim.try_acquire()
    conn = get_async_redis_connection()

    assert 0 < await conn.pttl(lim.key) <= 300
    await asyncio.sleep(0.4)
    assert await conn.exists(lim.key) == 0
    assert await lim.count() == 0


@pytest.mark.asyncio
async def test_the_wait_is_never_below_the_minimum():
    lim = limiter(limit=1, window_ms=100, min_wait_ms=80)
    await lim.try_acquire()
    await asyncio.sleep(0.05)
    assert await lim.try_acquire() >= 80


@pytest.mark.asyncio
async def test_an_explicit_connection_is_used():
    conn = get_async_redis_connection()
    lim = limiter(limit=1, window_ms=60_000, redis_conn=conn)
    assert lim.redis_conn is conn
    assert await lim.try_acquire() == 0
    assert await conn.zcard(lim.key) == 1


def test_bad_arguments_are_rejected():
    with pytest.raises(ValueError):
        AsyncRateLimiter(key="x", limit=0)
    with pytest.raises(ValueError):
        AsyncRateLimiter(key="x", limit=1, window_ms=0)
    with pytest.raises(ValueError):
        AsyncRateLimiter(key="x", limit=1, min_wait_ms=0)
