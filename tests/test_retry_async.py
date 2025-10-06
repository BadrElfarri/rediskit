# tests/test_retry_async.py
import asyncio
import inspect

import httpx
import pytest

import rediskit.retry_decorator as rm


@pytest.fixture
def sleep_calls(monkeypatch):
    """
    Replace asyncio.sleep with a fake that records requested sleep times (without actually sleeping).
    Returns a list you can assert on.
    """
    calls = []

    async def fake_sleep(s: float):
        calls.append(s)
        # do not actually sleep

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    return calls


@pytest.fixture
def fixed_random(monkeypatch):
    """
    Deterministic RNG. By default yields 0.5 repeatedly -> zero jitter in your formula.
    Call fixed_random.set([...]) to change the sequence per-test.
    """
    seq = {"vals": [0.5]}

    def iterator():
        while True:
            for v in seq["vals"]:
                yield v

    it = iterator()

    def fake_random():
        return next(it)

    # Patch the random imported inside your module
    monkeypatch.setattr(rm.random, "random", fake_random)

    class Ctl:
        def set(self, values):
            seq["vals"] = values

    return Ctl()


def failing_then_success(n_fail: int, exc: Exception):
    """
    Build an async function that fails n_fail times with `exc`, then returns "OK".
    Also captures call count via attribute for assertions.
    """
    state = {"count": 0}

    @rm.retry_async()
    async def fn(*args, **kwargs):
        state["count"] += 1
        if state["count"] <= n_fail:
            raise exc
        return "OK"

    fn._state = state  # type: ignore # attach for test introspection (ok in tests)
    return fn


@pytest.mark.asyncio
async def test_success_without_retry():
    @rm.retry_async()
    async def fn():
        return 42

    assert await fn() == 42


@pytest.mark.asyncio
async def test_disabled_retry_does_not_retry(sleep_calls):
    # enabled=False should bypass retry and not sleep
    err = httpx.RequestError("boom")
    calls = {"n": 0}

    @rm.retry_async(rm.RetryPolicy(enabled=False, attempts=5))
    async def fn():
        calls["n"] += 1
        raise err

    with pytest.raises(httpx.RequestError):
        await fn()

    assert calls["n"] == 1
    assert sleep_calls == []


@pytest.mark.asyncio
async def test_attempts_works_and_stops_on_success(sleep_calls, fixed_random):
    # 2 failures + 1 success with attempts=3
    fn = failing_then_success(2, httpx.RequestError("x"))

    result = await fn()
    assert result == "OK"
    # backoff default backoff=0.5, jitter ±0.1 but fixed_random produces 0 jitter
    assert 0.4 < sleep_calls[0] < 0.6 and 0.9 < sleep_calls[1] < 1.1  # exponential: 0.5, 1.0 for first two retries
    assert fn._state["count"] == 3


@pytest.mark.asyncio
async def test_exhausts_and_raises_after_max_attempts(sleep_calls, fixed_random):
    # fails 3 times, attempts=3 -> should raise after two sleeps
    err = httpx.RequestError("nope")
    state = {"n": 0}

    @rm.retry_async(rm.RetryPolicy(attempts=3, backoff=0.2, jitter=0.0))
    async def fn():
        state["n"] += 1
        raise err

    with pytest.raises(httpx.RequestError):
        await fn()

    assert state["n"] == 3
    assert sleep_calls == [0.2, 0.4]


@pytest.mark.asyncio
async def test_non_retriable_exception_bubbles_no_sleep(sleep_calls):
    # ValueError not in default exceptions -> no retry
    state = {"n": 0}

    @rm.retry_async()
    async def fn():
        state["n"] += 1
        raise ValueError("bad")

    with pytest.raises(ValueError):
        await fn()

    assert state["n"] == 1
    assert sleep_calls == []


@pytest.mark.asyncio
async def test_cancelled_error_propagates_without_retry(sleep_calls):
    state = {"n": 0}

    @rm.retry_async()
    async def fn():
        state["n"] += 1
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await fn()

    assert state["n"] == 1
    assert sleep_calls == []


@pytest.mark.asyncio
async def test_http_status_error_is_retried(sleep_calls, fixed_random):
    # httpx.HTTPStatusError is in default tuple -> should retry
    req = httpx.Request("GET", "https://x")
    resp = httpx.Response(500, request=req)
    err = httpx.HTTPStatusError("server err", request=req, response=resp)

    state = {"n": 0}

    @rm.retry_async(rm.RetryPolicy(attempts=2, backoff=0.1, jitter=0.0))
    async def fn():
        state["n"] += 1
        raise err

    with pytest.raises(httpx.HTTPStatusError):
        await fn()

    assert state["n"] == 2
    assert sleep_calls == [0.1]


@pytest.mark.asyncio
async def test_per_call_policy_override_with_dict(sleep_calls, fixed_random):
    # Override attempts/backoff/jitter/exceptions via dict
    req = httpx.Request("GET", "https://x")
    resp = httpx.Response(503, request=req)
    err = httpx.HTTPStatusError("svc err", request=req, response=resp)

    calls = {"n": 0}

    @rm.retry_async(rm.RetryPolicy(attempts=5, backoff=1.0, jitter=0.0))
    async def fn():
        calls["n"] += 1
        raise err

    # override to attempts=2, backoff=0.2
    with pytest.raises(httpx.HTTPStatusError):
        await fn(retry_policy={"attempts": 2, "backoff": 0.2, "jitter": 0.0})

    assert calls["n"] == 2
    assert sleep_calls == [0.2]


@pytest.mark.asyncio
async def test_per_call_policy_override_with_object(sleep_calls, fixed_random):
    err = httpx.RequestError("oh no")
    calls = {"n": 0}

    @rm.retry_async(rm.RetryPolicy(attempts=5, backoff=1.0, jitter=0.0))
    async def fn():
        calls["n"] += 1
        raise err

    policy = rm.RetryPolicy(attempts=3, backoff=0.05, jitter=0.0)
    with pytest.raises(httpx.RequestError):
        await fn(retry_policy=policy)

    assert calls["n"] == 3
    assert sleep_calls == [0.05, 0.10]


@pytest.mark.asyncio
async def test_retry_policy_kwarg_not_forwarded_to_fn_arguments(sleep_calls):
    # Ensure the decorator pops 'retry_policy' and doesn't pass it into the wrapped function
    seen_kwargs = {}

    @rm.retry_async(rm.RetryPolicy(attempts=1))
    async def fn(**kwargs):
        nonlocal seen_kwargs
        seen_kwargs = kwargs
        return "ok"

    res = await fn(retry_policy={"attempts": 1}, x=1)
    assert res == "ok"
    assert "retry_policy" not in seen_kwargs
    assert seen_kwargs == {"x": 1}


@pytest.mark.asyncio
async def test_attempts_equal_one_means_no_sleep_no_retry(sleep_calls):
    calls = {"n": 0}

    @rm.retry_async(rm.RetryPolicy(attempts=1, backoff=0.5))
    async def fn():
        calls["n"] += 1
        raise httpx.RequestError("fail once")

    with pytest.raises(httpx.RequestError):
        await fn()

    assert calls["n"] == 1
    assert sleep_calls == []


@pytest.mark.asyncio
async def test_non_positive_backoff_still_behaves_and_never_sleeps_negative(sleep_calls, fixed_random):
    # backoff <= 0 should not cause negative sleeps; first delay is <=0 -> sleep(0)
    calls = {"n": 0}

    @rm.retry_async(rm.RetryPolicy(attempts=3, backoff=-1.0, jitter=0.0))
    async def fn():
        calls["n"] += 1
        raise httpx.RequestError("x")

    with pytest.raises(httpx.RequestError):
        await fn()

    # delays: max(0, -1) -> 0, then doubles -> 0, then doubles -> 0 (but only two sleeps)
    assert sleep_calls == [0.0, 0.0]
    assert calls["n"] == 3


def test_preserves_function_metadata():
    async def original(a, b=2):
        """Docstring here."""
        return a + b

    wrapped = rm.retry_async()(original)

    assert wrapped.__name__ == "original"
    assert wrapped.__doc__ == "Docstring here."
    _sig_orig = inspect.signature(original)
    sig_wrap = inspect.signature(wrapped)
    # wrapper(*args, **kwargs) can't replicate exact signature at runtime,
    # but we at least ensure it's still a coroutine function and callable with same params.
    assert asyncio.iscoroutinefunction(wrapped)
    # quick callability check via param names
    for name in ["a", "b"]:
        assert name in str(sig_wrap)


@pytest.mark.asyncio
async def test_custom_exceptions_tuple(sleep_calls):
    # If exceptions tuple excludes httpx.RequestError, there should be no retry
    calls = {"n": 0}

    @rm.retry_async(rm.RetryPolicy(attempts=3, exceptions=(httpx.HTTPStatusError,)))
    async def fn():
        calls["n"] += 1
        raise httpx.RequestError("no retry for this")

    with pytest.raises(httpx.RequestError):
        await fn()

    assert calls["n"] == 1
    assert sleep_calls == []


@pytest.mark.asyncio
async def test_success_after_first_retry_returns_value(sleep_calls, fixed_random):
    calls = {"n": 0}

    @rm.retry_async(rm.RetryPolicy(attempts=4, backoff=0.05, jitter=0.0))
    async def fn(x):
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.RequestError("transient")
        return x * 2

    val = await fn(7)
    assert val == 14
    assert calls["n"] == 2
    assert sleep_calls == [0.05]


@pytest.mark.asyncio
async def test_policy_kwarg_wrong_type_ignored_uses_default(sleep_calls, fixed_random):
    # Passing a nonsense retry_policy value should fall back to default policy used in decorator
    calls = {"n": 0}

    @rm.retry_async(rm.RetryPolicy(attempts=2, backoff=0.1, jitter=0.0))
    async def fn():
        calls["n"] += 1
        raise httpx.RequestError("x")

    with pytest.raises(httpx.RequestError):
        await fn(retry_policy=object())  # not dict/rm.RetryPolicy

    assert calls["n"] == 2
    assert sleep_calls == [0.1]
