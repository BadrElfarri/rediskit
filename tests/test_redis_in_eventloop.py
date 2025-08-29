import asyncio
import importlib
import threading
from queue import Queue

import pytest

import rediskit.redis_in_eventloop as loop_redis


class FakePool:
    def __init__(self):
        self.disconnected = False

    async def disconnect(self, inuse_connections=True):
        self.disconnected = True


class FakeClient:
    _instances = 0

    def __init__(self):
        FakeClient._instances += 1
        self.instance_no = FakeClient._instances
        self.loop = asyncio.get_running_loop()
        self.closed = False
        self.connection_pool = FakePool()

    async def ping(self):
        return True

    async def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def patch_factory_and_reset(monkeypatch):
    """
    - Patch _make_client to return a FakeClient bound to the *current* loop.
    - Reload the module between tests to reset the internal registry.
    """
    importlib.reload(loop_redis)
    factory_calls = {"n": 0}

    def _fake_factory():
        factory_calls["n"] += 1
        return FakeClient()

    monkeypatch.setattr(loop_redis, "_make_client", _fake_factory, raising=True)
    yield factory_calls  # let tests assert how many times we constructed clients


@pytest.mark.asyncio
async def test_same_loop_returns_same_client(patch_factory_and_reset):
    c1 = await loop_redis.get_async_redis_connection_in_eventloop()
    c2 = await loop_redis.get_async_redis_connection_in_eventloop()
    assert c1 is c2
    assert c1.loop is asyncio.get_running_loop()
    # Only one factory call for this loop
    assert patch_factory_and_reset["n"] == 1


@pytest.mark.asyncio
async def test_concurrent_first_use_is_single_init(patch_factory_and_reset):
    async def grab():
        return await loop_redis.get_async_redis_connection_in_eventloop()

    # many concurrent callers on first use
    results = await asyncio.gather(*[grab() for _ in range(50)])
    first = results[0]
    assert all(r is first for r in results)
    # still only one construction on this loop
    assert patch_factory_and_reset["n"] == 1


def run_coro_in_new_loop(coro) -> object:
    """
    Run a coroutine in a fresh event loop on a new thread and return its result.
    """
    q: Queue = Queue()

    def _runner():
        result = asyncio.run(coro)
        q.put(result)

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    res = q.get()  # wait for completion
    t.join()
    return res


@pytest.mark.asyncio
async def test_different_loops_get_different_clients(patch_factory_and_reset):
    # Client on current loop
    c_main = await loop_redis.get_async_redis_connection_in_eventloop()

    # Client on a separate loop/thread
    c_other = run_coro_in_new_loop(loop_redis.get_async_redis_connection_in_eventloop())

    assert c_main is not c_other
    assert c_main.loop is asyncio.get_running_loop()
    # c_other.loop is from a different thread/loop
    assert c_other.loop is not asyncio.get_running_loop()

    # Two factories called (one per loop)
    assert patch_factory_and_reset["n"] == 2


@pytest.mark.asyncio
async def test_close_creates_new_client_afterward(patch_factory_and_reset):
    c1 = await loop_redis.get_async_redis_connection_in_eventloop()
    await loop_redis.close_loop_redis()
    # After close, getting again should create a fresh client
    c2 = await loop_redis.get_async_redis_connection_in_eventloop()

    assert c1 is not c2
    assert isinstance(c1, FakeClient) and isinstance(c2, FakeClient)
    assert c1.closed is True
    assert c1.connection_pool.disconnected is True

    # two constructions on this loop (before and after close)
    assert patch_factory_and_reset["n"] == 2
