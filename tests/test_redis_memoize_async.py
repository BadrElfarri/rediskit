import asyncio
import time

import pytest

from rediskit.memoize import a_redis_memoize
from rediskit.redis.a_client import init_async_redis_connection_pool

TEST_TENANT_ID = "TEST_TENANT_REDIS_CACHE"


@pytest.mark.asyncio
async def testAsyncCaching():
    await init_async_redis_connection_pool()

    @a_redis_memoize(memoize_key="testAsync", ttl=10, cache_type="zipJson")
    async def slowFunc(tenantId: str, x):
        await asyncio.sleep(1)
        return {"asyncResult": x}

    x = 777
    start = time.time()
    res1 = await slowFunc(TEST_TENANT_ID, x)
    duration1 = time.time() - start

    start = time.time()
    res2 = await slowFunc(TEST_TENANT_ID, x)
    duration2 = time.time() - start

    assert res1.get("asyncResult") == x
    assert res2.get("asyncResult") == x
    assert duration1 >= 1.0
    assert duration2 < 0.5


@pytest.mark.asyncio
async def testAsyncBypass():
    await init_async_redis_connection_pool()

    def bypassFunc(*args, **kwargs):
        return kwargs.get("forceBypass", False)

    @a_redis_memoize(memoize_key="testAsyncBypass", ttl=10, cache_type="zipJson", bypass_cache=bypassFunc)
    async def slowFunc(tenantId: str, x, forceBypass=False):
        await asyncio.sleep(1)
        return {"asyncResult": x}

    x = 888
    start = time.time()
    res1 = await slowFunc(TEST_TENANT_ID, x, forceBypass=False)
    duration1 = time.time() - start

    start = time.time()
    res2 = await slowFunc(TEST_TENANT_ID, x, forceBypass=False)
    duration2 = time.time() - start

    start = time.time()
    res3 = await slowFunc(TEST_TENANT_ID, x, forceBypass=True)
    duration3 = time.time() - start

    assert duration1 >= 1.0
    assert duration2 < 0.5
    assert duration3 >= 1.0
    assert res1.get("asyncResult") == x
    assert res2.get("asyncResult") == x
    assert res3.get("asyncResult") == x


@pytest.mark.asyncio
async def testReturnNoneAsync():
    await init_async_redis_connection_pool()

    @a_redis_memoize(memoize_key="returnNoneAsync", ttl=10, cache_type="zipJson")
    async def func(tenantId: str, x):
        await asyncio.sleep(1)
        return None

    res1 = await func(TEST_TENANT_ID, 42)
    res2 = await func(TEST_TENANT_ID, 42)
    assert res1 is None
    assert res2 is None


@pytest.mark.asyncio
async def testConcurrentAsyncCaching():
    await init_async_redis_connection_pool()
    # Test that concurrent async calls only execute the function once
    call_count = [0]

    @a_redis_memoize(memoize_key="concurrentAsync", ttl=10, cache_type="zipJson")
    async def func(tenantId: str, x):
        call_count[0] += 1
        await asyncio.sleep(1)
        return {"result": x}

    tasks = [func(TEST_TENANT_ID, 888) for _ in range(5)]
    results = await asyncio.gather(*tasks)
    for r in results:
        assert r.get("result") == 888
    # The function should have been executed only once
    assert call_count[0] == 1
