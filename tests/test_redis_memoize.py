import time

import polars as pl
import pytest

from rediskit.memoize import redis_memoize
from rediskit.redis import get_redis_top_node
from rediskit.redis.client import get_redis_connection

TEST_TENANT_ID = "TEST_TENANT_REDIS_CACHE"


@pytest.fixture
def Connection():
    return get_redis_connection()


# Ensure that before and after each test, the test key is cleared from Redis
@pytest.fixture(autouse=True)
def CleanupRedis(Connection):
    NodeKey = get_redis_top_node(TEST_TENANT_ID, "*")
    Connection.delete(NodeKey)
    yield
    Connection.delete(NodeKey)


###############################################
# Synchronous Function Tests
###############################################


def testSyncCaching():
    @redis_memoize(memoize_key="testKeySync", ttl=10, cache_type="zipJson", bypass_cache=False)
    def slowFunc(tenantId: str, x):
        time.sleep(1)
        return {"result": x}

    start = time.time()
    res1 = slowFunc(TEST_TENANT_ID, 42)
    duration1 = time.time() - start

    start = time.time()
    res2 = slowFunc(TEST_TENANT_ID, 42)
    duration2 = time.time() - start

    # First call should take ~1s; second call should be fast (cached)
    assert res1 == res2
    assert duration1 >= 1.0
    assert duration2 < 0.5


def testTtlExpiration():
    @redis_memoize(memoize_key="testTtl", ttl=2, cache_type="zipJson")
    def slowFunc(tenantId: str, x):
        time.sleep(1)
        return {"result": x}

    start = time.time()
    res1 = slowFunc(TEST_TENANT_ID, 100)
    duration1 = time.time() - start
    assert duration1 >= 1.0
    assert res1.get("result") == 100

    # Immediately call: should be cached
    start = time.time()
    res2 = slowFunc(TEST_TENANT_ID, 100)
    duration2 = time.time() - start
    assert duration2 < 0.5
    assert res2.get("result") == 100

    # Wait for TTL to expire then call: should take ~1s again
    time.sleep(2.5)
    start = time.time()
    res3 = slowFunc(TEST_TENANT_ID, 100)
    duration3 = time.time() - start
    assert duration3 >= 1.0
    assert res3.get("result") == 100


def testBypassCacheStatic():
    @redis_memoize(memoize_key="testBypassStatic", ttl=10, cache_type="zipJson", bypass_cache=True)
    def slowFunc(tenantId: str, x):
        time.sleep(1)
        return {"result": x}

    start = time.time()
    res1 = slowFunc(TEST_TENANT_ID, 200)
    duration1 = time.time() - start

    start = time.time()
    res2 = slowFunc(TEST_TENANT_ID, 200)
    duration2 = time.time() - start

    # Both calls should take ~1s because cache is bypassed.
    assert duration1 >= 1.0
    assert duration2 >= 1.0
    assert res1.get("result") == 200
    assert res2.get("result") == 200


def testBypassCacheCallable():
    def bypassFunc(*args, **kwargs):
        return kwargs.get("forceBypass", False)

    @redis_memoize(memoize_key="testBypassCallable", ttl=10, cache_type="zipJson", bypass_cache=bypassFunc)
    def slowFunc(tenantId: str, x, forceBypass=False):
        time.sleep(1)
        return {"result": x}

    start = time.time()
    res1 = slowFunc(TEST_TENANT_ID, 300, forceBypass=False)
    duration1 = time.time() - start

    start = time.time()
    res2 = slowFunc(TEST_TENANT_ID, 300, forceBypass=False)
    duration2 = time.time() - start

    start = time.time()
    res3 = slowFunc(TEST_TENANT_ID, 300, forceBypass=True)
    duration3 = time.time() - start

    # First call: ~1s; cached call: fast; bypass call: ~1s again.
    assert duration1 >= 1.0
    assert duration2 < 0.5
    assert duration3 >= 1.0
    assert res1.get("result") == 300
    assert res2.get("result") == 300
    assert res3.get("result") == 300


def testCallableMemoizeKey():
    @redis_memoize(memoize_key=lambda tenantId, x: f"key_{x}", ttl=10, cache_type="zipJson")
    def func(tenantId: str, x):
        time.sleep(1)
        return {"value": x}

    start = time.time()
    res1 = func(TEST_TENANT_ID, 555)
    duration1 = time.time() - start

    start = time.time()
    res2 = func(TEST_TENANT_ID, 555)
    duration2 = time.time() - start

    start = time.time()
    res3 = func(TEST_TENANT_ID, 666)  # Different key => no cache hit.
    duration3 = time.time() - start

    assert duration1 >= 1.0
    assert duration2 < 0.5  # Cached result.
    assert duration3 >= 1.0  # New computation.
    assert res1.get("value") == 555
    assert res2.get("value") == 555
    assert res3.get("value") == 666


def testCallableTtl():
    @redis_memoize(memoize_key="testCallableTtl", ttl=lambda tenantId, delay: delay, cache_type="zipJson")
    def func(tenantId: str, delay):
        time.sleep(1)
        return {"delay": delay}

    start = time.time()
    res1 = func(TEST_TENANT_ID, 2)
    duration1 = time.time() - start
    assert duration1 >= 1.0
    assert res1.get("delay") == 2

    start = time.time()
    res2 = func(TEST_TENANT_ID, 2)
    duration2 = time.time() - start
    assert duration2 < 0.5
    assert res2.get("delay") == 2

    time.sleep(2.5)
    start = time.time()
    res3 = func(TEST_TENANT_ID, 2)
    duration3 = time.time() - start
    assert duration3 >= 1.0
    assert res3.get("delay") == 2


def testMissingTenantIdAndKey():
    with pytest.raises(ValueError):

        @redis_memoize(memoize_key=lambda x: None, ttl=10, cache_type="zipJson")
        def func(x):
            return {"value": x}

        # Calling without tenantId should raise an error.
        func(x=123)


###############################################
# Cache Type Tests: zipPickled vs zipJson
###############################################


def testZipPickledVsZipJson():
    @redis_memoize(memoize_key="testZipPickled", ttl=10, cache_type="zipPickled")
    def funcPickled(tenantId: str, x) -> pl.DataFrame:
        time.sleep(1)
        return pl.DataFrame([{"value": x}])

    @redis_memoize(memoize_key="testZipJson", ttl=10, cache_type="zipJson")
    def funcJson(tenantId: str, x):
        time.sleep(1)
        return {"value": x}

    x = 999
    start = time.time()
    res1 = funcPickled(TEST_TENANT_ID, x)
    duration1 = time.time() - start

    start = time.time()
    res2 = funcPickled(TEST_TENANT_ID, x)
    duration2 = time.time() - start

    start = time.time()
    res3 = funcJson(TEST_TENANT_ID, x)
    duration3 = time.time() - start

    start = time.time()
    res4 = funcJson(TEST_TENANT_ID, x)
    duration4 = time.time() - start

    assert res1.equals(res2)
    assert res1.equals(pl.DataFrame([{"value": x}]))
    assert res3.get("value") == res4.get("value")
    assert res3.get("value") == x
    assert duration1 >= 1.0
    assert duration2 < 0.5
    assert duration3 >= 1.0
    assert duration4 < 0.5


########################
# Test raise conditions
########################
def testInvalidCacheType():
    with pytest.raises(ValueError):

        @redis_memoize(memoize_key="invalidCacheType", ttl=10, cache_type="invalid")
        def func(tenantId: str, x):
            return {"value": x}

        func(TEST_TENANT_ID, 123)


def testErrorNotCached():
    call_count = [0]  # mutable counter

    @redis_memoize(memoize_key="errorNotCached", ttl=10, cache_type="zipJson")
    def errorFunc(tenantId: str, x):
        call_count[0] += 1
        if call_count[0] == 1:
            raise ValueError("Test error")
        return {"result": x}

    with pytest.raises(ValueError):
        errorFunc(TEST_TENANT_ID, 123)
    # Second call should execute the function again since error is not cached
    result = errorFunc(TEST_TENANT_ID, 123)
    assert result.get("result") == 123


def testResetTtlUponReadTrue():
    # Test that TTL is reset upon reading the cache.
    @redis_memoize(memoize_key="resetTtlTrue", ttl=3, cache_type="zipJson", reset_ttl_upon_read=True)
    def func(tenantId: str, x):
        time.sleep(1)
        return {"result": x}

    # First call, should be slow (cache miss)
    start = time.time()
    res1 = func(TEST_TENANT_ID, 10)
    duration1 = time.time() - start
    assert duration1 >= 1.0

    # Wait less than TTL and call again, should be fast (cache hit)
    time.sleep(2)
    start = time.time()
    res2 = func(TEST_TENANT_ID, 10)
    duration2 = time.time() - start
    assert duration2 < 0.5

    # Because resetTtlUponRead is True, the TTL should have been refreshed.
    # Wait additional time (total time > original TTL) and call again, should still be fast.
    time.sleep(2)
    start = time.time()
    res3 = func(TEST_TENANT_ID, 10)
    duration3 = time.time() - start
    assert duration3 < 0.5
    assert res1 == res2 == res3


def testResetTtlUponReadFalse():
    # Test that TTL is not reset upon reading the cache when resetTtlUponRead is False.
    @redis_memoize(memoize_key="resetTtlFalse", ttl=3, cache_type="zipJson", reset_ttl_upon_read=False)
    def func(tenantId: str, x):
        time.sleep(1)
        return {"result": x}

    # First call, should be slow (cache miss)
    start = time.time()
    res1 = func(TEST_TENANT_ID, 20)
    duration1 = time.time() - start
    assert duration1 >= 1.0

    # Wait less than TTL and call again, should be fast (cache hit)
    time.sleep(2)
    start = time.time()
    res2 = func(TEST_TENANT_ID, 20)
    duration2 = time.time() - start
    assert duration2 < 0.5

    # Wait additional time so that total time exceeds the TTL (TTL is not reset)
    time.sleep(2)
    start = time.time()
    res3 = func(TEST_TENANT_ID, 20)
    duration3 = time.time() - start
    # This call should be slow because the cache expired
    assert duration3 >= 1.0
    assert res1 == res2
    assert res3.get("result") == 20


def testReturnNoneSync():
    @redis_memoize(memoize_key="returnNoneSync", ttl=10, cache_type="zipJson")
    def func(tenantId: str, x):
        time.sleep(1)
        return None

    res1 = func(TEST_TENANT_ID, 42)
    res2 = func(TEST_TENANT_ID, 42)
    assert res1 is None
    assert res2 is None


def testTtlNone():
    # Ttl is allowed to be null, no ttl is set if ttl is None
    @redis_memoize(memoize_key="ttlNone", ttl=None, cache_type="zipJson")
    def func(tenantId: str, x):
        return x

    func(TEST_TENANT_ID, 42)
    time.sleep(2)
    assert func(TEST_TENANT_ID, 42) == 42


def testMutableReturn():
    # Test that modifying the returned mutable object does not affect the cached value
    @redis_memoize(memoize_key="mutableReturn", ttl=10, cache_type="zipJson")
    def func(tenantId: str, x):
        time.sleep(1)
        return {"result": [x]}

    res1 = func(TEST_TENANT_ID, 123)
    # Modify the returned list
    res1["result"].append(456)
    res2 = func(TEST_TENANT_ID, 123)
    # The cached result should not include the appended value
    assert res2.get("result") == [123]


def testDifferentTenants():
    # Ensure that caching is isolated per tenantId
    @redis_memoize(memoize_key="differentTenants", ttl=10, cache_type="zipJson")
    def func(tenantId: str, x):
        time.sleep(1)
        return {"result": x}

    resA = func("TenantA", 50)
    resB = func("TenantB", 50)
    assert resA.get("result") == 50
    assert resB.get("result") == 50
    # Subsequent calls for each tenant should return cached values
    resA_cached = func("TenantA", 50)
    resB_cached = func("TenantB", 50)
    assert resA == resA_cached
    assert resB == resB_cached


def testConcurrentSyncCaching():
    # Test that concurrent calls only execute the function once
    call_count = [0]

    @redis_memoize(memoize_key="concurrentSync", ttl=10, cache_type="zipJson")
    def func(tenantId: str, x):
        call_count[0] += 1
        time.sleep(1)
        return {"result": x}

    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(func, TEST_TENANT_ID, 777) for _ in range(5)]
        results = [f.result() for f in futures]
    for r in results:
        assert r.get("result") == 777
    # The function should have been executed only once
    assert call_count[0] == 1


def testEnabledEncryption():
    # Test that modifying the returned mutable object does not affect the cached value
    @redis_memoize(memoize_key="encryptedTest", ttl=10, cache_type="zipJson", enable_encryption=True)
    def func(tenantId: str, x):
        time.sleep(1)
        return {"result": x}

    @redis_memoize(memoize_key="encryptedTestDf", ttl=10, cache_type="zipPickled", enable_encryption=True)
    def func2(tenantId: str, x):
        time.sleep(1)
        return x

    df = pl.DataFrame([{"a": 1}])
    res1 = func(TEST_TENANT_ID, 1)
    res2 = func(TEST_TENANT_ID, 1)
    res3 = func2(TEST_TENANT_ID, df)
    res4 = func2(TEST_TENANT_ID, df)

    # Modify the returned list
    assert res1.get("result") == 1
    assert res1.get("result") == res2.get("result")
    assert all(res3["a"] == df["a"])
    assert all(res3["a"] == res4["a"])
