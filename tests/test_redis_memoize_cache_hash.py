import asyncio
import time

import polars as pl
import pytest

from rediskit import redis_client
from rediskit.memoize import redis_memoize
from rediskit.redis_client import get_redis_connection, get_redis_top_node, init_async_redis_connection_pool

TEST_TENANT_ID = "TEST_TENANT_REDIS_CACHE"


@pytest.fixture
def Connection():
    return get_redis_connection()


@pytest.fixture(autouse=True)
def CleanupRedis(Connection):
    NodeKey = get_redis_top_node(TEST_TENANT_ID, "*")
    Connection.delete(NodeKey)
    yield
    Connection.delete(NodeKey)


def testSyncHashCaching():
    @redis_memoize(memoize_key=lambda tenantId, x: f"testHashKey:{tenantId}:{x}", ttl=10, cache_type="zipJson", storage_type="hash")
    def slowFunc(tenantId: str, x):
        time.sleep(1)
        return {"result": x}

    start = time.time()
    res1 = slowFunc(TEST_TENANT_ID, 101)
    duration1 = time.time() - start

    start = time.time()
    res2 = slowFunc(TEST_TENANT_ID, 101)
    duration2 = time.time() - start

    assert res1 == res2
    assert duration1 >= 1.0
    assert duration2 < 0.5
    assert res1.get("result") == 101


def testHashTtlExpiration():
    @redis_memoize(memoize_key="hashTtl:testField", ttl=2, cache_type="zipJson", storage_type="hash")
    def slowFunc(tenantId: str, x):
        time.sleep(1)
        return {"result": x}

    res1 = slowFunc(TEST_TENANT_ID, 1001)
    assert res1.get("result") == 1001

    res2 = slowFunc(TEST_TENANT_ID, 1001)
    assert res2.get("result") == 1001

    time.sleep(2.5)
    start = time.time()
    res3 = slowFunc(TEST_TENANT_ID, 1001)
    duration3 = time.time() - start
    assert duration3 >= 1.0
    assert res3.get("result") == 1001


def testResetTtlUponReadTrueHash():
    @redis_memoize(memoize_key="hashResetTtl:field", ttl=3, cache_type="zipJson", reset_ttl_upon_read=True, storage_type="hash")
    def func(tenantId: str, x):
        time.sleep(1)
        return {"result": x}

    res1 = func(TEST_TENANT_ID, 808)
    assert res1["result"] == 808

    time.sleep(2)
    res2 = func(TEST_TENANT_ID, 808)
    assert res2 == res1

    time.sleep(2)
    res3 = func(TEST_TENANT_ID, 808)
    assert res3 == res1


def testResetTtlUponReadFalseHash():
    @redis_memoize(memoize_key="hashResetTtlFalse:field", ttl=3, cache_type="zipJson", reset_ttl_upon_read=False, storage_type="hash")
    def func(tenantId: str, x):
        time.sleep(1)
        return {"result": x}

    res1 = func(TEST_TENANT_ID, 909)
    assert res1["result"] == 909

    time.sleep(2)
    res2 = func(TEST_TENANT_ID, 909)
    assert res2 == res1

    time.sleep(2)
    start = time.time()
    res3 = func(TEST_TENANT_ID, 909)
    duration3 = time.time() - start
    assert duration3 >= 1.0
    assert res3["result"] == 909


def testHashEncryption():
    @redis_memoize(memoize_key="encryptedHash:testField", ttl=10, cache_type="zipJson", enable_encryption=True, storage_type="hash")
    def func(tenantId: str, x):
        time.sleep(1)
        return {"secret": x}

    res1 = func(TEST_TENANT_ID, 444)
    res2 = func(TEST_TENANT_ID, 444)
    assert res1 == res2
    assert res1.get("secret") == 444


@pytest.mark.asyncio
async def testAsyncHashCaching():
    await redis_client.init_async_redis_connection_pool()

    @redis_memoize(memoize_key=lambda tenantId, x: f"testAsyncHashKey:{tenantId}:{x}", ttl=10, cache_type="zipJson", storage_type="hash")
    async def slowFunc(tenantId: str, x):
        await asyncio.sleep(1)
        return {"asyncResult": x}

    start = time.time()
    res1 = await slowFunc(TEST_TENANT_ID, 222)
    duration1 = time.time() - start

    start = time.time()
    res2 = await slowFunc(TEST_TENANT_ID, 222)
    duration2 = time.time() - start

    assert res1 == res2
    assert duration1 >= 1.0
    assert duration2 < 0.5
    assert res1.get("asyncResult") == 222


@pytest.mark.asyncio
async def testAsyncHashEncryption():
    await redis_client.init_async_redis_connection_pool()

    @redis_memoize(memoize_key="encryptedAsyncHash:testField", ttl=10, cache_type="zipJson", enable_encryption=True, storage_type="hash")
    async def func(tenantId: str, x):
        await asyncio.sleep(1)
        return {"secret": x}

    res1 = await func(TEST_TENANT_ID, 333)
    res2 = await func(TEST_TENANT_ID, 333)
    assert res1 == res2
    assert res1.get("secret") == 333


def testHashBypassCache():
    @redis_memoize(memoize_key="bypassHash:field", ttl=10, cache_type="zipJson", bypass_cache=True, storage_type="hash")
    def func(tenantId: str, x):
        time.sleep(1)
        return {"result": x}

    start = time.time()
    res1 = func(TEST_TENANT_ID, 555)
    duration1 = time.time() - start

    start = time.time()
    res2 = func(TEST_TENANT_ID, 555)
    duration2 = time.time() - start

    assert duration1 >= 1.0
    assert duration2 >= 1.0
    assert res1.get("result") == 555
    assert res2.get("result") == 555


def testHashPickledCaching():
    @redis_memoize(memoize_key="pickledHash:field", ttl=10, cache_type="zipPickled", storage_type="hash")
    def func(tenantId: str, x):
        time.sleep(1)
        # Use something not JSON serializable
        return pl.DataFrame([{"a": x}])

    df1 = func(TEST_TENANT_ID, 10)
    df2 = func(TEST_TENANT_ID, 10)
    assert df1.equals(df2)
    assert df1.equals(pl.DataFrame([{"a": 10}]))


def testHashPickledTtlExpiration():
    @redis_memoize(memoize_key="pickledHashTtl:field", ttl=2, cache_type="zipPickled", storage_type="hash")
    def func(tenantId: str, x):
        time.sleep(1)
        return pl.DataFrame([{"a": x}])

    df1 = func(TEST_TENANT_ID, 55)
    assert df1.equals(pl.DataFrame([{"a": 55}]))

    # Should be cached
    df2 = func(TEST_TENANT_ID, 55)
    assert df2.equals(df1)

    time.sleep(2.5)
    df3 = func(TEST_TENANT_ID, 55)
    assert df3.equals(pl.DataFrame([{"a": 55}]))


def testHashPickledEncryption():
    @redis_memoize(memoize_key="encryptedPickledHash:field", ttl=10, cache_type="zipPickled", enable_encryption=True, storage_type="hash")
    def func(tenantId: str, x):
        time.sleep(1)
        return pl.DataFrame([{"b": x}])

    df1 = func(TEST_TENANT_ID, 77)
    df2 = func(TEST_TENANT_ID, 77)
    assert df1.equals(df2)
    assert df1.equals(pl.DataFrame([{"b": 77}]))


@pytest.mark.asyncio
async def testAsyncHashPickledCaching():
    await init_async_redis_connection_pool()

    @redis_memoize(memoize_key="asyncPickledHash:field", ttl=10, cache_type="zipPickled", storage_type="hash")
    async def func(tenantId: str, x):
        await asyncio.sleep(1)
        return pl.DataFrame([{"c": x}])

    df1 = await func(TEST_TENANT_ID, 33)
    df2 = await func(TEST_TENANT_ID, 33)
    assert df1.equals(df2)
    assert df1.equals(pl.DataFrame([{"c": 33}]))


@pytest.mark.asyncio
async def testAsyncHashPickledEncryption():
    await init_async_redis_connection_pool()

    @redis_memoize(memoize_key="asyncEncryptedPickledHash:field", ttl=10, cache_type="zipPickled", enable_encryption=True, storage_type="hash")
    async def func(tenantId: str, x):
        await asyncio.sleep(1)
        return pl.DataFrame([{"d": x}])

    df1 = await func(TEST_TENANT_ID, 44)
    df2 = await func(TEST_TENANT_ID, 44)
    assert df1.equals(df2)
    assert df1.equals(pl.DataFrame([{"d": 44}]))


def testHashPickledBypassCache():
    @redis_memoize(memoize_key="pickledHashBypass:field", ttl=10, cache_type="zipPickled", bypass_cache=True, storage_type="hash")
    def func(tenantId: str, x):
        time.sleep(1)
        return pl.DataFrame([{"e": x}])

    df1 = func(TEST_TENANT_ID, 111)
    df2 = func(TEST_TENANT_ID, 111)
    assert df1.equals(pl.DataFrame([{"e": 111}]))
    assert df2.equals(pl.DataFrame([{"e": 111}]))
