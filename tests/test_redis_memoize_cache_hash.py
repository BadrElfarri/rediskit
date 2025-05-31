import asyncio
import time

import polars as pl
import pytest

from rediskit.memoize import RedisMemoize
from rediskit.redisClient import GetRedisConnection, GetRedisTopNode, InitAsyncRedisConnectionPool

TEST_TENANT_ID = "TEST_TENANT_REDIS_CACHE"


@pytest.fixture
def Connection():
    return GetRedisConnection()


@pytest.fixture(autouse=True)
def CleanupRedis(Connection):
    NodeKey = GetRedisTopNode(TEST_TENANT_ID, "*")
    Connection.delete(NodeKey)
    yield
    Connection.delete(NodeKey)


def testSyncHashCaching():
    @RedisMemoize(memoizeKey=lambda tenantId, x: f"testHashKey:{tenantId}:{x}", ttl=10, cacheType="zipJson", storageType="hash")
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
    @RedisMemoize(memoizeKey="hashTtl:testField", ttl=2, cacheType="zipJson", storageType="hash")
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
    @RedisMemoize(memoizeKey="hashResetTtl:field", ttl=3, cacheType="zipJson", resetTtlUponRead=True, storageType="hash")
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
    @RedisMemoize(memoizeKey="hashResetTtlFalse:field", ttl=3, cacheType="zipJson", resetTtlUponRead=False, storageType="hash")
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
    @RedisMemoize(memoizeKey="encryptedHash:testField", ttl=10, cacheType="zipJson", enableEncryption=True, storageType="hash")
    def func(tenantId: str, x):
        time.sleep(1)
        return {"secret": x}

    res1 = func(TEST_TENANT_ID, 444)
    res2 = func(TEST_TENANT_ID, 444)
    assert res1 == res2
    assert res1.get("secret") == 444


@pytest.mark.asyncio
async def testAsyncHashCaching():
    InitAsyncRedisConnectionPool()

    @RedisMemoize(memoizeKey=lambda tenantId, x: f"testAsyncHashKey:{tenantId}:{x}", ttl=10, cacheType="zipJson", storageType="hash")
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
    InitAsyncRedisConnectionPool()

    @RedisMemoize(memoizeKey="encryptedAsyncHash:testField", ttl=10, cacheType="zipJson", enableEncryption=True, storageType="hash")
    async def func(tenantId: str, x):
        await asyncio.sleep(1)
        return {"secret": x}

    res1 = await func(TEST_TENANT_ID, 333)
    res2 = await func(TEST_TENANT_ID, 333)
    assert res1 == res2
    assert res1.get("secret") == 333


def testHashBypassCache():
    @RedisMemoize(memoizeKey="bypassHash:field", ttl=10, cacheType="zipJson", bypassCache=True, storageType="hash")
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
    @RedisMemoize(memoizeKey="pickledHash:field", ttl=10, cacheType="zipPickled", storageType="hash")
    def func(tenantId: str, x):
        time.sleep(1)
        # Use something not JSON serializable
        return pl.DataFrame([{"a": x}])

    df1 = func(TEST_TENANT_ID, 10)
    df2 = func(TEST_TENANT_ID, 10)
    assert df1.equals(df2)
    assert df1.equals(pl.DataFrame([{"a": 10}]))


def testHashPickledTtlExpiration():
    @RedisMemoize(memoizeKey="pickledHashTtl:field", ttl=2, cacheType="zipPickled", storageType="hash")
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
    @RedisMemoize(memoizeKey="encryptedPickledHash:field", ttl=10, cacheType="zipPickled", enableEncryption=True, storageType="hash")
    def func(tenantId: str, x):
        time.sleep(1)
        return pl.DataFrame([{"b": x}])

    df1 = func(TEST_TENANT_ID, 77)
    df2 = func(TEST_TENANT_ID, 77)
    assert df1.equals(df2)
    assert df1.equals(pl.DataFrame([{"b": 77}]))


@pytest.mark.asyncio
async def testAsyncHashPickledCaching():
    InitAsyncRedisConnectionPool()

    @RedisMemoize(memoizeKey="asyncPickledHash:field", ttl=10, cacheType="zipPickled", storageType="hash")
    async def func(tenantId: str, x):
        await asyncio.sleep(1)
        return pl.DataFrame([{"c": x}])

    df1 = await func(TEST_TENANT_ID, 33)
    df2 = await func(TEST_TENANT_ID, 33)
    assert df1.equals(df2)
    assert df1.equals(pl.DataFrame([{"c": 33}]))


@pytest.mark.asyncio
async def testAsyncHashPickledEncryption():
    InitAsyncRedisConnectionPool()

    @RedisMemoize(memoizeKey="asyncEncryptedPickledHash:field", ttl=10, cacheType="zipPickled", enableEncryption=True, storageType="hash")
    async def func(tenantId: str, x):
        await asyncio.sleep(1)
        return pl.DataFrame([{"d": x}])

    df1 = await func(TEST_TENANT_ID, 44)
    df2 = await func(TEST_TENANT_ID, 44)
    assert df1.equals(df2)
    assert df1.equals(pl.DataFrame([{"d": 44}]))


def testHashPickledBypassCache():
    @RedisMemoize(memoizeKey="pickledHashBypass:field", ttl=10, cacheType="zipPickled", bypassCache=True, storageType="hash")
    def func(tenantId: str, x):
        time.sleep(1)
        return pl.DataFrame([{"e": x}])

    df1 = func(TEST_TENANT_ID, 111)
    df2 = func(TEST_TENANT_ID, 111)
    assert df1.equals(pl.DataFrame([{"e": 111}]))
    assert df2.equals(pl.DataFrame([{"e": 111}]))
