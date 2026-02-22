import asyncio
import uuid

import pytest
import pytest_asyncio

from rediskit.redis import get_redis_top_node
from rediskit.redis.a_client import (
    check_cache_matches,
    delete_cache_from_redis,
    dump_blob_to_redis,
    dump_cache_to_redis,
    dump_multiple_payload_to_redis,
    get_async_redis_connection,
    get_keys,
    h_del_cache_from_redis,
    h_get_cache_from_redis,
    h_scan_fields,
    h_set_cache_to_redis,
    hash_set_ttl_for_key,
    list_keys,
    load_blob_from_redis,
    load_cache_from_redis,
    load_exact_cache_from_redis,
    readiness_ping,
    redis_single_connection_context,
    set_redis_cache_expiry,
    set_ttl_for_key,
)
from rediskit.redis.a_client.connection import init_async_redis_connection_pool

TEST_TENANT_ID = "PYTEST_REDISKIT_TENANT_ASYNC"


@pytest_asyncio.fixture
async def connection():
    await init_async_redis_connection_pool()
    return get_async_redis_connection()


@pytest_asyncio.fixture(autouse=True)
async def cleanup_redis(connection):
    prefix = get_redis_top_node(TEST_TENANT_ID, "")
    async for key in connection.scan_iter(match=f"{prefix}*"):
        await connection.delete(key)
    yield
    async for key in connection.scan_iter(match=f"{prefix}*"):
        await connection.delete(key)


@pytest.mark.asyncio
async def test_redis_single_connection_context_sets_and_gets():
    test_key = f"test:{uuid.uuid4()}"
    test_val = "value42"
    async with redis_single_connection_context() as redis:
        await redis.set(test_key, test_val)
        val = await redis.get(test_key)
        assert val == test_val
        await redis.delete(test_key)
        val_after_delete = await redis.get(test_key)
        assert val_after_delete is None
    async with redis_single_connection_context() as redis2:
        val_after_context = await redis2.get(test_key)
        assert val_after_context is None


@pytest.mark.asyncio
async def test_dump_and_load_cache(connection):
    key = "basic"
    data = {"val": 123}
    await dump_cache_to_redis(TEST_TENANT_ID, key, data, connection=connection)
    results = await load_cache_from_redis(TEST_TENANT_ID, key, connection=connection)
    assert data in results
    out = await load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection)
    assert out == data


@pytest.mark.asyncio
async def test_dump_multiple_payload(connection):
    items = [{"key": f"k{i}", "payload": {"x": i}} for i in range(3)]
    await dump_multiple_payload_to_redis(TEST_TENANT_ID, items, ttl=30)
    found = 0
    for i in range(3):
        val = await load_exact_cache_from_redis(TEST_TENANT_ID, f"k{i}", connection=connection)
        assert val == {"x": i}
        found += 1
    assert found == 3


@pytest.mark.asyncio
async def test_delete_cache_from_redis(connection):
    key = "todelete"
    data = {"bye": "now"}
    await dump_cache_to_redis(TEST_TENANT_ID, key, data, connection=connection)
    assert await load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection) is not None
    await delete_cache_from_redis(TEST_TENANT_ID, key, connection=connection)
    assert await load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection) is None


@pytest.mark.asyncio
async def test_check_cache_matches(connection):
    key = "match"
    data = {"foo": 1, "bar": 2}
    await dump_cache_to_redis(TEST_TENANT_ID, key, data, connection=connection)
    assert await check_cache_matches(TEST_TENANT_ID, key, {"foo": 1, "bar": 2}, connection=connection)
    assert not await check_cache_matches(TEST_TENANT_ID, key, {"foo": 999}, connection=connection)


@pytest.mark.asyncio
async def test_set_redis_cache_expiry_and_ttl(connection):
    key = "exp"
    data = {"a": "ttl"}
    await dump_cache_to_redis(TEST_TENANT_ID, key, data, connection=connection)
    await set_redis_cache_expiry(TEST_TENANT_ID, key, 2, connection=connection)
    assert await load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection)
    await asyncio.sleep(2.1)
    assert await load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection) is None


@pytest.mark.asyncio
async def test_hash_set_ttl_for_key(connection):
    key = "hash_ttl"
    fields = {"a": 1, "b": 2}
    await h_set_cache_to_redis(TEST_TENANT_ID, key, fields, connection=connection)
    await hash_set_ttl_for_key(TEST_TENANT_ID, key, list(fields.keys()), 2, connection=connection)
    assert await h_get_cache_from_redis(TEST_TENANT_ID, key, None, connection=connection)
    await asyncio.sleep(2.1)
    assert await h_get_cache_from_redis(TEST_TENANT_ID, key, None, connection=connection) == {}


@pytest.mark.asyncio
async def test_get_keys(connection):
    k1, k2 = "gkey1", "gkey2"
    await h_set_cache_to_redis(TEST_TENANT_ID, k1, {"a": 1}, connection=connection)
    await h_set_cache_to_redis(TEST_TENANT_ID, k2, {"a": 2}, connection=connection)
    keys = await get_keys(TEST_TENANT_ID, "*", connection=connection, only_last_key=True)
    assert set([k1, k2]).issubset(set(keys))


@pytest.mark.asyncio
async def test_set_ttl_for_key(connection):
    key = "ttlkey"
    data = {"foo": "bar"}
    await dump_cache_to_redis(TEST_TENANT_ID, key, data, connection=connection)
    await set_ttl_for_key(TEST_TENANT_ID, key, 2, connection=connection)
    assert await load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection)
    await asyncio.sleep(2.1)
    assert await load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection) is None


@pytest.mark.asyncio
async def test_set_ttl_for_key_directly(connection):
    key = "ttlkey"
    data = {"foo": "bar"}
    await dump_cache_to_redis(TEST_TENANT_ID, key, data, connection=connection, ttl=2)
    assert await load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection)
    await asyncio.sleep(2.1)
    assert await load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection) is None


@pytest.mark.asyncio
async def test_dump_and_load_blob(connection):
    key = "blobkey"
    blob = "this is a test blob"
    await dump_blob_to_redis(TEST_TENANT_ID, key, blob, connection=connection, ttl=10)
    loaded = await load_blob_from_redis(TEST_TENANT_ID, key, connection=connection)
    assert loaded == blob


@pytest.mark.asyncio
async def test_h_scan_fields(connection):
    key = "scanfields"
    fields = {"f1": 10, "f2": 20, "hello": 30}
    await h_set_cache_to_redis(TEST_TENANT_ID, key, fields, connection=connection)
    matched = await h_scan_fields(TEST_TENANT_ID, key, match="f*", connection=connection)
    assert set(matched) == {"f1", "f2"}


@pytest.mark.asyncio
async def test_h_get_invalid_field_type(connection):
    key = "badtype"
    await h_set_cache_to_redis(TEST_TENANT_ID, key, {"f": 1}, connection=connection)
    with pytest.raises(ValueError):
        await h_get_cache_from_redis(TEST_TENANT_ID, key, 42, connection=connection)


@pytest.mark.asyncio
async def test_h_del_invalid_field_type(connection):
    key = "delfail"
    await h_set_cache_to_redis(TEST_TENANT_ID, key, {"f": 1}, connection=connection)
    with pytest.raises(ValueError):
        await h_del_cache_from_redis(TEST_TENANT_ID, key, 1.23, connection=connection)


@pytest.mark.asyncio
async def test_h_get_fields_variants(connection):
    key = "hfields"
    fields = {"s": "value", "n": 42}
    await h_set_cache_to_redis(TEST_TENANT_ID, key, fields, connection=connection)
    res = await h_get_cache_from_redis(TEST_TENANT_ID, key, "s", connection=connection)
    assert res == {"s": "value"}
    res2 = await h_get_cache_from_redis(TEST_TENANT_ID, key, ["s", "n"], connection=connection)
    assert res2 == fields
    res3 = await h_get_cache_from_redis(TEST_TENANT_ID, key, None, connection=connection)
    assert res3 == fields


@pytest.mark.asyncio
async def test_h_set_and_get_encrypted(connection):
    key = "cryptotest"
    fields = {"foo": [1, 2, 3], "bar": "baz"}
    await h_set_cache_to_redis(TEST_TENANT_ID, key, fields, connection=connection, enable_encryption=True)
    result = await h_get_cache_from_redis(TEST_TENANT_ID, key, list(fields.keys()), connection=connection, is_encrypted=True)
    assert result == fields


@pytest.mark.asyncio
async def test_list_keys_limit_over_10000_raises(connection):
    for i in range(10_001):
        await h_set_cache_to_redis(TEST_TENANT_ID, f"massive{i}", {"x": i}, connection=connection)
    with pytest.raises(ValueError, match="exceeded 10_000"):
        collected = []
        async for key in list_keys(TEST_TENANT_ID, "massive*", connection=connection):
            collected.append(key)


@pytest.mark.asyncio
async def test_async_readiness_ping(connection):
    """Ensure the async readiness ping works with a real Redis connection."""
    conn = get_async_redis_connection()
    assert await readiness_ping(conn) is True
    assert await readiness_ping() is True


@pytest.mark.asyncio
async def test_async_redis_connection_is_singleton():
    await init_async_redis_connection_pool(max_connections=10)
    r1 = get_async_redis_connection()
    r2 = get_async_redis_connection()
    assert r1 is r2, "Expected singleton async redis client; multiple clients = multiple pools = many connections"
