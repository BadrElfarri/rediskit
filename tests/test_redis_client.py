import time

import pytest

from rediskit.redis import get_redis_top_node
from rediskit.redis.client import (
    check_cache_matches,
    delete_cache_from_redis,
    dump_blob_to_redis,
    dump_cache_to_redis,
    dump_multiple_payload_to_redis,
    get_keys,
    get_redis_connection,
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
    set_redis_cache_expiry,
    set_ttl_for_key,
)

TEST_TENANT_ID = "PYTEST_REDISKIT_TENANT"


@pytest.fixture
def connection():
    return get_redis_connection()


@pytest.fixture(autouse=True)
def cleanup_redis(connection):
    prefix = get_redis_top_node(TEST_TENANT_ID, "")
    for key in connection.scan_iter(match=f"{prefix}*"):
        connection.delete(key)
    yield
    for key in connection.scan_iter(match=f"{prefix}*"):
        connection.delete(key)


def test_dump_and_load_cache(connection):
    key = "basic"
    data = {"val": 123}
    dump_cache_to_redis(TEST_TENANT_ID, key, data, connection=connection)
    results = load_cache_from_redis(TEST_TENANT_ID, key, connection=connection)
    assert data in results
    # Exact match
    out = load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection)
    assert out == data


def test_dump_multiple_payload(connection):
    items = [{"key": f"k{i}", "payload": {"x": i}} for i in range(3)]
    dump_multiple_payload_to_redis(TEST_TENANT_ID, items, ttl=30)
    found = 0
    for i in range(3):
        val = load_exact_cache_from_redis(TEST_TENANT_ID, f"k{i}", connection=connection)
        assert val == {"x": i}
        found += 1
    assert found == 3


def test_delete_cache_from_redis(connection):
    key = "todelete"
    data = {"bye": "now"}
    dump_cache_to_redis(TEST_TENANT_ID, key, data, connection=connection)
    assert load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection) is not None
    delete_cache_from_redis(TEST_TENANT_ID, key, connection=connection)
    assert load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection) is None


def test_check_cache_matches(connection):
    key = "match"
    data = {"foo": 1, "bar": 2}
    dump_cache_to_redis(TEST_TENANT_ID, key, data, connection=connection)
    assert check_cache_matches(TEST_TENANT_ID, key, {"foo": 1, "bar": 2}, connection=connection)
    assert not check_cache_matches(TEST_TENANT_ID, key, {"foo": 999}, connection=connection)


def test_set_redis_cache_expiry_and_ttl(connection):
    key = "exp"
    data = {"a": "ttl"}
    dump_cache_to_redis(TEST_TENANT_ID, key, data, connection=connection)
    set_redis_cache_expiry(TEST_TENANT_ID, key, 2, connection=connection)
    # Should still be there immediately
    assert load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection)
    # Should be gone after TTL
    time.sleep(2.1)
    assert load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection) is None


def test_hash_set_ttl_for_key(connection):
    key = "hash_ttl"
    fields = {"a": 1, "b": 2}
    h_set_cache_to_redis(TEST_TENANT_ID, key, fields, connection=connection)
    hash_set_ttl_for_key(TEST_TENANT_ID, key, list(fields.keys()), 2, connection=connection)
    # Should exist
    assert h_get_cache_from_redis(TEST_TENANT_ID, key, None, connection=connection)
    # Wait until expired
    time.sleep(2.1)
    assert h_get_cache_from_redis(TEST_TENANT_ID, key, None, connection=connection) == {}


def test_get_keys(connection):
    k1, k2 = "gkey1", "gkey2"
    h_set_cache_to_redis(TEST_TENANT_ID, k1, {"a": 1}, connection=connection)
    h_set_cache_to_redis(TEST_TENANT_ID, k2, {"a": 2}, connection=connection)
    keys = get_keys(TEST_TENANT_ID, "*", connection=connection, only_last_key=True)
    # At least the two we just created
    assert set([k1, k2]).issubset(set(keys))


def test_set_ttl_for_key(connection):
    key = "ttlkey"
    data = {"foo": "bar"}
    dump_cache_to_redis(TEST_TENANT_ID, key, data, connection=connection)
    set_ttl_for_key(TEST_TENANT_ID, key, 2, connection=connection)
    assert load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection)
    time.sleep(2.1)
    assert load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection) is None


def test_set_ttl_for_key_directly(connection):
    key = "ttlkey"
    data = {"foo": "bar"}
    dump_cache_to_redis(TEST_TENANT_ID, key, data, connection=connection, ttl=2)
    assert load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection)
    time.sleep(2.1)
    assert load_exact_cache_from_redis(TEST_TENANT_ID, key, connection=connection) is None


def test_dump_and_load_blob(connection):
    key = "blobkey"
    blob = "this is a test blob"
    dump_blob_to_redis(TEST_TENANT_ID, key, blob, connection=connection, ttl=10)
    loaded = load_blob_from_redis(TEST_TENANT_ID, key, connection=connection)
    assert loaded == blob


def test_h_scan_fields(connection):
    key = "scanfields"
    fields = {"f1": 10, "f2": 20, "hello": 30}
    h_set_cache_to_redis(TEST_TENANT_ID, key, fields, connection=connection)
    matched = h_scan_fields(TEST_TENANT_ID, key, match="f*", connection=connection)
    # Should only return fields that match "f*"
    assert set(matched) == {"f1", "f2"}


def test_list_keys_limit_over_10000_raises(connection):
    # Insert 10,001 keys (be aware this is slow; skip in CI unless needed)
    for i in range(10_001):
        h_set_cache_to_redis(TEST_TENANT_ID, f"massive{i}", {"x": i}, connection=connection)
    with pytest.raises(ValueError, match="exceeded 10_000"):
        list(list_keys(TEST_TENANT_ID, "massive*", connection=connection))


def test_h_get_invalid_field_type(connection):
    key = "badtype"
    h_set_cache_to_redis(TEST_TENANT_ID, key, {"f": 1}, connection=connection)
    with pytest.raises(ValueError):
        h_get_cache_from_redis(TEST_TENANT_ID, key, 42, connection=connection)


def test_h_del_invalid_field_type(connection):
    key = "delfail"
    h_set_cache_to_redis(TEST_TENANT_ID, key, {"f": 1}, connection=connection)
    with pytest.raises(ValueError):
        h_del_cache_from_redis(TEST_TENANT_ID, key, 1.23, connection=connection)


# All the "field variants" and encrypted hash tests you already have are great; you can copy your examples here too!


def test_h_get_fields_variants(connection):
    key = "hfields"
    fields = {"s": "value", "n": 42}
    h_set_cache_to_redis(TEST_TENANT_ID, key, fields, connection=connection)
    # Get single field (str)
    res = h_get_cache_from_redis(TEST_TENANT_ID, key, "s", connection=connection)
    assert res == {"s": "value"}
    # Get multiple (list)
    res2 = h_get_cache_from_redis(TEST_TENANT_ID, key, ["s", "n"], connection=connection)
    assert res2 == fields
    # All (None)
    res3 = h_get_cache_from_redis(TEST_TENANT_ID, key, None, connection=connection)
    assert res3 == fields


def test_h_set_and_get_encrypted(connection):
    key = "cryptotest"
    fields = {"foo": [1, 2, 3], "bar": "baz"}
    h_set_cache_to_redis(TEST_TENANT_ID, key, fields, connection=connection, enable_encryption=True)
    result = h_get_cache_from_redis(TEST_TENANT_ID, key, list(fields.keys()), connection=connection, is_encrypted=True)
    assert result == fields


def test_h_del_cache_from_redis_dict_and_list(connection):
    key = "deltest"
    fields = {"a": 111, "b": 222, "c": 333}
    h_set_cache_to_redis(TEST_TENANT_ID, key, fields, connection=connection)
    h_del_cache_from_redis(TEST_TENANT_ID, key, {"a": fields["a"]}, connection=connection)
    state = h_get_cache_from_redis(TEST_TENANT_ID, key, None, connection=connection)
    assert set(state.keys()) == {"b", "c"}
    h_del_cache_from_redis(TEST_TENANT_ID, key, ["b", "c"], connection=connection)
    state = h_get_cache_from_redis(TEST_TENANT_ID, key, None, connection=connection)
    assert state == {}


def test_list_keys_yields_all(connection):
    for i in range(3):
        h_set_cache_to_redis(TEST_TENANT_ID, f"key{i}", {"val": i}, connection=connection)
    result = list(list_keys(TEST_TENANT_ID, "*", connection=connection))
    assert len(result) >= 3


def test_sync_readiness_ping():
    conn = get_redis_connection()
    assert readiness_ping(conn) is True
    assert readiness_ping() is True
