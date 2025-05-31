import pytest

from rediskit.redisClient import GetRedisConnection, GetRedisTopNode, HDelCacheFromRedis, HGetCacheFromRedis, HSetCacheToRedis, ListKeys

TEST_TENANT_ID = "TEST_HASH_TENANT_REDIS"


@pytest.fixture
def Connection():
    return GetRedisConnection()


@pytest.fixture(autouse=True)
def CleanupRedis(Connection):
    prefix = GetRedisTopNode(TEST_TENANT_ID, "")
    for key in Connection.scan_iter(match=f"{prefix}*"):
        Connection.delete(key)
    yield
    for key in Connection.scan_iter(match=f"{prefix}*"):
        Connection.delete(key)


def test_HSetCacheToRedis_and_HGetCacheFromRedis_plain(Connection):
    node_key = "mytest"
    fields = {"a": 1, "b": "hello"}
    HSetCacheToRedis(TEST_TENANT_ID, node_key, fields, connection=Connection)
    result = HGetCacheFromRedis(TEST_TENANT_ID, node_key, None, connection=Connection)
    assert result == fields


def test_HSetCacheToRedis_with_ttl(Connection):
    node_key = "ttltest"
    fields = {"x": 99}
    HSetCacheToRedis(TEST_TENANT_ID, node_key, fields, connection=Connection, ttl=30)
    result = HGetCacheFromRedis(TEST_TENANT_ID, node_key, list(fields.keys()), connection=Connection)
    assert result == fields


def test_HSetCacheToRedis_and_HGetCacheFromRedis_encrypted(Connection):
    node_key = "cryptotest"
    fields = {"foo": [1, 2, 3], "bar": "baz"}
    HSetCacheToRedis(TEST_TENANT_ID, node_key, fields, connection=Connection, enableEncryption=True)
    # By default, must specify isEncrypted
    result = HGetCacheFromRedis(TEST_TENANT_ID, node_key, list(fields.keys()), connection=Connection, isEncrypted=True)
    assert result == fields


def test_HDelCacheFromRedis_dict_and_list(Connection):
    node_key = "deltest"
    fields = {"a": 111, "b": 222, "c": 333}
    HSetCacheToRedis(TEST_TENANT_ID, node_key, fields, connection=Connection)

    # Delete one by dict
    HDelCacheFromRedis(TEST_TENANT_ID, node_key, {"a": fields["a"]}, connection=Connection)
    state = HGetCacheFromRedis(TEST_TENANT_ID, node_key, None, connection=Connection)
    assert set(state.keys()) == {"b", "c"}

    # Delete rest by list
    HDelCacheFromRedis(TEST_TENANT_ID, node_key, ["b", "c"], connection=Connection)
    state = HGetCacheFromRedis(TEST_TENANT_ID, node_key, None, connection=Connection)
    assert state == {}


def test_HGetCacheFromRedis_fields_variants(Connection):
    node_key = "multitype"
    fields = {"s": "value", "n": 42}
    HSetCacheToRedis(TEST_TENANT_ID, node_key, fields, connection=Connection)
    # Get single field (str)
    res = HGetCacheFromRedis(TEST_TENANT_ID, node_key, "s", connection=Connection)
    assert res == {"s": "value"}
    # Get multiple (list)
    res2 = HGetCacheFromRedis(TEST_TENANT_ID, node_key, ["s", "n"], connection=Connection)
    assert res2 == fields
    # All (None)
    res3 = HGetCacheFromRedis(TEST_TENANT_ID, node_key, None, connection=Connection)
    assert res3 == fields


def test_HGetCacheFromRedis_invalid_field_type(Connection):
    node_key = "badtype"
    HSetCacheToRedis(TEST_TENANT_ID, node_key, {"f": 1}, connection=Connection)
    with pytest.raises(ValueError):
        HGetCacheFromRedis(TEST_TENANT_ID, node_key, 42, connection=Connection)


def test_HDelCacheFromRedis_invalid_field_type(Connection):
    node_key = "delfail"
    HSetCacheToRedis(TEST_TENANT_ID, node_key, {"f": 1}, connection=Connection)
    with pytest.raises(ValueError):
        HDelCacheFromRedis(TEST_TENANT_ID, node_key, 1.23, connection=Connection)


def test_ListKeys_yields_all(Connection):
    # Populate multiple keys
    for i in range(3):
        HSetCacheToRedis(TEST_TENANT_ID, f"key{i}", {"val": i}, connection=Connection)
    # All keys for this "directory"
    result = list(ListKeys(TEST_TENANT_ID, "*", connection=Connection))
    assert len(result) >= 3  # At least 3, may have more if other tests collide


def test_ListKeys_limit_over_10000_raises(Connection):
    # Insert 10,001 keys
    for i in range(10_001):
        HSetCacheToRedis(TEST_TENANT_ID, f"massive{i}", {"x": i}, connection=Connection)
    with pytest.raises(ValueError, match="exceeded 10_000"):
        list(ListKeys(TEST_TENANT_ID, "massive*", connection=Connection))
