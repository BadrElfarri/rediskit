import pytest

from rediskit.redis import get_redis_top_node
from rediskit.redis.client import (
    delete,
    drain_list,
    expire,
    get_redis_connection,
    llen,
    lpop,
    lpush,
    lrange,
    rpop,
    rpush,
)

TEST_TENANT_ID = "PYTEST_REDISKIT_TENANT_LIST"


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


def _k(name: str) -> str:
    return get_redis_top_node(TEST_TENANT_ID, name)


def test_rpush_lpop_preserves_arrival_order(connection):
    key = _k("inbox")
    rpush(key, "a", connection=connection)
    rpush(key, "b", connection=connection)
    rpush(key, "c", connection=connection)

    assert llen(key, connection=connection) == 3
    assert lpop(key, connection=connection) == "a"
    assert lpop(key, connection=connection) == "b"
    assert lpop(key, connection=connection) == "c"
    assert lpop(key, connection=connection) is None


def test_lpush_and_rpop(connection):
    key = _k("stack")
    lpush(key, "a", connection=connection)
    lpush(key, "b", connection=connection)
    assert rpop(key, connection=connection) == "a"
    assert rpop(key, connection=connection) == "b"


def test_lrange(connection):
    key = _k("range")
    rpush(key, "x", "y", "z", connection=connection)
    assert lrange(key, 0, -1, connection=connection) == ["x", "y", "z"]
    assert lrange(key, 0, 0, connection=connection) == ["x"]


def test_drain_list_returns_all_in_order_and_empties(connection):
    key = _k("drain")
    rpush(key, "1", connection=connection)
    rpush(key, "2", connection=connection)
    rpush(key, "3", connection=connection)

    drained = drain_list(key, connection=connection)
    assert drained == ["1", "2", "3"]
    assert llen(key, connection=connection) == 0
    assert drain_list(key, connection=connection) == []


def test_expire_and_delete_raw_key(connection):
    key = _k("expiring")
    rpush(key, "only", connection=connection)
    assert expire(key, 60, connection=connection) is True
    assert connection.ttl(key) > 0

    assert delete(key, connection=connection) == 1
    assert llen(key, connection=connection) == 0
