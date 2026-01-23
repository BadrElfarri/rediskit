import pytest

from rediskit import get_redis_top_node
from rediskit.redis.client import get_redis_connection
from rediskit.redis.client.count_op import counter, counter_value


@pytest.fixture
def tenant_id() -> str:
    return "TEST_TENANT"


@pytest.fixture
def counter_key() -> str:
    # Use a test-specific key for the counter
    return "test_counter"


@pytest.fixture
def field_name() -> str:
    return "counter_field"


@pytest.fixture
def connection():
    return get_redis_connection()


@pytest.fixture(autouse=True)
def cleanup_redis(tenant_id: str, counter_key: str, connection):
    node_key = get_redis_top_node(tenant_id, counter_key)
    connection.delete(node_key)
    yield
    connection.delete(node_key)


def test_increment(tenant_id: str, counter_key: str, field_name: str, connection):
    new_count = counter(tenant_id, field_name, 1, key=counter_key, connection=connection)
    assert new_count == 1

    value = counter_value(tenant_id, field_name, key=counter_key, connection=connection)
    assert value == 1


def test_multiple_increments(tenant_id: str, counter_key: str, field_name: str, connection):
    counter(tenant_id, field_name, 1, key=counter_key, connection=connection)
    new_count = counter(tenant_id, field_name, 1, key=counter_key, connection=connection)
    assert new_count == 2

    value = counter_value(tenant_id, field_name, key=counter_key, connection=connection)
    assert value == 2


def test_decrement(tenant_id: str, counter_key: str, field_name: str, connection):
    counter(tenant_id, field_name, 2, key=counter_key, connection=connection)
    new_count = counter(tenant_id, field_name, -1, key=counter_key, connection=connection)
    assert new_count == 1

    value = counter_value(tenant_id, field_name, key=counter_key, connection=connection)
    assert value == 1


def test_decrement_to_zero_deletes_field(tenant_id: str, counter_key: str, field_name: str, connection):
    counter(tenant_id, field_name, 1, key=counter_key, connection=connection)

    new_count = counter(
        tenant_id,
        field_name,
        -1,
        key=counter_key,
        connection=connection,
        min_value=0,
    )
    assert new_count == 0

    value = counter_value(tenant_id, field_name, key=counter_key, connection=connection)
    assert value is None


def test_decrement_nonexistent_with_default_min_value_deletes_field(tenant_id: str, counter_key: str, connection):
    # With default minValue=0:
    # - hincrby on a missing field returns -1
    # - counter() then deletes the field because -1 <= 0
    field = "nonexistent_field"

    new_count = counter(tenant_id, field, -1, key=counter_key, connection=connection)
    assert new_count == -1

    value = counter_value(tenant_id, field, key=counter_key, connection=connection)
    assert value is None


def test_decrement_nonexistent_when_min_value_is_lower_keeps_field(tenant_id: str, counter_key: str, connection):
    # If you ever want "negative counts are allowed and should persist",
    # you must set minValue below the expected negative count.
    field = "nonexistent_field"

    new_count = counter(
        tenant_id,
        field,
        -1,
        key=counter_key,
        connection=connection,
        min_value=-999,
    )
    assert new_count == -1

    value = counter_value(tenant_id, field, key=counter_key, connection=connection)
    assert value == -1
