import pytest
import pytest_asyncio

from rediskit import get_redis_top_node
from rediskit.redis.a_client import (
    get_async_redis_connection,
    init_async_redis_connection_pool,
)
from rediskit.redis.a_client.count_op import counter, counter_value


@pytest_asyncio.fixture
async def tenant_id() -> str:
    return "TEST_TENANT"


@pytest_asyncio.fixture
async def counter_key() -> str:
    return "test_counter"


@pytest_asyncio.fixture
async def field_name() -> str:
    return "counter_field"


@pytest_asyncio.fixture
async def connection():
    # Match your existing async pattern
    await init_async_redis_connection_pool()
    return get_async_redis_connection()


@pytest_asyncio.fixture(autouse=True)
async def cleanup_redis(tenant_id: str, counter_key: str, connection):
    # Clear only the hash key used by these tests
    node_key = get_redis_top_node(tenant_id, counter_key)
    await connection.delete(node_key)
    yield
    await connection.delete(node_key)


@pytest.mark.asyncio
async def test_increment(tenant_id: str, counter_key: str, field_name: str, connection):
    new_count = await counter(tenant_id, field_name, 1, key=counter_key, connection=connection)
    assert new_count == 1

    value = await counter_value(tenant_id, field_name, key=counter_key, connection=connection)
    assert value == 1


@pytest.mark.asyncio
async def test_multiple_increments(tenant_id: str, counter_key: str, field_name: str, connection):
    await counter(tenant_id, field_name, 1, key=counter_key, connection=connection)
    new_count = await counter(tenant_id, field_name, 1, key=counter_key, connection=connection)
    assert new_count == 2

    value = await counter_value(tenant_id, field_name, key=counter_key, connection=connection)
    assert value == 2


@pytest.mark.asyncio
async def test_decrement(tenant_id: str, counter_key: str, field_name: str, connection):
    await counter(tenant_id, field_name, 2, key=counter_key, connection=connection)
    new_count = await counter(tenant_id, field_name, -1, key=counter_key, connection=connection)
    assert new_count == 1

    value = await counter_value(tenant_id, field_name, key=counter_key, connection=connection)
    assert value == 1


@pytest.mark.asyncio
async def test_decrement_to_zero_deletes_field(tenant_id: str, counter_key: str, field_name: str, connection):
    await counter(tenant_id, field_name, 1, key=counter_key, connection=connection)

    new_count = await counter(
        tenant_id,
        field_name,
        -1,
        key=counter_key,
        connection=connection,
        min_value=0,
    )
    assert new_count == 0

    value = await counter_value(tenant_id, field_name, key=counter_key, connection=connection)
    assert value is None


@pytest.mark.asyncio
async def test_decrement_nonexistent_with_default_min_value_deletes_field(tenant_id: str, counter_key: str, connection):
    # Default min_value=0:
    # - hincrby on a missing field returns -1
    # - counter() deletes the field because -1 <= 0
    field = "nonexistent_field"

    new_count = await counter(tenant_id, field, -1, key=counter_key, connection=connection)
    assert new_count == -1

    value = await counter_value(tenant_id, field, key=counter_key, connection=connection)
    assert value is None


@pytest.mark.asyncio
async def test_decrement_nonexistent_when_min_value_is_lower_keeps_field(tenant_id: str, counter_key: str, connection):
    field = "nonexistent_field"

    new_count = await counter(
        tenant_id,
        field,
        -1,
        key=counter_key,
        connection=connection,
        min_value=-999,
    )
    assert new_count == -1

    value = await counter_value(tenant_id, field, key=counter_key, connection=connection)
    assert value == -1
