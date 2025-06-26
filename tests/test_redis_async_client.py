import uuid

import pytest

from rediskit.redis_client import redis_single_connection_context

TEST_TENANT_ID = "TEST_SEMAPHORE_TENANT_REDIS"


@pytest.mark.asyncio
async def test_redis_single_connection_context_sets_and_gets():
    test_key = f"test:{uuid.uuid4()}"
    test_val = "value42"

    # Set and delete within the context
    async with redis_single_connection_context() as redis:
        await redis.set(test_key, test_val)
        val = await redis.get(test_key)
        assert val == test_val

        await redis.delete(test_key)
        val_after_delete = await redis.get(test_key)
        assert val_after_delete is None

    # New context: confirm key is still deleted
    async with redis_single_connection_context() as redis2:
        val_after_context = await redis2.get(test_key)
        assert val_after_context is None
