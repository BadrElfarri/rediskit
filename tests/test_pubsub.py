import asyncio
import uuid

import pytest

from rediskit import redis_client
from rediskit.pubsub import FanoutBroker, apublish, iter_channel, publish, subscribe_channel


@pytest.mark.asyncio
async def test_pubsub_roundtrip_recovers_python_objects():
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()
    channel = f"rediskit:test:pubsub:{uuid.uuid4()}"

    payloads = [
        {"id": "1", "status": "created", "total": 10},
        "plain-text",
        b"raw-bytes",
    ]

    received: list[object] = []

    async def consume() -> None:
        async for message in iter_channel(channel):
            received.append(message)
            if len(received) == len(payloads):
                break

    consumer_task = asyncio.create_task(consume())

    await asyncio.sleep(0.05)
    publish(channel, payloads[0])
    await apublish(channel, payloads[1])
    await apublish(channel, payloads[2])

    await asyncio.wait_for(consumer_task, timeout=5)

    assert received == [payloads[0], payloads[1], "raw-bytes"]


@pytest.mark.asyncio
async def test_channel_subscription_can_be_closed():
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()
    channel = f"rediskit:test:pubsub:close:{uuid.uuid4()}"

    subscription = await subscribe_channel(channel)

    payload = {"id": "99", "status": "done", "total": 1}
    await apublish(channel, payload)

    received = await asyncio.wait_for(anext(subscription), timeout=5)
    assert received == payload

    await subscription.aclose()

    # Allow Redis to process the unsubscribe before inspection
    await asyncio.sleep(0.05)
    conn = redis_client.get_async_redis_connection()
    counts = await conn.pubsub_numsub(channel)
    if counts:
        assert counts[0][1] == 0

    with pytest.raises(StopAsyncIteration):
        await anext(subscription)


@pytest.mark.asyncio
async def test_fanout_broker_broadcasts_to_multiple_handles():
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()
    channel = f"rediskit:test:fanout:{uuid.uuid4()}"

    broker = FanoutBroker()
    await broker.start(channels=[channel])

    handle_one = await broker.subscribe(channel)
    handle_two = await broker.subscribe(channel)

    payload = {"id": "1", "status": "created", "total": 42}

    waiter_one = asyncio.create_task(asyncio.wait_for(anext(handle_one), timeout=5))
    waiter_two = asyncio.create_task(asyncio.wait_for(anext(handle_two), timeout=5))

    await asyncio.sleep(0.05)
    await apublish(channel, payload)

    received_one, received_two = await asyncio.gather(waiter_one, waiter_two)

    assert received_one == payload
    assert received_two == payload

    await handle_one.unsubscribe()
    await handle_two.unsubscribe()
    await broker.stop()


@pytest.mark.asyncio
async def test_fanout_broker_pattern_delivery_and_queue_overflow():
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()
    base = f"rediskit:test:fanout-pattern:{uuid.uuid4()}"
    channel = f"{base}:orders"

    broker = FanoutBroker(patterns=[f"{base}:*"])
    await broker.start()

    handle = await broker.subscribe(channel, maxsize=1)

    # Publish multiple messages before the consumer pulls to force queue eviction.
    await apublish(channel, {"id": "1", "status": "created"})
    await apublish(channel, {"id": "2", "status": "done"})

    await asyncio.sleep(0.1)

    latest = await asyncio.wait_for(anext(handle), timeout=5)
    assert latest == {"id": "2", "status": "done"}

    await handle.unsubscribe()
    await broker.stop()

