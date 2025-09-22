import asyncio
import json
import uuid

import pytest

from rediskit import redis_client
from rediskit.pubsub import (
    FanoutBroker,
    _default_decoder,
    _default_encoder,
    apublish,
    iter_channel,
    publish,
    subscribe_channel,
)

# ------------------- Encoding / Decoding -------------------


def test_default_encoder_decoder_variants_roundtrip():
    redis_client.init_redis_connection_pool()

    # bytes -> stays bytes if invalid UTF-8, else becomes str
    b = b"\xff\xfe"  # invalid UTF-8
    assert _default_encoder(b) == b
    assert _default_decoder(b) == b

    # bytearray -> bytes on encode; decoder converts valid UTF-8 to str
    ba = bytearray(b"abc")
    assert _default_encoder(ba) == b"abc"
    # "xyz" is valid utf-8 so decoder returns str, not bytes:
    assert _default_decoder(bytearray(b"xyz")) == "xyz"

    # plain string stays string when not JSON
    s = "hello world"
    assert _default_encoder(s) == s
    assert _default_decoder(s) == s

    # JSON-serializable object encodes as JSON string and decodes back to object
    obj = {"id": 123, "tags": ["a", "b"], "flag": True}
    enc = _default_encoder(obj)
    assert isinstance(enc, str)
    assert _default_decoder(enc) == obj


@pytest.mark.asyncio
async def test_publish_with_custom_encoder_delivers_known_string_even_with_decode_responses():
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()

    channel = f"rediskit:test:encoder:{uuid.uuid4()}"

    # Use a UTF-8 *safe* custom encoding so clients with decode_responses=True don’t throw.
    EXPECT = "__CUSTOM_BYTES_OK__☃"

    def custom_encoder(_):
        return EXPECT  # valid UTF-8, not JSON

    received = []

    async def consume_once():
        async for msg in iter_channel(channel):
            received.append(msg)
            break

    t = asyncio.create_task(consume_once())
    await asyncio.sleep(0.05)
    publish(channel, {"any": "data"}, encoder=custom_encoder)
    await asyncio.wait_for(t, timeout=5)

    # With default decoder, this will come through as the same string
    assert received == [EXPECT]


# ------------------- subscribe_channel & iter_channel cleanup -------------------


@pytest.mark.asyncio
async def test_iter_channel_closes_subscription_when_consumer_breaks():
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()

    channel = f"rediskit:test:iter-clean:{uuid.uuid4()}"
    conn = redis_client.get_async_redis_connection()

    async def consumer():
        async for m in iter_channel(channel):
            _ = m
            break  # stop quickly; iterator should close underlying subscription

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.05)
    await apublish(channel, {"ok": 1})
    await asyncio.wait_for(task, timeout=5)

    await asyncio.sleep(0.05)
    counts = await conn.pubsub_numsub(channel)
    if counts:
        assert counts[0][1] == 0


@pytest.mark.asyncio
async def test_subscribe_channel_as_context_manager_unsubscribes():
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()

    channel = f"rediskit:test:ctx:{uuid.uuid4()}"
    conn = redis_client.get_async_redis_connection()

    sub = await subscribe_channel(channel)
    async with sub:
        await apublish(channel, {"x": 1})
        got = await asyncio.wait_for(anext(sub), timeout=5)
        assert got == {"x": 1}

    await asyncio.sleep(0.05)
    counts = await conn.pubsub_numsub(channel)
    if counts:
        assert counts[0][1] == 0


@pytest.mark.asyncio
async def test_subscribe_channel_with_health_check_interval_if_supported():
    import asyncio
    import uuid

    import pytest

    from rediskit import redis_client
    from rediskit.pubsub import apublish, subscribe_channel

    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()

    channel = f"rediskit:test:health:{uuid.uuid4()}"

    try:
        sub = await subscribe_channel(channel, health_check_interval=1.0)
    except TypeError:
        pytest.skip("redis-py PubSub does not support health_check_interval in this environment")

    try:
        await apublish(channel, {"ok": True})
        got = await asyncio.wait_for(anext(sub), timeout=5)
        assert got == {"ok": True}
    finally:
        await sub.aclose()
        # allow unsubscribing/close to flush
        await asyncio.sleep(0)


# ------------------- FanoutBroker lifecycle & behavior -------------------


@pytest.mark.asyncio
async def test_fanout_broker_requires_start_before_subscribe():
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()

    broker = FanoutBroker()
    with pytest.raises(RuntimeError):
        await broker.subscribe("whatever")


@pytest.mark.asyncio
async def test_fanout_broker_stop_drains_and_consumers_finish():
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()

    channel = f"rediskit:test:drain:{uuid.uuid4()}"
    broker = FanoutBroker()
    await broker.start(channels=[channel])

    handle = await broker.subscribe(channel)

    # consume from the handle directly (avoid async generator StopAsyncIteration -> RuntimeError)
    async def consume_all():
        async for _ in handle:
            pass

    task = asyncio.create_task(consume_all())

    # tick the broker loop once
    await apublish(channel, {"noop": True})
    await asyncio.sleep(0.05)

    await broker.stop()

    await asyncio.wait_for(task, timeout=5)


@pytest.mark.asyncio
async def test_fanout_broker_broadcasts_to_multiple_handles_channel_and_pattern():
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()

    base = f"rediskit:test:fanout-multi:{uuid.uuid4()}"
    channel = f"{base}:news"
    pattern = f"{base}:*"

    broker = FanoutBroker(patterns=[pattern])
    await broker.start(channels=[channel])

    on_channel = await broker.subscribe(channel)
    on_pattern = await broker.subscribe(pattern)

    payload = {"msg": "hello"}

    waiter_channel = asyncio.create_task(asyncio.wait_for(anext(on_channel), timeout=5))
    waiter_pattern = asyncio.create_task(asyncio.wait_for(anext(on_pattern), timeout=5))

    await asyncio.sleep(0.05)
    await apublish(channel, payload)

    got_channel, got_pattern = await asyncio.gather(waiter_channel, waiter_pattern)
    assert got_channel == payload
    assert got_pattern == payload

    await on_channel.unsubscribe()
    await on_pattern.unsubscribe()
    await broker.stop()


@pytest.mark.asyncio
async def test_fanout_broker_queue_overflow_keeps_latest():
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()

    base = f"rediskit:test:overflow:{uuid.uuid4()}"
    channel = f"{base}:orders"

    broker = FanoutBroker()
    await broker.start(channels=[channel])

    handle = await broker.subscribe(channel, maxsize=1)

    await apublish(channel, {"id": "1"})
    await apublish(channel, {"id": "2"})
    await asyncio.sleep(0.1)

    latest = await asyncio.wait_for(anext(handle), timeout=5)
    assert latest == {"id": "2"}

    await handle.unsubscribe()
    await broker.stop()


@pytest.mark.asyncio
async def test_fanout_broker_decoder_exception_falls_back_to_raw():
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()

    channel = f"rediskit:test:decoder-fallback:{uuid.uuid4()}"

    def bad_decoder(_):
        raise RuntimeError("boom")

    broker = FanoutBroker(decoder=bad_decoder)
    await broker.start(channels=[channel])

    handle = await broker.subscribe(channel)

    payload = {"x": 1}
    await asyncio.sleep(0.05)
    await apublish(channel, payload)

    received = await asyncio.wait_for(anext(handle), timeout=5)
    # apublish uses default encoder -> JSON string; since decoder failed, raw JSON string should be delivered
    assert received == json.dumps(payload)

    await handle.unsubscribe()
    await broker.stop()


# ------------------- Core flows -------------------


@pytest.mark.asyncio
async def test_pubsub_roundtrip_recovers_python_objects_basics():
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
async def test_channel_subscription_can_be_closed_and_rejected_after():
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()

    channel = f"rediskit:test:pubsub:close:{uuid.uuid4()}"

    subscription = await subscribe_channel(channel)

    payload = {"id": "99", "status": "done", "total": 1}
    await apublish(channel, payload)

    received = await asyncio.wait_for(anext(subscription), timeout=5)
    assert received == payload

    await subscription.aclose()

    await asyncio.sleep(0.05)
    conn = redis_client.get_async_redis_connection()
    counts = await conn.pubsub_numsub(channel)
    if counts:
        assert counts[0][1] == 0

    with pytest.raises(StopAsyncIteration):
        await anext(subscription)


# ------------------- SubscriptionHandle conveniences -------------------
@pytest.mark.asyncio
async def test_subscription_handle_iter_auto_unsubscribes_and_stays_silent():
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()

    channel = f"rediskit:test:handle-iter:{uuid.uuid4()}"
    broker = FanoutBroker()
    await broker.start(channels=[channel])
    handle = await broker.subscribe(channel)

    # Publish just one message; we'll consume it and then close the iterator.
    await apublish(channel, {"n": 1})

    it = handle.iter()
    got_first = await asyncio.wait_for(anext(it), timeout=5)
    assert got_first == {"n": 1}

    # Closing the iterator unsubscribes from the broker.
    await it.aclose()

    # Publish again; since we're unsubscribed, the handle should not receive it.
    await apublish(channel, {"n": 2})

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(anext(handle), timeout=0.3)

    await broker.stop()


# ------------------- 10_000 subscribers -------------------
CHANNEL = "test:fanout:stress"
RECEIVE_TIMEOUT = 5.0  # seconds


async def _subscribe_many(broker: FanoutBroker, n: int):
    # Use maxsize=1 to keep memory footprint minimal (we only need 1 message)
    handles = await asyncio.gather(*[broker.subscribe(CHANNEL, maxsize=1) for _ in range(n)])
    return handles


async def _recv_one(handle):
    # Each handle should receive exactly one message after we publish
    return await asyncio.wait_for(handle.__anext__(), timeout=RECEIVE_TIMEOUT)


@pytest.mark.asyncio
async def test_stress_test_10_000_subscribers_and_1_message_per_subscriber():
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()

    N = 10_000
    broker = FanoutBroker()
    await broker.start(channels=[CHANNEL])

    # Create 10,000 subscribers (maxsize=1 keeps memory small)
    handles = await _subscribe_many(broker, N)

    # Single publish all should see
    payload = 1  # keep the message tiny
    await apublish(CHANNEL, payload)

    # Drain one message from each subscriber
    received = await asyncio.gather(*[_recv_one(h) for h in handles])

    assert len(received) == N
    assert all(r == payload for r in received)

    # Cleanup
    await asyncio.gather(*[h.unsubscribe() for h in handles])


# ------------------- Test patterns -------------------


async def _next(item, timeout=1.0):
    return await asyncio.wait_for(item.__anext__(), timeout)


@pytest.mark.asyncio
async def test_exact_and_pattern_subscribers_receive_expected_messages():
    """
    Given a broker PSUBSCRIBE'd to 'SomeRouteName:*':
      - sub1 (exact 'SomeRouteName:SomeName') gets only message 1
      - sub2 (exact 'SomeRouteName:SomeName:SomethingElse') gets only message 2
      - subAll (pattern 'SomeRouteName:*') gets both
    """
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()

    broker = FanoutBroker(patterns=["SomeRouteName:*"])
    await broker.start()

    try:
        sub1 = await broker.subscribe("SomeRouteName:SomeName")
        sub2 = await broker.subscribe("SomeRouteName:SomeName:SomethingElse")
        subAll = await broker.subscribe("SomeRouteName:*")

        # Publish two different channel messages
        await apublish("SomeRouteName:SomeName", {"msg": 1})
        await apublish("SomeRouteName:SomeName:SomethingElse", {"msg": 2})

        # sub1: should only see msg 1
        m1 = await _next(sub1)
        assert m1 == {"msg": 1}
        # Ensure sub1 does not receive msg 2 quickly
        with pytest.raises(asyncio.TimeoutError):
            await _next(sub1, timeout=0.2)

        # sub2: should only see msg 2
        m2 = await _next(sub2)
        assert m2 == {"msg": 2}
        with pytest.raises(asyncio.TimeoutError):
            await _next(sub2, timeout=0.2)

        # subAll (pattern): should see both, in order of publish
        p1 = await _next(subAll)
        p2 = await _next(subAll)
        assert {tuple(sorted(p.items())) for p in (p1, p2)} == {
            tuple(sorted({"msg": 1}.items())),
            tuple(sorted({"msg": 2}.items())),
        }

    finally:
        await broker.stop()


@pytest.mark.asyncio
async def test_wildcard_in_channels_is_treated_as_pattern():
    """
    start(channels=['SomeRouteName:*']) should internally PSUBSCRIBE,
    so pattern subscriber and exact subscribers both get messages.
    (Relies on the start() patch that auto-moves wildcards to patterns.)
    """
    redis_client.init_redis_connection_pool()
    await redis_client.init_async_redis_connection_pool()

    broker = FanoutBroker()
    # Pass wildcard in channels on purpose; start() should convert it to PSUBSCRIBE
    await broker.start(channels=["SomeRouteName:*"])

    try:
        sub_exact = await broker.subscribe("SomeRouteName:SomeName")
        sub_pattern = await broker.subscribe("SomeRouteName:*")

        await apublish("SomeRouteName:SomeName", {"ok": True})

        e = await _next(sub_exact)
        p = await _next(sub_pattern)
        assert e == {"ok": True}
        assert p == {"ok": True}

    finally:
        await broker.stop()
