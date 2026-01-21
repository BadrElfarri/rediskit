import asyncio
import contextlib
import json
import uuid

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from rediskit.redis.a_client import get_async_redis_connection, init_async_redis_connection_pool
from rediskit.redis.a_client.pubsub import (
    FanoutBroker,
    iter_channel,
    publish,
    subscribe_channel,
)
from rediskit.redis.client import init_redis_connection_pool
from rediskit.redis.client import publish as sync_publish
from rediskit.redis.encoder import _default_decoder, _default_encoder

# ------------------- Encoding / Decoding -------------------


def test_default_encoder_decoder_variants_roundtrip():
    init_redis_connection_pool()

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
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

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
    sync_publish(channel, {"any": "data"}, encoder=custom_encoder)
    await asyncio.wait_for(t, timeout=5)

    # With default decoder, this will come through as the same string
    assert received == [EXPECT]


# ------------------- subscribe_channel & iter_channel cleanup -------------------


@pytest.mark.asyncio
async def test_iter_channel_closes_subscription_when_consumer_breaks():
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

    channel = f"rediskit:test:iter-clean:{uuid.uuid4()}"
    conn = get_async_redis_connection()

    async def consumer():
        async for m in iter_channel(channel):
            _ = m
            break  # stop quickly; iterator should close underlying subscription

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.05)
    await publish(channel, {"ok": 1})
    await asyncio.wait_for(task, timeout=5)

    await asyncio.sleep(0.05)
    counts = await conn.pubsub_numsub(channel)
    if counts:
        assert counts[0][1] == 0


@pytest.mark.asyncio
async def test_subscribe_channel_as_context_manager_unsubscribes():
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

    channel = f"rediskit:test:ctx:{uuid.uuid4()}"
    conn = get_async_redis_connection()

    sub = await subscribe_channel(channel)
    async with sub:
        await publish(channel, {"x": 1})
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

    from rediskit.redis.a_client.pubsub import publish, subscribe_channel

    init_redis_connection_pool()
    await init_async_redis_connection_pool()

    channel = f"rediskit:test:health:{uuid.uuid4()}"

    try:
        sub = await subscribe_channel(channel, health_check_interval=1.0)
    except TypeError:
        pytest.skip("redis-py PubSub does not support health_check_interval in this environment")

    try:
        await publish(channel, {"ok": True})
        got = await asyncio.wait_for(anext(sub), timeout=5)
        assert got == {"ok": True}
    finally:
        await sub.aclose()
        # allow unsubscribing/close to flush
        await asyncio.sleep(0)


# ------------------- FanoutBroker lifecycle & behavior -------------------


@pytest.mark.asyncio
async def test_fanout_broker_requires_start_before_subscribe():
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

    broker = FanoutBroker()
    with pytest.raises(RuntimeError):
        await broker.subscribe("whatever")


@pytest.mark.asyncio
async def test_fanout_broker_stop_drains_and_consumers_finish():
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

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
    await publish(channel, {"noop": True})
    await asyncio.sleep(0.05)

    await broker.stop()

    await asyncio.wait_for(task, timeout=5)


@pytest.mark.asyncio
async def test_fanout_broker_broadcasts_to_multiple_handles_channel_and_pattern():
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

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
    await publish(channel, payload)

    got_channel, got_pattern = await asyncio.gather(waiter_channel, waiter_pattern)
    assert got_channel == payload
    assert got_pattern == payload

    await on_channel.unsubscribe()
    await on_pattern.unsubscribe()
    await broker.stop()


@pytest.mark.asyncio
async def test_fanout_broker_queue_overflow_keeps_latest():
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

    base = f"rediskit:test:overflow:{uuid.uuid4()}"
    channel = f"{base}:orders"

    broker = FanoutBroker()
    await broker.start(channels=[channel])

    handle = await broker.subscribe(channel, maxsize=1)

    await publish(channel, {"id": "1"})
    await publish(channel, {"id": "2"})
    await asyncio.sleep(0.1)

    latest = await asyncio.wait_for(anext(handle), timeout=5)
    assert latest == {"id": "2"}

    await handle.unsubscribe()
    await broker.stop()


@pytest.mark.asyncio
async def test_fanout_broker_decoder_exception_falls_back_to_raw():
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

    channel = f"rediskit:test:decoder-fallback:{uuid.uuid4()}"

    def bad_decoder(_):
        raise RuntimeError("boom")

    broker = FanoutBroker(decoder=bad_decoder)
    await broker.start(channels=[channel])

    handle = await broker.subscribe(channel)

    payload = {"x": 1}
    await asyncio.sleep(0.05)
    await publish(channel, payload)

    received = await asyncio.wait_for(anext(handle), timeout=5)
    # apublish uses default encoder -> JSON string; since decoder failed, raw JSON string should be delivered
    assert received == json.dumps(payload)

    await handle.unsubscribe()
    await broker.stop()


# ------------------- Core flows -------------------


@pytest.mark.asyncio
async def test_pubsub_roundtrip_recovers_python_objects_basics():
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

    channel = f"rediskit:test:pubsub:{uuid.uuid4()}"

    payloads = [
        {"id": "1", "status": "created", "total": 10},
        "plain-text",
        b"raw-bytes",
    ]

    received = []

    async def consume() -> None:
        async for message in iter_channel(channel):
            received.append(message)
            if len(received) == len(payloads):
                break

    consumer_task = asyncio.create_task(consume())

    await asyncio.sleep(0.05)
    sync_publish(channel, payloads[0])
    await publish(channel, payloads[1])
    await publish(channel, payloads[2])

    await asyncio.wait_for(consumer_task, timeout=5)
    assert received == [payloads[0], payloads[1], "raw-bytes"]


@pytest.mark.asyncio
async def test_channel_subscription_can_be_closed_and_rejected_after():
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

    channel = f"rediskit:test:pubsub:close:{uuid.uuid4()}"

    subscription = await subscribe_channel(channel)

    payload = {"id": "99", "status": "done", "total": 1}
    await publish(channel, payload)

    received = await asyncio.wait_for(anext(subscription), timeout=5)
    assert received == payload

    await subscription.aclose()

    await asyncio.sleep(0.05)
    conn = get_async_redis_connection()
    counts = await conn.pubsub_numsub(channel)
    if counts:
        assert counts[0][1] == 0

    with pytest.raises(StopAsyncIteration):
        await anext(subscription)


# ------------------- SubscriptionHandle conveniences -------------------
@pytest.mark.asyncio
async def test_subscription_handle_iter_auto_unsubscribes_and_stays_silent():
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

    channel = f"rediskit:test:handle-iter:{uuid.uuid4()}"
    broker = FanoutBroker()
    await broker.start(channels=[channel])
    handle = await broker.subscribe(channel)

    # Publish just one message; we'll consume it and then close the iterator.
    await publish(channel, {"n": 1})

    it = handle.iter()
    got_first = await asyncio.wait_for(anext(it), timeout=5)
    assert got_first == {"n": 1}

    # Closing the iterator unsubscribes from the broker.
    await it.aclose()

    # Publish again; since we're unsubscribed, the handle should not receive it.
    await publish(channel, {"n": 2})

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
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

    N = 10_000
    broker = FanoutBroker()
    await broker.start(channels=[CHANNEL])

    # Create 10,000 subscribers (maxsize=1 keeps memory small)
    handles = await _subscribe_many(broker, N)

    # Single publish all should see
    payload = 1  # keep the message tiny
    await publish(CHANNEL, payload)

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
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

    broker = FanoutBroker(patterns=["SomeRouteName:*"])
    await broker.start()

    try:
        sub1 = await broker.subscribe("SomeRouteName:SomeName")
        sub2 = await broker.subscribe("SomeRouteName:SomeName:SomethingElse")
        subAll = await broker.subscribe("SomeRouteName:*")

        # Publish two different channel messages
        await publish("SomeRouteName:SomeName", {"msg": 1})
        await publish("SomeRouteName:SomeName:SomethingElse", {"msg": 2})

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
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

    broker = FanoutBroker()
    # Pass wildcard in channels on purpose; start() should convert it to PSUBSCRIBE
    await broker.start(channels=["SomeRouteName:*"])

    try:
        sub_exact = await broker.subscribe("SomeRouteName:SomeName")
        sub_pattern = await broker.subscribe("SomeRouteName:*")

        await publish("SomeRouteName:SomeName", {"ok": True})

        e = await _next(sub_exact)
        p = await _next(sub_pattern)
        assert e == {"ok": True}
        assert p == {"ok": True}

    finally:
        await broker.stop()


@pytest.mark.asyncio
async def test_fanout_broker_auto_restart_on_subscribe_after_task_dies():
    """
    If the background task dies, subscribe() should auto-restart the broker using last start kwargs.
    """
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

    channel = f"rediskit:test:auto-restart:{uuid.uuid4()}"
    broker = FanoutBroker()
    await broker.start(channels=[channel])

    # Simulate a crash: cancel the internal task without calling broker.stop()
    assert broker._task is not None
    broker._task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await broker._task
    # Now task is done
    assert broker._task is not None and broker._task.done()

    # subscribe() should auto-restart using the remembered start kwargs
    handle = await broker.subscribe(channel)

    # Roundtrip still works
    await publish(channel, {"ok": 1})
    got = await asyncio.wait_for(anext(handle), timeout=5)
    assert got == {"ok": 1}

    await handle.unsubscribe()
    await broker.stop()


@pytest.mark.asyncio
async def test_fanout_broker_reconnects_and_resubscribes_after_connection_error(monkeypatch):
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

    base = f"rediskit:test:reconnect:{uuid.uuid4()}"
    channel = f"{base}:news"
    pattern = f"{base}:*"

    broker = FanoutBroker(patterns=[pattern])
    await broker.start(channels=[channel])

    exact = await broker.subscribe(channel)
    pat = await broker.subscribe(pattern)

    ps = broker._ps
    assert ps is not None

    # Make get_message() raise once, then behave
    raised = {"done": False}

    async def flaky_get_message(*args, **kwargs):
        if not raised["done"]:
            raised["done"] = True
            raise RedisConnectionError("simulated drop")
        await asyncio.sleep(0)  # yield
        return None

    monkeypatch.setattr(ps, "get_message", flaky_get_message)

    # Track reconnect via an Event (avoid timing races)
    reconnected = asyncio.Event()
    orig_reconnect = broker._reconnect

    async def tracked_reconnect():
        try:
            await orig_reconnect()
        finally:
            reconnected.set()

    monkeypatch.setattr(broker, "_reconnect", tracked_reconnect)

    # Wait until the broker actually attempted a reconnect
    await asyncio.wait_for(reconnected.wait(), timeout=3.0)

    # After reconnect, normal delivery should work for both subs
    payload = {"hello": "world"}
    await publish(channel, payload)
    g1 = await asyncio.wait_for(anext(exact), timeout=5)
    g2 = await asyncio.wait_for(anext(pat), timeout=5)
    assert g1 == payload
    assert g2 == payload

    await exact.unsubscribe()
    await pat.unsubscribe()
    await broker.stop()


@pytest.mark.asyncio
async def test_start_idempotent_and_remembers_subscriptions():
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

    base = f"rediskit:test:start-idem:{uuid.uuid4()}"
    channel = f"{base}:news"
    pattern = f"{base}:*"

    broker = FanoutBroker(patterns=[pattern])
    await broker.start(channels=[channel])
    # idempotent start
    await broker.start(channels=[channel], patterns=[pattern])

    assert broker._chan_list == [channel]
    assert pattern in broker._merged_patterns

    h1 = await broker.subscribe(channel)
    h2 = await broker.subscribe(pattern)

    await publish(channel, {"ok": True})
    r1 = await asyncio.wait_for(anext(h1), timeout=5)
    r2 = await asyncio.wait_for(anext(h2), timeout=5)
    assert r1 == {"ok": True}
    assert r2 == {"ok": True}

    await h1.unsubscribe()
    await h2.unsubscribe()
    await broker.stop()


@pytest.mark.asyncio
async def test_exception_payload_is_passed_through_undecoded(monkeypatch):
    """
    If Redis delivers an Exception as message data, broker should pass it through (no decoding).
    """
    init_redis_connection_pool()
    await init_async_redis_connection_pool()

    channel = f"rediskit:test:exception-payload:{uuid.uuid4()}"
    broker = FanoutBroker()
    await broker.start(channels=[channel])

    handle = await broker.subscribe(channel)

    # Monkeypatch the pubsub to simulate a delivered Exception payload
    ps = broker._ps
    assert ps is not None

    delivered = {"done": False}

    async def fake_get_message(*args, **kwargs):
        if not delivered["done"]:
            delivered["done"] = True
            return {
                "type": "message",
                "channel": channel,
                "pattern": None,
                "data": RuntimeError("boom!"),
            }
        await asyncio.sleep(0)
        return None

    monkeypatch.setattr(ps, "get_message", fake_get_message)

    msg = await asyncio.wait_for(anext(handle), timeout=5)
    assert isinstance(msg, RuntimeError)
    assert str(msg) == "boom!"

    await handle.unsubscribe()
    await broker.stop()
