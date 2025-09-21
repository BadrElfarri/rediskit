"""Utility helpers for Redis pub/sub messaging."""

from __future__ import annotations

import json
import contextlib
import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any, Dict, Iterable, Set

import redis.asyncio as redis_async
from redis import Redis

from .redis_client import get_redis_connection
from .redis_in_eventloop import get_async_client_for_current_loop

Serializer = Callable[[Any], Any]

_QUEUE_STOP = object()


def _default_encoder(message: Any) -> Any:
    """Encode a message into a type publishable by Redis."""

    if isinstance(message, (bytes, bytearray)):
        return bytes(message)
    if isinstance(message, str):
        return message
    return json.dumps(message)


def _default_decoder(payload: Any) -> Any:
    """Decode a Redis pub/sub payload back into Python objects."""

    if isinstance(payload, (bytes, bytearray)):
        try:
            payload = payload.decode("utf-8")
        except UnicodeDecodeError:
            return bytes(payload)

    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return payload

    return payload


def publish(channel: str, message: Any, *, encoder: Serializer | None = None, connection: Redis | None = None) -> int:
    """Synchronously publish ``message`` to ``channel`` using the shared Redis pool."""

    encoder = encoder or _default_encoder
    connection = connection or get_redis_connection()
    encoded = encoder(message)
    return connection.publish(channel, encoded)


async def apublish(
    channel: str,
    message: Any,
    *,
    encoder: Serializer | None = None,
    connection: redis_async.Redis | None = None,
) -> int:
    """Asynchronously publish ``message`` to ``channel`` using the event-loop Redis client."""

    encoder = encoder or _default_encoder
    connection = connection or get_async_client_for_current_loop()
    encoded = encoder(message)
    return await connection.publish(channel, encoded)


class ChannelSubscription(AsyncIterator[Any]):
    """Async iterator representing a subscription to a Redis channel."""

    def __init__(self, channel: str, pubsub: redis_async.client.PubSub, decoder: Serializer):
        self._channel = channel
        self._pubsub = pubsub
        self._decoder = decoder
        self._closed = False
        self._iterator = self._listen()

    async def _ensure_closed(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self._pubsub.unsubscribe(self._channel)
        finally:
            await self._pubsub.aclose()

    async def _listen(self) -> AsyncIterator[Any]:
        try:
            async for raw in self._pubsub.listen():
                if raw.get("type") != "message":
                    continue
                data = raw.get("data")
                yield self._decoder(data)
        finally:
            await self._ensure_closed()

    def __aiter__(self) -> "ChannelSubscription":
        return self

    async def __anext__(self) -> Any:
        try:
            return await self._iterator.__anext__()
        except StopAsyncIteration:
            await self._ensure_closed()
            raise

    async def aclose(self) -> None:
        await self._iterator.aclose()
        await self._ensure_closed()

    async def __aenter__(self) -> "ChannelSubscription":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()


async def subscribe_channel(
    channel: str,
    *,
    decoder: Serializer | None = None,
    connection: redis_async.Redis | None = None,
    health_check_interval: float | None = None,
) -> ChannelSubscription:
    """Create a subscription for ``channel`` and return an async iterator over its messages."""

    decoder = decoder or _default_decoder
    connection = connection or get_async_client_for_current_loop()
    pubsub_kwargs: dict[str, Any] = {"ignore_subscribe_messages": True}
    if health_check_interval is not None:
        pubsub_kwargs["health_check_interval"] = health_check_interval
    pubsub = connection.pubsub(**pubsub_kwargs)

    try:
        await pubsub.subscribe(channel)
    except Exception:
        await pubsub.aclose()
        raise

    return ChannelSubscription(channel, pubsub, decoder)


async def iter_channel(
    channel: str,
    *,
    decoder: Serializer | None = None,
    connection: redis_async.Redis | None = None,
    health_check_interval: float | None = None,
) -> AsyncIterator[Any]:
    """Yield decoded messages published to ``channel`` until the consumer stops iteration."""

    subscription = await subscribe_channel(
        channel,
        decoder=decoder,
        connection=connection,
        health_check_interval=health_check_interval,
    )

    try:
        async for item in subscription:
            yield item
    finally:
        await subscription.aclose()


class FanoutBroker:
    """Single Redis subscription fan-out for local asyncio consumers.

    ``FanoutBroker`` keeps a single Redis pub/sub connection running in the
    background. Incoming messages are decoded and pushed into per-topic
    ``asyncio.Queue`` instances for any interested consumers.  Each consumer
    owns a :class:`SubscriptionHandle` returned from :meth:`subscribe` which
    exposes both an async iterator and an explicit :meth:`unsubscribe` method.

    The broker can be configured to subscribe to specific channels and/or
    patterns on startup.  Additional Redis subscription management (such as
    dynamic channel subscriptions) should be handled by the application itself.
    """

    def __init__(
        self,
        *,
        patterns: Iterable[str] | None = None,
        decoder: Serializer | None = None,
        connection: redis_async.Redis | None = None,
    ) -> None:
        self._patterns = list(patterns or [])
        self._decoder = decoder or _default_decoder
        self._external_connection = connection
        self._subs: Dict[str, Set[asyncio.Queue[Any]]] = {}
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._client: redis_async.Redis | None = None
        self._ps: redis_async.client.PubSub | None = None
        self._stopping = asyncio.Event()

    async def start(
        self,
        *,
        channels: Iterable[str] | None = None,
        health_check_interval: float | None = None,
    ) -> None:
        """Start the broker background task if it isn't already running."""

        if self._task and not self._task.done():
            return

        self._client = self._external_connection or get_async_client_for_current_loop()
        pubsub_kwargs: dict[str, Any] = {"ignore_subscribe_messages": True}
        if health_check_interval is not None:
            pubsub_kwargs["health_check_interval"] = health_check_interval
        self._ps = self._client.pubsub(**pubsub_kwargs)

        try:
            if channels:
                await self._ps.subscribe(*channels)
            if self._patterns:
                await self._ps.psubscribe(*self._patterns)
        except Exception:
            await self._ps.aclose()
            self._ps = None
            if not self._external_connection and self._client is not None:
                await self._client.aclose()
            self._client = None
            raise

        self._stopping.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop the broker task and close Redis resources."""

        task = self._task
        if not task:
            return

        self._stopping.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        if self._ps is not None:
            with contextlib.suppress(Exception):
                await self._ps.aclose()
        self._ps = None

        if self._client is not None and not self._external_connection:
            with contextlib.suppress(Exception):
                await self._client.aclose()
        self._client = None

        self._task = None
        self._stopping.clear()

    async def subscribe(self, topic: str, *, maxsize: int = 100) -> "SubscriptionHandle":
        """Register a local subscriber queue for ``topic``."""

        if self._task is None or self._task.done():
            raise RuntimeError("FanoutBroker.start() must be awaited before subscribing")

        queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=maxsize)
        async with self._lock:
            self._subs.setdefault(topic, set()).add(queue)
        return SubscriptionHandle(topic, queue, self)

    async def _unsubscribe_queue(self, topic: str, queue: asyncio.Queue[Any]) -> None:
        async with self._lock:
            subscribers = self._subs.get(topic)
            if subscribers is None:
                return
            subscribers.discard(queue)
            if not subscribers:
                del self._subs[topic]

    async def _run(self) -> None:
        assert self._ps is not None
        pubsub = self._ps
        try:
            while not self._stopping.is_set():
                try:
                    message = await pubsub.get_message(timeout=1.0)
                except asyncio.CancelledError:
                    raise

                if message is None:
                    await asyncio.sleep(0)
                    continue

                mtype = message.get("type")
                if mtype not in {"message", "pmessage"}:
                    continue

                channel = message.get("channel")
                pattern = message.get("pattern")
                raw_data = message.get("data")

                try:
                    data = (
                        self._decoder(raw_data)
                        if not isinstance(raw_data, Exception)
                        else raw_data
                    )
                except Exception:
                    data = raw_data

                targets = []
                async with self._lock:
                    if channel is not None:
                        targets.extend(self._subs.get(channel, ()))
                    if pattern is not None:
                        targets.extend(self._subs.get(pattern, ()))

                for queue in targets:
                    try:
                        queue.put_nowait(data)
                    except asyncio.QueueFull:
                        with contextlib.suppress(asyncio.QueueEmpty):
                            queue.get_nowait()
                        with contextlib.suppress(asyncio.QueueFull):
                            queue.put_nowait(data)
        finally:
            await self._drain_all_queues()

    async def _drain_all_queues(self) -> None:
        async with self._lock:
            queues = [queue for subscribers in self._subs.values() for queue in subscribers]
            self._subs.clear()

        for queue in queues:
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(_QUEUE_STOP)


class SubscriptionHandle(AsyncIterator[Any]):
    """Handle returned from :meth:`FanoutBroker.subscribe`."""

    def __init__(self, topic: str, queue: asyncio.Queue[Any], broker: FanoutBroker) -> None:
        self.topic = topic
        self.queue = queue
        self._broker = broker
        self._closed = False

    def __aiter__(self) -> "SubscriptionHandle":
        return self

    async def __anext__(self) -> Any:
        item = await self.queue.get()
        if item is _QUEUE_STOP:
            await self.unsubscribe()
            raise StopAsyncIteration
        return item

    async def iter(self) -> AsyncIterator[Any]:
        try:
            while True:
                item = await self.__anext__()
                yield item
        finally:
            await self.unsubscribe()

    async def unsubscribe(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._broker._unsubscribe_queue(self.topic, self.queue)


