"""Utility helpers for Redis pub/sub messaging."""

from __future__ import annotations

import asyncio
import contextlib
import random
from collections.abc import AsyncIterator, Callable
from typing import Any, Dict, Iterable, Set

import redis.asyncio as redis_async
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from rediskit.redis.a_client.redis_in_eventloop import get_async_client_for_current_loop
from rediskit.redis.encoder import _default_decoder, _default_encoder

Serializer = Callable[[Any], Any]

_QUEUE_STOP = object()


async def publish(
    channel: str,
    message: Any,
    *,
    encoder: Serializer | None = None,
    connection: redis_async.Redis | None = None,
) -> int:
    """Asynchronously publish ``message`` to ``channel`` using the event-loop Redis redis."""

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
        self._iterator: AsyncIterator = self._listen()

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
        await self._iterator.aclose()  # type: ignore # fix later
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
    """Single Redis subscription fan-out for local asyncio consumers."""

    def __init__(
        self,
        *,
        patterns: Iterable[str] | None = None,
        decoder: Callable[..., Any] | None = None,
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

        # NEW: remember last subscriptions & start args for reconnects
        self._chan_list: list[str] = []
        self._merged_patterns: list[str] = []
        self._last_start_kwargs: dict[str, Any] = {}

    @staticmethod
    def _is_pattern(topic: str) -> bool:
        # Redis glob-style wildcards
        return any(ch in topic for ch in ("*", "?", "["))

    async def start(
        self,
        *,
        channels: Iterable[str] | None = None,
        patterns: Iterable[str] | None = None,
        health_check_interval: float | None = None,
    ) -> None:
        """Start (or restart) the broker background task."""
        # remember for auto-restart
        self._last_start_kwargs = {
            "channels": list(channels or []),
            "patterns": list(patterns or []),
            "health_check_interval": health_check_interval,
        }

        # already running
        if self._task and not self._task.done():
            return

        self._client = self._external_connection or get_async_client_for_current_loop()
        await self._open_pubsub(
            channels=self._last_start_kwargs["channels"],
            patterns=self._last_start_kwargs["patterns"],
            health_check_interval=health_check_interval,
        )

        self._stopping.clear()
        self._task = asyncio.create_task(self._run())

    async def _open_pubsub(
        self,
        *,
        channels: Iterable[str],
        patterns: Iterable[str],
        health_check_interval: float | None,
    ) -> None:
        """(Re)create pubsub and subscribe to channels/patterns."""
        if self._ps is not None:
            with contextlib.suppress(Exception):
                await self._ps.aclose()

        pubsub_kwargs: dict[str, Any] = {"ignore_subscribe_messages": True}
        if health_check_interval is not None:
            pubsub_kwargs["health_check_interval"] = health_check_interval

        assert self._client
        self._ps = self._client.pubsub(**pubsub_kwargs)

        # merge init-time patterns with provided patterns
        merged_patterns: list[str] = list(self._patterns)
        if patterns:
            merged_patterns.extend(patterns)

        # split channels vs patterns
        chan_list: list[str] = []
        if channels:
            for c in channels:
                if self._is_pattern(c):
                    merged_patterns.append(c)
                else:
                    chan_list.append(c)

        # remember for reconnect
        self._chan_list = chan_list
        self._merged_patterns = merged_patterns

        try:
            if self._chan_list:
                await self._ps.subscribe(*self._chan_list)
            if self._merged_patterns:
                await self._ps.psubscribe(*self._merged_patterns)
        except Exception:
            await self._ps.aclose()
            self._ps = None
            if not self._external_connection and self._client is not None:
                with contextlib.suppress(Exception):
                    await self._client.aclose()
            self._client = None
            raise

    async def _reconnect(self) -> None:
        """Tear down and re-open redis + pubsub; re-subscribe."""
        if self._ps is not None:
            with contextlib.suppress(Exception):
                await self._ps.aclose()
        self._ps = None

        if self._client is not None and not self._external_connection:
            with contextlib.suppress(Exception):
                await self._client.aclose()
        self._client = None

        self._client = self._external_connection or get_async_client_for_current_loop()
        await self._open_pubsub(
            channels=self._chan_list,
            patterns=self._merged_patterns,
            health_check_interval=self._last_start_kwargs.get("health_check_interval"),
        )

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

    async def subscribe(self, topic: str, *, maxsize: int = 1_000) -> "SubscriptionHandle":
        """Register a local subscriber queue for ``topic``."""
        # Auto-restart if previously started but task died
        if self._task is None or self._task.done():
            if not self._last_start_kwargs:
                raise RuntimeError("FanoutBroker.start() must be awaited before subscribing")
            await self.start(**self._last_start_kwargs)

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

        # exponential backoff for reconnects
        backoff = 0.5
        max_backoff = 15.0

        try:
            while not self._stopping.is_set():
                try:
                    message = await pubsub.get_message(timeout=1.0)
                    # successful call → reset backoff
                    backoff = 0.5
                except asyncio.CancelledError:
                    raise
                except (RedisConnectionError, RedisTimeoutError, OSError) as _e:
                    # connection dropped; try to reconnect
                    delay = backoff + random.uniform(0, backoff / 2)
                    try:
                        await asyncio.sleep(delay)
                        await self._reconnect()
                        pubsub = self._ps  # refresh handle
                        continue
                    except Exception:
                        backoff = min(max_backoff, backoff * 2)
                        continue

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
                    data = self._decoder(raw_data) if not isinstance(raw_data, Exception) else raw_data
                except Exception:
                    data = raw_data

                targets: list[asyncio.Queue] = []
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
