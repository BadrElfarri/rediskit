from __future__ import annotations

from typing import Any

from redis import Redis

from rediskit.redis.a_client.pubsub import Serializer
from rediskit.redis.client.connection import get_redis_connection
from rediskit.redis.encoder import _default_encoder


def publish(channel: str, message: Any, *, encoder: Serializer | None = None, connection: Redis | None = None) -> int:
    """Synchronously publish ``message`` to ``channel`` using the shared Redis pool."""

    encoder = encoder or _default_encoder
    connection = connection or get_redis_connection()
    encoded = encoder(message)
    return connection.publish(channel, encoded)
