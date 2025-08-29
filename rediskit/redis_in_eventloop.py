import asyncio
import logging
import threading
import weakref
from dataclasses import dataclass
from typing import Optional

import redis.asyncio as redis_async
from redis.asyncio import ConnectionPool

from rediskit import config

log = logging.getLogger(__name__)


@dataclass
class _LoopSlot:
    lock: asyncio.Lock  # created on the loop
    client: Optional[redis_async.Redis] = None


# One registry entry per *event loop*
_registry: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, _LoopSlot]" = weakref.WeakKeyDictionary()
_registry_lock = threading.Lock()  # protects _registry mutations only


def _make_client() -> redis_async.Redis:
    loop = asyncio.get_running_loop()
    log.info("Creating new Redis client for event loop id=%s", id(loop))
    pool = ConnectionPool(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        password=config.REDIS_PASSWORD,
        # max_connections=5,
        # health_check_interval=30,
        # socket_timeout=5,
        # socket_connect_timeout=3,
        retry_on_timeout=True,
        decode_responses=True,
    )
    return redis_async.Redis(connection_pool=pool, client_name="rediskit")


def _get_or_create_slot_for(loop: asyncio.AbstractEventLoop) -> _LoopSlot:
    with _registry_lock:
        slot = _registry.get(loop)
        if slot is None:
            slot = _LoopSlot(lock=asyncio.Lock())
            _registry[loop] = slot
        return slot


async def get_async_redis_connection_in_eventloop() -> redis_async.Redis:
    loop = asyncio.get_running_loop()
    slot = _get_or_create_slot_for(loop)

    if slot.client is not None:
        return slot.client

    async with slot.lock:
        if slot.client is None:
            client = _make_client()
            await client.ping()
            slot.client = client
    return slot.client


async def close_loop_redis():
    """
    Close the Redis client associated with the current loop.
    Useful for graceful shutdown in tests or workers.
    """
    loop = asyncio.get_running_loop()
    slot = _registry.get(loop)
    if slot and slot.client:
        try:
            await slot.client.close()
            await slot.client.connection_pool.disconnect(inuse_connections=True)
        finally:
            slot.client = None
