import logging
from typing import Callable

from redis import asyncio as redis_async

from rediskit.redis.a_client.connection import get_async_redis_connection
from rediskit.redis.a_client.keys_op import set_ttl_for_key
from rediskit.redis.node import get_redis_top_node

log = logging.getLogger(__name__)


async def load_blob_from_redis(
    tenant_id: str | None,
    match: str | None,
    connection: redis_async.Redis | None = None,
    set_ttl_on_read: int | None = None,
) -> str | bytes | None:
    log.debug("Loading cache from redis tenantId: %s, key: %s", tenant_id, match)
    conn = connection if connection is not None else get_async_redis_connection()
    node_match = get_redis_top_node(tenant_id, match)
    encoded = await conn.get(node_match)
    if encoded is None:
        return None
    if set_ttl_on_read:
        await set_ttl_for_key(tenant_id, match, ttl=set_ttl_on_read, connection=conn)
    return encoded


async def dump_blob_to_redis(
    tenant_id: str | None,
    key: str | None,
    payload: str,
    top_node: Callable = get_redis_top_node,
    connection: redis_async.Redis | None = None,
    ttl: int | None = None,
) -> None:
    log.debug("Dump cache tenantId: %s, key: %s", tenant_id, key)
    node_key = top_node(tenant_id, key)
    conn = connection if connection is not None else get_async_redis_connection()
    await conn.set(node_key, payload)
    if ttl is not None:
        await conn.expire(node_key, ttl)
