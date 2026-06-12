import logging
from typing import Any, Callable, cast

from redis import Redis

from rediskit.redis.client.connection import get_redis_connection
from rediskit.redis.client.keys_op import set_ttl_for_key
from rediskit.redis.node import get_redis_top_node

log = logging.getLogger(__name__)


def load_blob_from_redis(tenant_id: str | None, match: str | None, connection: Redis | None = None, set_ttl_on_read: int | None = None) -> Any | None:
    log.debug("Loading cache from redis tenantId: %s, key: %s", tenant_id, match)
    connection = connection if connection is not None else get_redis_connection()
    node_match = get_redis_top_node(tenant_id, match)
    # Retrieve raw bytes directly from Redis.
    encoded = cast(Any, connection.get(node_match))
    if encoded is None:
        return None
    if set_ttl_on_read:
        set_ttl_for_key(tenant_id, match, ttl=set_ttl_on_read, connection=connection)

    return encoded


def dump_blob_to_redis(
    tenant_id: str | None, key: str | None, payload: str, top_node: Callable = get_redis_top_node, connection: Redis | None = None, ttl: int | None = None
) -> None:
    log.debug("Dump cache tenantId: %s, key: %s", tenant_id, key)
    node_key = top_node(tenant_id, key)
    connection = connection if connection is not None else get_redis_connection()
    connection.set(node_key, payload)
    if ttl is not None:
        connection.expire(node_key, ttl)
