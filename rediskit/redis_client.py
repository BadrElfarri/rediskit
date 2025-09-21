import json
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable, Iterator, Mapping

import redis.asyncio as redis_async
from redis import ConnectionPool, Redis

from rediskit import config
from rediskit.encrypter import Encrypter
from rediskit.redis_in_eventloop import get_async_client_for_current_loop, get_async_redis_connection_in_eventloop
from rediskit.utils import check_matching_dict_data

log = logging.getLogger(__name__)
redis_connection_pool: ConnectionPool | None = None


def init_redis_connection_pool() -> None:
    global redis_connection_pool
    log.info("Initializing redis connection pool")
    redis_connection_pool = ConnectionPool(host=config.REDIS_HOST, port=config.REDIS_PORT, password=config.REDIS_PASSWORD, decode_responses=True)


async def init_async_redis_connection_pool() -> None:
    await get_async_redis_connection_in_eventloop()


@asynccontextmanager
async def redis_single_connection_context():
    pool = redis_async.ConnectionPool(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        password=config.REDIS_PASSWORD,
        decode_responses=True,
        max_connections=1,
    )
    client = redis_async.Redis(connection_pool=pool)
    try:
        yield client
    finally:
        await client.aclose()
        await pool.disconnect()


def get_redis_connection() -> Redis:
    if redis_connection_pool is None:
        raise Exception("Redis connection pool is not initialized!")
    return Redis(connection_pool=redis_connection_pool)


def get_async_redis_connection() -> redis_async.Redis:
    return get_async_client_for_current_loop()


def get_redis_top_node(tenant_id: str | None, key: str | None, top_node: str = config.REDIS_TOP_NODE) -> str:
    if tenant_id is None and key is None:
        raise ValueError("Tenant and key are missing!")
    return f"{top_node}:{tenant_id}:{key}" if tenant_id is not None else f"{top_node}:{key}"


def dump_cache_to_redis(
    tenant_id: str | None,
    key: str,
    payload: dict | list[dict],
    connection: Redis | None = None,
    top_node: Callable[..., str] = get_redis_top_node,
    ttl: int | None = None,
) -> None:
    nodeKey = top_node(tenant_id, key)
    connection = connection if connection is not None else get_redis_connection()
    connection.execute_command("JSON.SET", nodeKey, ".", json.dumps(payload))
    if ttl is not None:
        set_redis_cache_expiry(tenant_id, key, expiry=ttl, connection=connection, top_node=top_node)


def dump_multiple_payload_to_redis(
    tenant_id: str | None, payloads_and_keys: list[dict[str, Any]], ttl: int | None = None, top_node: Callable[..., str] = get_redis_top_node
) -> None:
    if tenant_id is None:
        raise Exception("Tenant or key is missing!")
    if len(payloads_and_keys) == 0:
        return

    for payload_and_key in payloads_and_keys:
        if "key" not in payload_and_key or "payload" not in payload_and_key:
            raise Exception("Key or payload is missing!")
        key = payload_and_key["key"]
        payload = payload_and_key["payload"]
        dump_cache_to_redis(tenant_id, key, payload, top_node=top_node)
        if ttl is not None:
            set_redis_cache_expiry(tenant_id, key, expiry=ttl, top_node=top_node)


def load_cache_from_redis(
    tenant_id: str | None, match: str, count: int | None = None, connection: Redis | None = None, top_node: Callable[..., str] = get_redis_top_node
) -> list[dict]:
    count = count if count is not None else config.REDIS_SCAN_COUNT
    node_match = top_node(tenant_id, match)
    payloads: list[dict] = []
    if config.REDIS_SKIP_CACHING:
        return payloads
    connection = connection if connection is not None else get_redis_connection()
    keys = connection.scan_iter(match=node_match, count=count)
    for key in keys:
        payload = json.loads(connection.execute_command("JSON.GET", key))
        payloads.append(payload)
    return payloads


def load_exact_cache_from_redis(
    tenant_id: str | None, match: str, connection: Redis | None = None, top_node: Callable[..., str] = get_redis_top_node
) -> dict | None:
    node_match = top_node(tenant_id, match)
    if config.REDIS_SKIP_CACHING:
        return None
    connection = connection if connection is not None else get_redis_connection()
    if connection.exists(node_match):
        payload = json.loads(connection.execute_command("JSON.GET", node_match))
        return payload
    return None


def delete_cache_from_redis(tenant_id: str | None, match: str, connection: Redis | None = None) -> None:
    nodeMatch = get_redis_top_node(tenant_id, match)
    connection = connection if connection is not None else get_redis_connection()
    connection.delete(nodeMatch)


def check_cache_matches(tenant_id: str | None, match: str, payload_match: dict, count: int | None = None, connection: Redis | None = None) -> bool:
    connection = connection if connection is not None else get_redis_connection()
    cacheMatches = load_cache_from_redis(tenant_id, match, count=count, connection=connection)
    cleanPayloadMatch = json.loads(json.dumps(payload_match))
    for cacheMatch in cacheMatches:
        if check_matching_dict_data(cacheMatch, cleanPayloadMatch):
            return True
    return False


def set_redis_cache_expiry(
    tenant_id: str | None, key: str, expiry: int, connection: Redis | None = None, top_node: Callable[..., str] = get_redis_top_node
) -> None:
    node_key = top_node(tenant_id, key)
    connection = connection if connection is not None else get_redis_connection()
    connection.expire(node_key, expiry)


def hash_set_ttl_for_key(
    tenant_id: str | None, key: str, fields: list[str], ttl: int, connection: Redis | None = None, top_node: Callable = get_redis_top_node
) -> None:
    node_key = top_node(tenant_id, key)
    connection = connection if connection is not None else get_redis_connection()
    connection.hexpire(node_key, ttl, *fields)  # type: ignore  # hexpire do exist in new redis version


def h_set_cache_to_redis(
    tenant_id: str | None,
    key: str | None,
    fields: dict[str, Any],
    top_node: Callable = get_redis_top_node,
    connection: Redis | None = None,
    ttl: int | None = None,
    enable_encryption: bool = False,
) -> None:
    node_key = top_node(tenant_id, key)
    connection = connection if connection is not None else get_redis_connection()
    # Create a mapping with JSON-serialized values
    mapping: dict[str | bytes, bytes | float | int | str]
    if enable_encryption:
        mapping = {field: Encrypter().encrypt(json.dumps(value).encode("utf-8")) for field, value in fields.items()}
    else:
        mapping = {field: json.dumps(value) for field, value in fields.items()}
    connection.hset(node_key, mapping=mapping)
    if ttl is not None:
        connection.hexpire(node_key, ttl, *mapping.keys())  # type: ignore  # hexpire do exist in new redis version


def h_get_cache_from_redis(
    tenant_id: str | None,
    key: str | None,
    fields: str | list[str] | None = None,
    top_node: Callable = get_redis_top_node,
    connection: Redis | None = None,
    set_ttl_on_read: int | None = None,
    is_encrypted: bool = False,
) -> dict[str, Any] | None:
    """Retrieve one or more fields from a Redis hash."""
    node_key = top_node(tenant_id, key)
    connection = connection if connection is not None else get_redis_connection()

    if fields is None:
        # Return all fields in the hash
        result = connection.hgetall(node_key)
        data = {field: value for field, value in result.items()} if isinstance(result, dict) else {}
    elif isinstance(fields, str):
        # Return a single field's value
        value = connection.hget(node_key, fields)
        data = {fields: (value if value is not None else None)}
    elif isinstance(fields, list):
        # Return a list of values for the specified fields
        values = connection.hmget(node_key, fields)
        data = {fields[i]: (value if value is not None else None) for i, value in enumerate(values)}
    else:
        raise ValueError("fields must be either None, a string, or a list of strings")

    if set_ttl_on_read is not None and data:
        connection.hexpire(node_key, set_ttl_on_read, *data.keys())  # type: ignore  # hexpire do exist in new redis version

    if is_encrypted:
        result = {k: json.loads(Encrypter().decrypt(v)) for k, v in data.items() if v is not None}
    else:
        result = {k: json.loads(v) for k, v in data.items() if v is not None}

    return result


def h_scan_fields(
    tenant_id: str | None,
    key: str | None,
    match: str,
    top_node: Callable = get_redis_top_node,
    connection: Redis | None = None,
) -> list[str]:
    node_key = top_node(tenant_id, key)
    connection = connection if connection is not None else get_redis_connection()

    matched = []
    # Use HSCAN to iterate over the hash fields with a MATCH filter
    for field, value in connection.hscan_iter(node_key, match=match):
        matched.append(field)
    return matched


def h_del_cache_from_redis(
    tenant_id: str | None,
    key: str | None,
    fields: dict[str, Any] | list[str],
    top_node: Callable = get_redis_top_node,
    connection: Redis | None = None,
) -> None:
    node_key = top_node(tenant_id, key)
    connection = connection if connection is not None else get_redis_connection()
    # Determine the list of fields to delete
    if isinstance(fields, dict):
        field_names = list(fields.keys())
    elif isinstance(fields, list):
        field_names = fields
    else:
        raise ValueError("fields must be either a dictionary or a list of strings")
    # Delete the specified fields from the hash
    connection.hdel(node_key, *field_names)


def get_keys(
    tenant_id: str | None, key: str | None, top_node: Callable[..., str] = get_redis_top_node, connection: Redis | None = None, only_last_key: bool = True
) -> list[str]:
    node_key = top_node(tenant_id, key)
    connection = connection if connection is not None else get_redis_connection()
    keys = connection.keys(node_key)
    if only_last_key:
        keys = [k.split(":")[-1] for k in keys]
    return keys


def set_ttl_for_key(
    tenant_id: str | None, key: str | None, ttl: int, connection: Redis | None = None, top_node: Callable[..., str] = get_redis_top_node
) -> None:
    nodeKey = top_node(tenant_id, key)
    connection = connection if connection is not None else get_redis_connection()
    connection.expire(nodeKey, ttl)


def load_blob_from_redis(tenant_id: str | None, match: str | None, connection: Redis | None = None, set_ttl_on_read: int | None = None) -> bytes | None:
    log.info(f"Loading cache from redis tenantId:{tenant_id}, key: {match}")
    connection = connection if connection is not None else get_redis_connection()
    node_match = get_redis_top_node(tenant_id, match)
    # Retrieve raw bytes directly from Redis.
    encoded = connection.get(node_match)
    if encoded is None:
        return None
    if set_ttl_on_read:
        set_ttl_for_key(tenant_id, match, ttl=set_ttl_on_read)

    return encoded


def dump_blob_to_redis(
    tenant_id: str | None, key: str | None, payload: str, top_node: Callable = get_redis_top_node, connection: Redis | None = None, ttl: int | None = None
) -> None:
    log.info(f"Dump cache tenantId:{tenant_id}, key: {key}")
    node_key = top_node(tenant_id, key)
    connection = connection if connection is not None else get_redis_connection()
    connection.set(node_key, payload)
    if ttl is not None:
        connection.expire(node_key, ttl)


def list_keys(
    tenant_id: str | None,
    math_key: str,
    count: int = 1_000,
    top_node: Callable = get_redis_top_node,
    connection: Redis | None = None,
) -> Iterator[str]:
    pattern = top_node(tenant_id, math_key)
    conn = connection if connection is not None else get_redis_connection()
    for i, key in enumerate(conn.scan_iter(match=pattern, count=count)):
        if i >= 10_000:
            raise ValueError("Redis keys exceeded 10_000 matches")
        yield key


# ----------------------------------------------------------------------------
# Async versions of the redis helpers above. These mirror the synchronous
# implementations but operate on ``redis.asyncio`` connections and use
# ``await`` when talking to Redis.


async def async_dump_cache_to_redis(
    tenant_id: str | None,
    key: str,
    payload: dict | list[dict],
    connection: redis_async.Redis | None = None,
    top_node: Callable[..., str] = get_redis_top_node,
    ttl: int | None = None,
) -> None:
    node_key = top_node(tenant_id, key)
    conn = connection if connection is not None else get_async_redis_connection()
    await conn.execute_command("JSON.SET", node_key, ".", json.dumps(payload))
    if ttl is not None:
        await async_set_redis_cache_expiry(tenant_id, key, expiry=ttl, connection=conn, top_node=top_node)


async def async_dump_multiple_payload_to_redis(
    tenant_id: str | None,
    payloads_and_keys: list[dict[str, Any]],
    ttl: int | None = None,
    top_node: Callable[..., str] = get_redis_top_node,
) -> None:
    if tenant_id is None:
        raise Exception("Tenant or key is missing!")
    if len(payloads_and_keys) == 0:
        return

    for payload_and_key in payloads_and_keys:
        if "key" not in payload_and_key or "payload" not in payload_and_key:
            raise Exception("Key or payload is missing!")
        key = payload_and_key["key"]
        payload = payload_and_key["payload"]
        await async_dump_cache_to_redis(tenant_id, key, payload, top_node=top_node)
        if ttl is not None:
            await async_set_redis_cache_expiry(tenant_id, key, expiry=ttl, top_node=top_node)


async def async_load_cache_from_redis(
    tenant_id: str | None,
    match: str,
    count: int | None = None,
    connection: redis_async.Redis | None = None,
    top_node: Callable[..., str] = get_redis_top_node,
) -> list[dict]:
    count = count if count is not None else config.REDIS_SCAN_COUNT
    node_match = top_node(tenant_id, match)
    payloads: list[dict] = []
    if config.REDIS_SKIP_CACHING:
        return payloads
    conn = connection if connection is not None else get_async_redis_connection()
    async for key in conn.scan_iter(match=node_match, count=count):
        payload = json.loads(await conn.execute_command("JSON.GET", key))
        payloads.append(payload)
    return payloads


async def async_load_exact_cache_from_redis(
    tenant_id: str | None,
    match: str,
    connection: redis_async.Redis | None = None,
    top_node: Callable[..., str] = get_redis_top_node,
) -> dict | None:
    node_match = top_node(tenant_id, match)
    if config.REDIS_SKIP_CACHING:
        return None
    conn = connection if connection is not None else get_async_redis_connection()
    if await conn.exists(node_match):
        payload = json.loads(await conn.execute_command("JSON.GET", node_match))
        return payload
    return None


async def async_delete_cache_from_redis(
    tenant_id: str | None,
    match: str,
    connection: redis_async.Redis | None = None,
) -> None:
    node_match = get_redis_top_node(tenant_id, match)
    conn = connection if connection is not None else get_async_redis_connection()
    await conn.delete(node_match)


async def async_check_cache_matches(
    tenant_id: str | None,
    match: str,
    payload_match: dict,
    count: int | None = None,
    connection: redis_async.Redis | None = None,
) -> bool:
    conn = connection if connection is not None else get_async_redis_connection()
    cache_matches = await async_load_cache_from_redis(tenant_id, match, count=count, connection=conn)
    clean_payload_match = json.loads(json.dumps(payload_match))
    for cache_match in cache_matches:
        if check_matching_dict_data(cache_match, clean_payload_match):
            return True
    return False


async def async_set_redis_cache_expiry(
    tenant_id: str | None,
    key: str,
    expiry: int,
    connection: redis_async.Redis | None = None,
    top_node: Callable[..., str] = get_redis_top_node,
) -> None:
    node_key = top_node(tenant_id, key)
    conn = connection if connection is not None else get_async_redis_connection()
    await conn.expire(node_key, expiry)


async def async_hash_set_ttl_for_key(
    tenant_id: str | None,
    key: str,
    fields: list[str],
    ttl: int,
    connection: redis_async.Redis | None = None,
    top_node: Callable[..., str] = get_redis_top_node,
) -> None:
    node_key = top_node(tenant_id, key)
    conn = connection if connection is not None else get_async_redis_connection()
    await conn.hexpire(node_key, ttl, *fields)  # type: ignore[attr-defined]


async def async_h_set_cache_to_redis(
    tenant_id: str | None,
    key: str | None,
    fields: dict[str, Any],
    top_node: Callable = get_redis_top_node,
    connection: redis_async.Redis | None = None,
    ttl: int | None = None,
    enable_encryption: bool = False,
) -> None:
    node_key = top_node(tenant_id, key)
    conn = connection if connection is not None else get_async_redis_connection()
    if enable_encryption:
        mapping: Mapping[str | bytes, str | bytes | int | float] = {
            field: Encrypter().encrypt(json.dumps(value).encode("utf-8")) for field, value in fields.items()
        }
    else:
        mapping = {field: json.dumps(value) for field, value in fields.items()}
    await conn.hset(node_key, mapping=mapping)
    if ttl is not None:
        await conn.hexpire(node_key, ttl, *mapping.keys())  # type: ignore[attr-defined]


async def async_h_get_cache_from_redis(
    tenant_id: str | None,
    key: str | None,
    fields: str | list[str] | None = None,
    top_node: Callable = get_redis_top_node,
    connection: redis_async.Redis | None = None,
    set_ttl_on_read: int | None = None,
    is_encrypted: bool = False,
) -> dict[str, Any] | None:
    node_key = top_node(tenant_id, key)
    conn = connection if connection is not None else get_async_redis_connection()

    if fields is None:
        result = await conn.hgetall(node_key)
        data = {field: value for field, value in result.items()}
    elif isinstance(fields, str):
        value = await conn.hget(node_key, fields)
        data = {fields: (value if value is not None else None)}
    elif isinstance(fields, list):
        values = await conn.hmget(node_key, fields)
        data = {fields[i]: (value if value is not None else None) for i, value in enumerate(values)}
    else:
        raise ValueError("fields must be either None, a string, or a list of strings")

    if set_ttl_on_read is not None and data:
        await conn.hexpire(node_key, set_ttl_on_read, *data.keys())  # type: ignore[attr-defined]

    if is_encrypted:
        result = {k: json.loads(Encrypter().decrypt(v)) for k, v in data.items() if v is not None}
    else:
        result = {k: json.loads(v) for k, v in data.items() if v is not None}

    return result


async def async_h_scan_fields(
    tenant_id: str | None,
    key: str | None,
    match: str,
    top_node: Callable = get_redis_top_node,
    connection: redis_async.Redis | None = None,
) -> list[str]:
    node_key = top_node(tenant_id, key)
    conn = connection if connection is not None else get_async_redis_connection()

    matched: list[str] = []
    async for field, value in conn.hscan_iter(node_key, match=match):
        matched.append(field)
    return matched


async def async_h_del_cache_from_redis(
    tenant_id: str | None,
    key: str | None,
    fields: dict[str, Any] | list[str],
    top_node: Callable = get_redis_top_node,
    connection: redis_async.Redis | None = None,
) -> None:
    node_key = top_node(tenant_id, key)
    conn = connection if connection is not None else get_async_redis_connection()
    if isinstance(fields, dict):
        field_names = list(fields.keys())
    elif isinstance(fields, list):
        field_names = fields
    else:
        raise ValueError("fields must be either a dictionary or a list of strings")
    await conn.hdel(node_key, *field_names)


async def async_get_keys(
    tenant_id: str | None,
    key: str | None,
    top_node: Callable[..., str] = get_redis_top_node,
    connection: redis_async.Redis | None = None,
    only_last_key: bool = True,
) -> list[str]:
    node_key = top_node(tenant_id, key)
    conn = connection if connection is not None else get_async_redis_connection()
    keys = await conn.keys(node_key)
    if only_last_key:
        keys = [k.split(":")[-1] for k in keys]
    return keys


async def async_set_ttl_for_key(
    tenant_id: str | None,
    key: str | None,
    ttl: int,
    connection: redis_async.Redis | None = None,
    top_node: Callable[..., str] = get_redis_top_node,
) -> None:
    node_key = top_node(tenant_id, key)
    conn = connection if connection is not None else get_async_redis_connection()
    await conn.expire(node_key, ttl)


async def async_load_blob_from_redis(
    tenant_id: str | None,
    match: str | None,
    connection: redis_async.Redis | None = None,
    set_ttl_on_read: int | None = None,
) -> bytes | None:
    log.info(f"Loading cache from redis tenantId:{tenant_id}, key: {match}")
    conn = connection if connection is not None else get_async_redis_connection()
    node_match = get_redis_top_node(tenant_id, match)
    encoded = await conn.get(node_match)
    if encoded is None:
        return None
    if set_ttl_on_read:
        await async_set_ttl_for_key(tenant_id, match, ttl=set_ttl_on_read)
    return encoded


async def async_dump_blob_to_redis(
    tenant_id: str | None,
    key: str | None,
    payload: str,
    top_node: Callable = get_redis_top_node,
    connection: redis_async.Redis | None = None,
    ttl: int | None = None,
) -> None:
    log.info(f"Dump cache tenantId:{tenant_id}, key: {key}")
    node_key = top_node(tenant_id, key)
    conn = connection if connection is not None else get_async_redis_connection()
    await conn.set(node_key, payload)
    if ttl is not None:
        await conn.expire(node_key, ttl)


async def async_list_keys(
    tenant_id: str | None,
    math_key: str,
    count: int = 1_000,
    top_node: Callable = get_redis_top_node,
    connection: redis_async.Redis | None = None,
) -> AsyncIterator[str]:
    pattern = top_node(tenant_id, math_key)
    conn = connection if connection is not None else get_async_redis_connection()
    i = 0
    async for key in conn.scan_iter(match=pattern, count=count):
        if i >= 10_000:
            raise ValueError("Redis keys exceeded 10_000 matches")
        i += 1
        yield key
