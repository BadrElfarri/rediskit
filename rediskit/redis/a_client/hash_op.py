import json
from typing import Any, Callable, Mapping

from redis import asyncio as redis_async

from rediskit.encrypter import Encrypter
from rediskit.redis.a_client.connection import get_async_redis_connection
from rediskit.redis.node import get_redis_top_node


async def hash_set_ttl_for_key(
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


async def h_set_cache_to_redis(
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


async def h_get_cache_from_redis(
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


async def h_scan_fields(
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


async def h_del_cache_from_redis(
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
