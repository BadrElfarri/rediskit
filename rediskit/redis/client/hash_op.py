import json
from typing import Any, Callable

from redis import Redis

from rediskit.encrypter import Encrypter
from rediskit.redis.client.connection import get_redis_connection
from rediskit.redis.node import get_redis_top_node


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
