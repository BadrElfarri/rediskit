import json
from typing import Any, Callable

from redis import asyncio as redis_async

from rediskit import config
from rediskit.redis.a_client.connection import get_async_redis_connection
from rediskit.redis.a_client.keys_op import set_redis_cache_expiry
from rediskit.redis.node import get_redis_top_node
from rediskit.utils import check_matching_dict_data


async def check_cache_matches(
    tenant_id: str | None,
    match: str,
    payload_match: dict,
    count: int | None = None,
    connection: redis_async.Redis | None = None,
) -> bool:
    conn = connection if connection is not None else get_async_redis_connection()
    cache_matches = await load_cache_from_redis(tenant_id, match, count=count, connection=conn)
    clean_payload_match = json.loads(json.dumps(payload_match))
    for cache_match in cache_matches:
        if check_matching_dict_data(cache_match, clean_payload_match):
            return True
    return False


async def dump_cache_to_redis(
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
        await set_redis_cache_expiry(tenant_id, key, expiry=ttl, connection=conn, top_node=top_node)


async def dump_multiple_payload_to_redis(
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
        await dump_cache_to_redis(tenant_id, key, payload, top_node=top_node)
        if ttl is not None:
            await set_redis_cache_expiry(tenant_id, key, expiry=ttl, top_node=top_node)


async def load_cache_from_redis(
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


async def load_exact_cache_from_redis(
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


async def delete_cache_from_redis(
    tenant_id: str | None,
    match: str,
    connection: redis_async.Redis | None = None,
) -> None:
    node_match = get_redis_top_node(tenant_id, match)
    conn = connection if connection is not None else get_async_redis_connection()
    await conn.delete(node_match)
