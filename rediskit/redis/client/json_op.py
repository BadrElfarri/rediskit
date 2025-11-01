import json
from typing import Any, Callable

from redis import Redis

from rediskit import config
from rediskit.redis.client.connection import get_redis_connection
from rediskit.redis.client.keys_op import set_redis_cache_expiry
from rediskit.redis.node import get_redis_top_node
from rediskit.utils import check_matching_dict_data


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
