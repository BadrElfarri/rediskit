import json
import logging
from typing import Any, Callable, Iterator

import redis.asyncio as redis_async
from redis import ConnectionPool, Redis

from rediskit import config
from rediskit.encrypter import Encrypter
from rediskit.utils import CheckMatchingDictData

log = logging.getLogger(__name__)
redisConnectionPool: ConnectionPool | None = None
asyncRedisConnectionPool: redis_async.ConnectionPool | None = None


def InitRedisConnectionPool() -> None:
    global redisConnectionPool
    log.info("Initializing redis connection pool")
    redisConnectionPool = ConnectionPool(host=config.REDIS_HOST, port=config.REDIS_PORT, password=config.REDIS_PASSWORD, decode_responses=True)


def InitAsyncRedisConnectionPool() -> None:
    global asyncRedisConnectionPool
    log.info("Initializing async redis connection pool")
    asyncRedisConnectionPool = redis_async.ConnectionPool(host=config.REDIS_HOST, port=config.REDIS_PORT, password=config.REDIS_PASSWORD, decode_responses=True)


def GetRedisConnection() -> Redis:
    if redisConnectionPool is None:
        raise Exception("Redis connection pool is not initialized!")
    return Redis(connection_pool=redisConnectionPool)


def GetAsyncRedisConnection() -> redis_async.Redis:
    if asyncRedisConnectionPool is None:
        raise Exception("Async Redis connection pool is not initialized!")
    return redis_async.Redis(connection_pool=asyncRedisConnectionPool)


def GetRedisTopNode(tenant: str | None, key: str | None, topNode: str = config.REDIS_TOP_NODE) -> str:
    if tenant is None and key is None:
        raise ValueError("Tenant and key are missing!")
    return f"{topNode}:{tenant}:{key}" if tenant is not None else f"{topNode}:{key}"


def DumpCacheToRedis(tenant: str | None, key: str, payload: dict | list[dict], connection: Redis | None = None, topNode: str = config.REDIS_TOP_NODE) -> None:
    nodeKey = GetRedisTopNode(tenant, key, topNode)
    connection = connection if connection is not None else GetRedisConnection()
    connection.execute_command("JSON.SET", nodeKey, ".", json.dumps(payload))


def LoadCacheFromRedis(tenant: str | None, match: str, count: int | None = None, connection: Redis | None = None) -> list[dict]:
    count = count if count is not None else config.REDIS_SCAN_COUNT
    nodeMatch = GetRedisTopNode(tenant, match)
    payloads: list[dict] = []
    if config.REDIS_SKIP_CACHING:
        return payloads
    connection = connection if connection is not None else GetRedisConnection()
    keys = connection.scan_iter(match=nodeMatch, count=count)
    for key in keys:
        payload = json.loads(connection.execute_command("JSON.GET", key))
        payloads.append(payload)
    return payloads


def LoadExactCacheFromRedis(tenant: str | None, match: str, connection: Redis | None = None, topNode: str = config.REDIS_TOP_NODE) -> dict | None:
    nodeMatch = GetRedisTopNode(tenant, match, topNode)
    if config.REDIS_SKIP_CACHING:
        return None
    connection = connection if connection is not None else GetRedisConnection()
    if connection.exists(nodeMatch):
        payload = json.loads(connection.execute_command("JSON.GET", nodeMatch))
        return payload
    return None


def DeleteCacheFromRedis(tenant: str | None, match: str, connection: Redis | None = None) -> None:
    nodeMatch = GetRedisTopNode(tenant, match)
    connection = connection if connection is not None else GetRedisConnection()
    connection.delete(nodeMatch)


def CheckCacheMatches(tenant: str | None, match: str, payloadMatch: dict, count: int | None = None, connection: Redis | None = None) -> bool:
    connection = connection if connection is not None else GetRedisConnection()
    cacheMatches = LoadCacheFromRedis(tenant, match, count=count, connection=connection)
    cleanPayloadMatch = json.loads(json.dumps(payloadMatch))
    for cacheMatch in cacheMatches:
        if CheckMatchingDictData(cacheMatch, cleanPayloadMatch):
            return True
    return False


def SetRedisCacheExpiry(tenant: str | None, key: str, expiry: int, connection: Redis | None = None) -> None:
    nodeKey = GetRedisTopNode(tenant, key)
    connection = connection if connection is not None else GetRedisConnection()
    connection.expire(nodeKey, expiry)


def hashSetTtlForKey(tenant: str | None, key: str, fields: list[str], ttl: int, connection: Redis | None = None, topNode: Callable = GetRedisTopNode) -> None:
    nodeKey = topNode(tenant, key)
    connection = connection if connection is not None else GetRedisConnection()
    connection.hexpire(nodeKey, ttl, *fields)  # type: ignore  # hexpire do exist in new redis version


def HSetCacheToRedis(
    tenantId: str | None,
    key: str | None,
    fields: dict[str, Any],
    topNode: Callable = GetRedisTopNode,
    connection: Redis | None = None,
    ttl: int | None = None,
    enableEncryption: bool = False,
) -> None:
    nodeKey = topNode(tenantId, key)
    connection = connection if connection is not None else GetRedisConnection()
    # Create a mapping with JSON-serialized values
    mapping: dict[str | bytes, bytes | float | int | str]
    if enableEncryption:
        mapping = {field: Encrypter().encrypt(json.dumps(value).encode("utf-8")) for field, value in fields.items()}
    else:
        mapping = {field: json.dumps(value) for field, value in fields.items()}
    connection.hset(nodeKey, mapping=mapping)
    if ttl is not None:
        connection.hexpire(nodeKey, ttl, *mapping.keys())  # type: ignore  # hexpire do exist in new redis version


def HGetCacheFromRedis(
    tenantId: str | None,
    key: str | None,
    fields: str | list[str] | None = None,
    topNode: Callable = GetRedisTopNode,
    connection: Redis | None = None,
    setTtlOnRead: int | None = None,
    isEncrypted: bool = False,
) -> dict[str, Any] | None:
    """Retrieve one or more fields from a Redis hash."""
    nodeKey = topNode(tenantId, key)
    connection = connection if connection is not None else GetRedisConnection()

    if fields is None:
        # Return all fields in the hash
        result = connection.hgetall(nodeKey)
        data = {field: value for field, value in result.items()}
    elif isinstance(fields, str):
        # Return a single field's value
        value = connection.hget(nodeKey, fields)
        data = {fields: (value if value is not None else None)}
    elif isinstance(fields, list):
        # Return a list of values for the specified fields
        values = connection.hmget(nodeKey, fields)
        data = {fields[i]: (value if value is not None else None) for i, value in enumerate(values)}
    else:
        raise ValueError("fields must be either None, a string, or a list of strings")

    if setTtlOnRead is not None and data:
        connection.hexpire(nodeKey, setTtlOnRead, *data.keys())  # type: ignore  # hexpire do exist in new redis version

    if isEncrypted:
        result = {k: json.loads(Encrypter().decrypt(v)) for k, v in data.items() if v is not None}
    else:
        result = {k: json.loads(v) for k, v in data.items() if v is not None}

    return result


def HScanFields(
    tenantId: str | None,
    key: str | None,
    match: str,
    topNode: Callable = GetRedisTopNode,
    connection: Redis | None = None,
) -> list[str]:
    nodeKey = topNode(tenantId, key)
    connection = connection if connection is not None else GetRedisConnection()

    matched = []
    # Use HSCAN to iterate over the hash fields with a MATCH filter
    for field, value in connection.hscan_iter(nodeKey, match=match):
        matched.append(field)
    return matched


def HDelCacheFromRedis(
    tenantId: str | None,
    key: str | None,
    fields: dict[str, Any] | list[str],
    topNode: Callable = GetRedisTopNode,
    connection: Redis | None = None,
) -> None:
    nodeKey = topNode(tenantId, key)
    connection = connection if connection is not None else GetRedisConnection()
    # Determine the list of fields to delete
    if isinstance(fields, dict):
        field_names = list(fields.keys())
    elif isinstance(fields, list):
        field_names = fields
    else:
        raise ValueError("fields must be either a dictionary or a list of strings")
    # Delete the specified fields from the hash
    connection.hdel(nodeKey, *field_names)


def GetKeys(tenantId: str | None, key: str | None, topNode: Callable = GetRedisTopNode, connection: Redis | None = None, onlyLastKey: bool = True) -> list[str]:
    nodeKey = topNode(tenantId, key)
    connection = connection if connection is not None else GetRedisConnection()
    keys = connection.keys(nodeKey)
    if onlyLastKey:
        keys = [k.split(":")[-1] for k in keys]
    return keys


def SetTtlForKey(tenant: str | None, key: str | None, ttl: int, connection: Redis | None = None, topNode: Callable[..., str] = GetRedisTopNode) -> None:
    nodeKey = topNode(tenant, key)
    connection = connection if connection is not None else GetRedisConnection()
    connection.expire(nodeKey, ttl)


def LoadBlobFromRedis(tenantId: str | None, match: str | None, connection: Redis | None = None, setTtlOnRead: int | None = None) -> bytes | None:
    log.info(f"Loading cache from redis tenantId:{tenantId}, key: {match}")
    connection = connection if connection is not None else GetRedisConnection()
    nodeMatch = GetRedisTopNode(tenantId, match)
    # Retrieve raw bytes directly from Redis.
    encoded = connection.get(nodeMatch)
    if encoded is None:
        return None
    if setTtlOnRead:
        SetTtlForKey(tenantId, match, ttl=setTtlOnRead)

    return encoded


def DumpBlobToRedis(
    tenantId: str | None, key: str | None, payload: str, topNode: Callable = GetRedisTopNode, connection: Redis | None = None, ttl: int | None = None
) -> None:
    log.info(f"Dump cache tenantId:{tenantId}, key: {key}")
    nodeKey = topNode(tenantId, key)
    connection = connection if connection is not None else GetRedisConnection()
    connection.set(nodeKey, payload)
    if ttl is not None:
        connection.expire(nodeKey, ttl)


def ListKeys(
    tenantId: str | None,
    mathKey: str,
    count: int = 1_000,
    topNode: Callable = GetRedisTopNode,
    connection: Redis | None = None,
) -> Iterator[str]:
    pattern = topNode(tenantId, mathKey)
    conn = connection if connection is not None else GetRedisConnection()
    for i, key in enumerate(conn.scan_iter(match=pattern, count=count)):
        if i >= 10_000:
            raise ValueError("Redis keys exceeded 10_000 matches")
        yield key
