import base64
import functools
import inspect
import json
import logging
import pickle
from typing import Any, Callable, Literal

import zstd
from redis import Redis

from rediskit import config, redis_client
from rediskit.encrypter import Encrypter
from rediskit.redis_client import h_get_cache_from_redis, h_set_cache_to_redis
from rediskit.redisLock import GetAsyncRedisMutexLock, GetRedisMutexLock

log = logging.getLogger(__name__)
CacheTypeOptions = Literal["zipPickled", "zipJson"]
RedisStorageOptions = Literal["string", "hash"]


def splitHashKey(key: str) -> tuple[str, str]:
    *parts, field = key.split(":")
    if not parts:
        raise ValueError("Cannot use a single-part key with hash storage.")
    return ":".join(parts), field


def compressAndSign(data: Any, serializeFn: Callable[[Any], bytes], enableEncryption: bool = False) -> str:
    serializedData = serializeFn(data)
    if enableEncryption:
        compressedData = Encrypter().encrypt(serializedData)
    else:
        compressedData = zstd.compress(serializedData)

    return base64.b64encode(compressedData).decode("utf-8")


def verifyAndDecompress(payload: bytes, deserializeFn: Callable[[bytes], Any], enableEncryption: bool = False) -> Any:
    if enableEncryption:
        serializedData = Encrypter().decrypt(payload)
    else:
        serializedData = zstd.decompress(payload)
    return deserializeFn(serializedData)


def deserializeData(
    data: Any,
    cacheType: CacheTypeOptions,
    enableEncryption: bool = False,
) -> bytes:
    if cacheType == "zipPickled":
        cachedData = verifyAndDecompress(base64.b64decode(data), lambda b: pickle.loads(b), enableEncryption)
    elif cacheType == "zipJson":
        cachedData = verifyAndDecompress(base64.b64decode(data), lambda b: json.loads(b.decode("utf-8")), enableEncryption)
    else:
        raise ValueError("Unknown cacheType specified.")

    return cachedData


def serializeData(
    data: Any,
    cacheType: CacheTypeOptions,
    enableEncryption: bool = False,
) -> str:
    if cacheType == "zipPickled":
        payload = compressAndSign(data, lambda d: pickle.dumps(d), enableEncryption)
    elif cacheType == "zipJson":
        payload = compressAndSign(data, lambda d: json.dumps(d).encode("utf-8"), enableEncryption)
    else:
        raise ValueError("Unknown cacheType specified.")
    return payload


def computeValue[T](param: T | Callable[..., T], *args, **kwargs) -> T:
    if callable(param):
        sig = inspect.signature(param)
        accepts_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())

        if accepts_kwargs:
            # Pass all kwargs directly
            value = param(*args, **kwargs)
        else:
            # Filter only matching kwargs
            filtered_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
            bound = sig.bind(*args, **filtered_kwargs)
            bound.apply_defaults()
            value = param(*bound.args, **bound.kwargs)
        return value
    else:
        return param


def maybeDataInCache(
    tenantId: str | None,
    computedMemoizeKey: str,
    computedTtl: int | None,
    cacheType: CacheTypeOptions,
    resetTtlUponRead: bool,
    byPassCachedData: bool,
    enableEncryption: bool,
    storageType: RedisStorageOptions = "string",
    connection: Redis | None = None,
) -> Any:
    if byPassCachedData:
        log.info(f"Cache bypassed for tenantId: {tenantId}, key {computedMemoizeKey}")
        return None

    cachedData = None
    if storageType == "string":
        cached = redis_client.load_blob_from_redis(
            tenantId, match=computedMemoizeKey, set_ttl_on_read=computedTtl if resetTtlUponRead and computedTtl is not None else None, connection=connection
        )
        if cached:
            log.info(f"Cache hit tenantId: {tenantId}, key: {computedMemoizeKey}")
            cachedData = cached
    elif storageType == "hash":
        hashKey, field = splitHashKey(computedMemoizeKey)
        cachedDict = h_get_cache_from_redis(
            tenantId, hashKey, field, set_ttl_on_read=computedTtl if resetTtlUponRead and computedTtl is not None else None, connection=connection
        )
        if cachedDict and field in cachedDict and cachedDict[field] is not None:
            log.info(f"HASH cache hit tenantId: {tenantId}, key: {hashKey}, field: {field}")
            cachedData = cachedDict[field]
    else:
        raise ValueError(f"Unknown storageType: {storageType}")

    if cachedData:
        return deserializeData(cachedData, cacheType, enableEncryption)
    else:
        log.info(f"No cache found tenantId: {tenantId}, key: {computedMemoizeKey}")
        return None


def dumpData(
    data: Any,
    tenantId: str | None,
    computedMemoizeKey: str,
    cacheType: CacheTypeOptions,
    computedTtl: int | None,
    enableEncryption: bool,
    storageType: RedisStorageOptions = "string",
    connection: Redis | None = None,
) -> None:
    payload = serializeData(data, cacheType, enableEncryption)
    if storageType == "string":
        redis_client.dump_blob_to_redis(tenantId, computedMemoizeKey, payload=payload, ttl=computedTtl, connection=connection)
    elif storageType == "hash":
        hashKey, field = splitHashKey(computedMemoizeKey)
        h_set_cache_to_redis(tenantId, hashKey, fields={field: payload}, ttl=computedTtl, connection=connection)
    else:
        raise ValueError(f"Unknown storageType: {storageType}")


def RedisMemoize[T](
    memoizeKey: Callable[..., str] | str,
    ttl: Callable[..., int] | int | None = None,
    bypassCache: Callable[..., bool] | bool = False,
    cacheType: CacheTypeOptions = "zipJson",
    resetTtlUponRead: bool = True,
    enableEncryption: bool = False,
    storageType: RedisStorageOptions = "string",
    connection: Redis | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Caches the result of any function in Redis using either pickle or JSON.

    The decorated function must have 'tenantId' as an arg or kwarg.

    Params:
    -------
    - memoizeKey: Callable computing a memoize key based on wrapped funcs args and kwargs, callable shall define the logic to compute the correct memoize key.
    - ttl: Time To Live, either fixed value, or callable consuming args+kwargs to return a ttl. Default None, if None no ttl is set.
    - bypassCache: Don't get data from cache, run wrapped func and update cache. run new values.
    - cacheType: "zipPickled" Uses pickle for arbitrary Python objects, "zipJson" Uses JSON for data that is JSON serializable.
    - resetTtlUponRead: Set the ttl to the initial value upon reading the value from redis cache
    - connection: Custom Redis connection to use instead of the default connection pool
    """

    def computeMemoizeKey(*args, **kwargs) -> str:
        if not (isinstance(memoizeKey, str) or callable(memoizeKey)):
            raise ValueError(f"Expected memoizeKey to be Callable or a str. got {type(memoizeKey)}")
        return computeValue(memoizeKey, *args, **kwargs)

    def computeTtl(*args, **kwargs) -> int | None:
        if ttl is None:
            return None
        if not (isinstance(ttl, int) or callable(ttl)):
            raise ValueError(f"Expected ttl to be Callable or an int. got {type(ttl)}")
        return computeValue(ttl, *args, **kwargs)

    def computeByPassCache(*args, **kwargs) -> bool:
        if not (isinstance(bypassCache, bool) or callable(bypassCache)):
            raise ValueError(f"Expected bypassCache to be Callable or an int. got {type(bypassCache)}")
        return computeValue(bypassCache, *args, **kwargs)

    def computeTenantId(wrappedFunc: Callable[..., Any], *args, **kwargs) -> str | None:
        boundArgs = inspect.signature(wrappedFunc).bind(*args, **kwargs)
        boundArgs.apply_defaults()
        tenantId = boundArgs.arguments.get("tenantId") or boundArgs.kwargs.get("tenantId")
        return tenantId

    def getLockName(tenantId: str | None, computedMemoizeKey: str) -> str:
        if tenantId is None:
            return f"{config.REDIS_KIT_LOCK_CACHE_MUTEX}:{computedMemoizeKey}"
        else:
            return f"{config.REDIS_KIT_LOCK_CACHE_MUTEX}:{tenantId}:{computedMemoizeKey}"

    def getParams(func, *args, **kwargs) -> tuple[str, int | None, str | None, str, bool]:
        computedMemoizeKey = computeMemoizeKey(*args, **kwargs)
        computedTtl = computeTtl(*args, **kwargs)
        tenantId = computeTenantId(func, *args, **kwargs)
        lockName = getLockName(tenantId, computedMemoizeKey)
        byPassCachedData = computeByPassCache(*args, **kwargs)

        return computedMemoizeKey, computedTtl, tenantId, lockName, byPassCachedData

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        isAsyncFunc = inspect.iscoroutinefunction(func)
        if isAsyncFunc:

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs) -> T:
                computedMemoizeKey, computedTtl, tenantId, lockName, byPassCachedData = getParams(func, *args, **kwargs)
                async with await GetAsyncRedisMutexLock(lockName, expire=60):
                    inCache = maybeDataInCache(
                        tenantId, computedMemoizeKey, computedTtl, cacheType, resetTtlUponRead, byPassCachedData, enableEncryption, storageType, connection
                    )
                    if inCache is not None:
                        return inCache
                    result = await func(*args, **kwargs)
                    if result is not None:
                        dumpData(result, tenantId, computedMemoizeKey, cacheType, computedTtl, enableEncryption, storageType, connection)
                    return result

            return async_wrapper

        else:

            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> T:
                computedMemoizeKey, computedTtl, tenantId, lockName, byPassCachedData = getParams(func, *args, **kwargs)
                with GetRedisMutexLock(lockName, auto_renewal=True, expire=60):
                    inCache = maybeDataInCache(
                        tenantId, computedMemoizeKey, computedTtl, cacheType, resetTtlUponRead, byPassCachedData, enableEncryption, storageType, connection
                    )
                    if inCache is not None:
                        return inCache
                    result = func(*args, **kwargs)
                    if result is not None:
                        dumpData(result, tenantId, computedMemoizeKey, cacheType, computedTtl, enableEncryption, storageType, connection)
                    return result

            return wrapper

    return decorator
