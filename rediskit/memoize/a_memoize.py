import functools
import logging
from typing import Any, Callable

from redis.asyncio import Redis as Redis

from rediskit.memoize.tools import cache_type_options, deserialize_data, get_params, redis_storage_options, serialize_data, split_hash_key
from rediskit.redis import a_client
from rediskit.redis_lock import get_async_redis_mutex_lock

log = logging.getLogger(__name__)


# TODO: Update this to use fully async. Have a a_redis_memoize and a normal one. Now both use sync redis connection....Not good!


async def maybe_data_in_cache(
    tenant_id: str | None,
    computed_memoize_key: str,
    computed_ttl: int | None,
    cache_type: cache_type_options,
    reset_ttl_upon_read: bool,
    by_pass_cached_data: bool,
    enable_encryption: bool,
    storage_type: redis_storage_options = "string",
    connection: Redis | None = None,
) -> Any:
    if by_pass_cached_data:
        log.info(f"Cache bypassed for tenantId: {tenant_id}, key {computed_memoize_key}")
        return None

    cached_data = None
    if storage_type == "string":
        cached = await a_client.load_blob_from_redis(
            tenant_id,
            match=computed_memoize_key,
            set_ttl_on_read=computed_ttl if reset_ttl_upon_read and computed_ttl is not None else None,
            connection=connection,
        )
        if cached:
            log.info(f"Cache hit tenantId: {tenant_id}, key: {computed_memoize_key}")
            cached_data = cached
    elif storage_type == "hash":
        hash_key, field = split_hash_key(computed_memoize_key)
        cached_dict = await a_client.h_get_cache_from_redis(
            tenant_id, hash_key, field, set_ttl_on_read=computed_ttl if reset_ttl_upon_read and computed_ttl is not None else None, connection=connection
        )
        if cached_dict and field in cached_dict and cached_dict[field] is not None:
            log.info(f"HASH cache hit tenantId: {tenant_id}, key: {hash_key}, field: {field}")
            cached_data = cached_dict[field]
    else:
        raise ValueError(f"Unknown storageType: {storage_type}")

    if cached_data:
        return deserialize_data(cached_data, cache_type, enable_encryption)
    else:
        log.info(f"No cache found tenantId: {tenant_id}, key: {computed_memoize_key}")
        return None


async def dump_data(
    data: Any,
    tenant_id: str | None,
    computed_memoize_key: str,
    cache_type: cache_type_options,
    computed_ttl: int | None,
    enable_encryption: bool,
    storage_type: redis_storage_options = "string",
    connection: Redis | None = None,
) -> None:
    payload = serialize_data(data, cache_type, enable_encryption)
    if storage_type == "string":
        await a_client.dump_blob_to_redis(tenant_id, computed_memoize_key, payload=payload, ttl=computed_ttl, connection=connection)
    elif storage_type == "hash":
        hashKey, field = split_hash_key(computed_memoize_key)
        await a_client.h_set_cache_to_redis(tenant_id, hashKey, fields={field: payload}, ttl=computed_ttl, connection=connection)
    else:
        raise ValueError(f"Unknown storageType: {storage_type}")


def a_redis_memoize[T](
    memoize_key: Callable[..., str] | str,
    ttl: Callable[..., int] | int | None = None,
    bypass_cache: Callable[..., bool] | bool = False,
    cache_type: cache_type_options = "zipJson",
    reset_ttl_upon_read: bool = True,
    enable_encryption: bool = False,
    storage_type: redis_storage_options = "string",
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

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> T:
            computed_memoize_key, computed_ttl, tenant_id, lock_name, by_pass_cached_data = get_params(func, memoize_key, ttl, bypass_cache, *args, **kwargs)
            async with get_async_redis_mutex_lock(lock_name, expire=60):
                in_cache = await maybe_data_in_cache(
                    tenant_id,
                    computed_memoize_key,
                    computed_ttl,
                    cache_type,
                    reset_ttl_upon_read,
                    by_pass_cached_data,
                    enable_encryption,
                    storage_type,
                    connection,
                )
                if in_cache is not None:
                    return in_cache
                result = await func(*args, **kwargs)  # type: ignore # need to fix this
                if result is not None:
                    await dump_data(result, tenant_id, computed_memoize_key, cache_type, computed_ttl, enable_encryption, storage_type, connection)
                return result

        return async_wrapper  # type: ignore # need to fix this

    return decorator
