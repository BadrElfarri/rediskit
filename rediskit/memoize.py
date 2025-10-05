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
from rediskit.redis_lock import get_async_redis_mutex_lock, get_redis_mutex_lock

log = logging.getLogger(__name__)
cache_type_options = Literal["zipPickled", "zipJson"]
redis_storage_options = Literal["string", "hash"]


def split_hash_key(key: str) -> tuple[str, str]:
    *parts, field = key.split(":")
    if not parts:
        raise ValueError("Cannot use a single-part key with hash storage.")
    return ":".join(parts), field


def compress_and_sign(data: Any, serialize_fn: Callable[[Any], bytes], enable_encryption: bool = False) -> str:
    serialized_data = serialize_fn(data)
    if enable_encryption:
        compressed_data = Encrypter().encrypt(serialized_data)
    else:
        compressed_data = zstd.compress(serialized_data)

    return base64.b64encode(compressed_data).decode("utf-8")


def verify_and_decompress(payload: bytes, deserialize_fn: Callable[[bytes], Any], enable_encryption: bool = False) -> Any:
    if enable_encryption:
        serialized_data = Encrypter().decrypt(payload)
    else:
        serialized_data = zstd.decompress(payload)
    return deserialize_fn(serialized_data)


def deserialize_data(
    data: Any,
    cache_type: cache_type_options,
    enable_encryption: bool = False,
) -> bytes:
    if cache_type == "zipPickled":
        cached_data = verify_and_decompress(base64.b64decode(data), lambda b: pickle.loads(b), enable_encryption)
    elif cache_type == "zipJson":
        cached_data = verify_and_decompress(base64.b64decode(data), lambda b: json.loads(b.decode("utf-8")), enable_encryption)
    else:
        raise ValueError("Unknown cacheType specified.")

    return cached_data


def serialize_data(
    data: Any,
    cache_type: cache_type_options,
    enable_encryption: bool = False,
) -> str:
    if cache_type == "zipPickled":
        payload = compress_and_sign(data, lambda d: pickle.dumps(d), enable_encryption)
    elif cache_type == "zipJson":
        payload = compress_and_sign(data, lambda d: json.dumps(d).encode("utf-8"), enable_encryption)
    else:
        raise ValueError("Unknown cacheType specified.")
    return payload


def compute_value[T](param: T | Callable[..., T], *args, **kwargs) -> T:
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


def maybe_data_in_cache(
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
        cached = redis_client.load_blob_from_redis(
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
        cached_dict = h_get_cache_from_redis(
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


def dump_data(
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
        redis_client.dump_blob_to_redis(tenant_id, computed_memoize_key, payload=payload, ttl=computed_ttl, connection=connection)
    elif storage_type == "hash":
        hashKey, field = split_hash_key(computed_memoize_key)
        h_set_cache_to_redis(tenant_id, hashKey, fields={field: payload}, ttl=computed_ttl, connection=connection)
    else:
        raise ValueError(f"Unknown storageType: {storage_type}")


def redis_memoize[T](
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

    def compute_memoize_key(*args, **kwargs) -> str:
        if not (isinstance(memoize_key, str) or callable(memoize_key)):
            raise ValueError(f"Expected memoizeKey to be Callable or a str. got {type(memoize_key)}")
        return compute_value(memoize_key, *args, **kwargs)

    def compute_ttl(*args, **kwargs) -> int | None:
        if ttl is None:
            return None
        if not (isinstance(ttl, int) or callable(ttl)):
            raise ValueError(f"Expected ttl to be Callable or an int. got {type(ttl)}")
        return compute_value(ttl, *args, **kwargs)

    def compute_by_pass_cache(*args, **kwargs) -> bool:
        if not (isinstance(bypass_cache, bool) or callable(bypass_cache)):
            raise ValueError(f"Expected bypassCache to be Callable or an int. got {type(bypass_cache)}")
        return compute_value(bypass_cache, *args, **kwargs)

    def compute_tenant_id(wrapped_func: Callable[..., Any], *args, **kwargs) -> str | None:
        bound_args = inspect.signature(wrapped_func).bind(*args, **kwargs)
        bound_args.apply_defaults()
        tenant_id = bound_args.arguments.get("tenantId") or bound_args.kwargs.get("tenantId")
        return tenant_id

    def get_lock_name(tenant_id: str | None, computed_memoize_key: str) -> str:
        if tenant_id is None:
            return f"{config.REDIS_KIT_LOCK_CACHE_MUTEX}:{computed_memoize_key}"
        else:
            return f"{config.REDIS_KIT_LOCK_CACHE_MUTEX}:{tenant_id}:{computed_memoize_key}"

    def get_params(func, *args, **kwargs) -> tuple[str, int | None, str | None, str, bool]:
        computed_memoize_key = compute_memoize_key(*args, **kwargs)
        computed_ttl = compute_ttl(*args, **kwargs)
        tenant_id = compute_tenant_id(func, *args, **kwargs)
        lock_name = get_lock_name(tenant_id, computed_memoize_key)
        by_pass_cached_data = compute_by_pass_cache(*args, **kwargs)

        return computed_memoize_key, computed_ttl, tenant_id, lock_name, by_pass_cached_data

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        is_async_func = inspect.iscoroutinefunction(func)
        # TODO: Create a separate decorator for async outside this.
        if is_async_func:

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs) -> T:
                computed_memoize_key, computed_ttl, tenant_id, lock_name, by_pass_cached_data = get_params(func, *args, **kwargs)
                async with await get_async_redis_mutex_lock(lock_name, expire=60):
                    in_cache = maybe_data_in_cache(
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
                        dump_data(result, tenant_id, computed_memoize_key, cache_type, computed_ttl, enable_encryption, storage_type, connection)
                    return result

            return async_wrapper  # type: ignore # need to fix this

        else:

            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> T:
                computed_memoize_key, computed_ttl, tenant_id, lock_name, by_pass_cached_data = get_params(func, *args, **kwargs)
                with get_redis_mutex_lock(lock_name, auto_renewal=True, expire=60):
                    in_cache = maybe_data_in_cache(
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
                    result = func(*args, **kwargs)
                    if result is not None:
                        dump_data(result, tenant_id, computed_memoize_key, cache_type, computed_ttl, enable_encryption, storage_type, connection)
                    return result

            return wrapper

    return decorator
