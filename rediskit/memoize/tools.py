import base64
import inspect
import json
import pickle
from typing import Any, Callable, Literal

import zstd

from rediskit import Encrypter, config

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


def get_params(
    func: Callable, memoize_key: Callable[..., str] | str, ttl: Callable[..., int] | int | None, bypass_cache: Callable[..., bool] | bool, *args, **kwargs
) -> tuple:
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
        tenant_id = (
            bound_args.arguments.get("tenantId")
            or bound_args.kwargs.get("tenantId")
            or bound_args.arguments.get("tenant_id")
            or bound_args.kwargs.get("tenant_id")
        )
        return tenant_id

    def get_lock_name(tenant_id: str | None, computed_memoize_key: str) -> str:
        if tenant_id is None:
            return f"{config.REDIS_KIT_LOCK_CACHE_MUTEX}:{computed_memoize_key}"
        else:
            return f"{config.REDIS_KIT_LOCK_CACHE_MUTEX}:{tenant_id}:{computed_memoize_key}"

    def params_calc(func, *args, **kwargs) -> tuple[str, int | None, str | None, str, bool]:
        computed_memoize_key = compute_memoize_key(*args, **kwargs)
        computed_ttl = compute_ttl(*args, **kwargs)
        tenant_id = compute_tenant_id(func, *args, **kwargs)
        lock_name = get_lock_name(tenant_id, computed_memoize_key)
        by_pass_cached_data = compute_by_pass_cache(*args, **kwargs)

        return computed_memoize_key, computed_ttl, tenant_id, lock_name, by_pass_cached_data

    return params_calc(func, *args, **kwargs)
