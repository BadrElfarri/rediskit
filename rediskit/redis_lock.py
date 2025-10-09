import logging

import redis_lock

from rediskit import config, redis_client

log = logging.getLogger(__name__)


def get_redis_mutex_lock(lock_name: str, expire: int = 30, auto_renewal: bool = True, id: str | None = None) -> redis_lock.Lock:
    return redis_lock.Lock(
        redis_client.get_redis_connection(),
        name=f"{config.REDIS_KIT_LOCK_SETTINGS_REDIS_NAMESPACE}:{lock_name}",
        id=id,
        expire=expire,
        auto_renewal=auto_renewal,
    )


def get_async_redis_mutex_lock(
    lock_name: str,
    expire: int | None = 30,  # timeout
    sleep: float = 0.1,
    blocking: bool = True,
    blocking_timeout: float | None = None,
    lock_class: type[redis_lock.Lock] | None = None,
    thread_local: bool = True,
    raise_on_release_error: bool = True,
) -> redis_lock.Lock:
    conn = redis_client.get_async_redis_connection()
    lock = conn.lock(
        f"{config.REDIS_KIT_LOCK_ASYNC_SETTINGS_REDIS_NAMESPACE}:{lock_name}",
        timeout=expire,  # lock TTL
        sleep=sleep,
        blocking=blocking,  # wait to acquire
        blocking_timeout=blocking_timeout,  # how long to wait
        lock_class=lock_class,
        thread_local=thread_local,
        raise_on_release_error=raise_on_release_error,  # avoid exception if expired
    )
    return lock
