import logging
from contextlib import asynccontextmanager, suppress

import redis_lock
from redis.asyncio.lock import Lock as AsyncRedisLock

from rediskit import config
from rediskit.redis.a_client import get_async_redis_connection
from rediskit.redis.client import get_redis_connection

log = logging.getLogger(__name__)


def get_redis_mutex_lock(lock_name: str, expire: int = 30, auto_renewal: bool = True, id: str | None = None) -> redis_lock.Lock:
    return redis_lock.Lock(
        get_redis_connection(),
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
) -> AsyncRedisLock:
    conn = get_async_redis_connection()
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


@asynccontextmanager
async def nonblocking_mutex(name: str, **lock_kwargs):
    lock = get_async_redis_mutex_lock(name, **lock_kwargs)
    acquired = await lock.acquire(blocking=False)
    if not acquired:
        yield False
        return

    try:
        yield True
    finally:
        with suppress(Exception, getattr(redis_lock, "NotAcquired", Exception)):
            await lock.release()
