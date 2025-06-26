import logging

import redis_lock

from rediskit import config, redis_client

log = logging.getLogger(__name__)


def GetRedisMutexLock(lockName: str, expire: int = 30, auto_renewal: bool = True, id: str | None = None) -> redis_lock.Lock:
    return redis_lock.Lock(
        redis_client.get_redis_connection(),
        name=f"{config.REDIS_KIT_LOCK_SETTINGS_REDIS_NAMESPACE}:{lockName}",
        id=id,
        expire=expire,
        auto_renewal=auto_renewal,
    )


async def GetAsyncRedisMutexLock(lockName: str, expire: int = 30, auto_renewal: bool = True) -> redis_lock.Lock:
    connection = redis_client.get_async_redis_connection()
    return connection.lock(f"{config.REDIS_KIT_LOCK_ASYNC_SETTINGS_REDIS_NAMESPACE}:{lockName}", timeout=expire)
