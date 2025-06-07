import time
import random
from typing import Optional

from redis import RedisError
from rediskit import config, redisClient

import logging
import uuid


# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class Semaphore:
    def __init__(self, redisConn, namespace: str, count: int, acquireTimeOut: int, lockTimeToLive: int, token: str | None = None):
        """
        acquireTimeOut: Time used to try to acquire a semaphore. Raises exception if fail to acquire within the timeout
        lockTimeToLive: Time in second to keep the lock alive. Lock cleared after the set time
        """
        self.redisConn = redisConn
        self.namespace = namespace
        self.count = count
        self.acquireTimeOut = acquireTimeOut
        self.ttl = lockTimeToLive
        self.acquired = False
        self.token = str(uuid.uuid4()) if not token else token  # Hash field
        self.hashKey = f"{namespace}:holders"

    def AcquireLock(self):
        acquiredTimeStamp = int(time.time())
        lua_script = """
        local current_count = redis.call('HLEN', KEYS[1])
        if current_count < tonumber(ARGV[2]) then
            if redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[3]) == 1 then
                -- Use HEXPIRE to set TTL on the specific field
                redis.call('HEXPIRE', KEYS[1], tonumber(ARGV[4]), 'FIELDS', 1, ARGV[1])
                return 1
            end
        end
        return 0
        """
        try:
            result = self.redisConn.eval(lua_script, 1, self.hashKey, self.token, self.count, acquiredTimeStamp, self.ttl)
            if result == 1:
                log.info(f"Acquired semaphore lock: {self.hashKey}, total locks holding: {self.GetActiveCount()} out of {self.count}")
            return result == 1
        except RedisError:
            raise

    def ReleaseLock(self):
        try:
            self.redisConn.hdel(self.hashKey, self.token)
            log.info(f"Released semaphore lock: {self.hashKey}, total locks holding {self.GetActiveCount()} out of {self.count}")
        except RedisError as e:
            raise RuntimeError(f"Failed to release semaphore: {e}")

    def AcquireBlocking(self):
        if self.acquired:
            raise RuntimeError("Semaphore already acquired")

        end_time = time.time() + self.acquireTimeOut
        backoff = 0.1

        while time.time() < end_time:
            if self.AcquireLock():
                self.acquired = True
                return self.token
            jitter = random.uniform(0, 0.1)
            time.sleep(backoff + jitter)
            backoff = min(backoff * 2, 2)

        raise RuntimeError(f"Timeout: Unable to acquire the semaphore lock {self.hashKey}, total locks holding {self.GetActiveCount()} out of {self.count}")

    def __enter__(self):
        self.AcquireBlocking()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.acquired:
            return
        try:
            self.ReleaseLock()
            self.acquired = False
        except RedisError as e:
            raise RuntimeError(f"Failed to release semaphore: {e}")

    def GetActiveCount(self):
        try:
            return self.redisConn.hlen(self.hashKey)
        except RedisError as e:
            raise RuntimeError(f"Failed to get active count: {e}")


def GetRedisSemaphore(
        key: str,
        token: str | None = None,
        count: int = 2,
        acquireTimeOut: int = config.REDIS_KIT_SEMAPHORE_SETTINGS_STALE_TIMEOUT_SECONDS,
        lockTimeToLive: int = config.REDIS_KIT_SEMAPHORE_LOCK_TIME_TO_LIVE,
        ) -> Semaphore:
    return Semaphore(redisClient.GetRedisConnection(),
                     namespace=f'{config.REDIS_KIT_SEMAPHORE_SETTINGS_REDIS_NAMESPACE}:{key}',
                     count=count,
                     acquireTimeOut=acquireTimeOut,
                     lockTimeToLive=lockTimeToLive,
                     token=token,)


def GetSemaphore(someType: str) -> Semaphore:
    fiveMin = 60*5
    return GetRedisSemaphore(someType,
                             count=config.REDIS_KIT_SEMAPHORE_SETTINGS_WEXTRACTOR_QUERY_SEMAPHORE_COUNT,
                             acquireTimeOut=fiveMin,
                             lockTimeToLive=fiveMin)

