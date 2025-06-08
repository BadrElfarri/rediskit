import time
import random

from redis import RedisError
from rediskit import config, redisClient

import logging
import uuid

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class Semaphore:
    def __init__(
        self,
        namespace: str,
        limit: int,
        acquire_time_out: int,
        lock_time_to_live: int | None,
        redis_conn: str | None = None,
        process_unique_id: str | None = None,
    ):
        self.redisConn = redis_conn if redis_conn else redisClient.GetRedisConnection()
        self.namespace = namespace
        self.limit = limit  # Limit on the number of processes that can acquire the lock concurrently
        self.acquireTimeOut = acquire_time_out
        self.ttl = lock_time_to_live
        self.process_unique_id = str(uuid.uuid4()) if not process_unique_id else process_unique_id
        self.hashKey = f"{namespace}:holders"

    def get_active_count(self):
        try:
            return self.redisConn.hlen(self.hashKey)
        except RedisError as e:
            raise RuntimeError(f"Failed to get active count: {e}")

    def lock_limit_reached(self):
        return self.get_active_count() >= self.limit

    def is_acquired_by_process(self):
        try:
            return self.redisConn.hexists(self.hashKey, self.process_unique_id)
        except RedisError as e:
            raise RuntimeError(f"Failed to check semaphore ownership: {e}")

    def acquire_lock(self):
        acquired_time_stamp = int(time.time())
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
        if self.lock_limit_reached():
            return False
        try:
            # Ensure argument order matches your script
            if self.ttl:
                result = self.redisConn.eval(lua_script, 1, self.hashKey, self.process_unique_id, self.limit, acquired_time_stamp, self.ttl)
            else:
                result = self.redisConn.hset(self.hashKey, mapping={self.process_unique_id: acquired_time_stamp})

            if result == 1:
                log.info(f"Acquired semaphore lock: {self.hashKey}, total locks holding: {self.get_active_count()} out of {self.limit}")
            return result == 1
        except RedisError as e:
            raise RuntimeError(f"Failed to acquire semaphore: {e}")

    def release_lock(self):
        try:
            self.redisConn.hdel(self.hashKey, self.process_unique_id)
            log.info(f"Released semaphore lock: {self.hashKey}, total locks holding {self.get_active_count()} out of {self.limit}")
        except RedisError as e:
            raise RuntimeError(f"Failed to release semaphore: {e}")

    def acquire_blocking(self):
        if self.is_acquired_by_process():
            raise RuntimeError("Semaphore already acquired")
        end_time = time.time() + self.acquireTimeOut
        backoff = 0.1
        while time.time() < end_time:
            if self.acquire_lock():
                return self.process_unique_id
            jitter = random.uniform(0, 0.1)
            time.sleep(backoff + jitter)
            backoff = min(backoff * 2, 2)
        raise RuntimeError(f"Timeout: Unable to acquire the semaphore lock {self.hashKey}, total locks holding {self.get_active_count()} out of {self.limit}")

    def __enter__(self):
        self.acquire_blocking()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.is_acquired_by_process():
            return
        self.release_lock()


# The rest of your helper functions can stay as they were, for example:
def get_redis_semaphore(
    key: str,
    process_unique_id: str | None = None,
    count: int = 2,
    acquire_time_out: int = 60,
    lock_time_to_live: int = 60,
) -> Semaphore:
    return Semaphore(
        namespace=f"{config.REDIS_KIT_SEMAPHORE_SETTINGS_REDIS_NAMESPACE}:{key}",
        limit=count,
        acquire_time_out=acquire_time_out,
        lock_time_to_live=lock_time_to_live,
        process_unique_id=process_unique_id,
    )


if __name__ == "__main__":
    redisClient.InitRedisConnectionPool()
    key = "SomeTTL_TEST"
    sem = Semaphore(
        namespace=f"{config.REDIS_KIT_SEMAPHORE_SETTINGS_REDIS_NAMESPACE}:{key}",
        limit=1,
        acquire_time_out=60,
        lock_time_to_live=None,
        process_unique_id=None,
    )
    sem.acquire_blocking()
    with sem:
        print(sem.get_active_count())
