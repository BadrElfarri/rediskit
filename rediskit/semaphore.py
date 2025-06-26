import logging
import random
import threading
import time
import uuid

from redis import RedisError

from rediskit import config, redis_client

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class Semaphore:
    def __init__(
        self,
        key: str,
        limit: int,
        acquire_timeout: int,
        lock_ttl: int | None,
        redis_conn: str | None = None,
        process_unique_id: str | None = None,
        ttl_auto_renewal: bool = True,
    ):
        if limit <= 0:
            raise ValueError("Limit must be positive")
        if acquire_timeout <= 0:
            raise ValueError("Acquire timeout must be positive")
        if lock_ttl is not None and lock_ttl <= 0:
            raise ValueError("Lock TTL must be positive or None")

        self.redisConn = redis_conn if redis_conn else redis_client.get_redis_connection()
        self.namespace = f"{config.REDIS_TOP_NODE}:{key}"
        self.limit = limit
        self.acquireTimeOut = acquire_timeout
        self.ttl = lock_ttl or 0
        self.process_unique_id = str(uuid.uuid4()) if not process_unique_id else process_unique_id
        self.hashKey = f"{self.namespace}:holders"
        self.ttl_auto_renewal = ttl_auto_renewal if self.ttl else False
        self._renew_ttl_thread = None
        self._stop_ttl_renew = threading.Event()

    def _stop_ttl_renewal(self):
        if not self.ttl:
            raise ValueError("TTL must be set to stop TTL renewal")

        self._stop_ttl_renew.set()
        if self._renew_ttl_thread and self._renew_ttl_thread.is_alive():
            self._renew_ttl_thread.join(timeout=2)
        self._renew_ttl_thread = None

    def _start_ttl_renewal(self):
        if not self.ttl:
            raise ValueError("TTL must be set to start TTL renewal")

        self._stop_ttl_renew.clear()
        self._renew_ttl_thread = threading.Thread(target=self._renew_loop, daemon=True)
        self._renew_ttl_thread.start()

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
            if self.ttl:
                result = self.redisConn.eval(lua_script, 1, self.hashKey, self.process_unique_id, self.limit, acquired_time_stamp, self.ttl)
            else:
                result = self.redisConn.hset(self.hashKey, mapping={self.process_unique_id: acquired_time_stamp})

            if result == 1:
                log.info(f"Acquired semaphore lock: {self.hashKey}, total locks holding: {self.get_active_count()} out of {self.limit}")
                if self.ttl and self.ttl_auto_renewal:
                    self._start_ttl_renewal()
            return result == 1
        except RedisError as e:
            raise RuntimeError(f"Failed to acquire semaphore: {e}")

    def _renew_loop(self):
        renew_interval = max(1, self.ttl // 2)
        while not self._stop_ttl_renew.is_set():
            time.sleep(renew_interval)
            try:
                self.redisConn.hexpire(self.hashKey, self.ttl, self.process_unique_id)  # type: ignore  # hexpire do exist in new redis version
                log.debug(f"Renewed TTL for {self.hashKey} - {self.process_unique_id}")
            except Exception as e:
                log.warning(f"Semaphore TTL renewal failed: {e}")

    def release_lock(self):
        if self.ttl and self.ttl_auto_renewal:
            self._stop_ttl_renewal()
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

    def get_active_process_unique_ids(self) -> set[str]:
        return set(self.redisConn.hkeys(self.hashKey))  # type: ignore

    def __enter__(self):
        self.acquire_blocking()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.is_acquired_by_process():
            return
        self.release_lock()
