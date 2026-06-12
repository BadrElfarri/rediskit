"""
rediskit - Redis-backed performance and concurrency primitives for Python applications.

Provides caching, distributed coordination, and data protection using Redis.
"""

from rediskit.async_semaphore import AsyncSemaphore
from rediskit.encrypter import Encrypter
from rediskit.memoize import a_redis_memoize, redis_memoize
from rediskit.redis import a_client, client, get_redis_top_node
from rediskit.redis.a_client import async_connection_close, get_async_redis_connection, init_async_redis_connection_pool
from rediskit.redis.client import close, get_redis_connection, init_redis_connection_pool
from rediskit.redis_lock import get_async_redis_mutex_lock, get_redis_mutex_lock
from rediskit.retry_decorator import RetryPolicy, retry_async
from rediskit.semaphore import Semaphore

__all__ = [
    # Redis client
    "get_redis_top_node",
    "a_client",
    "client",
    # Connection management
    "init_redis_connection_pool",
    "get_redis_connection",
    "close",
    "init_async_redis_connection_pool",
    "get_async_redis_connection",
    "async_connection_close",
    # Redis
    "redis_memoize",
    "a_redis_memoize",
    "get_redis_mutex_lock",
    "get_async_redis_mutex_lock",
    "Semaphore",
    "AsyncSemaphore",
    "RetryPolicy",
    "retry_async",
    # Encryption,
    "Encrypter",
]
