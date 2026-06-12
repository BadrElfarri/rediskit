# rediskit

A Python toolkit that provides Redis-backed performance and concurrency primitives for applications. It enables developers to add caching, distributed coordination, and data protection to their Python applications with minimal effort.

## Still work in progress
Many features are still under development, there will be many breaking changes. Please use at your own risk.


## Features

- **Function Result Caching**: Use the `@RedisMemoize` decorator to cache expensive function calls with automatic serialization, compression, and encryption
- **Distributed Coordination**: Redis-based distributed locks and semaphores for coordinating access across multiple processes/machines
- **Data Protection**: Multi-version encryption keys with automatic key rotation for sensitive cached data
- **Async Support**: Full support for both synchronous and asynchronous applications
- **Flexible Storage**: Choose between string or hash-based Redis storage patterns
- **Modern Type Hints**: Full type safety with Python 3.12+ syntax

## Installation

```bash
uv add rediskit
# or
poetry add rediskit
```

## Quick Start

### Basic Setup

```python
from rediskit import redis_memoize, init_redis_connection_pool

# Initialize Redis connection pool (call once at app startup)
init_redis_connection_pool()


# Cache expensive function results
@redis_memoize(memoize_key="expensive_calc", ttl=300)
def expensive_calculation(tenantId: str, value: int) -> dict:
    # Simulate expensive computation
    import time
    time.sleep(2)
    return {"result": value * 42}


# Usage
result = expensive_calculation("tenant1", 10)  # Takes 2 seconds
result = expensive_calculation("tenant1", 10)  # Returns instantly from cache
```

### Custom Redis Connection

```python
import redis
from rediskit import redis_memoize

# Use your own Redis connection
my_redis = redis.Redis(host='my-redis-host', port=6379, db=1)


@redis_memoize(
    memoize_key="custom_calc",
    ttl=600,
    connection=my_redis
)
def my_function(tenantId: str, data: dict) -> dict:
    return {"processed": data}
```

### Advanced Caching Options

```python
from rediskit import redis_memoize


# Hash-based storage with encryption
@redis_memoize(
    memoize_key=lambda tenantId, user_id: f"user_profile:{tenantId}:{user_id}",
    ttl=3600,
    storage_type="hash",  # Store in Redis hash for efficient field access
    enable_encryption=True,  # Encrypt sensitive data
    cache_type="zipJson"  # JSON serialization with compression
)
def get_user_profile(tenantId: str, user_id: str) -> dict:
    # Fetch user data from database
    return {"user_id": user_id, "name": "John Doe", "email": "john@example.com"}


# Dynamic TTL and cache bypass
@redis_memoize(
    memoize_key="dynamic_data",
    ttl=lambda tenantId, priority: 3600 if priority == "high" else 300,
    bypass_cache=lambda tenantId, force_refresh: force_refresh
)
def get_dynamic_data(tenantId: str, priority: str, force_refresh: bool = False) -> dict:
    return {"data": "fresh_data", "priority": priority}
```

### Async Support

```python
import asyncio
from rediskit import a_redis_memoize, init_async_redis_connection_pool


# Use a_redis_memoize for async functions
@a_redis_memoize(memoize_key="async_calc", ttl=300)
async def async_expensive_function(tenantId: str, value: int) -> dict:
    await asyncio.sleep(1)  # Simulate async work
    return {"async_result": value * 100}


async def main() -> None:
    # Initialize async Redis connection pool (once per event loop)
    await init_async_redis_connection_pool()
    result = await async_expensive_function("tenant1", 5)


asyncio.run(main())
```

### Distributed Locking

```python
from rediskit import get_redis_mutex_lock, get_async_redis_mutex_lock

# Synchronous distributed lock
with get_redis_mutex_lock("critical_section", expire=30) as lock:
    # Only one process can execute this block at a time
    perform_critical_operation()

# Async distributed lock
async with get_async_redis_mutex_lock("async_critical_section", expire=30) as lock:
    await perform_async_critical_operation()
```

### Encryption Management

```python
from rediskit import Encrypter

# Generate new encryption keys
encrypter = Encrypter()
new_key = encrypter.generate_new_hex_key()

# Encrypt/decrypt data manually
encrypted = encrypter.encrypt("sensitive data", useZstd=True)
decrypted = encrypter.decrypt(encrypted)
```

## Configuration

Configure rediskit using environment variables (loaded from `.env` / `private.env` if present):

```bash
# Redis connection settings
export REDIS_HOST="localhost"
export REDIS_PORT="6379"
export REDIS_PASSWORD=""

# Encryption keys (base64-encoded JSON, e.g. produced by Encrypter.encode_keys_dict_to_base64)
export REDIS_KIT_ENCRYPTION_SECRET="eyJfX2VuY192MSI6ICI0MGViODJlNWJhNTJiNmQ4..."

# Cache settings
export REDIS_TOP_NODE="my_app_cache"      # key prefix, default "redis_kit_node"
export REDIS_SKIP_CACHING="false"         # short-circuit cache reads
export REDIS_SCAN_COUNT="10000"           # SCAN batch size hint
```

## API Reference

### Core Decorators

#### `@redis_memoize` / `@a_redis_memoize`

Cache function results in Redis with configurable options. Use `redis_memoize` for sync functions and `a_redis_memoize` for async functions.

**Parameters:**
- `memoize_key`: Cache key (string or callable)
- `ttl`: Time to live in seconds (int, callable, or None)
- `bypass_cache`: Skip cache lookup (bool or callable)
- `cache_type`: Serialization method ("zipJson" or "zipPickled")
- `reset_ttl_upon_read`: Refresh TTL when reading from cache
- `enable_encryption`: Encrypt cached data
- `storage_type`: Redis storage pattern ("string" or "hash")
- `connection`: Custom Redis connection (optional)
- `lock_sleep` (`a_redis_memoize` only): Seconds between mutex acquisition attempts

### Connection Management

- `init_redis_connection_pool()`: Initialize sync Redis connection pool
- `init_async_redis_connection_pool()`: Initialize async Redis connection pool (per event loop)
- `get_redis_connection()`: Get sync Redis connection
- `get_async_redis_connection()`: Get async Redis connection
- `close()` / `async_connection_close()`: Tear down the sync / async pool

### Distributed Coordination

- `get_redis_mutex_lock(lock_name, expire, auto_renewal, id)`: Get sync distributed lock
- `get_async_redis_mutex_lock(lock_name, expire, sleep, blocking, blocking_timeout, ...)`: Get async distributed lock
- `Semaphore` / `AsyncSemaphore`: Distributed semaphore limiting concurrent holders across processes

### Encryption

- `Encrypter(keyHexDict)`: Encryption/decryption with key versioning

## Requirements

- Python 3.12+
- Redis server (RedisJSON module required for the JSON cache helpers, hash-field TTLs require Redis 7.4+)
- Dependencies: redis, python-redis-lock, pynacl, zstd, httpx, python-dotenv

## License
Apache-2.0 license
