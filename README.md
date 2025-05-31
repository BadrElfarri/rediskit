# rediskit

A Python toolkit that provides Redis-backed performance and concurrency primitives for applications. It enables developers to add caching, distributed coordination, and data protection to their Python applications with minimal effort.

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
pip install rediskit
```

## Quick Start

### Basic Setup

```python
from rediskit import RedisMemoize, InitRedisConnectionPool

# Initialize Redis connection pool (call once at app startup)
InitRedisConnectionPool()

# Cache expensive function results
@RedisMemoize(memoizeKey="expensive_calc", ttl=300)
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
from rediskit import RedisMemoize

# Use your own Redis connection
my_redis = redis.Redis(host='my-redis-host', port=6379, db=1)

@RedisMemoize(
    memoizeKey="custom_calc", 
    ttl=600,
    connection=my_redis
)
def my_function(tenantId: str, data: dict) -> dict:
    return {"processed": data}
```

### Advanced Caching Options

```python
from rediskit import RedisMemoize

# Hash-based storage with encryption
@RedisMemoize(
    memoizeKey=lambda tenantId, user_id: f"user_profile:{tenantId}:{user_id}",
    ttl=3600,
    storageType="hash",  # Store in Redis hash for efficient field access
    enableEncryption=True,  # Encrypt sensitive data
    cacheType="zipJson"  # JSON serialization with compression
)
def get_user_profile(tenantId: str, user_id: str) -> dict:
    # Fetch user data from database
    return {"user_id": user_id, "name": "John Doe", "email": "john@example.com"}

# Dynamic TTL and cache bypass
@RedisMemoize(
    memoizeKey="dynamic_data",
    ttl=lambda tenantId, priority: 3600 if priority == "high" else 300,
    bypassCache=lambda tenantId, force_refresh: force_refresh
)
def get_dynamic_data(tenantId: str, priority: str, force_refresh: bool = False) -> dict:
    return {"data": "fresh_data", "priority": priority}
```

### Async Support

```python
import asyncio
from rediskit import RedisMemoize, InitAsyncRedisConnectionPool

# Initialize async Redis connection pool
await InitAsyncRedisConnectionPool()

@RedisMemoize(memoizeKey="async_calc", ttl=300)
async def async_expensive_function(tenantId: str, value: int) -> dict:
    await asyncio.sleep(1)  # Simulate async work
    return {"async_result": value * 100}

# Usage
result = await async_expensive_function("tenant1", 5)
```

### Distributed Locking

```python
from rediskit import GetRedisMutexLock, GetAsyncRedisMutexLock

# Synchronous distributed lock
with GetRedisMutexLock("critical_section", expire=30) as lock:
    # Only one process can execute this block at a time
    perform_critical_operation()

# Async distributed lock
async with GetAsyncRedisMutexLock("async_critical_section", expire=30) as lock:
    await perform_async_critical_operation()
```

### Encryption Management

```python
from rediskit import Encrypter

# Generate new encryption keys
encrypter = Encrypter()
new_key = encrypter.generateNewHexKey()

# Encrypt/decrypt data manually
encrypted = encrypter.encrypt("sensitive data", useZstd=True)
decrypted = encrypter.decrypt(encrypted)
```

## Configuration

Configure rediskit using environment variables:

```bash
# Redis connection settings
export REDISKIT_REDIS_HOST="localhost"
export REDISKIT_REDIS_PORT="6379"
export REDISKIT_REDIS_PASSWORD=""

# Encryption keys (base64-encoded JSON)
export REDISKIT_ENCRYPTION_SECRET="eyJfX2VuY192MSI6ICI0MGViODJlNWJhNTJiNmQ4..."

# Cache settings
export REDISKIT_REDIS_TOP_NODE="my_app_cache"
export REDISKIT_REDIS_SKIP_CACHING="false"
```

## API Reference

### Core Decorators

#### `@RedisMemoize`

Cache function results in Redis with configurable options.

**Parameters:**
- `memoizeKey`: Cache key (string or callable)
- `ttl`: Time to live in seconds (int, callable, or None)
- `bypassCache`: Skip cache lookup (bool or callable)
- `cacheType`: Serialization method ("zipJson" or "zipPickled")
- `resetTtlUponRead`: Refresh TTL when reading from cache
- `enableEncryption`: Encrypt cached data
- `storageType`: Redis storage pattern ("string" or "hash")
- `connection`: Custom Redis connection (optional)

### Connection Management

- `InitRedisConnectionPool()`: Initialize sync Redis connection pool
- `InitAsyncRedisConnectionPool()`: Initialize async Redis connection pool
- `GetRedisConnection()`: Get sync Redis connection
- `GetAsyncRedisConnection()`: Get async Redis connection

### Distributed Locking

- `GetRedisMutexLock(name, expire, auto_renewal, id)`: Get sync distributed lock
- `GetAsyncRedisMutexLock(name, expire, auto_renewal)`: Get async distributed lock

### Encryption

- `Encrypter(keyHexDict)`: Encryption/decryption with key versioning

## Requirements

- Python 3.12+
- Redis server
- Dependencies: redis, redis-lock, nacl, zstd

## License

[Add your license information here]
Powerful Python toolkit for Redis: fast function memoization, distributed locks, and semaphores—ready for sync and async apps.
