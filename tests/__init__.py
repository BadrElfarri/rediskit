from rediskit import redis_client

redis_client.init_redis_connection_pool()
redis_client.init_async_redis_connection_pool()
