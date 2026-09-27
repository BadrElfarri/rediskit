# Re-export the two client modules under tidy names
from rediskit.redis import a_client, client
from rediskit.redis.node import get_redis_top_node

__all__ = ["a_client", "client", "get_redis_top_node"]
