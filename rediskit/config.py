import os

from DockerBuildSystem import TerminalTools

from rediskit.utils import base64JsonToDict

TerminalTools.LoadDefaultEnvironmentVariablesFile("private.env")
TerminalTools.LoadDefaultEnvironmentVariablesFile(".env")

# Redis Settings
REDISKIT_REDIS_HOST = os.environ.get("REDISKIT_REDIS_HOST", "localhost")
REDISKIT_REDIS_PORT = int(os.environ.get("REDISKIT_REDIS_PORT", "6379"))
REDISKIT_REDIS_PASSWORD = os.environ.get("REDISKIT_REDIS_PASSWORD", "")
REDISKIT_REDIS_TOP_NODE = os.environ.get("REDISKIT_REDIS_TOP_NODE", "redis_kit_node")
REDISKIT_REDIS_SCAN_COUNT = int(os.environ.get("REDISKIT_REDIS_SCAN_COUNT", "10000"))
REDISKIT_REDIS_SKIP_CACHING = os.environ.get("REDISKIT_REDIS_SKIP_CACHING", "false").upper() == "TRUE"

# Lock Settings
REDISKIT_LOCK_SETTINGS_REDIS_NAMESPACE = os.environ.get("REDISKIT_LOCK_SETTINGS_REDIS_NAMESPACE", f"{REDISKIT_REDIS_TOP_NODE}:LOCK")
REDISKIT_LOCK_ASYNC_SETTINGS_REDIS_NAMESPACE = os.environ.get("REDISKIT_LOCK_ASYNC_SETTINGS_REDIS_NAMESPACE", f"{REDISKIT_REDIS_TOP_NODE}:LOCK_ASYNC")
REDISKIT_LOCK_CACHE_REDIS_MUTEX = os.environ.get("REDISKIT_LOCK_CACHE_REDIS_MUTEX", "REDISKIT_LOCK_CACHE_REDIS_MUTEX")

REDISKIT_ENCRYPTION_SECRET = base64JsonToDict(os.environ.get("REDISKIT_ENCRYPTION_SECRET", ""))
