import os

from dotenv import load_dotenv

from rediskit.utils import base64_json_to_dict

if os.path.isfile("private.env"):
    load_dotenv("private.env")
if os.path.isfile(".env"):
    load_dotenv(".env")

# Redis Settings
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")
REDIS_TOP_NODE = os.environ.get("REDIS_TOP_NODE", "redis_kit_node")
REDIS_SCAN_COUNT = int(os.environ.get("REDIS_SCAN_COUNT", "10000"))
REDIS_SKIP_CACHING = os.environ.get("REDIS_SKIP_CACHING", "false").upper() == "TRUE"

# Sentinel Settings (high-availability master discovery, sync + async).
#
# When REDIS_SENTINEL_ENABLED is true, BOTH clients are built through Redis
# Sentinel instead of connecting straight to REDIS_HOST. Sentinel is queried for
# the current master on every (re)connect, so the clients automatically follow a
# failover instead of staying pinned to a node that was demoted to a read-only
# replica (the classic `ReadOnlyError: You can't write against a read only
# replica` after a Sentinel-driven failover).
REDIS_SENTINEL_ENABLED = os.environ.get("REDIS_SENTINEL_ENABLED", "false").upper() == "TRUE"
# Comma-separated sentinel endpoints, e.g. "sentinel-a:26379,sentinel-b:26379".
# A bare host with no ":port" falls back to REDIS_SENTINEL_PORT. A single k8s
# Service DNS name that resolves to all sentinels (e.g.
# "redis-sentinel-sentinel.redis.svc.cluster.local:26379") is fine. No default:
# enabling Sentinel without setting this raises at client build time instead of
# silently targeting a guessed hostname.
REDIS_SENTINEL_HOSTS = os.environ.get("REDIS_SENTINEL_HOSTS", "")
REDIS_SENTINEL_PORT = int(os.environ.get("REDIS_SENTINEL_PORT", "26379"))
# The monitored master's name as configured in Sentinel (Sentinel's
# `sentinel monitor <name> ...`). The opstree operator names it "myMaster".
REDIS_SENTINEL_MASTER_NAME = os.environ.get("REDIS_SENTINEL_MASTER_NAME", "myMaster")
# Sentinels usually run without auth; set only if your sentinels require a
# password. This is independent of REDIS_PASSWORD (which authenticates the data
# nodes) — sending a password to a no-auth sentinel is an error, so it is only
# forwarded when non-empty.
REDIS_SENTINEL_PASSWORD = os.environ.get("REDIS_SENTINEL_PASSWORD", "")
# Number of transparent retries for a single command during a failover, each
# re-resolving the current master via Sentinel. The retry budget (with the
# exponential backoff used in sentinel.py, ~N seconds for N>~6 retries) MUST
# exceed the failover window or writes raise mid-failover
# (`ConnectionError: The previous master is now a slave` / retries exhausted).
# The window is roughly Sentinel `down-after-milliseconds` + promotion (+ up to
# `failover-timeout` in the worst case). Default 20 ≈ 16.6s budget, covering a
# down-after 5s + failover-timeout 10s setup. Raise it if your window is larger,
# or lower `down-after-milliseconds` to shrink the window (and the block time).
REDIS_KIT_RETRY_ATTEMPTS = int(os.environ.get("REDIS_KIT_RETRY_ATTEMPTS", "20"))

# Lock Settings
REDIS_KIT_LOCK_SETTINGS_REDIS_NAMESPACE = os.environ.get("REDIS_KIT_LOCK_SETTINGS_REDIS_NAMESPACE", f"{REDIS_TOP_NODE}:LOCK")
REDIS_KIT_LOCK_ASYNC_SETTINGS_REDIS_NAMESPACE = os.environ.get("REDIS_KIT_LOCK_ASYNC_SETTINGS_REDIS_NAMESPACE", f"{REDIS_TOP_NODE}:LOCK_ASYNC")
REDIS_KIT_LOCK_CACHE_MUTEX = os.environ.get("REDIS_KIT_LOCK_CACHE_MUTEX", "REDIS_KIT_LOCK_CACHE_MUTEX")

# Encryption Settings
REDIS_KIT_ENCRYPTION_SECRET = base64_json_to_dict(os.environ.get("REDIS_KIT_ENCRYPTION_SECRET", ""))
