import redis

from rediskit.memoize import RedisMemoize
from rediskit.redisClient import InitRedisConnectionPool


def test_basic_usage():
    """Test basic decorator usage with default connection."""
    print("Testing basic usage...")

    InitRedisConnectionPool()

    @RedisMemoize(memoizeKey="test_basic", ttl=300)
    def basic_function(tenantId: str, value: int) -> dict:
        print(f"Computing for {value}")
        return {"result": value * 2}

    result1 = basic_function("tenant1", 5)
    print(f"First call result: {result1}")

    result2 = basic_function("tenant1", 5)  # Should be cached
    print(f"Second call result: {result2}")

    assert result1 == result2
    print("✓ Basic usage test passed")


def test_custom_connection():
    """Test decorator with custom Redis connection."""
    print("\nTesting custom connection...")

    custom_redis = redis.Redis(host="localhost", port=6379, db=0)

    @RedisMemoize(memoizeKey="test_custom", ttl=300, connection=custom_redis)
    def custom_function(tenantId: str, data: str) -> dict:
        print(f"Processing {data}")
        return {"processed": data.upper()}

    result = custom_function("tenant1", "hello")
    print(f"Custom connection result: {result}")

    assert result["processed"] == "HELLO"
    print("✓ Custom connection test passed")


def test_hash_storage():
    """Test hash-based storage."""
    print("\nTesting hash storage...")

    @RedisMemoize(memoizeKey=lambda tenantId, user_id: f"user:{tenantId}:{user_id}", ttl=600, storageType="hash", enableEncryption=True)
    def get_user_data(tenantId: str, user_id: str) -> dict:
        print(f"Fetching user {user_id}")
        return {"user_id": user_id, "name": f"User {user_id}"}

    result = get_user_data("tenant1", "123")
    print(f"Hash storage result: {result}")

    assert result["user_id"] == "123"
    print("✓ Hash storage test passed")


if __name__ == "__main__":
    try:
        test_basic_usage()
        test_custom_connection()
        test_hash_storage()
        print("\n🎉 All public API tests passed!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        raise
