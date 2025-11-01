from rediskit import config


def get_redis_top_node(tenant_id: str | None, key: str | None, top_node: str = config.REDIS_TOP_NODE) -> str:
    if tenant_id is None and key is None:
        raise ValueError("Tenant and key are missing!")
    return f"{top_node}:{tenant_id}:{key}" if tenant_id is not None else f"{top_node}:{key}"
