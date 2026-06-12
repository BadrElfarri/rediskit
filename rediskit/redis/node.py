from rediskit import config


def get_redis_top_node(tenant_id: str | None, key: str | None, top_node: str = config.REDIS_TOP_NODE) -> str:
    if tenant_id is None and key is None:
        raise ValueError("Tenant and key are missing!")
    return ":".join(part for part in (top_node, tenant_id, key) if part is not None)
