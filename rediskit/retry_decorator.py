import asyncio
import functools
import random
from dataclasses import asdict, dataclass
from typing import Callable, ParamSpec, TypeVar

import httpx

P = ParamSpec("P")
R = TypeVar("R")


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 3
    backoff: float = 0.5  # initial delay (seconds)
    jitter: float = 0.1  # +/- seconds (fixed) — see note below
    exceptions: tuple[type[BaseException], ...] = (httpx.RequestError, httpx.HTTPStatusError)
    enabled: bool = True
    max_backoff: float | None = None  # optional cap


def _normalize_policy(default: RetryPolicy, override: "RetryPolicy | dict | None") -> RetryPolicy:
    if override is None:
        return default
    if isinstance(override, RetryPolicy):
        return override
    if isinstance(override, dict):
        base = asdict(default)
        base.update(override)
        # normalize exceptions if provided as a single class or iterable
        excs = base.get("exceptions")
        if excs is not None and not isinstance(excs, tuple):
            if isinstance(excs, type) and issubclass(excs, BaseException):
                base["exceptions"] = (excs,)
            else:
                base["exceptions"] = tuple(excs)
        return RetryPolicy(**base)
    return default


def retry_async(default: RetryPolicy | None = None) -> Callable[[Callable[P, R]], Callable[P, R]]:
    default = default or RetryPolicy()

    def deco(fn: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(fn)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            policy = _normalize_policy(default, kwargs.pop("retry_policy", None))
            if not policy.enabled or policy.attempts <= 1:
                return await fn(*args, **kwargs)

            delay = max(0.0, policy.backoff)
            for attempt in range(1, policy.attempts + 1):
                try:
                    return await fn(*args, **kwargs)
                except policy.exceptions:
                    if attempt == policy.attempts:
                        raise
                    # fixed jitter around delay; consider decorrelated/full jitter below
                    j = random.uniform(-policy.jitter, policy.jitter) if policy.jitter else 0.0
                    sleep_for = max(0.0, delay + j)
                    await asyncio.sleep(sleep_for)
                    delay = min(delay * 2, policy.max_backoff) if policy.max_backoff else delay * 2

        return wrapper

    return deco
