import asyncio
import functools
import random
from dataclasses import asdict, dataclass
from typing import Any, Awaitable, Callable, ParamSpec, TypeVar, cast

import httpx

P = ParamSpec("P")
R = TypeVar("R")


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 3
    backoff: float = 0.5  # initial delay (seconds), doubled after each retry
    jitter: float = 0.1  # +/- seconds applied to each delay
    exceptions: tuple[type[BaseException], ...] = (httpx.RequestError, httpx.HTTPStatusError)
    enabled: bool = True
    max_backoff: float | None = None  # optional cap


def _normalize_policy(default: RetryPolicy, override: "RetryPolicy | dict[str, Any] | None") -> RetryPolicy:
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


def retry_async(default: RetryPolicy | None = None) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Retry an async function on the policy's exceptions with exponential backoff.

    Callers can override the policy per call by passing a ``retry_policy``
    keyword argument (a RetryPolicy or a dict of field overrides); it is
    consumed by the wrapper and not forwarded to the wrapped function.
    """
    default_policy = default or RetryPolicy()

    def deco(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @functools.wraps(fn)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            override = cast("RetryPolicy | dict[str, Any] | None", kwargs.pop("retry_policy", None))
            policy = _normalize_policy(default_policy, override)
            if not policy.enabled or policy.attempts <= 1:
                return await fn(*args, **kwargs)

            delay = max(0.0, policy.backoff)
            for _attempt in range(1, policy.attempts):
                try:
                    return await fn(*args, **kwargs)
                except policy.exceptions:
                    j = random.uniform(-policy.jitter, policy.jitter) if policy.jitter else 0.0
                    await asyncio.sleep(max(0.0, delay + j))
                    delay = min(delay * 2, policy.max_backoff) if policy.max_backoff else delay * 2
            # Final attempt: let any exception propagate.
            return await fn(*args, **kwargs)

        return wrapper

    return deco
