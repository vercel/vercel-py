from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx

SleepFn = Callable[[float], Awaitable[None] | None]


@dataclass(frozen=True)
class RetryPolicy:
    """Configuration for automatic request retries."""

    retries: int = 0
    retry_on_network_error: bool = True
    retry_on_response: Callable[[httpx.Response], bool] | None = None
    backoff_base: float = 0.1
    backoff_max: float = 2.0


__all__ = ["RetryPolicy"]
