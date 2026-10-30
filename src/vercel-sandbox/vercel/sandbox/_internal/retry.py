"""Sandbox request retry policy, shared with command stream recovery."""

import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

from vercel._internal.core.http._compat import transport_errors
from vercel.sandbox._internal.errors import SandboxApiError

AsyncSleep = Callable[[float], Awaitable[None]]
_ResultT = TypeVar("_ResultT")
_RETRIES = 2


def _is_retryable(error: Exception) -> bool:
    return isinstance(error, transport_errors()) or (
        isinstance(error, SandboxApiError)
        and (error.status_code == 429 or 500 <= error.status_code < 600)
    )


def _retry_delay(error: Exception, failures: int) -> float | None:
    """Return randomized backoff plus any short server-requested wait.

    Like the TypeScript SDK, only numeric Retry-After values are interpreted,
    and a rate limit asking for more than 20 seconds is returned immediately.
    """
    retry_after = 0.0
    if isinstance(error, SandboxApiError) and error.status_code == 429:
        try:
            retry_after = float(error.response.headers.get("retry-after", ""))
        except ValueError:
            pass
        if retry_after > 20:
            return None
        # Also handles NaN, missing, negative, and zero values like JS Number.
        if not retry_after > 0:
            retry_after = 0.0
    return retry_after + random.uniform(0.4, 0.8) * 2**failures


async def _with_request_retries(
    operation: Callable[[], Awaitable[_ResultT]],
    *,
    sleep: AsyncSleep,
    retries: int = _RETRIES,
    is_retryable: Callable[[Exception], bool] = _is_retryable,
) -> _ResultT:
    """Retry transport failures, 429s, and 5xx responses; preserve final errors."""
    for failures in range(retries + 1):
        try:
            return await operation()
        except Exception as error:
            if failures == retries or not is_retryable(error):
                raise
            delay = _retry_delay(error, failures)
            if delay is None:
                raise
        # Cancellation propagates from both requests and waits without retrying.
        await sleep(delay)
    raise AssertionError("unreachable")
