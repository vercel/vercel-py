from __future__ import annotations

from typing import TypeVar

import time
from collections.abc import Awaitable, Callable

import anyio

from .errors import RetryableError, ThrottledError
from .log import debug_log

T = TypeVar("T")

_TRANSIENT_FOLLOW_UP_RETRY_SECONDS = 0.1
DIRECTIVE_FOLLOW_UP_ATTEMPTS = 3


async def retry_async_follow_up(
    operation: Callable[[], Awaitable[T]],
    *,
    event_prefix: str = "follow_up",
    sleep: Callable[[float], Awaitable[None]] | None = None,
) -> T:
    sleep = sleep or anyio.sleep
    last_error: BaseException | None = None
    for attempt in range(DIRECTIVE_FOLLOW_UP_ATTEMPTS):
        try:
            debug_log(f"{event_prefix}.retry_attempt", attempt=attempt + 1)
            return await operation()
        except Exception as exc:
            if not _is_retryable_follow_up_error(exc):
                raise
            last_error = exc
            if attempt == DIRECTIVE_FOLLOW_UP_ATTEMPTS - 1:
                break
            await sleep(_follow_up_retry_delay(exc))
    if last_error is not None:
        debug_log(
            f"{event_prefix}.retry_exhausted",
            attempts=DIRECTIVE_FOLLOW_UP_ATTEMPTS,
            exception_class=last_error.__class__.__name__,
            exception_message=str(last_error),
        )
        raise last_error
    raise RuntimeError("retry loop exited without result or error")


def retry_sync_follow_up(
    operation: Callable[[], T],
    *,
    event_prefix: str = "follow_up",
    sleep: Callable[[float], None] | None = None,
) -> T:
    sleep = sleep or time.sleep
    last_error: BaseException | None = None
    for attempt in range(DIRECTIVE_FOLLOW_UP_ATTEMPTS):
        try:
            debug_log(f"{event_prefix}.retry_attempt", attempt=attempt + 1)
            return operation()
        except Exception as exc:
            if not _is_retryable_follow_up_error(exc):
                raise
            last_error = exc
            if attempt == DIRECTIVE_FOLLOW_UP_ATTEMPTS - 1:
                break
            sleep(_follow_up_retry_delay(exc))
    if last_error is not None:
        debug_log(
            f"{event_prefix}.retry_exhausted",
            attempts=DIRECTIVE_FOLLOW_UP_ATTEMPTS,
            exception_class=last_error.__class__.__name__,
            exception_message=str(last_error),
        )
        raise last_error
    raise RuntimeError("retry loop exited without result or error")


def _is_retryable_follow_up_error(exc: BaseException) -> bool:
    return isinstance(exc, RetryableError) and exc.retryable


def _follow_up_retry_delay(exc: BaseException) -> float:
    if isinstance(exc, ThrottledError) and exc.retry_after is not None:
        return max(1.0, float(exc.retry_after))
    return _TRANSIENT_FOLLOW_UP_RETRY_SECONDS
