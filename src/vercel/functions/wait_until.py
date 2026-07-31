from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

from vercel.cache.context import get_context

if TYPE_CHECKING:
    from collections.abc import Awaitable


def wait_until(awaitable: Awaitable[object]) -> None:
    """Keep the current Function invocation alive for an awaitable.

    The response may be sent before the awaitable completes. Its completion is
    still bounded by the Function's configured maximum duration.

    For synchronous work, pass ``asyncio.to_thread(func)``.
    """
    if not inspect.isawaitable(awaitable):
        hint = (
            "; call it first, or wrap synchronous work in asyncio.to_thread(...)"
            if callable(awaitable)
            else ""
        )
        msg = (
            f"wait_until can only be called with an awaitable, got {type(awaitable).__name__}{hint}"
        )
        raise TypeError(msg)

    callback = get_context().wait_until
    if callback is not None:
        callback(awaitable)
    elif inspect.iscoroutine(awaitable):
        # JavaScript Promises are already active when waitUntil is called, so
        # its no-context behavior can safely be a no-op. A Python coroutine is
        # lazy; close it to preserve that no-op without leaking a warning.
        awaitable.close()
