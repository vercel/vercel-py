"""Context owned by the current Vercel Function invocation."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import TypeAlias

WaitUntil: TypeAlias = Callable[[Awaitable[object]], None]

_wait_until: ContextVar[WaitUntil | None] = ContextVar("vercel_wait_until", default=None)


def get_wait_until() -> WaitUntil | None:
    """Return the current invocation's wait-until callback, if any."""
    return _wait_until.get()


def set_wait_until(callback: WaitUntil | None) -> None:
    """Set or clear the current invocation's wait-until callback."""
    _wait_until.set(callback)


__all__ = [
    "WaitUntil",
    "get_wait_until",
    "set_wait_until",
]
