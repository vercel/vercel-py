from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Awaitable, Callable, Mapping

from .types import PurgeAPI


_cv_wait_until: ContextVar[Callable[[Awaitable[object]], None] | None] = ContextVar(
    "vercel_wait_until", default=None
)
_cv_cache: ContextVar[object | None] = ContextVar("vercel_cache", default=None)
_cv_purge: ContextVar[PurgeAPI | None] = ContextVar("vercel_purge", default=None)
_cv_headers: ContextVar[Mapping[str, str] | None] = ContextVar("vercel_headers", default=None)


@dataclass
class _ContextSnapshot:
    wait_until: Callable[[Awaitable[object]], None] | None
    cache: object | None
    purge: PurgeAPI | None
    headers: Mapping[str, str] | None


def get_context() -> _ContextSnapshot:
    return _ContextSnapshot(
        wait_until=_cv_wait_until.get(),
        cache=_cv_cache.get(),
        purge=_cv_purge.get(),
        headers=_cv_headers.get(),
    )


def set_context(
    *,
    wait_until: Callable[[Awaitable[object]], None] | None = None,
    cache: object | None = None,
    purge: PurgeAPI | None = None,
    headers: Mapping[str, str] | None = None,
) -> None:
    if wait_until is not None:
        _cv_wait_until.set(wait_until)
    if cache is not None:
        _cv_cache.set(cache)
    if purge is not None:
        _cv_purge.set(purge)
    if headers is not None:
        _cv_headers.set(headers)


def set_headers(headers: Mapping[str, str] | None) -> None:
    _cv_headers.set(headers)


def get_headers() -> Mapping[str, str] | None:
    return _cv_headers.get()
