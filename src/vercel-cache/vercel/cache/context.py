from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from contextvars import ContextVar
from dataclasses import dataclass

from vercel.headers import get_headers as _get_headers, set_headers as _set_headers

from .types import PurgeAPI

_cv_wait_until: ContextVar[Callable[[Awaitable[object]], None] | None] = ContextVar(
    "vercel_wait_until", default=None
)
_cv_cache: ContextVar[object | None] = ContextVar("vercel_cache", default=None)
_cv_async_cache: ContextVar[object | None] = ContextVar("vercel_async_cache", default=None)
_cv_purge: ContextVar[PurgeAPI | None] = ContextVar("vercel_purge", default=None)


class _Unset: ...


UNSET = _Unset()


@dataclass
class _ContextSnapshot:
    wait_until: Callable[[Awaitable[object]], None] | None
    cache: object | None
    async_cache: object | None
    purge: PurgeAPI | None
    headers: Mapping[str, str] | None


def get_context() -> _ContextSnapshot:
    return _ContextSnapshot(
        wait_until=_cv_wait_until.get(),
        cache=_cv_cache.get(),
        async_cache=_cv_async_cache.get(),
        purge=_cv_purge.get(),
        headers=_get_headers(),
    )


def set_context(
    *,
    wait_until: Callable[[Awaitable[object]], None] | None | _Unset = UNSET,
    cache: object | None | _Unset = UNSET,
    async_cache: object | None | _Unset = UNSET,
    purge: PurgeAPI | None | _Unset = UNSET,
    headers: Mapping[str, str] | None | _Unset = UNSET,
) -> None:
    if not isinstance(wait_until, _Unset):
        _cv_wait_until.set(wait_until)
    if not isinstance(cache, _Unset):
        _cv_cache.set(cache)
    if not isinstance(async_cache, _Unset):
        _cv_async_cache.set(async_cache)
    if not isinstance(purge, _Unset):
        _cv_purge.set(purge)
    if not isinstance(headers, _Unset):
        _set_headers(headers)


def set_headers(headers: Mapping[str, str] | None) -> None:
    _set_headers(headers)


def get_headers() -> Mapping[str, str] | None:
    return _get_headers()
