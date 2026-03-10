from __future__ import annotations

from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import dataclass

from .types import PurgeAPI

_cv_cache: ContextVar[object | None] = ContextVar("vercel_cache", default=None)
_cv_purge: ContextVar[PurgeAPI | None] = ContextVar("vercel_purge", default=None)
_cv_headers: ContextVar[Mapping[str, str] | None] = ContextVar("vercel_headers", default=None)


@dataclass
class _ContextSnapshot:
    cache: object | None
    purge: PurgeAPI | None
    headers: Mapping[str, str] | None


def get_context() -> _ContextSnapshot:
    return _ContextSnapshot(
        cache=_cv_cache.get(),
        purge=_cv_purge.get(),
        headers=_cv_headers.get(),
    )


def set_context(
    *,
    cache: object | None = None,
    purge: PurgeAPI | None = None,
    headers: Mapping[str, str] | None = None,
) -> None:
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
