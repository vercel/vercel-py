"""Private pagination helpers for the stable client surface."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Generic, TypeVar

_TPage = TypeVar("_TPage")


class PageIterator(Generic[_TPage]):
    """Minimal sync iterator wrapper used by public paginator adapters."""

    def __init__(self, fetch_next: Callable[[], _TPage | None]) -> None:
        self._fetch_next = fetch_next

    def __iter__(self) -> Iterator[_TPage]:
        while True:
            page = self._fetch_next()
            if page is None:
                return
            yield page


class AsyncPageIterator(Generic[_TPage]):
    """Minimal async iterator wrapper used by public paginator adapters."""

    def __init__(self, fetch_next: Callable[[], Awaitable[_TPage | None]]) -> None:
        self._fetch_next = fetch_next

    async def __aiter__(self) -> AsyncIterator[_TPage]:
        while True:
            page = await self._fetch_next()
            if page is None:
                return
            yield page


__all__ = ["AsyncPageIterator", "PageIterator"]
