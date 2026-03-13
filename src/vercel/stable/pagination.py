"""Public pagination types for the stable client surface."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from typing import Generic, TypeVar

from vercel._internal.stable.pagination import AsyncPageIterator, PageIterator

_TItem = TypeVar("_TItem")
_TPage = TypeVar("_TPage")


@dataclass(frozen=True, slots=True)
class Page(Generic[_TItem]):
    items: tuple[_TItem, ...] = field(default_factory=tuple)
    next_cursor: str | None = None
    has_next_page: bool = False


class Paginator(Generic[_TPage, _TItem]):
    def __init__(self, iterator: PageIterator[_TPage]) -> None:
        self._iterator = iterator

    def __iter__(self) -> Iterator[_TPage]:
        return iter(self._iterator)


class AsyncPaginator(Generic[_TPage, _TItem]):
    def __init__(self, iterator: AsyncPageIterator[_TPage]) -> None:
        self._iterator = iterator

    def __aiter__(self) -> AsyncIterator[_TPage]:
        return self._iterator.__aiter__()


__all__ = ["Page", "Paginator", "AsyncPaginator"]
