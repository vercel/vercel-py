from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import Generic, TypeVar

from vercel._internal.iter_coroutine import iter_coroutine

PageT = TypeVar("PageT")
ItemT = TypeVar("ItemT")
PageInfoT = TypeVar("PageInfoT")


@dataclass(frozen=True)
class PageController(Generic[PageT, ItemT, PageInfoT]):
    get_items: Callable[[PageT], Sequence[ItemT]]
    get_next_page_info: Callable[[PageT], PageInfoT | None]
    fetch_next_page: Callable[[PageInfoT], Awaitable[PageT]]

    def has_next_page(self, page: PageT) -> bool:
        return self.get_next_page_info(page) is not None

    def next_page_info(self, page: PageT) -> PageInfoT | None:
        return self.get_next_page_info(page)

    async def get_next_page(self, page: PageT) -> PageT | None:
        next_page_info = self.get_next_page_info(page)
        if next_page_info is None:
            return None
        return await self.fetch_next_page(next_page_info)

    def get_next_page_sync(self, page: PageT) -> PageT | None:
        return iter_coroutine(self.get_next_page(page))

    async def iter_pages(self, initial_page: PageT) -> AsyncGenerator[PageT, None]:
        page = initial_page
        while True:
            yield page
            next_page = await self.get_next_page(page)
            if next_page is None:
                return
            page = next_page

    async def iter_items(self, initial_page: PageT) -> AsyncGenerator[ItemT, None]:
        async for page in self.iter_pages(initial_page):
            for item in self.get_items(page):
                yield item

    def iter_pages_sync(self, initial_page: PageT) -> Iterator[PageT]:
        iterator = self.iter_pages(initial_page)
        try:
            while True:
                try:
                    yield iter_coroutine(iterator.__anext__())
                except StopAsyncIteration:
                    return
        finally:
            try:
                iter_coroutine(iterator.aclose())
            except RuntimeError:
                pass

    def iter_items_sync(self, initial_page: PageT) -> Iterator[ItemT]:
        iterator = self.iter_items(initial_page)
        try:
            while True:
                try:
                    yield iter_coroutine(iterator.__anext__())
                except StopAsyncIteration:
                    return
        finally:
            try:
                iter_coroutine(iterator.aclose())
            except RuntimeError:
                pass


__all__ = ["PageController"]
