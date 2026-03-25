from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Generator, Iterator
from dataclasses import dataclass, field
from typing import Any

from vercel._internal.pagination import PageController
from vercel._internal.sandbox.models import (
    Pagination,
    Sandbox as SandboxModel,
    Snapshot as SnapshotModel,
)
from vercel._internal.sandbox.pagination import (
    SandboxPageInfo,
    SnapshotPageInfo,
    next_sandbox_page_info,
    next_snapshot_page_info,
)


@dataclass(slots=True)
class SandboxPage:
    sandboxes: list[SandboxModel]
    pagination: Pagination
    _controller: PageController[SandboxPage, SandboxModel, SandboxPageInfo] = field(
        init=False,
        repr=False,
    )

    @classmethod
    def create(
        cls,
        *,
        sandboxes: list[SandboxModel],
        pagination: Pagination,
        fetch_next_page: Callable[[SandboxPageInfo], Awaitable[SandboxPage]],
    ) -> SandboxPage:
        page = cls(sandboxes=list(sandboxes), pagination=pagination)
        page._controller = PageController(
            get_items=lambda current_page: current_page.sandboxes,
            get_next_page_info=lambda current_page: next_sandbox_page_info(current_page.pagination),
            fetch_next_page=fetch_next_page,
        )
        return page

    def has_next_page(self) -> bool:
        return self._controller.has_next_page(self)

    def next_page_info(self) -> SandboxPageInfo | None:
        return self._controller.next_page_info(self)

    def get_next_page(self) -> SandboxPage | None:
        return self._controller.get_next_page_sync(self)

    def iter_pages(self) -> Iterator[SandboxPage]:
        return self._controller.iter_pages_sync(self)

    def iter_items(self) -> Iterator[SandboxModel]:
        return self._controller.iter_items_sync(self)


@dataclass(slots=True)
class AsyncSandboxPage:
    sandboxes: list[SandboxModel]
    pagination: Pagination
    _controller: PageController[AsyncSandboxPage, SandboxModel, SandboxPageInfo] = field(
        init=False,
        repr=False,
    )

    @classmethod
    def create(
        cls,
        *,
        sandboxes: list[SandboxModel],
        pagination: Pagination,
        fetch_next_page: Callable[[SandboxPageInfo], Awaitable[AsyncSandboxPage]],
    ) -> AsyncSandboxPage:
        page = cls(sandboxes=list(sandboxes), pagination=pagination)
        page._controller = PageController(
            get_items=lambda current_page: current_page.sandboxes,
            get_next_page_info=lambda current_page: next_sandbox_page_info(current_page.pagination),
            fetch_next_page=fetch_next_page,
        )
        return page

    def has_next_page(self) -> bool:
        return self._controller.has_next_page(self)

    def next_page_info(self) -> SandboxPageInfo | None:
        return self._controller.next_page_info(self)

    async def get_next_page(self) -> AsyncSandboxPage | None:
        return await self._controller.get_next_page(self)

    def iter_pages(self) -> AsyncIterator[AsyncSandboxPage]:
        return self._controller.iter_pages(self)

    def iter_items(self) -> AsyncIterator[SandboxModel]:
        return self._controller.iter_items(self)


@dataclass(slots=True)
class AsyncSandboxPager:
    _fetch_first_page: Callable[[], Awaitable[AsyncSandboxPage]]
    _first_page: AsyncSandboxPage | None = field(init=False, default=None, repr=False)

    async def _get_first_page(self) -> AsyncSandboxPage:
        if self._first_page is None:
            self._first_page = await self._fetch_first_page()
        return self._first_page

    def __await__(self) -> Generator[Any, None, AsyncSandboxPage]:
        return self._get_first_page().__await__()

    def __aiter__(self) -> AsyncIterator[SandboxModel]:
        return self.iter_items()

    async def iter_pages(self) -> AsyncIterator[AsyncSandboxPage]:
        first_page = await self._get_first_page()
        async for page in first_page.iter_pages():
            yield page

    async def iter_items(self) -> AsyncIterator[SandboxModel]:
        first_page = await self._get_first_page()
        async for item in first_page.iter_items():
            yield item


@dataclass(slots=True)
class SnapshotPage:
    snapshots: list[SnapshotModel]
    pagination: Pagination
    _controller: PageController[SnapshotPage, SnapshotModel, SnapshotPageInfo] = field(
        init=False,
        repr=False,
    )

    @classmethod
    def create(
        cls,
        *,
        snapshots: list[SnapshotModel],
        pagination: Pagination,
        fetch_next_page: Callable[[SnapshotPageInfo], Awaitable[SnapshotPage]],
    ) -> SnapshotPage:
        page = cls(snapshots=list(snapshots), pagination=pagination)
        page._controller = PageController(
            get_items=lambda current_page: current_page.snapshots,
            get_next_page_info=lambda current_page: next_snapshot_page_info(
                current_page.pagination
            ),
            fetch_next_page=fetch_next_page,
        )
        return page

    def has_next_page(self) -> bool:
        return self._controller.has_next_page(self)

    def next_page_info(self) -> SnapshotPageInfo | None:
        return self._controller.next_page_info(self)

    def get_next_page(self) -> SnapshotPage | None:
        return self._controller.get_next_page_sync(self)

    def iter_pages(self) -> Iterator[SnapshotPage]:
        return self._controller.iter_pages_sync(self)

    def iter_items(self) -> Iterator[SnapshotModel]:
        return self._controller.iter_items_sync(self)


@dataclass(slots=True)
class AsyncSnapshotPage:
    snapshots: list[SnapshotModel]
    pagination: Pagination
    _controller: PageController[AsyncSnapshotPage, SnapshotModel, SnapshotPageInfo] = field(
        init=False,
        repr=False,
    )

    @classmethod
    def create(
        cls,
        *,
        snapshots: list[SnapshotModel],
        pagination: Pagination,
        fetch_next_page: Callable[[SnapshotPageInfo], Awaitable[AsyncSnapshotPage]],
    ) -> AsyncSnapshotPage:
        page = cls(snapshots=list(snapshots), pagination=pagination)
        page._controller = PageController(
            get_items=lambda current_page: current_page.snapshots,
            get_next_page_info=lambda current_page: next_snapshot_page_info(
                current_page.pagination
            ),
            fetch_next_page=fetch_next_page,
        )
        return page

    def has_next_page(self) -> bool:
        return self._controller.has_next_page(self)

    def next_page_info(self) -> SnapshotPageInfo | None:
        return self._controller.next_page_info(self)

    async def get_next_page(self) -> AsyncSnapshotPage | None:
        return await self._controller.get_next_page(self)

    def iter_pages(self) -> AsyncIterator[AsyncSnapshotPage]:
        return self._controller.iter_pages(self)

    def iter_items(self) -> AsyncIterator[SnapshotModel]:
        return self._controller.iter_items(self)


@dataclass(slots=True)
class AsyncSnapshotPager:
    _fetch_first_page: Callable[[], Awaitable[AsyncSnapshotPage]]
    _first_page: AsyncSnapshotPage | None = field(init=False, default=None, repr=False)

    async def _get_first_page(self) -> AsyncSnapshotPage:
        if self._first_page is None:
            self._first_page = await self._fetch_first_page()
        return self._first_page

    def __await__(self) -> Generator[Any, None, AsyncSnapshotPage]:
        return self._get_first_page().__await__()

    def __aiter__(self) -> AsyncIterator[SnapshotModel]:
        return self.iter_items()

    async def iter_pages(self) -> AsyncIterator[AsyncSnapshotPage]:
        first_page = await self._get_first_page()
        async for page in first_page.iter_pages():
            yield page

    async def iter_items(self) -> AsyncIterator[SnapshotModel]:
        first_page = await self._get_first_page()
        async for item in first_page.iter_items():
            yield item


__all__ = [
    "AsyncSnapshotPager",
    "AsyncSnapshotPage",
    "AsyncSandboxPager",
    "AsyncSandboxPage",
    "SandboxPage",
    "SnapshotPage",
    "SandboxPageInfo",
    "SnapshotPageInfo",
]
