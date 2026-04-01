from __future__ import annotations

from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field

from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.pagination import Page
from vercel._internal.sandbox.models import (
    Sandbox as SandboxModel,
    Snapshot as SnapshotModel,
)


@dataclass(slots=True)
class SandboxPage(Page[SandboxModel]):
    _fetch_next_page: Callable[[int], Coroutine[None, None, SandboxPage]] | None = field(
        repr=False,
        default=None,
    )

    @property
    def sandboxes(self) -> list[SandboxModel]:
        return self.items

    def get_next_page(self) -> SandboxPage | None:
        next_until = self.pagination.next
        if next_until is None:
            return None
        fetch_next_page = self._fetch_next_page
        if fetch_next_page is None:
            return None
        return iter_coroutine(fetch_next_page(next_until))


@dataclass(slots=True)
class AsyncSandboxPage(Page[SandboxModel]):
    _fetch_next_page: Callable[[int], Awaitable[AsyncSandboxPage]] | None = field(
        repr=False,
        default=None,
    )

    @property
    def sandboxes(self) -> list[SandboxModel]:
        return self.items

    async def get_next_page(self) -> AsyncSandboxPage | None:
        next_until = self.pagination.next
        if next_until is None:
            return None
        fetch_next_page = self._fetch_next_page
        if fetch_next_page is None:
            return None
        return await fetch_next_page(next_until)


@dataclass(slots=True)
class SnapshotPage(Page[SnapshotModel]):
    _fetch_next_page: Callable[[int], Coroutine[None, None, SnapshotPage]] | None = field(
        repr=False,
        default=None,
    )

    @property
    def snapshots(self) -> list[SnapshotModel]:
        return self.items

    def get_next_page(self) -> SnapshotPage | None:
        next_until = self.pagination.next
        if next_until is None:
            return None
        fetch_next_page = self._fetch_next_page
        if fetch_next_page is None:
            return None
        return iter_coroutine(fetch_next_page(next_until))


@dataclass(slots=True)
class AsyncSnapshotPage(Page[SnapshotModel]):
    _fetch_next_page: Callable[[int], Awaitable[AsyncSnapshotPage]] | None = field(
        repr=False,
        default=None,
    )

    @property
    def snapshots(self) -> list[SnapshotModel]:
        return self.items

    async def get_next_page(self) -> AsyncSnapshotPage | None:
        next_until = self.pagination.next
        if next_until is None:
            return None
        fetch_next_page = self._fetch_next_page
        if fetch_next_page is None:
            return None
        return await fetch_next_page(next_until)


__all__ = [
    "AsyncSnapshotPage",
    "AsyncSandboxPage",
    "SandboxPage",
    "SnapshotPage",
]
