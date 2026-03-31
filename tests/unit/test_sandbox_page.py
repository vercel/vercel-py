"""Tests for sandbox page helpers."""

from __future__ import annotations

import pytest

from vercel._internal.sandbox.models import Pagination, Snapshot as SnapshotModel
from vercel.sandbox.page import AsyncSnapshotPage, SnapshotPage


def _snapshot_model(snapshot_id: str, *, created_at: int) -> SnapshotModel:
    return SnapshotModel.model_validate(
        {
            "id": snapshot_id,
            "sourceSandboxId": "sbx_test123456",
            "region": "iad1",
            "status": "created",
            "sizeBytes": 1024,
            "expiresAt": created_at + 86_400_000,
            "createdAt": created_at,
            "updatedAt": created_at,
        }
    )


async def _collect_async_pages(page: AsyncSnapshotPage) -> list[AsyncSnapshotPage]:
    return [current_page async for current_page in page.iter_pages()]


async def _collect_async_items(page: AsyncSnapshotPage) -> list:
    return [snapshot async for snapshot in page.iter_items()]


async def _collect_async_current_page_items(page: AsyncSnapshotPage) -> list:
    return [snapshot async for snapshot in page]


class TestSnapshotPage:
    def test_iterates_pages_and_items(self) -> None:
        second_page = SnapshotPage.create(
            snapshots=[_snapshot_model("snap_2", created_at=1705320000000)],
            pagination=Pagination(count=2, next=None, prev=1705320600000),
            fetch_next_page=self._fetch_unreachable_page,
        )

        async def fetch_next_page(_page_info):
            return second_page

        first_page = SnapshotPage.create(
            snapshots=[_snapshot_model("snap_1", created_at=1705320600000)],
            pagination=Pagination(count=2, next=1705320000000, prev=None),
            fetch_next_page=fetch_next_page,
        )

        assert first_page.has_next_page() is True
        next_page_info = first_page.next_page_info()
        assert next_page_info is not None
        assert next_page_info.until == 1705320000000
        assert [
            [snapshot.id for snapshot in page.snapshots] for page in first_page.iter_pages()
        ] == [
            ["snap_1"],
            ["snap_2"],
        ]
        assert [snapshot.id for snapshot in first_page] == ["snap_1"]
        assert [snapshot.id for snapshot in first_page.iter_items()] == ["snap_1", "snap_2"]

    def test_terminal_page_does_not_fetch_more(self) -> None:
        page = SnapshotPage.create(
            snapshots=[_snapshot_model("snap_terminal", created_at=1705320600000)],
            pagination=Pagination(count=1, next=None, prev=None),
            fetch_next_page=self._fetch_unreachable_page,
        )

        assert page.has_next_page() is False
        assert page.next_page_info() is None
        assert page.get_next_page() is None
        assert [
            [snapshot.id for snapshot in current.snapshots] for current in page.iter_pages()
        ] == [
            ["snap_terminal"],
        ]
        assert [snapshot.id for snapshot in page] == ["snap_terminal"]
        assert [snapshot.id for snapshot in page.iter_items()] == ["snap_terminal"]

    @staticmethod
    async def _fetch_unreachable_page(_page_info) -> SnapshotPage:
        raise AssertionError("fetch_next_page should not be called")


class TestAsyncSnapshotPage:
    @pytest.mark.asyncio
    async def test_iterates_pages_and_items(self) -> None:
        second_page = AsyncSnapshotPage.create(
            snapshots=[_snapshot_model("snap_async_2", created_at=1705320000000)],
            pagination=Pagination(count=2, next=None, prev=1705320600000),
            fetch_next_page=self._fetch_unreachable_page,
        )

        async def fetch_next_page(_page_info):
            return second_page

        first_page = AsyncSnapshotPage.create(
            snapshots=[_snapshot_model("snap_async_1", created_at=1705320600000)],
            pagination=Pagination(count=2, next=1705320000000, prev=None),
            fetch_next_page=fetch_next_page,
        )

        assert first_page.has_next_page() is True
        next_page_info = first_page.next_page_info()
        assert next_page_info is not None
        assert next_page_info.until == 1705320000000
        pages = await _collect_async_pages(first_page)
        assert [[snapshot.id for snapshot in page.snapshots] for page in pages] == [
            ["snap_async_1"],
            ["snap_async_2"],
        ]
        current_page_items = await _collect_async_current_page_items(first_page)
        assert [snapshot.id for snapshot in current_page_items] == ["snap_async_1"]
        items = await _collect_async_items(first_page)
        assert [snapshot.id for snapshot in items] == ["snap_async_1", "snap_async_2"]

    @pytest.mark.asyncio
    async def test_terminal_page_does_not_fetch_more(self) -> None:
        page = AsyncSnapshotPage.create(
            snapshots=[_snapshot_model("snap_async_terminal", created_at=1705320600000)],
            pagination=Pagination(count=1, next=None, prev=None),
            fetch_next_page=self._fetch_unreachable_page,
        )

        assert page.has_next_page() is False
        assert page.next_page_info() is None
        assert await page.get_next_page() is None
        pages = await _collect_async_pages(page)
        assert [[snapshot.id for snapshot in current.snapshots] for current in pages] == [
            ["snap_async_terminal"],
        ]
        current_page_items = await _collect_async_current_page_items(page)
        assert [snapshot.id for snapshot in current_page_items] == ["snap_async_terminal"]
        items = await _collect_async_items(page)
        assert [snapshot.id for snapshot in items] == ["snap_async_terminal"]

    @staticmethod
    async def _fetch_unreachable_page(_page_info) -> AsyncSnapshotPage:
        raise AssertionError("fetch_next_page should not be called")
