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


class TestSnapshotPage:
    def test_iterates_current_page_and_fetches_next_page(self) -> None:
        second_page = SnapshotPage(
            items=[_snapshot_model("snap_2", created_at=1705320000000)],
            pagination=Pagination(count=2, next=None, prev=1705320600000),
            _fetch_next_page=self._fetch_unreachable_page,
        )

        async def fetch_next_page(_next_until: int) -> SnapshotPage:
            return second_page

        first_page = SnapshotPage(
            items=[_snapshot_model("snap_1", created_at=1705320600000)],
            pagination=Pagination(count=2, next=1705320000000, prev=None),
            _fetch_next_page=fetch_next_page,
        )

        assert first_page.pagination.next == 1705320000000
        assert [snapshot.id for snapshot in first_page] == ["snap_1"]

        next_page = first_page.get_next_page()
        assert next_page is second_page
        assert [snapshot.id for snapshot in next_page] == ["snap_2"]

    def test_terminal_page_does_not_fetch_more(self) -> None:
        page = SnapshotPage(
            items=[_snapshot_model("snap_terminal", created_at=1705320600000)],
            pagination=Pagination(count=1, next=None, prev=None),
            _fetch_next_page=self._fetch_unreachable_page,
        )

        assert page.pagination.next is None
        assert page.get_next_page() is None
        assert [snapshot.id for snapshot in page] == ["snap_terminal"]

    @staticmethod
    async def _fetch_unreachable_page(_next_until: int) -> SnapshotPage:
        raise AssertionError("fetch_next_page should not be called")


class TestAsyncSnapshotPage:
    @pytest.mark.asyncio
    async def test_iterates_current_page_and_fetches_next_page(self) -> None:
        second_page = AsyncSnapshotPage(
            items=[_snapshot_model("snap_async_2", created_at=1705320000000)],
            pagination=Pagination(count=2, next=None, prev=1705320600000),
            _fetch_next_page=self._fetch_unreachable_page,
        )

        async def fetch_next_page(_next_until: int) -> AsyncSnapshotPage:
            return second_page

        first_page = AsyncSnapshotPage(
            items=[_snapshot_model("snap_async_1", created_at=1705320600000)],
            pagination=Pagination(count=2, next=1705320000000, prev=None),
            _fetch_next_page=fetch_next_page,
        )

        assert first_page.pagination.next == 1705320000000
        assert [snapshot.id for snapshot in first_page] == ["snap_async_1"]

        next_page = await first_page.get_next_page()
        assert next_page is second_page
        assert [snapshot.id for snapshot in next_page] == ["snap_async_2"]

    @pytest.mark.asyncio
    async def test_terminal_page_does_not_fetch_more(self) -> None:
        page = AsyncSnapshotPage(
            items=[_snapshot_model("snap_async_terminal", created_at=1705320600000)],
            pagination=Pagination(count=1, next=None, prev=None),
            _fetch_next_page=self._fetch_unreachable_page,
        )

        assert page.pagination.next is None
        assert await page.get_next_page() is None
        assert [snapshot.id for snapshot in page] == ["snap_async_terminal"]

    @staticmethod
    async def _fetch_unreachable_page(_next_until: int) -> AsyncSnapshotPage:
        raise AssertionError("fetch_next_page should not be called")
