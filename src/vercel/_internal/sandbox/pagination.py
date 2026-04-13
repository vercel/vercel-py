from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from vercel._internal.polyfills import UTC
from vercel._internal.sandbox.models import Pagination


def _exclusive_until_cursor(value: int) -> int:
    return max(value - 1, 0)


@dataclass(frozen=True, slots=True)
class SandboxPageInfo:
    until: int


@dataclass(frozen=True, slots=True, init=False)
class _BaseListParams:
    project_id: str | None
    limit: int | None
    remaining: int | None
    internal_page_size: int | None
    since: int | None
    until: int | None

    def __init__(
        self,
        project_id: str | None = None,
        limit: int | None = None,
        remaining: int | None = None,
        internal_page_size: int | None = None,
        since: datetime | int | None = None,
        until: datetime | int | None = None,
    ) -> None:
        object.__setattr__(self, "project_id", project_id)
        object.__setattr__(self, "limit", limit)
        object.__setattr__(self, "remaining", limit if remaining is None else remaining)
        object.__setattr__(self, "internal_page_size", internal_page_size)
        object.__setattr__(self, "since", normalize_list_timestamp(since))
        object.__setattr__(self, "until", normalize_list_timestamp(until))

    @property
    def request_limit(self) -> int | None:
        if self.remaining is None:
            return self.internal_page_size
        if self.internal_page_size is None:
            return self.remaining
        return min(self.remaining, self.internal_page_size)


@dataclass(frozen=True, slots=True, init=False)
class SandboxListParams(_BaseListParams):
    def with_until(self, until: int, *, yielded_count: int = 0) -> SandboxListParams:
        remaining = None if self.remaining is None else max(self.remaining - yielded_count, 0)
        return SandboxListParams(
            project_id=self.project_id,
            limit=self.limit,
            remaining=remaining,
            internal_page_size=self.internal_page_size,
            since=self.since,
            until=_exclusive_until_cursor(until),
        )


def next_sandbox_page_info(pagination: Pagination) -> SandboxPageInfo | None:
    if pagination.next is None:
        return None
    return SandboxPageInfo(until=pagination.next)


@dataclass(frozen=True, slots=True)
class SnapshotPageInfo:
    until: int


@dataclass(frozen=True, slots=True, init=False)
class SnapshotListParams(_BaseListParams):
    def with_until(self, until: int, *, yielded_count: int = 0) -> SnapshotListParams:
        remaining = None if self.remaining is None else max(self.remaining - yielded_count, 0)
        return SnapshotListParams(
            project_id=self.project_id,
            limit=self.limit,
            remaining=remaining,
            internal_page_size=self.internal_page_size,
            since=self.since,
            until=_exclusive_until_cursor(until),
        )


def next_snapshot_page_info(pagination: Pagination) -> SnapshotPageInfo | None:
    if pagination.next is None:
        return None
    return SnapshotPageInfo(until=pagination.next)


def normalize_list_timestamp(value: datetime | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return int(value.timestamp() * 1000)
    raise TypeError("List timestamps must be datetime or integer milliseconds")


__all__ = [
    "SandboxListParams",
    "SandboxPageInfo",
    "SnapshotListParams",
    "SnapshotPageInfo",
    "next_sandbox_page_info",
    "next_snapshot_page_info",
    "normalize_list_timestamp",
]
