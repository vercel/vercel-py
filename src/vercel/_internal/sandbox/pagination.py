from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from vercel._internal.sandbox.models import Pagination


@dataclass(frozen=True, slots=True)
class SandboxPageInfo:
    until: int


@dataclass(frozen=True, slots=True)
class SandboxListParams:
    project_id: str | None = None
    limit: int | None = None
    since: int | None = None
    until: int | None = None

    def with_until(self, until: int) -> SandboxListParams:
        return SandboxListParams(
            project_id=self.project_id,
            limit=self.limit,
            since=self.since,
            until=until,
        )


def next_sandbox_page_info(pagination: Pagination) -> SandboxPageInfo | None:
    if pagination.next is None:
        return None
    return SandboxPageInfo(until=pagination.next)


@dataclass(frozen=True, slots=True)
class SnapshotPageInfo:
    until: int


@dataclass(frozen=True, slots=True)
class SnapshotListParams:
    project_id: str | None = None
    limit: int | None = None
    since: int | None = None
    until: int | None = None

    def with_until(self, until: int) -> SnapshotListParams:
        return SnapshotListParams(
            project_id=self.project_id,
            limit=self.limit,
            since=self.since,
            until=until,
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
            value = value.replace(tzinfo=timezone.utc)
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
