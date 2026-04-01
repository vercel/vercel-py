from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True, slots=True, init=False)
class _BaseListParams:
    project_id: str | None
    limit: int | None
    since: int | None
    until: int | None

    def __init__(
        self,
        project_id: str | None = None,
        limit: int | None = None,
        since: datetime | int | None = None,
        until: datetime | int | None = None,
    ) -> None:
        object.__setattr__(self, "project_id", project_id)
        object.__setattr__(self, "limit", limit)
        object.__setattr__(self, "since", normalize_list_timestamp(since))
        object.__setattr__(self, "until", normalize_list_timestamp(until))


@dataclass(frozen=True, slots=True, init=False)
class SandboxListParams(_BaseListParams):
    def with_until(self, until: int) -> SandboxListParams:
        return SandboxListParams(
            project_id=self.project_id,
            limit=self.limit,
            since=self.since,
            until=until,
        )


@dataclass(frozen=True, slots=True, init=False)
class SnapshotListParams(_BaseListParams):
    def with_until(self, until: int) -> SnapshotListParams:
        return SnapshotListParams(
            project_id=self.project_id,
            limit=self.limit,
            since=self.since,
            until=until,
        )


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
    "SnapshotListParams",
    "normalize_list_timestamp",
]
