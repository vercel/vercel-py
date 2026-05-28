"""Pagination helpers for unstable sandbox APIs."""

from dataclasses import dataclass
from typing import Generic, TypeVar

MAX_QUERY_SANDBOXES_PAGE_SIZE = 50
MAX_QUERY_SESSIONS_PAGE_SIZE = 50
MAX_QUERY_SNAPSHOTS_PAGE_SIZE = 50


PageItemT = TypeVar("PageItemT")


@dataclass(frozen=True, slots=True)
class QuerySandboxesPage(Generic[PageItemT]):
    sandboxes: list[PageItemT]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class QuerySessionsPage(Generic[PageItemT]):
    sessions: list[PageItemT]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class QuerySnapshotsPage(Generic[PageItemT]):
    snapshots: list[PageItemT]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class QuerySandboxesParams:
    page_size: int | None = None
    cursor: str | None = None

    def __init__(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
    ) -> None:
        if page_size is not None and not 1 <= page_size <= MAX_QUERY_SANDBOXES_PAGE_SIZE:
            raise ValueError(
                f"query_sandboxes page_size must be between 1 and {MAX_QUERY_SANDBOXES_PAGE_SIZE}"
            )
        object.__setattr__(self, "page_size", page_size)
        object.__setattr__(self, "cursor", cursor)

    def with_cursor(self, cursor: str) -> "QuerySandboxesParams":
        return QuerySandboxesParams(
            page_size=self.page_size,
            cursor=cursor,
        )


@dataclass(frozen=True, slots=True)
class QuerySessionsParams:
    page_size: int | None = None
    cursor: str | None = None

    def __init__(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
    ) -> None:
        if page_size is not None and not 1 <= page_size <= MAX_QUERY_SESSIONS_PAGE_SIZE:
            raise ValueError(
                f"query_sessions page_size must be between 1 and {MAX_QUERY_SESSIONS_PAGE_SIZE}"
            )
        object.__setattr__(self, "page_size", page_size)
        object.__setattr__(self, "cursor", cursor)

    def with_cursor(self, cursor: str) -> "QuerySessionsParams":
        return QuerySessionsParams(
            page_size=self.page_size,
            cursor=cursor,
        )


@dataclass(frozen=True, slots=True)
class QuerySnapshotsParams:
    page_size: int | None = None
    cursor: str | None = None

    def __init__(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
    ) -> None:
        if page_size is not None and not 1 <= page_size <= MAX_QUERY_SNAPSHOTS_PAGE_SIZE:
            raise ValueError(
                f"query_snapshots page_size must be between 1 and {MAX_QUERY_SNAPSHOTS_PAGE_SIZE}"
            )
        object.__setattr__(self, "page_size", page_size)
        object.__setattr__(self, "cursor", cursor)

    def with_cursor(self, cursor: str) -> "QuerySnapshotsParams":
        return QuerySnapshotsParams(
            page_size=self.page_size,
            cursor=cursor,
        )


__all__ = [
    "MAX_QUERY_SANDBOXES_PAGE_SIZE",
    "MAX_QUERY_SESSIONS_PAGE_SIZE",
    "MAX_QUERY_SNAPSHOTS_PAGE_SIZE",
    "QuerySandboxesPage",
    "QuerySandboxesParams",
    "QuerySessionsPage",
    "QuerySessionsParams",
    "QuerySnapshotsPage",
    "QuerySnapshotsParams",
]
