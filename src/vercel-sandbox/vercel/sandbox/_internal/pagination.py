"""Pagination helpers for Sandbox APIs."""

from typing import Generic, TypeVar

from pydantic import field_validator

from vercel._internal.core.polyfills import Self
from vercel.sandbox._internal.value_model import SandboxValueModel

MAX_QUERY_SANDBOXES_PAGE_SIZE = 50
MAX_QUERY_SESSIONS_PAGE_SIZE = 50
MAX_QUERY_SNAPSHOTS_PAGE_SIZE = 50
MAX_QUERY_DRIVES_PAGE_SIZE = 50


PageItemT = TypeVar("PageItemT")


class QuerySandboxesPage(SandboxValueModel, Generic[PageItemT]):
    sandboxes: list[PageItemT]
    next_cursor: str | None


class QuerySessionsPage(SandboxValueModel, Generic[PageItemT]):
    sessions: list[PageItemT]
    next_cursor: str | None


class QuerySnapshotsPage(SandboxValueModel, Generic[PageItemT]):
    snapshots: list[PageItemT]
    next_cursor: str | None


class QueryDrivesPage(SandboxValueModel, Generic[PageItemT]):
    drives: list[PageItemT]
    next_cursor: str | None


class QuerySandboxesParams(SandboxValueModel):
    page_size: int | None = None
    cursor: str | None = None

    @field_validator("page_size")
    @classmethod
    def _validate_page_size(cls, page_size: int | None) -> int | None:
        if page_size is not None and not 1 <= page_size <= MAX_QUERY_SANDBOXES_PAGE_SIZE:
            raise ValueError(
                f"query_sandboxes page_size must be between 1 and {MAX_QUERY_SANDBOXES_PAGE_SIZE}"
            )
        return page_size

    def with_cursor(self, cursor: str) -> Self:
        return type(self)(
            page_size=self.page_size,
            cursor=cursor,
        )


class QuerySessionsParams(SandboxValueModel):
    page_size: int | None = None
    cursor: str | None = None

    @field_validator("page_size")
    @classmethod
    def _validate_page_size(cls, page_size: int | None) -> int | None:
        if page_size is not None and not 1 <= page_size <= MAX_QUERY_SESSIONS_PAGE_SIZE:
            raise ValueError(
                f"query_sessions page_size must be between 1 and {MAX_QUERY_SESSIONS_PAGE_SIZE}"
            )
        return page_size

    def with_cursor(self, cursor: str) -> Self:
        return type(self)(
            page_size=self.page_size,
            cursor=cursor,
        )


class QuerySnapshotsParams(SandboxValueModel):
    page_size: int | None = None
    cursor: str | None = None

    @field_validator("page_size")
    @classmethod
    def _validate_page_size(cls, page_size: int | None) -> int | None:
        if page_size is not None and not 1 <= page_size <= MAX_QUERY_SNAPSHOTS_PAGE_SIZE:
            raise ValueError(
                f"query_snapshots page_size must be between 1 and {MAX_QUERY_SNAPSHOTS_PAGE_SIZE}"
            )
        return page_size

    def with_cursor(self, cursor: str) -> Self:
        return type(self)(
            page_size=self.page_size,
            cursor=cursor,
        )


class QueryDrivesParams(SandboxValueModel):
    page_size: int | None = None
    cursor: str | None = None

    @field_validator("page_size")
    @classmethod
    def _validate_page_size(cls, page_size: int | None) -> int | None:
        if page_size is not None and not 1 <= page_size <= MAX_QUERY_DRIVES_PAGE_SIZE:
            raise ValueError(
                f"query_drives page_size must be between 1 and {MAX_QUERY_DRIVES_PAGE_SIZE}"
            )
        return page_size

    def with_cursor(self, cursor: str) -> Self:
        return type(self)(
            page_size=self.page_size,
            cursor=cursor,
        )


__all__ = [
    "MAX_QUERY_DRIVES_PAGE_SIZE",
    "MAX_QUERY_SANDBOXES_PAGE_SIZE",
    "MAX_QUERY_SESSIONS_PAGE_SIZE",
    "MAX_QUERY_SNAPSHOTS_PAGE_SIZE",
    "QueryDrivesPage",
    "QueryDrivesParams",
    "QuerySandboxesPage",
    "QuerySandboxesParams",
    "QuerySessionsPage",
    "QuerySessionsParams",
    "QuerySnapshotsPage",
    "QuerySnapshotsParams",
]
