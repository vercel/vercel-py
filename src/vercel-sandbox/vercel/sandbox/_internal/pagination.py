"""Pagination helpers for Sandbox APIs."""

from typing import ClassVar, Generic, TypeVar

from pydantic import field_validator

from vercel._internal.core.polyfills import Self
from vercel.sandbox._internal.value_model import SandboxValueModel

MAX_QUERY_SANDBOXES_PAGE_SIZE = 50
MAX_QUERY_SESSIONS_PAGE_SIZE = 50
MAX_QUERY_SNAPSHOTS_PAGE_SIZE = 50
MAX_QUERY_DRIVES_PAGE_SIZE = 50


PageItemT = TypeVar("PageItemT")


def _require_int_page_size(page_size: object) -> object:
    # Runs before Pydantic's lax coercion, which would accept "5", 5.0 and True.
    if page_size is not None and (isinstance(page_size, bool) or not isinstance(page_size, int)):
        raise TypeError("page_size must be an integer")
    return page_size


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


class _PageParams(SandboxValueModel):
    _max_page_size: ClassVar[int]
    _label: ClassVar[str]

    page_size: int | None = None
    cursor: str | None = None

    _check_page_size_type = field_validator("page_size", mode="before")(_require_int_page_size)

    @field_validator("page_size")
    @classmethod
    def _validate_page_size(cls, page_size: int | None) -> int | None:
        if page_size is not None and not 1 <= page_size <= cls._max_page_size:
            raise ValueError(f"{cls._label} page_size must be between 1 and {cls._max_page_size}")
        return page_size

    def with_cursor(self, cursor: str) -> Self:
        return type(self)(page_size=self.page_size, cursor=cursor)


class QuerySandboxesParams(_PageParams):
    _max_page_size = MAX_QUERY_SANDBOXES_PAGE_SIZE
    _label = "query_sandboxes"


class QuerySessionsParams(_PageParams):
    _max_page_size = MAX_QUERY_SESSIONS_PAGE_SIZE
    _label = "query_sessions"


class QuerySnapshotsParams(_PageParams):
    _max_page_size = MAX_QUERY_SNAPSHOTS_PAGE_SIZE
    _label = "query_snapshots"


class QueryDrivesParams(_PageParams):
    _max_page_size = MAX_QUERY_DRIVES_PAGE_SIZE
    _label = "query_drives"


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
