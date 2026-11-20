"""Drive value types shared by the sync and async APIs."""

from typing import Any, ClassVar, Generic, Literal, Protocol, Self, TypeAlias, TypeVar

from pydantic import BaseModel, ConfigDict, SkipValidation, field_serializer, field_validator

MAX_QUERY_DRIVES_PAGE_SIZE = 50

PageItemT = TypeVar("PageItemT")


# Explicit **data constructors keep Pydantic's mypy plugin from synthesizing
# empty base signatures for subclass constructors that call super().__init__.
class DriveValueModel(BaseModel):
    """Validate value objects while preserving their frozen fields."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True, frozen=True)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)


class DriveState(DriveValueModel):
    """Drive metadata as the service returned it."""

    id: str
    name: str
    project_id: str
    region: str
    max_size_bytes: int
    current_session_id: str | None
    current_sandbox_name: str | None
    created_at: int
    updated_at: int
    parent_drive_id: str | None = None
    root_drive_id: str | None = None


class DrivesPageState(DriveValueModel):
    drives: tuple[DriveState, ...]
    next_cursor: str | None


class DriveHandle(Protocol):
    """The Drive attributes a sandbox mount reads."""

    @property
    def name(self) -> str: ...

    @property
    def project_id(self) -> str: ...

    @property
    def region(self) -> str: ...


DriveReference: TypeAlias = str | DriveHandle
"""A Drive handle, or a project-local Drive name."""

DriveMountMode: TypeAlias = Literal["snapshot", "read-write"]


class DriveMount(DriveValueModel):
    """Configure how a Drive is mounted in a sandbox."""

    drive: SkipValidation[DriveReference]
    mode: DriveMountMode = "read-write"

    def __init__(self, drive: DriveReference, mode: DriveMountMode = "read-write") -> None:
        super().__init__(drive=drive, mode=mode)

    @field_serializer("drive")
    def _serialize_drive(self, drive: DriveReference) -> str:
        return drive if isinstance(drive, str) else drive.name

    def to_api_dict(self) -> dict[str, Any]:
        return self.model_dump()


class _DriveQueryModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DriveQueryByCreatedAt(_DriveQueryModel):
    """Drive listing ordered by creation time."""

    sort_order: Literal["asc", "desc"] = "desc"


class DriveQueryByUpdatedAt(_DriveQueryModel):
    """Drive listing ordered by update time."""

    sort_order: Literal["asc", "desc"] = "desc"


class DriveQueryByName(_DriveQueryModel):
    """Drive listing ordered by name."""

    sort_order: Literal["asc", "desc"] = "desc"
    name_prefix: str | None = None


DriveQuery: TypeAlias = DriveQueryByCreatedAt | DriveQueryByUpdatedAt | DriveQueryByName


class QueryDrivesPage(DriveValueModel, Generic[PageItemT]):
    drives: list[PageItemT]
    next_cursor: str | None


class QueryDrivesParams(DriveValueModel):
    _max_page_size: ClassVar[int] = MAX_QUERY_DRIVES_PAGE_SIZE

    page_size: int | None = None
    cursor: str | None = None

    @field_validator("page_size", mode="before")
    @classmethod
    def _require_int_page_size(cls, page_size: object) -> object:
        # Runs before Pydantic's lax coercion, which would accept "5", 5.0 and True.
        if page_size is not None and (
            isinstance(page_size, bool) or not isinstance(page_size, int)
        ):
            raise TypeError("page_size must be an integer")
        return page_size

    @field_validator("page_size")
    @classmethod
    def _validate_page_size(cls, page_size: int | None) -> int | None:
        if page_size is not None and not 1 <= page_size <= cls._max_page_size:
            raise ValueError(f"query_drives page_size must be between 1 and {cls._max_page_size}")
        return page_size

    def with_cursor(self, cursor: str) -> Self:
        return type(self)(page_size=self.page_size, cursor=cursor)


__all__ = [
    "MAX_QUERY_DRIVES_PAGE_SIZE",
    "DriveHandle",
    "DriveMount",
    "DriveMountMode",
    "DriveQuery",
    "DriveQueryByCreatedAt",
    "DriveQueryByName",
    "DriveQueryByUpdatedAt",
    "DriveReference",
    "DriveState",
    "DriveValueModel",
    "DrivesPageState",
    "QueryDrivesPage",
    "QueryDrivesParams",
]
