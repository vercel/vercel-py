"""Public value types for the experimental Sandbox API."""

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, TypeAlias, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue as PydanticJsonValue,
    field_serializer,
    field_validator,
)

from vercel._internal.polyfills import StrEnum
from vercel._internal.time import parse_duration_seconds, to_ms_int

JSONValue: TypeAlias = PydanticJsonValue
JSONObject: TypeAlias = dict[str, JSONValue]
DurationInput: TypeAlias = int | float | timedelta | None


@dataclass(frozen=True, slots=True)
class WriteFile:
    path: str
    content: str | bytes
    mode: int | None = None


@dataclass(frozen=True, slots=True)
class DirectoryEntry:
    path: str
    kind: Literal["file", "directory", "symlink", "other"]


class SandboxStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    ABORTED = "aborted"
    SNAPSHOTTING = "snapshotting"


class _InputModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, serialize_by_alias=True)

    def to_api_dict(self, *, exclude: set[str] | None = None) -> JSONObject:
        return cast(
            JSONObject,
            self.model_dump(by_alias=True, exclude_none=True, exclude=exclude or set()),
        )


class GitSource(_InputModel):
    """Git repository source for creating a sandbox."""

    type: Literal["git"] = "git"
    url: str
    depth: int | None = None
    revision: str | None = None
    username: str | None = None
    password: str | None = None


class TarballSource(_InputModel):
    """Tarball URL source for creating a sandbox."""

    type: Literal["tarball"] = "tarball"
    url: str


class SnapshotSource(_InputModel):
    """Snapshot source for creating a sandbox."""

    type: Literal["snapshot"] = "snapshot"
    snapshot_id: str = Field(serialization_alias="snapshotId")


SandboxSource: TypeAlias = GitSource | TarballSource | SnapshotSource


class SandboxResources(_InputModel):
    """CPU and memory request values for sandbox creation."""

    vcpus: int | None = None
    memory: int | None = None


class SnapshotRetention(_InputModel):
    """Snapshot retention policy for sandboxes created from a sandbox."""

    count: int
    expiration: DurationInput = None
    delete_evicted: bool = Field(default=True, serialization_alias="deleteEvicted")

    @field_validator("expiration", mode="before")
    @classmethod
    def _coerce_duration(cls, value: object) -> timedelta | None:
        return parse_duration_seconds(value)

    @field_serializer("expiration")
    def _serialize_duration(self, value: timedelta | None) -> int | None:
        return None if value is None else to_ms_int(value)


class TagFilter(_InputModel):
    """Exact-match sandbox tag query filter."""

    key: str
    value: str

    def to_query_value(self) -> str:
        return f"{self.key}:{self.value}"


class SandboxQueryByCreatedAt(_InputModel):
    """Sandbox listing ordered by creation time."""

    sort_order: Literal["asc", "desc"] = "desc"
    tag: TagFilter | None = None


class SandboxQueryByName(_InputModel):
    """Sandbox listing ordered by name."""

    sort_order: Literal["asc", "desc"] = "desc"
    name_prefix: str | None = None
    tag: TagFilter | None = None


class SandboxQueryByStatusUpdatedAt(_InputModel):
    """Sandbox listing ordered by its status update time."""

    sort_order: Literal["asc", "desc"] = "desc"


class SandboxQueryByCurrentSnapshotId(_InputModel):
    """Sandbox listing ordered by current snapshot identifier."""

    sort_order: Literal["asc", "desc"] = "desc"


SandboxQuery: TypeAlias = (
    SandboxQueryByCreatedAt
    | SandboxQueryByName
    | SandboxQueryByStatusUpdatedAt
    | SandboxQueryByCurrentSnapshotId
)


class SandboxCommandLogStream(StrEnum):
    """Output stream represented by a command log event."""

    STDOUT = "stdout"
    STDERR = "stderr"


class SandboxCommandLog(BaseModel):
    """One streamed command output event."""

    data: str
    stream: SandboxCommandLogStream
