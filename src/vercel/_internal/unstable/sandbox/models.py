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
from vercel._internal.time import MILLISECOND, parse_duration, to_ms_int

JSONValue: TypeAlias = PydanticJsonValue
JSONObject: TypeAlias = dict[str, JSONValue]
DurationInput: TypeAlias = int | float | timedelta | None


@dataclass(frozen=True, slots=True)
class WriteFile:
    path: str
    content: str | bytes
    mode: int | None = None


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


def _duration_to_milliseconds(value: object) -> timedelta | None:
    return parse_duration(value, MILLISECOND)


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
        return _duration_to_milliseconds(value)

    @field_serializer("expiration")
    def _serialize_duration(self, value: DurationInput) -> int | None:
        duration = parse_duration(value, MILLISECOND)
        return None if duration is None else to_ms_int(duration)


class TagFilter(_InputModel):
    """Exact-match sandbox tag query filter."""

    key: str
    value: str

    def to_query_value(self) -> str:
        return f"{self.key}:{self.value}"


class SandboxCommandLog(BaseModel):
    """One streamed command output event."""

    data: str
    stream: Literal["unspecified", "stdin", "stdout", "stderr", "error"]
