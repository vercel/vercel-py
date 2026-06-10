"""Public value types for the experimental Sandbox API."""

from dataclasses import dataclass
from datetime import timedelta
from subprocess import CalledProcessError
from typing import Literal, TypeAlias, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue as PydanticJsonValue,
    field_validator,
)

from vercel._internal.polyfills import StrEnum
from vercel._internal.time import SECOND, coerce_duration, to_ms_int

JSONValue: TypeAlias = PydanticJsonValue
JSONObject: TypeAlias = dict[str, JSONValue]
DurationInput: TypeAlias = int | float | timedelta | None
_MIN_SNAPSHOT_EXPIRATION = timedelta(days=1)
_MAX_SNAPSHOT_EXPIRATION = timedelta(days=365 * 10)
_ZERO_DELTA = timedelta(0)


@dataclass(frozen=True, slots=True)
class SnapshotExpiration:
    """Represent a platform-side snapshot lifetime.

    Args:
        value: Lifetime in seconds or as a ``timedelta``. Zero disables
            expiration; nonzero values must be between one day and ten years.

    Raises:
        ValueError: If the lifetime is outside the supported range.
    """

    value: timedelta

    def __init__(self, value: int | float | timedelta) -> None:
        normalized = coerce_duration(value, SECOND)
        if normalized != _ZERO_DELTA and not (
            _MIN_SNAPSHOT_EXPIRATION <= normalized <= _MAX_SNAPSHOT_EXPIRATION
        ):
            raise ValueError(
                "Snapshot expiration must be 0 or between one day and ten years inclusive"
            )
        object.__setattr__(self, "value", normalized)


SnapshotExpirationInput: TypeAlias = int | float | timedelta | SnapshotExpiration | None


def _parse_snapshot_expiration(value: object) -> SnapshotExpiration | None:
    match value:
        case None | SnapshotExpiration():
            return value
        case int() | float() | timedelta():
            return SnapshotExpiration(value)
        case _:
            raise TypeError(
                "snapshot expiration must be an int, float, timedelta, SnapshotExpiration, or None"
            )


class _Omitted:
    __slots__ = ()


_OMITTED = _Omitted()


@dataclass(frozen=True, slots=True)
class _WriteFile:
    path: str
    content: bytes
    mode: int | None = None


@dataclass(frozen=True, slots=True)
class DirectoryEntry:
    """Describe one entry returned by a sandbox directory listing.

    Attributes:
        path: Entry name relative to the listed directory, not a full path.
        kind: Filesystem entry type.
    """

    path: str
    kind: Literal["file", "directory", "symlink", "other"]


class SandboxStatus(StrEnum):
    """Lifecycle status reported for a sandbox or runtime session."""

    PENDING = "pending"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    ABORTED = "aborted"
    SNAPSHOTTING = "snapshotting"


class ProcessStatus(StrEnum):
    """Lifecycle status derived for a sandbox process."""

    RUNNING = "running"
    EXITED = "exited"


@dataclass(frozen=True, slots=True)
class CompletedProcess:
    """The captured result of one completed remote process."""

    id: str
    name: str
    args: tuple[str, ...]
    cwd: str
    session_id: str
    started_at: int
    returncode: int
    stdout: str | None
    stderr: str | None

    def check_returncode(self) -> None:
        """Raise an error when the process exited unsuccessfully.

        Raises:
            subprocess.CalledProcessError: If ``returncode`` is nonzero.
        """
        if self.returncode:
            raise CalledProcessError(
                self.returncode,
                list(self.args),
                output=self.stdout,
                stderr=self.stderr,
            )


class _InputModel(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        populate_by_name=True,
        serialize_by_alias=True,
    )

    def to_api_dict(self, *, exclude: set[str] | None = None) -> JSONObject:
        """Serialize this input model using Sandbox API field names."""
        return cast(
            JSONObject,
            self.model_dump(by_alias=True, exclude_none=True, exclude=exclude or set()),
        )


class GitSource(_InputModel):
    """Configure a Git repository as sandbox source.

    ``username`` and ``password`` may be used for repositories that require
    HTTP basic authentication.
    """

    type: Literal["git"] = "git"
    url: str
    depth: int | None = None
    revision: str | None = None
    username: str | None = None
    password: str | None = None


class TarballSource(_InputModel):
    """Configure a remotely accessible tarball as sandbox source."""

    type: Literal["tarball"] = "tarball"
    url: str


class SnapshotSource(_InputModel):
    """Configure an existing snapshot as sandbox source."""

    type: Literal["snapshot"] = "snapshot"
    snapshot_id: str = Field(serialization_alias="snapshotId")


SandboxSource: TypeAlias = GitSource | TarballSource | SnapshotSource


class SandboxResources(_InputModel):
    """Configure CPU and memory requested for a sandbox."""

    vcpus: int | None = None
    memory: int | None = None


class SnapshotRetention(_InputModel):
    """Configure automatic snapshot retention.

    Attributes:
        count: Maximum number of retained snapshots, between 1 and 100.
        expiration: Lifetime applied to retained snapshots.
        delete_evicted: Whether snapshots removed from the retention window are
            deleted from the project.
    """

    count: int
    expiration: SnapshotExpirationInput = None
    delete_evicted: bool = Field(default=True, serialization_alias="deleteEvicted")

    @field_validator("expiration", mode="before")
    @classmethod
    def _coerce_expiration(cls, value: object) -> SnapshotExpiration | None:
        return _parse_snapshot_expiration(value)

    def to_api_dict(self, *, exclude: set[str] | None = None) -> JSONObject:
        """Serialize the retention policy for the Sandbox API."""
        data = super().to_api_dict(exclude=exclude)
        expiration = _parse_snapshot_expiration(self.expiration)
        if expiration is not None:
            data["expiration"] = to_ms_int(expiration.value)
        return data


SnapshotRetentionUpdate: TypeAlias = SnapshotRetention | None | _Omitted


class TagFilter(_InputModel):
    """Filter sandbox queries by an exact tag key and value."""

    key: str
    value: str

    def to_query_value(self) -> str:
        """Serialize the filter for use as an API query parameter."""
        return f"{self.key}:{self.value}"


class SandboxQueryByCreatedAt(_InputModel):
    """Order sandbox results by creation time, optionally filtering by tag."""

    sort_order: Literal["asc", "desc"] = "desc"
    tag: TagFilter | None = None


class SandboxQueryByName(_InputModel):
    """Order sandbox results by name with optional prefix and tag filters."""

    sort_order: Literal["asc", "desc"] = "desc"
    name_prefix: str | None = None
    tag: TagFilter | None = None


class SandboxQueryByStatusUpdatedAt(_InputModel):
    """Order sandbox results by their latest status update time."""

    sort_order: Literal["asc", "desc"] = "desc"


class SandboxQueryByCurrentSnapshotId(_InputModel):
    """Order sandbox results by their current snapshot identifier."""

    sort_order: Literal["asc", "desc"] = "desc"


SandboxQuery: TypeAlias = (
    SandboxQueryByCreatedAt
    | SandboxQueryByName
    | SandboxQueryByStatusUpdatedAt
    | SandboxQueryByCurrentSnapshotId
)


class ProcessLogStream(StrEnum):
    """Output stream represented by a process log event."""

    STDOUT = "stdout"
    STDERR = "stderr"


class ProcessLog(BaseModel):
    """One streamed process output event."""

    data: str
    stream: ProcessLogStream
