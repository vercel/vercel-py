"""Neutral domain state for unstable Sandbox operations."""

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

from vercel._internal.unstable.sandbox.models import JSONObject, JSONValue, SandboxStatus


@dataclass(frozen=True, slots=True)
class SandboxRouteState:
    url: str
    port: int
    subdomain: str
    system: bool = False


@dataclass(frozen=True, slots=True)
class ProcessState:
    id: str
    name: str
    args: tuple[str, ...]
    cwd: str
    session_id: str
    returncode: int | None
    started_at: int


@dataclass(frozen=True, slots=True)
class CompletedProcessState:
    process: ProcessState
    stdout: str | None
    stderr: str | None


@dataclass(frozen=True, slots=True)
class SandboxRuntimeSessionState:
    id: str
    sandbox_name: str | None = None
    project_id: str | None = None
    status: SandboxStatus | None = None
    runtime: str | None = None
    cwd: str | None = None
    region: str | None = None
    memory: int | None = None
    vcpus: int | None = None
    execution_time_limit: timedelta | None = None
    network_policy: JSONValue | None = None
    requested_at: int | None = None
    started_at: int | None = None
    stopped_at: int | None = None


@dataclass(frozen=True, slots=True)
class SnapshotRetentionState:
    """Describe the snapshot retention policy returned by the service."""

    count: int
    expiration: timedelta | None = None
    delete_evicted: bool = True


@dataclass(frozen=True, slots=True)
class SandboxState:
    name: str
    current_session_id: str
    runtime: str | None = None
    status: SandboxStatus | None = None
    persistent: bool | None = None
    current_snapshot_id: str | None = None
    project_id: str | None = None
    cwd: str | None = None
    region: str | None = None
    memory: int | None = None
    vcpus: int | None = None
    execution_time_limit: timedelta | None = None
    network_policy: JSONValue | None = None
    snapshot_expiration: timedelta | None = None
    snapshot_retention: SnapshotRetentionState | None = None
    status_updated_at: int | None = None
    created_at: int | None = None
    updated_at: int | None = None
    tags: dict[str, str] | None = None
    routes: tuple[SandboxRouteState, ...] = ()
    current_session: SandboxRuntimeSessionState | None = None
    raw: JSONObject | None = None
    _routes_attached: bool = True
    _current_session_attached: bool = True


@dataclass(frozen=True, slots=True)
class SnapshotState:
    id: str
    source_session_id: str
    region: str
    status: Literal["created", "deleted", "failed"]
    size_bytes: int
    expires_at: int | None = None
    created_at: int = 0
    updated_at: int = 0
    last_used_at: int | None = None
    creation_method: str | None = None
    parent_id: str | None = None


@dataclass(frozen=True, slots=True)
class SandboxesPageState:
    sandboxes: tuple[SandboxState, ...]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class RuntimeSessionsPageState:
    sessions: tuple[SandboxRuntimeSessionState, ...]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class SnapshotsPageState:
    snapshots: tuple[SnapshotState, ...]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class SnapshotSessionState:
    snapshot: SnapshotState
    session: SandboxRuntimeSessionState
