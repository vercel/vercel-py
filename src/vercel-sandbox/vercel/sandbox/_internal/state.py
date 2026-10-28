"""Neutral domain state for Sandbox operations."""

from datetime import timedelta
from typing import Literal, TypeAlias

from vercel.sandbox._internal.models import (
    JSONObject,
    NetworkPolicyReadback,
    SandboxMount,
    SandboxStatus,
)
from vercel.sandbox._internal.value_model import SandboxValueModel


class SandboxRouteState(SandboxValueModel):
    url: str
    port: int
    subdomain: str
    system: bool = False


class ProcessState(SandboxValueModel):
    id: str
    name: str
    args: tuple[str, ...]
    cwd: str
    session_id: str
    returncode: int | None
    started_at: int


class CompletedProcessState(SandboxValueModel):
    process: ProcessState
    stdout: str | None
    stderr: str | None


class InteractiveSessionState(SandboxValueModel):
    url: str
    token: str


class SandboxRuntimeSessionState(SandboxValueModel):
    id: str
    sandbox_name: str | None = None
    project_id: str | None = None
    status: SandboxStatus | None = None
    cwd: str | None = None
    region: str | None = None
    memory: int | None = None
    vcpus: int | None = None
    execution_time_limit: timedelta | None = None
    network_policy: NetworkPolicyReadback | None = None
    network_id: str | None = None
    requested_at: int | None = None
    started_at: int | None = None
    stopped_at: int | None = None


class SnapshotRetentionState(SandboxValueModel):
    """Describe the snapshot retention policy returned by the service."""

    count: int
    expiration: timedelta | None = None
    delete_evicted: bool = True


class SandboxState(SandboxValueModel):
    name: str
    current_session_id: str
    image: str | None = None
    status: SandboxStatus | None = None
    persistent: bool | None = None
    current_snapshot_id: str | None = None
    project_id: str | None = None
    cwd: str | None = None
    region: str | None = None
    failover_regions: tuple[str, ...] = ()
    memory: int | None = None
    vcpus: int | None = None
    execution_time_limit: timedelta | None = None
    network_policy: NetworkPolicyReadback | None = None
    network_id: str | None = None
    snapshot_expiration: timedelta | None = None
    snapshot_retention: SnapshotRetentionState | None = None
    status_updated_at: int | None = None
    created_at: int | None = None
    updated_at: int | None = None
    tags: dict[str, str] | None = None
    mounts: dict[str, SandboxMount] | None = None
    routes: tuple[SandboxRouteState, ...] = ()
    current_session: SandboxRuntimeSessionState | None = None
    raw: JSONObject | None = None
    mounts_attached: bool = True
    routes_attached: bool = True
    current_session_attached: bool = True


UpdateSandboxStep: TypeAlias = SandboxRuntimeSessionState | SandboxState
"""A live current-session result or the final saved sandbox configuration."""


class RuntimeSessionStopState(SandboxValueModel):
    """State returned after stopping one exact runtime session."""

    session: SandboxRuntimeSessionState
    sandbox: SandboxState | None = None
    sandbox_attached: bool = False


class SnapshotState(SandboxValueModel):
    id: str
    source_session_id: str
    region: str
    regions: tuple[str, ...]
    status: Literal["created", "deleted", "failed"]
    size_bytes: int
    expires_at: int | None = None
    created_at: int = 0
    updated_at: int = 0
    last_used_at: int | None = None
    creation_method: str | None = None
    parent_id: str | None = None


class DriveState(SandboxValueModel):
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


class SandboxesPageState(SandboxValueModel):
    sandboxes: tuple[SandboxState, ...]
    next_cursor: str | None


class RuntimeSessionsPageState(SandboxValueModel):
    sessions: tuple[SandboxRuntimeSessionState, ...]
    next_cursor: str | None


class SnapshotsPageState(SandboxValueModel):
    snapshots: tuple[SnapshotState, ...]
    next_cursor: str | None


class DrivesPageState(SandboxValueModel):
    drives: tuple[DriveState, ...]
    next_cursor: str | None


class SnapshotSessionState(SandboxValueModel):
    snapshot: SnapshotState
    session: SandboxRuntimeSessionState
