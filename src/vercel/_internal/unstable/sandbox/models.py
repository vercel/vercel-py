"""Internal Sandbox domain handles and state types for unstable APIs."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from vercel._internal.polyfills import StrEnum

if TYPE_CHECKING:
    from vercel._internal.unstable.session import (
        Session as _SdkSession,
        SyncSession as _SdkSyncSession,
    )


class SandboxStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    ABORTED = "aborted"
    SNAPSHOTTING = "snapshotting"


@dataclass(frozen=True, slots=True)
class Session:
    """V2 runtime session attached to a named Sandbox."""

    id: str
    status: SandboxStatus | None = None
    memory: int | None = None
    vcpus: int | None = None
    region: str | None = None
    runtime: str | None = None
    timeout: int | timedelta | None = None
    requested_at: int | None = None
    started_at: int | None = None
    cwd: str | None = None
    project_id: str | None = None
    source_sandbox_name: str | None = None
    source_snapshot_id: str | None = None
    active_cpu_duration_ms: int | None = None
    network_transfer: int | None = None


@dataclass(frozen=True, slots=True)
class SandboxRoute:
    url: str
    subdomain: str
    port: int


@dataclass(slots=True)
class Sandbox:
    name: str
    persistent: bool | None = None
    current_snapshot_id: str | None = None
    current_session: Session | None = None
    routes: list[SandboxRoute] = field(default_factory=list)
    _session: _SdkSession | None = field(default=None, repr=False, compare=False)
    _raw: dict[str, Any] | None = field(default=None, repr=False, compare=False)


@dataclass(slots=True)
class SyncSandbox:
    name: str
    persistent: bool | None = None
    current_snapshot_id: str | None = None
    current_session: Session | None = None
    routes: list[SandboxRoute] = field(default_factory=list)
    _session: _SdkSyncSession | None = field(default=None, repr=False, compare=False)
    _raw: dict[str, Any] | None = field(default=None, repr=False, compare=False)


__all__ = [
    "Sandbox",
    "SandboxRoute",
    "SandboxStatus",
    "Session",
    "SyncSandbox",
]
