"""Internal Sandbox domain types and error adapters for unstable APIs."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from vercel._internal.polyfills import StrEnum
from vercel._internal.sandbox.errors import APIError as StableSandboxAPIError
from vercel._internal.sandbox.models import NetworkPolicy, Resources, Source
from vercel._internal.unstable.errors import VercelError
from vercel._internal.unstable.sandbox_errors import (
    normalize_retry_after,
    sandbox_api_error_context,
)

from .auth import SandboxCredentialProvider, SyncSandboxCredentialProvider

if TYPE_CHECKING:
    from vercel._internal.unstable.session import (
        Session as _SdkSession,
        SyncSession as _SdkSyncSession,
    )


class SandboxError(VercelError):
    """Base class for unstable Sandbox errors."""


class SandboxStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    ABORTED = "aborted"
    SNAPSHOTTING = "snapshotting"


class SandboxAPIError(SandboxError):
    """Wraps Sandbox API failures at the unstable boundary."""

    def __init__(
        self,
        message: str,
        *,
        response: object,
        status_code: int,
        data: object | None = None,
        retry_after: str | int | None = None,
    ) -> None:
        super().__init__(message)
        self.response = response
        self.status_code = status_code
        self.data = data
        self.retry_after: int | None = normalize_retry_after(retry_after)

    @classmethod
    def from_stable_error(cls, error: StableSandboxAPIError) -> SandboxAPIError:
        message, response, status_code, data, retry_after = sandbox_api_error_context(error)
        return cls(
            message,
            response=response,
            status_code=status_code,
            data=data,
            retry_after=retry_after,
        )


class SandboxOperationTimeoutError(SandboxError):
    """Raised when a Sandbox operation exceeds its whole-operation deadline."""


class SandboxTerminalStateError(SandboxError):
    """Raised when a Sandbox operation reaches a terminal failure state."""


@dataclass(frozen=True, slots=True)
class SandboxCreateParams:
    runtime: str | None = None
    name: str | None = None
    source: Source | None = None
    ports: list[int] | None = None
    timeout: timedelta | None = None
    resources: Resources | None = None
    interactive: bool | None = None
    env: dict[str, str] | None = None
    network_policy: NetworkPolicy | None = None
    persistent: bool | None = None
    snapshot_expiration: timedelta | None = None
    tags: list[str] | None = None


@dataclass(frozen=True, slots=True)
class SandboxOptions:
    api_url: str | None = None
    team_id: str | None = None
    project_id: str | None = None
    request_timeout: timedelta | None = None
    retry_attempts: int | None = None
    credential_provider: SandboxCredentialProvider | SyncSandboxCredentialProvider | None = None


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


def is_ready_for_create(status: SandboxStatus | None) -> bool:
    """Return True if the sandbox has reached the RUNNING state."""
    return status is not None and status == SandboxStatus.RUNNING


def is_terminal_for_create(status: SandboxStatus | None) -> bool:
    """Return True if the sandbox has reached a terminal failure state.

    Terminal states cannot transition to RUNNING, so a create that reaches
    them has definitively failed.
    """
    if status is None:
        return False
    return status in {
        SandboxStatus.FAILED,
        SandboxStatus.ABORTED,
        SandboxStatus.STOPPED,
        SandboxStatus.STOPPING,
    }


@dataclass(slots=True)
class Sandbox:
    name: str
    persistent: bool | None = None
    current_snapshot_id: str | None = None
    current_session: Session | None = None
    routes: list[Any] | None = None
    _session: _SdkSession | None = field(default=None, repr=False, compare=False)
    _raw: dict[str, Any] | None = field(default=None, repr=False, compare=False)


@dataclass(slots=True)
class SyncSandbox:
    name: str
    persistent: bool | None = None
    current_snapshot_id: str | None = None
    current_session: Session | None = None
    routes: list[Any] | None = None
    _session: _SdkSyncSession | None = field(default=None, repr=False, compare=False)
    _raw: dict[str, Any] | None = field(default=None, repr=False, compare=False)


@dataclass(slots=True)
class Snapshot:
    id: str
    sandbox_id: str | None = None
    _session: _SdkSession | None = field(default=None, repr=False, compare=False)
    _raw: dict[str, Any] | None = field(default=None, repr=False, compare=False)


@dataclass(slots=True)
class SyncSnapshot:
    id: str
    sandbox_id: str | None = None
    _session: _SdkSyncSession | None = field(default=None, repr=False, compare=False)
    _raw: dict[str, Any] | None = field(default=None, repr=False, compare=False)


__all__ = [
    "Sandbox",
    "SandboxAPIError",
    "SandboxCreateParams",
    "SandboxError",
    "SandboxOptions",
    "SandboxOperationTimeoutError",
    "SandboxStatus",
    "SandboxTerminalStateError",
    "Session",
    "Snapshot",
    "SyncSandbox",
    "SyncSnapshot",
]
