"""Internal Sandbox domain types and error adapters for unstable APIs."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from vercel._internal.sandbox.errors import APIError as StableSandboxAPIError
from vercel._internal.sandbox.models import NetworkPolicy, Resources, SandboxStatus, Source
from vercel._internal.unstable.errors import VercelError
from vercel._internal.unstable.sandbox_errors import (
    normalize_retry_after,
    sandbox_api_error_context,
)

from .auth import SandboxCredentialProvider

if TYPE_CHECKING:
    from vercel._internal.unstable.session import Session, SyncSession


class SandboxError(VercelError):
    """Base class for unstable Sandbox errors."""


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
    source: Source | None = None
    ports: list[int] | None = None
    timeout: timedelta | None = None
    resources: Resources | None = None
    interactive: bool | None = None
    env: dict[str, str] | None = None
    network_policy: NetworkPolicy | None = None


@dataclass(frozen=True, slots=True)
class SandboxOptions:
    api_url: str | None = None
    team_id: str | None = None
    project_id: str | None = None
    request_timeout: timedelta | None = None
    retry_attempts: int | None = None
    credential_provider: SandboxCredentialProvider | None = None


@dataclass(slots=True)
class Sandbox:
    id: str
    status: SandboxStatus | None = None
    _session: Session | None = field(default=None, repr=False, compare=False)
    _raw: dict[str, Any] | None = field(default=None, repr=False, compare=False)


@dataclass(slots=True)
class SyncSandbox:
    id: str
    status: SandboxStatus | None = None
    _session: SyncSession | None = field(default=None, repr=False, compare=False)
    _raw: dict[str, Any] | None = field(default=None, repr=False, compare=False)


@dataclass(slots=True)
class Snapshot:
    id: str
    sandbox_id: str | None = None
    _session: Session | None = field(default=None, repr=False, compare=False)
    _raw: dict[str, Any] | None = field(default=None, repr=False, compare=False)


@dataclass(slots=True)
class SyncSnapshot:
    id: str
    sandbox_id: str | None = None
    _session: SyncSession | None = field(default=None, repr=False, compare=False)
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
    "Snapshot",
    "SyncSandbox",
    "SyncSnapshot",
]
