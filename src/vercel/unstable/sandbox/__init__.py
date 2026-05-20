"""Unstable Sandbox domain types and options."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from vercel._internal.sandbox.models import NetworkPolicy, Resources, SandboxStatus, Source
from vercel._internal.unstable.errors import VercelError
from vercel.unstable.auth import AccessTokenCredentials, CredentialProvider, OIDCCredentials

if TYPE_CHECKING:
    from vercel._internal.unstable.session import Session, SyncSession


SandboxCredentialProvider = (
    CredentialProvider[OIDCCredentials] | CredentialProvider[AccessTokenCredentials]
)


class SandboxError(VercelError):
    """Base class for unstable Sandbox errors."""


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
    "SandboxCreateParams",
    "SandboxError",
    "SandboxOptions",
    "SandboxStatus",
    "Snapshot",
    "SyncSandbox",
    "SyncSnapshot",
]
