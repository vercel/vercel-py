"""Internal Sandbox accessor and client policy options."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from .auth import SandboxCredentialProvider, SyncSandboxCredentialProvider


@dataclass(frozen=True, slots=True)
class SandboxOptions:
    api_url: str | None = None
    team_id: str | None = None
    project_id: str | None = None
    request_timeout: timedelta | None = None
    retry_attempts: int | None = None
    credential_provider: SandboxCredentialProvider | SyncSandboxCredentialProvider | None = None


__all__ = ["SandboxOptions"]
