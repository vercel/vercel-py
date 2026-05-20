"""Internal Sandbox operation parameter types for unstable APIs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from vercel._internal.sandbox.models import NetworkPolicy, Resources, Source


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


__all__ = ["SandboxCreateParams"]
