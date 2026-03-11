"""Public immutable option dataclasses for the stable client surface."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class RootOptions:
    timeout: float | None = None


@dataclass(frozen=True, slots=True)
class SdkOptions:
    token: str | None = None
    base_url: str | None = None
    team_id: str | None = None
    team_slug: str | None = None
    headers: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class BlobOptions:
    token: str | None = None
    base_url: str | None = None


@dataclass(frozen=True, slots=True)
class CacheOptions:
    endpoint: str | None = None
    headers: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SandboxOptions:
    token: str | None = None
    base_url: str | None = None
    team_id: str | None = None
    team_slug: str | None = None


@dataclass(frozen=True, slots=True)
class QueueOptions:
    token: str | None = None
    base_url: str | None = None


__all__ = [
    "RootOptions",
    "SdkOptions",
    "BlobOptions",
    "CacheOptions",
    "SandboxOptions",
    "QueueOptions",
]
