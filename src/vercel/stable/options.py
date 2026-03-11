"""Public immutable option dataclasses for the stable client surface."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class RootOptions:
    timeout: float | None = None
    env: Mapping[str, str] = field(default_factory=lambda: os.environ)


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
    namespace: str | None = None
    namespace_separator: str | None = None
    key_hash_function: Callable[[str], str] | None = None


@dataclass(frozen=True, slots=True)
class CacheSetOptions:
    ttl: float | None = None
    tags: Sequence[str] = ()
    name: str | None = None


@dataclass(frozen=True, slots=True)
class ProjectWriteRequest:
    name: str | None = None
    framework: str | None = None
    public_source: bool | None = None
    build_command: str | None = None
    dev_command: str | None = None
    install_command: str | None = None
    output_directory: str | None = None
    root_directory: str | None = None


@dataclass(frozen=True, slots=True)
class DeploymentCreateRequest:
    name: str | None = None
    project: str | None = None
    target: str | None = None
    files: Sequence[Mapping[str, Any]] = ()


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
    "CacheSetOptions",
    "ProjectWriteRequest",
    "DeploymentCreateRequest",
    "SandboxOptions",
    "QueueOptions",
]
