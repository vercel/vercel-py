"""Sync mirror for the experimental Sandbox SDK surface."""

from collections.abc import Mapping

from vercel._internal.unstable.sandbox.errors import (
    SandboxApiError,
    SandboxCredentialsError,
    SandboxError,
    SandboxInvalidHandleError,
    SandboxResponseError,
    SandboxTerminalStateError,
)
from vercel._internal.unstable.sandbox.models import (
    DurationInput,
    JSONValue,
    Sandbox,
    SandboxRuntimeSession,
    SandboxStatus,
)
from vercel._internal.unstable.sandbox.options import SandboxServiceOptions


def create_sandbox(
    *,
    project_id: str | None = None,
    name: str | None = None,
    runtime: str | None = None,
    source: JSONValue | None = None,
    ports: list[int] | None = None,
    timeout: DurationInput = None,
    resources: JSONValue | None = None,
    persistent: bool | None = None,
    network_policy: JSONValue | None = None,
    env: Mapping[str, str] | None = None,
    tags: Mapping[str, str] | None = None,
    snapshot_expiration: DurationInput = None,
    keep_last_snapshots: JSONValue | None = None,
) -> Sandbox:
    raise NotImplementedError("sandbox.sync.create_sandbox(...) is not implemented yet")


def get_sandbox(
    *,
    name: str,
    project_id: str | None = None,
    resume: bool = True,
    include_system_routes: bool | None = None,
) -> Sandbox:
    raise NotImplementedError("sandbox.sync.get_sandbox(...) is not implemented yet")


def query_sandboxes(
    *,
    project_id: str | None = None,
    limit: int | None = None,
    cursor: str | None = None,
    sort_by: str | None = None,
    sort_order: str | None = None,
    name_prefix: str | None = None,
    tags: str | list[str] | None = None,
) -> list[Sandbox]:
    raise NotImplementedError("sandbox.sync.query_sandboxes(...) is not implemented yet")


__all__ = [
    "Sandbox",
    "SandboxApiError",
    "SandboxCredentialsError",
    "SandboxError",
    "SandboxInvalidHandleError",
    "SandboxResponseError",
    "SandboxRuntimeSession",
    "SandboxServiceOptions",
    "SandboxStatus",
    "SandboxTerminalStateError",
    "create_sandbox",
    "get_sandbox",
    "query_sandboxes",
]
