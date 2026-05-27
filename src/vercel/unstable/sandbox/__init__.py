"""Experimental Sandbox SDK surface."""

from collections.abc import Mapping

from vercel._internal.unstable.context import get_active_session
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
from vercel._internal.unstable.sandbox.operations import (
    CreateSandboxOperation,
    create_sandbox_operation,
)
from vercel._internal.unstable.sandbox.options import SandboxServiceOptions

from . import sync


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
) -> CreateSandboxOperation:
    return create_sandbox_operation(
        project_id=project_id,
        name=name,
        runtime=runtime,
        source=source,
        ports=ports,
        timeout=timeout,
        resources=resources,
        persistent=persistent,
        network_policy=network_policy,
        env=env,
        tags=tags,
        snapshot_expiration=snapshot_expiration,
        keep_last_snapshots=keep_last_snapshots,
    )


async def get_sandbox(
    *,
    name: str,
    project_id: str | None = None,
    resume: bool = True,
    include_system_routes: bool | None = None,
) -> Sandbox:
    return (
        await get_active_session()
        .sandbox_service()
        .get_sandbox(
            name=name,
            project_id=project_id,
            resume=resume,
            include_system_routes=include_system_routes,
        )
    )


async def query_sandboxes(
    *,
    project_id: str | None = None,
    limit: int | None = None,
    cursor: str | None = None,
    sort_by: str | None = None,
    sort_order: str | None = None,
    name_prefix: str | None = None,
    tags: str | list[str] | None = None,
) -> list[Sandbox]:
    return (
        await get_active_session()
        .sandbox_service()
        .query_sandboxes(
            project_id=project_id,
            limit=limit,
            cursor=cursor,
            sort_by=sort_by,
            sort_order=sort_order,
            name_prefix=name_prefix,
            tags=tags,
        )
    )


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
    "sync",
]
