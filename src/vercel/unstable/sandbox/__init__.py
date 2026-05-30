"""Experimental Sandbox SDK surface."""

from collections.abc import AsyncIterator, Mapping

from vercel._internal.unstable.sandbox.async_runtime import (
    CreateSandboxOperation,
    Sandbox,
    SandboxCommand,
    SandboxFilesystem,
    SandboxRuntimeSession,
    Snapshot,
    create_sandbox_operation as _create_sandbox_operation,
    get_sandbox as _get_sandbox,
    get_snapshot as _get_snapshot,
    query_sandboxes as _query_sandboxes,
    query_sessions as _query_sessions,
    query_snapshots as _query_snapshots,
)
from vercel._internal.unstable.sandbox.errors import (
    SandboxApiError,
    SandboxCleanupError,
    SandboxCredentialsError,
    SandboxError,
    SandboxFilesystemCommandError,
    SandboxFilesystemError,
    SandboxInvalidHandleError,
    SandboxPathNotFoundError,
    SandboxResponseError,
    SandboxStreamError,
    SandboxTerminalStateError,
)
from vercel._internal.unstable.sandbox.models import (
    DirectoryEntry,
    DurationInput,
    GitSource,
    JSONValue,
    SandboxCommandLog,
    SandboxCommandLogStream,
    SandboxQuery,
    SandboxQueryByCreatedAt,
    SandboxQueryByCurrentSnapshotId,
    SandboxQueryByName,
    SandboxQueryByStatusUpdatedAt,
    SandboxResources,
    SandboxSource,
    SandboxStatus,
    SnapshotRetention,
    SnapshotSource,
    TagFilter,
    TarballSource,
    WriteFile,
)
from vercel._internal.unstable.sandbox.options import SandboxServiceOptions
from vercel._internal.unstable.session import get_active_session

from . import sync


def create_sandbox(
    *,
    project_id: str | None = None,
    name: str | None = None,
    runtime: str | None = None,
    source: SandboxSource | None = None,
    ports: list[int] | None = None,
    execution_time_limit: DurationInput = None,
    resources: SandboxResources | None = None,
    persistent: bool | None = None,
    network_policy: JSONValue | None = None,
    env: Mapping[str, str] | None = None,
    tags: Mapping[str, str] | None = None,
    snapshot_expiration: DurationInput = None,
    snapshot_retention: SnapshotRetention | None = None,
) -> CreateSandboxOperation:
    return _create_sandbox_operation(
        get_active_session().sandbox_service(),
        project_id=project_id,
        name=name,
        runtime=runtime,
        source=source,
        ports=ports,
        execution_time_limit=execution_time_limit,
        resources=resources,
        persistent=persistent,
        network_policy=network_policy,
        env=env,
        tags=tags,
        snapshot_expiration=snapshot_expiration,
        snapshot_retention=snapshot_retention,
    )


async def get_sandbox(
    *,
    name: str,
    project_id: str | None = None,
    resume: bool = True,
    include_system_routes: bool | None = None,
) -> Sandbox:
    return await _get_sandbox(
        get_active_session().sandbox_service(),
        name=name,
        project_id=project_id,
        resume=resume,
        include_system_routes=include_system_routes,
    )


def query_sandboxes(
    *,
    query: SandboxQuery | None = None,
    project_id: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> AsyncIterator[Sandbox]:
    return _query_sandboxes(
        get_active_session().sandbox_service(),
        query=query,
        project_id=project_id,
        page_size=page_size,
        cursor=cursor,
    )


def query_sessions(
    *,
    project_id: str | None = None,
    name: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
    sort_order: str | None = None,
) -> AsyncIterator[SandboxRuntimeSession]:
    return _query_sessions(
        get_active_session().sandbox_service(),
        project_id=project_id,
        name=name,
        page_size=page_size,
        cursor=cursor,
        sort_order=sort_order,
    )


def query_snapshots(
    *,
    project_id: str | None = None,
    name: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
    sort_order: str | None = None,
) -> AsyncIterator[Snapshot]:
    return _query_snapshots(
        get_active_session().sandbox_service(),
        project_id=project_id,
        name=name,
        page_size=page_size,
        cursor=cursor,
        sort_order=sort_order,
    )


async def get_snapshot(*, snapshot_id: str) -> Snapshot:
    return await _get_snapshot(get_active_session().sandbox_service(), snapshot_id=snapshot_id)


__all__ = [
    "Sandbox",
    "SandboxApiError",
    "SandboxCleanupError",
    "SandboxCommand",
    "SandboxCommandLog",
    "SandboxCommandLogStream",
    "SandboxCredentialsError",
    "SandboxError",
    "SandboxFilesystem",
    "SandboxFilesystemCommandError",
    "SandboxFilesystemError",
    "SandboxInvalidHandleError",
    "SandboxPathNotFoundError",
    "SandboxResources",
    "SandboxQuery",
    "SandboxQueryByCreatedAt",
    "SandboxQueryByCurrentSnapshotId",
    "SandboxQueryByName",
    "SandboxQueryByStatusUpdatedAt",
    "SandboxResponseError",
    "SandboxStreamError",
    "SandboxRuntimeSession",
    "SandboxServiceOptions",
    "SandboxSource",
    "SandboxStatus",
    "SandboxTerminalStateError",
    "DirectoryEntry",
    "GitSource",
    "Snapshot",
    "SnapshotRetention",
    "SnapshotSource",
    "TagFilter",
    "TarballSource",
    "WriteFile",
    "create_sandbox",
    "get_sandbox",
    "get_snapshot",
    "query_sandboxes",
    "query_sessions",
    "query_snapshots",
    "sync",
]
