"""Sync mirror for the experimental Sandbox SDK surface."""

from collections.abc import Iterator, Mapping

from vercel._internal.unstable.context import get_active_sync_session
from vercel._internal.unstable.sandbox.errors import (
    SandboxApiError,
    SandboxCleanupError,
    SandboxCredentialsError,
    SandboxError,
    SandboxInvalidHandleError,
    SandboxResponseError,
    SandboxStreamError,
    SandboxTerminalStateError,
)
from vercel._internal.unstable.sandbox.handles import (
    SandboxCommand,
    SyncSandbox,
    SyncSandboxCommand,
    SyncSandboxRuntimeSession,
    SyncSnapshot,
)
from vercel._internal.unstable.sandbox.models import (
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
) -> SyncSandbox:
    return (
        get_active_sync_session()
        .sandbox_service()
        .create_sandbox(
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
    )


def get_sandbox(
    *,
    name: str,
    project_id: str | None = None,
    resume: bool = True,
    include_system_routes: bool | None = None,
) -> SyncSandbox:
    return (
        get_active_sync_session()
        .sandbox_service()
        .get_sandbox(
            name=name,
            project_id=project_id,
            resume=resume,
            include_system_routes=include_system_routes,
        )
    )


def query_sandboxes(
    *,
    query: SandboxQuery | None = None,
    project_id: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> Iterator[SyncSandbox]:
    return (
        get_active_sync_session()
        .sandbox_service()
        .query_sandboxes(query=query, project_id=project_id, page_size=page_size, cursor=cursor)
    )


def query_sessions(
    *,
    project_id: str | None = None,
    name: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
    sort_order: str | None = None,
) -> Iterator[SyncSandboxRuntimeSession]:
    return (
        get_active_sync_session()
        .sandbox_service()
        .query_sessions(
            project_id=project_id,
            name=name,
            page_size=page_size,
            cursor=cursor,
            sort_order=sort_order,
        )
    )


def query_snapshots(
    *,
    project_id: str | None = None,
    name: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
    sort_order: str | None = None,
) -> Iterator[SyncSnapshot]:
    return (
        get_active_sync_session()
        .sandbox_service()
        .query_snapshots(
            project_id=project_id,
            name=name,
            page_size=page_size,
            cursor=cursor,
            sort_order=sort_order,
        )
    )


def get_snapshot(*, snapshot_id: str) -> SyncSnapshot:
    return get_active_sync_session().sandbox_service().get_snapshot(snapshot_id=snapshot_id)


__all__ = [
    "SandboxApiError",
    "SandboxCleanupError",
    "SandboxCommand",
    "SandboxCommandLog",
    "SandboxCommandLogStream",
    "SandboxCredentialsError",
    "SandboxError",
    "SandboxInvalidHandleError",
    "SandboxResources",
    "SandboxQuery",
    "SandboxQueryByCreatedAt",
    "SandboxQueryByCurrentSnapshotId",
    "SandboxQueryByName",
    "SandboxQueryByStatusUpdatedAt",
    "SandboxResponseError",
    "SandboxStreamError",
    "SandboxServiceOptions",
    "SandboxSource",
    "SandboxStatus",
    "SandboxTerminalStateError",
    "GitSource",
    "SnapshotRetention",
    "SnapshotSource",
    "SyncSandbox",
    "SyncSandboxCommand",
    "SyncSandboxRuntimeSession",
    "SyncSnapshot",
    "TagFilter",
    "TarballSource",
    "WriteFile",
    "create_sandbox",
    "get_sandbox",
    "get_snapshot",
    "query_sandboxes",
    "query_sessions",
    "query_snapshots",
]
