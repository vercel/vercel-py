"""Sync mirror for the experimental Sandbox SDK surface."""

from collections.abc import Iterator, Mapping

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
    SnapshotExpiration,
    SnapshotExpirationInput,
    SnapshotRetention,
    SnapshotSource,
    TagFilter,
    TarballSource,
    WriteFile,
)
from vercel._internal.unstable.sandbox.options import SandboxServiceOptions
from vercel._internal.unstable.sandbox.service import SandboxService, get_sandbox_service
from vercel._internal.unstable.sandbox.state import SnapshotRetentionState
from vercel._internal.unstable.sandbox.sync_runtime import (
    SyncSandbox,
    SyncSandboxCommand,
    SyncSandboxFilesystem,
    SyncSandboxRuntimeSession,
    SyncSnapshot,
    create_sandbox as _create_sandbox,
    get_sandbox as _get_sandbox,
    get_snapshot as _get_snapshot,
    query_sandboxes as _query_sandboxes,
    query_sessions as _query_sessions,
    query_snapshots as _query_snapshots,
)
from vercel._internal.unstable.session import get_active_sync_session


def _service() -> SandboxService:
    return get_sandbox_service(get_active_sync_session())


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
    snapshot_expiration: SnapshotExpirationInput = None,
    snapshot_retention: SnapshotRetention | None = None,
) -> SyncSandbox:
    return _create_sandbox(
        _service(),
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


def get_sandbox(
    *,
    name: str,
    project_id: str | None = None,
    resume: bool = True,
    include_system_routes: bool | None = None,
) -> SyncSandbox:
    return _get_sandbox(
        _service(),
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
) -> Iterator[SyncSandbox]:
    return _query_sandboxes(
        _service(),
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
) -> Iterator[SyncSandboxRuntimeSession]:
    return _query_sessions(
        _service(),
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
) -> Iterator[SyncSnapshot]:
    return _query_snapshots(
        _service(),
        project_id=project_id,
        name=name,
        page_size=page_size,
        cursor=cursor,
        sort_order=sort_order,
    )


def get_snapshot(*, snapshot_id: str) -> SyncSnapshot:
    return _get_snapshot(_service(), snapshot_id=snapshot_id)


__all__ = [
    "SandboxApiError",
    "SandboxCleanupError",
    "SandboxCommandLog",
    "SandboxCommandLogStream",
    "SandboxCredentialsError",
    "SandboxError",
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
    "SandboxServiceOptions",
    "SandboxSource",
    "SandboxStatus",
    "SandboxTerminalStateError",
    "DirectoryEntry",
    "GitSource",
    "SnapshotRetention",
    "SnapshotExpiration",
    "SnapshotRetentionState",
    "SnapshotSource",
    "SyncSandbox",
    "SyncSandboxCommand",
    "SyncSandboxFilesystem",
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
