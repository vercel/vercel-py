"""Sync mirror for the experimental Sandbox SDK surface."""

from collections.abc import Iterator, Mapping

from vercel._internal.unstable.sandbox.errors import (
    SandboxApiError,
    SandboxCleanupError,
    SandboxCredentialsError,
    SandboxError,
    SandboxFilesystemCommandError,
    SandboxFilesystemError,
    SandboxFilesystemTransferError,
    SandboxFilesystemWriteError,
    SandboxInvalidHandleError,
    SandboxPathNotFoundError,
    SandboxResponseError,
    SandboxStreamError,
    SandboxTerminalStateError,
    SandboxUploadSizeMismatchError,
)
from vercel._internal.unstable.sandbox.models import (
    CompletedProcess,
    DirectoryEntry,
    DurationInput,
    GitSource,
    NetworkPolicy,
    NetworkPolicyKeyValueMatcher,
    NetworkPolicyMatcher,
    NetworkPolicyRequestMatcher,
    NetworkPolicyRule,
    NetworkPolicySubnets,
    NetworkPolicyTransform,
    ProcessStatus,
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
)
from vercel._internal.unstable.sandbox.options import SandboxServiceOptions
from vercel._internal.unstable.sandbox.service import SandboxService, get_sandbox_service
from vercel._internal.unstable.sandbox.state import SnapshotRetentionState
from vercel._internal.unstable.sandbox.sync_filesystem_handle import (
    SyncSandboxBinaryReader,
    SyncSandboxBinaryWriter,
    SyncSandboxTextReader,
    SyncSandboxTextWriter,
)
from vercel._internal.unstable.sandbox.sync_runtime import (
    SyncProcess,
    SyncSandbox,
    SyncSandboxFilesystem,
    SyncSandboxFilesystemBatch,
    SyncSandboxRuntimeSession,
    SyncSnapshot,
    _ManagedSyncSandbox,
    create_sandbox as _create_sandbox,
    get_sandbox as _get_sandbox,
    get_snapshot as _get_snapshot,
    query_sandboxes as _query_sandboxes,
    query_sessions as _query_sessions,
    query_snapshots as _query_snapshots,
    resume_sandbox as _resume_sandbox,
)
from vercel._internal.unstable.sandbox.text_reader import SyncTextReader
from vercel._internal.unstable.session import get_active_sync_session


def _service() -> SandboxService:
    return get_sandbox_service(get_active_sync_session())


def create_sandbox(
    *,
    project_id: str | None = None,
    name: str | None = None,
    runtime: str | None = None,
    image: str | None = None,
    source: SandboxSource | None = None,
    ports: list[int] | None = None,
    execution_time_limit: DurationInput = None,
    resources: SandboxResources | None = None,
    persistent: bool | None = None,
    network_policy: NetworkPolicy | None = None,
    env: Mapping[str, str] | None = None,
    tags: Mapping[str, str] | None = None,
    snapshot_expiration: SnapshotExpirationInput = None,
    snapshot_retention: SnapshotRetention | None = None,
    destroy: bool = True,
) -> _ManagedSyncSandbox:
    """Create a sandbox and wait until it is ready.

    The returned handle is also a context manager that stops the sandbox on
    exit and destroys it by default. Calling this function without entering
    the handle performs no automatic cleanup.

    Args:
        project_id: Project that owns the sandbox. Uses the active credentials
            when omitted.
        name: Requested sandbox name. The service generates one when omitted.
        runtime: Runtime identifier. Mutually exclusive with ``image``.
        image: Vercel Container Registry image to start the sandbox from.
            Mutually exclusive with ``runtime``.
        source: Git, tarball, or snapshot source used to initialize the sandbox.
        ports: Ports to expose from the sandbox.
        execution_time_limit: Maximum session runtime in seconds or as a
            duration.
        resources: Requested CPU and memory resources.
        persistent: Whether the sandbox persists beyond its current session.
        network_policy: Network access policy sent to the Sandbox API.
        env: Environment variables for the sandbox.
        tags: Metadata tags used to organize and query sandboxes.
        snapshot_expiration: Default lifetime for snapshots created from this
            sandbox.
        snapshot_retention: Automatic snapshot retention policy.
        destroy: Whether context-manager exit destroys the sandbox after
            stopping it.

    Returns:
        A handle for the newly created sandbox.

    Raises:
        SandboxTerminalStateError: If creation reaches a terminal failure state.
    """
    return _create_sandbox(
        _service(),
        project_id=project_id,
        name=name,
        runtime=runtime,
        image=image,
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
        destroy=destroy,
    )


def get_sandbox(
    *,
    name: str,
    project_id: str | None = None,
    include_system_routes: bool | None = None,
) -> SyncSandbox:
    """Fetch a sandbox by name without resuming it.

    Args:
        name: Sandbox name.
        project_id: Project that owns the sandbox.
        include_system_routes: Whether to include platform-managed routes.

    Returns:
        A handle for the requested sandbox.

    Raises:
        SandboxApiError: If the sandbox cannot be retrieved.
    """
    return _get_sandbox(
        _service(),
        name=name,
        project_id=project_id,
        include_system_routes=include_system_routes,
    )


def resume_sandbox(
    *,
    name: str,
    project_id: str | None = None,
    include_system_routes: bool | None = None,
) -> _ManagedSyncSandbox:
    """Resume a sandbox and return a managed handle.

    The returned handle is a context manager that stops the active session on
    exit. Calling this function without entering the handle performs no
    automatic cleanup.

    Args:
        name: Sandbox name.
        project_id: Project that owns the sandbox.
        include_system_routes: Whether to include platform-managed routes.

    Returns:
        A sandbox handle with an active current session.

    Raises:
        SandboxApiError: If the sandbox cannot be resumed.
    """
    return _resume_sandbox(
        _service(),
        name=name,
        project_id=project_id,
        include_system_routes=include_system_routes,
    )


def query_sandboxes(
    *,
    query: SandboxQuery | None = None,
    project_id: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> Iterator[SyncSandbox]:
    """Iterate over sandboxes matching a query.

    Args:
        query: Ordering and filtering options.
        project_id: Project whose sandboxes should be queried.
        page_size: Maximum number of sandboxes fetched per API request.
        cursor: Cursor at which to begin pagination.

    Returns:
        An iterator that transparently follows pagination cursors.
    """
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
    """Iterate over runtime sessions.

    Args:
        project_id: Project whose sessions should be queried.
        name: Sandbox name used to restrict the results.
        page_size: Maximum number of sessions fetched per API request.
        cursor: Cursor at which to begin pagination.
        sort_order: Result order by creation time, either ``"asc"`` or
            ``"desc"``.

    Returns:
        An iterator that transparently follows pagination cursors.
    """
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
    """Iterate over snapshots.

    Args:
        project_id: Project whose snapshots should be queried.
        name: Sandbox name used to restrict the results.
        page_size: Maximum number of snapshots fetched per API request.
        cursor: Cursor at which to begin pagination.
        sort_order: Result order by creation time, either ``"asc"`` or
            ``"desc"``.

    Returns:
        An iterator that transparently follows pagination cursors.
    """
    return _query_snapshots(
        _service(),
        project_id=project_id,
        name=name,
        page_size=page_size,
        cursor=cursor,
        sort_order=sort_order,
    )


def get_snapshot(*, snapshot_id: str) -> SyncSnapshot:
    """Get a snapshot by identifier.

    Args:
        snapshot_id: Snapshot identifier.

    Returns:
        A handle for the requested snapshot.

    Raises:
        SandboxApiError: If the snapshot cannot be retrieved.
    """
    return _get_snapshot(_service(), snapshot_id=snapshot_id)


__all__ = [
    "SandboxApiError",
    "SandboxCleanupError",
    "ProcessStatus",
    "CompletedProcess",
    "SandboxCredentialsError",
    "SandboxError",
    "SandboxFilesystemCommandError",
    "SandboxFilesystemError",
    "SandboxFilesystemTransferError",
    "SandboxFilesystemWriteError",
    "SandboxInvalidHandleError",
    "SandboxPathNotFoundError",
    "SandboxUploadSizeMismatchError",
    "NetworkPolicy",
    "NetworkPolicyKeyValueMatcher",
    "NetworkPolicyMatcher",
    "NetworkPolicyRequestMatcher",
    "NetworkPolicyRule",
    "NetworkPolicySubnets",
    "NetworkPolicyTransform",
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
    "SyncSandboxBinaryReader",
    "SyncSandboxBinaryWriter",
    "SyncProcess",
    "SyncSandboxFilesystem",
    "SyncSandboxFilesystemBatch",
    "SyncSandboxRuntimeSession",
    "SyncSandboxTextReader",
    "SyncSandboxTextWriter",
    "SyncSnapshot",
    "TagFilter",
    "TarballSource",
    "SyncTextReader",
    "create_sandbox",
    "get_sandbox",
    "get_snapshot",
    "query_sandboxes",
    "query_sessions",
    "query_snapshots",
    "resume_sandbox",
]
