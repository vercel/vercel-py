"""Synchronous mirror for the Sandbox SDK surface."""

from collections.abc import Iterator, Mapping

from vercel._internal.core.session import get_active_sync_session
from vercel.sandbox._internal.errors import (
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
    SandboxTimeoutError as SandboxTimeoutError,
    SandboxUploadSizeMismatchError,
)
from vercel.sandbox._internal.models import (
    CompletedProcess,
    DirectoryEntry,
    DriveHandle,
    DriveMount,
    DriveMountsInput,
    DriveQuery,
    DriveQueryByCreatedAt,
    DriveQueryByName,
    DriveQueryByUpdatedAt,
    DriveReference,
    DurationInput,
    FailoverRegionsInput,
    GitSource,
    JSONValue as _JSONValue,
    NetworkPolicy,
    NetworkPolicyKeyValueMatcher,
    NetworkPolicyMatcher,
    NetworkPolicyRequestMatcher,
    NetworkPolicyRule,
    NetworkPolicySubnets,
    NetworkPolicyTransform,
    ProcessSignal,
    ProcessStatus,
    SandboxMount,
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
    _RemotePathT,
    normalize_private_parameters as _normalize_private_parameters,
)
from vercel.sandbox._internal.options import (
    SandboxCredentials,
    SyncSandboxCredentialsFactory,
    SyncSandboxServiceOptions as SandboxServiceOptions,
)
from vercel.sandbox._internal.service import SandboxService, get_sync_sandbox_service
from vercel.sandbox._internal.state import SnapshotRetentionState
from vercel.sandbox._internal.sync_filesystem_handle import (
    SyncSandboxBinaryReader,
    SyncSandboxBinaryWriter,
    SyncSandboxTextReader,
    SyncSandboxTextWriter,
)
from vercel.sandbox._internal.sync_runtime import (
    SyncDrive,
    SyncProcess,
    SyncSandbox,
    SyncSandboxFilesystem,
    SyncSandboxFilesystemBatch,
    SyncSandboxRuntimeSession,
    SyncSnapshot,
    _ManagedSyncSandbox,
    create_sandbox as _create_sandbox,
    delete_drive as _delete_drive,
    fork_sandbox as _fork_sandbox,
    get_or_create_drive as _get_or_create_drive,
    get_or_create_sandbox as _get_or_create_sandbox,
    get_sandbox as _get_sandbox,
    get_snapshot as _get_snapshot,
    query_drives as _query_drives,
    query_sandboxes as _query_sandboxes,
    query_sessions as _query_sessions,
    query_snapshots as _query_snapshots,
    resume_sandbox as _resume_sandbox,
)
from vercel.sandbox._internal.text_reader import SyncTextReader

from .client import SyncSandboxClient


def _service() -> SandboxService:
    return get_sync_sandbox_service(get_active_sync_session())


def create_sandbox(
    *,
    name: str | None = None,
    image: str | None = None,
    source: SandboxSource | None = None,
    ports: list[int] | None = None,
    execution_time_limit: DurationInput = None,
    resources: SandboxResources | None = None,
    persistent: bool | None = None,
    network_policy: NetworkPolicy | None = None,
    env: Mapping[str, str] | None = None,
    tags: Mapping[str, str] | None = None,
    mounts: DriveMountsInput[_RemotePathT] | None = None,
    snapshot_expiration: SnapshotExpirationInput = None,
    snapshot_retention: SnapshotRetention | None = None,
    region: str | None = None,
    failover_regions: FailoverRegionsInput = None,
    destroy: bool = True,
    **private_parameters: _JSONValue,
) -> _ManagedSyncSandbox:
    """Create a sandbox and wait until it is ready.

    The returned handle is also a context manager that stops the sandbox on
    exit, then destroys it and any snapshots that no other sandbox uses by
    default. Calling this function without entering the handle performs no
    automatic cleanup.

    Args:
        name: Requested sandbox name. The service generates one when omitted.
        image: Vercel Container Registry image reference. The backend validates
            and resolves the reference.
        source: Git, tarball, or snapshot source used to initialize the sandbox.
        ports: Ports to expose from the sandbox.
        execution_time_limit: Maximum session runtime in seconds or as a
            duration.
        resources: Requested CPU and memory resources.
        persistent: Whether the sandbox persists beyond its current session.
        network_policy: Network access policy sent to the Sandbox API.
        env: Environment variables for the sandbox.
        tags: Metadata tags used to organize and query sandboxes.
        mounts: Drive names or handles keyed by absolute mount path.
        snapshot_expiration: Default lifetime for snapshots created from this
            sandbox.
        snapshot_retention: Automatic snapshot retention policy.
        region: Preferred region for the sandbox.
        failover_regions: Regions available if creation in ``region`` fails.
        destroy: Whether context-manager exit destroys the sandbox and its
            orphaned snapshots after stopping it.

    Returns:
        A handle for the newly created sandbox.

    Raises:
        SandboxTerminalStateError: If creation reaches a terminal failure state.
    """
    return _create_sandbox(
        _service(),
        name=name,
        image=image,
        source=source,
        ports=ports,
        execution_time_limit=execution_time_limit,
        resources=resources,
        persistent=persistent,
        network_policy=network_policy,
        env=env,
        tags=tags,
        mounts=mounts,
        snapshot_expiration=snapshot_expiration,
        snapshot_retention=snapshot_retention,
        region=region,
        failover_regions=failover_regions,
        destroy=destroy,
        private_parameters=_normalize_private_parameters("create_sandbox", private_parameters),
    )


def fork_sandbox(
    *,
    source_sandbox: str,
    name: str | None = None,
    ports: list[int] | None = None,
    execution_time_limit: DurationInput = None,
    resources: SandboxResources | None = None,
    image: str | None = None,
    persistent: bool | None = None,
    network_policy: NetworkPolicy | None = None,
    env: Mapping[str, str] | None = None,
    tags: Mapping[str, str] | None = None,
    mounts: DriveMountsInput[_RemotePathT] | None = None,
    snapshot_expiration: SnapshotExpirationInput = None,
    snapshot_retention: SnapshotRetention | None = None,
    region: str | None = None,
    failover_regions: FailoverRegionsInput = None,
    destroy: bool = True,
    **private_parameters: _JSONValue,
) -> _ManagedSyncSandbox:
    """Fork a sandbox and wait until the fork is ready.

    The server initializes the fork from the source sandbox's current snapshot,
    or from its runtime or image when it has no snapshot. Configuration is
    inherited from the source; values supplied here replace the corresponding
    inherited values.

    The returned handle is also a context manager that stops the fork on exit
    and destroys it by default. Calling this function without entering the
    handle performs no automatic cleanup.

    Args:
        source_sandbox: Name of the sandbox to fork.
        name: Requested name for the fork. The service generates one when
            omitted.
        ports: Ports to expose instead of the source sandbox's ports.
        execution_time_limit: Maximum session runtime override.
        resources: CPU and memory resource override.
        image: Vercel Container Registry image override.
        persistent: Persistence override.
        network_policy: Network access policy override.
        env: Environment variable override.
        tags: Metadata tag override.
        mounts: Drives to attach explicitly to the fork. Forks never inherit
            source mounts; omit this argument or pass an empty mapping for an
            unmounted fork.
        snapshot_expiration: Default snapshot lifetime override.
        snapshot_retention: Automatic snapshot retention override.
        region: Preferred region override.
        failover_regions: Failover region override.
        destroy: Whether context-manager exit destroys the fork after stopping
            it.

    Returns:
        A managed handle for the fork.

    Raises:
        SandboxTerminalStateError: If the fork reaches a terminal failure state.
    """
    return _fork_sandbox(
        _service(),
        source_sandbox=source_sandbox,
        name=name,
        ports=ports,
        execution_time_limit=execution_time_limit,
        resources=resources,
        image=image,
        persistent=persistent,
        network_policy=network_policy,
        env=env,
        tags=tags,
        mounts=mounts,
        snapshot_expiration=snapshot_expiration,
        snapshot_retention=snapshot_retention,
        region=region,
        failover_regions=failover_regions,
        destroy=destroy,
        private_parameters=_normalize_private_parameters("fork_sandbox", private_parameters),
    )


def get_or_create_sandbox(
    *,
    name: str,
    resume: bool = True,
    include_system_routes: bool | None = None,
    image: str | None = None,
    source: SandboxSource | None = None,
    ports: list[int] | None = None,
    execution_time_limit: DurationInput = None,
    resources: SandboxResources | None = None,
    persistent: bool | None = None,
    network_policy: NetworkPolicy | None = None,
    env: Mapping[str, str] | None = None,
    tags: Mapping[str, str] | None = None,
    mounts: DriveMountsInput[_RemotePathT] | None = None,
    snapshot_expiration: SnapshotExpirationInput = None,
    snapshot_retention: SnapshotRetention | None = None,
    region: str | None = None,
    failover_regions: FailoverRegionsInput = None,
    **private_parameters: _JSONValue,
) -> tuple[SyncSandbox, bool]:
    """Get a named sandbox or create it when it does not exist.

    The existing-sandbox lookup resumes by default. If its latest snapshot is
    missing, the stale named sandbox is destroyed and recreated.
    Creation options are used only when the sandbox must be created or
    recreated.

    Args:
        name: Sandbox name to retrieve or create.
        resume: Whether to resume an existing stopped sandbox during lookup.
        include_system_routes: Whether to include platform-managed routes.
        image: Vercel Container Registry image reference. The backend validates
            and resolves the reference.
        source: Git, tarball, or snapshot source used to initialize the sandbox.
        ports: Ports to expose from the sandbox.
        execution_time_limit: Maximum session runtime in seconds or as a
            duration.
        resources: Requested CPU and memory resources.
        persistent: Whether the sandbox persists beyond its current session.
        network_policy: Network access policy sent to the Sandbox API.
        env: Environment variables for the sandbox.
        tags: Metadata tags used to organize and query sandboxes.
        mounts: Drive names or handles keyed by absolute mount path.
        snapshot_expiration: Default lifetime for snapshots created from this
            sandbox.
        snapshot_retention: Automatic snapshot retention policy.
        region: Preferred region for a newly created sandbox.
        failover_regions: Failover regions for a newly created sandbox.

    Returns:
        A ``(sandbox, created)`` tuple. ``created`` is true when this call
        created or recreated the sandbox, and false when it returned an
        existing sandbox.
    """
    return _get_or_create_sandbox(
        _service(),
        name=name,
        resume=resume,
        include_system_routes=include_system_routes,
        image=image,
        source=source,
        ports=ports,
        execution_time_limit=execution_time_limit,
        resources=resources,
        persistent=persistent,
        network_policy=network_policy,
        env=env,
        tags=tags,
        mounts=mounts,
        snapshot_expiration=snapshot_expiration,
        snapshot_retention=snapshot_retention,
        region=region,
        failover_regions=failover_regions,
        private_parameters=_normalize_private_parameters(
            "get_or_create_sandbox", private_parameters
        ),
    )


def get_sandbox(
    *,
    name: str,
    include_system_routes: bool | None = None,
    **private_parameters: _JSONValue,
) -> SyncSandbox:
    """Fetch a sandbox by name without resuming it.

    Session-bound operations on the returned handle resume lazily when needed.
    Use ``current_session`` for passive inspection or ``session()`` for an
    authoritative exact-session acquisition and optional managed scope.

    Args:
        name: Sandbox name.
        include_system_routes: Whether to include platform-managed routes.

    Returns:
        A handle for the requested sandbox.

    Raises:
        SandboxApiError: If the sandbox cannot be retrieved.
    """
    return _get_sandbox(
        _service(),
        name=name,
        include_system_routes=include_system_routes,
        private_parameters=_normalize_private_parameters("get_sandbox", private_parameters),
    )


def resume_sandbox(
    *,
    name: str,
    include_system_routes: bool | None = None,
    **private_parameters: _JSONValue,
) -> _ManagedSyncSandbox:
    """Resume a sandbox and return a managed handle.

    The returned handle is a context manager that stops the active session on
    exit. Calling this function without entering the handle performs no
    automatic cleanup.

    Args:
        name: Sandbox name.
        include_system_routes: Whether to include platform-managed routes.

    Returns:
        A sandbox handle with an active current session.

    Raises:
        SandboxApiError: If the sandbox cannot be resumed.
    """
    return _resume_sandbox(
        _service(),
        name=name,
        include_system_routes=include_system_routes,
        private_parameters=_normalize_private_parameters("resume_sandbox", private_parameters),
    )


def get_or_create_drive(
    *,
    name: str,
    max_size_bytes: int | None = None,
    region: str | None = None,
) -> tuple[SyncDrive, bool]:
    """Return a named Drive, creating it when necessary.

    Args:
        name: Project-local Drive name.
        max_size_bytes: Maximum size for a new Drive. Uses the project's default
            when omitted.
        region: Storage region for a new Drive. Uses ``SyncSandboxServiceOptions.region``
            when omitted. An existing Drive must already use the requested region.

    Returns:
        A ``(drive, created)`` tuple. ``created`` is true when this call created
        the Drive, and false when it returned an existing Drive.
    """
    return _get_or_create_drive(
        _service(),
        name=name,
        max_size_bytes=max_size_bytes,
        region=region,
    )


def delete_drive(*, name: str) -> SyncDrive:
    """Delete a Drive by project-local name.

    Use this function when no handle is available, such as after an uncertain
    creation failure. Otherwise, prefer ``drive.delete()``.

    Args:
        name: Project-local Drive name.

    Returns:
        The Drive returned by the deletion request.
    """
    return _delete_drive(_service(), name=name)


def query_drives(
    *,
    query: DriveQuery | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> Iterator[SyncDrive]:
    """Iterate over Drives in a project.

    Args:
        query: Ordering and optional name-prefix filter.
        page_size: Maximum number of Drives fetched per API request.
        cursor: Cursor at which to begin pagination.

    Returns:
        An iterator that transparently follows pagination cursors.
    """
    return _query_drives(
        _service(),
        query=query,
        page_size=page_size,
        cursor=cursor,
    )


def query_sandboxes(
    *,
    query: SandboxQuery | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> Iterator[SyncSandbox]:
    """Iterate over sandboxes matching a query.

    Args:
        query: Ordering and filtering options.
        page_size: Maximum number of sandboxes fetched per API request.
        cursor: Cursor at which to begin pagination.

    Returns:
        An iterator that transparently follows pagination cursors.
    """
    return _query_sandboxes(
        _service(),
        query=query,
        page_size=page_size,
        cursor=cursor,
    )


def query_sessions(
    *,
    name: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
    sort_order: str | None = None,
) -> Iterator[SyncSandboxRuntimeSession]:
    """Iterate over runtime sessions.

    Args:
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
        name=name,
        page_size=page_size,
        cursor=cursor,
        sort_order=sort_order,
    )


def query_snapshots(
    *,
    name: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
    sort_order: str | None = None,
) -> Iterator[SyncSnapshot]:
    """Iterate over snapshots.

    Args:
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
    "DriveHandle",
    "DriveMount",
    "DriveReference",
    "SandboxMount",
    "DriveQuery",
    "DriveQueryByCreatedAt",
    "DriveQueryByName",
    "DriveQueryByUpdatedAt",
    "SandboxApiError",
    "SandboxCleanupError",
    "ProcessStatus",
    "ProcessSignal",
    "CompletedProcess",
    "SandboxCredentials",
    "SandboxCredentialsError",
    "SyncSandboxCredentialsFactory",
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
    "SyncSandboxClient",
    "SyncSandboxBinaryReader",
    "SyncSandboxBinaryWriter",
    "SyncProcess",
    "SyncSandboxFilesystem",
    "SyncSandboxFilesystemBatch",
    "SyncSandboxRuntimeSession",
    "SyncSandboxTextReader",
    "SyncSandboxTextWriter",
    "SyncSnapshot",
    "SyncDrive",
    "TagFilter",
    "TarballSource",
    "SyncTextReader",
    "create_sandbox",
    "delete_drive",
    "fork_sandbox",
    "get_or_create_sandbox",
    "get_sandbox",
    "get_or_create_drive",
    "get_snapshot",
    "query_sandboxes",
    "query_drives",
    "query_sessions",
    "query_snapshots",
    "resume_sandbox",
]
