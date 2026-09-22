"""Standalone synchronous Sandbox service client."""

from collections.abc import Iterator, Mapping

from vercel._internal.core.options import collect_service_options
from vercel._internal.core.polyfills import Self
from vercel._internal.core.session import HttpxClientFactory, SyncSdkSession
from vercel.sandbox._internal.models import (
    DriveMountsInput,
    DriveQuery,
    DurationInput,
    FailoverRegionsInput,
    JSONValue as _JSONValue,
    NetworkPolicy,
    SandboxQuery,
    SandboxResources,
    SandboxSource,
    SnapshotExpirationInput,
    SnapshotRetention,
    _RemotePathT,
    normalize_private_parameters as _normalize_private_parameters,
)
from vercel.sandbox._internal.options import SyncSandboxServiceOptions
from vercel.sandbox._internal.service import SandboxService, get_sync_sandbox_service
from vercel.sandbox._internal.sync_runtime import (
    SyncDrive,
    SyncSandbox,
    SyncSandboxRuntimeSession,
    SyncSnapshot,
    _ManagedSyncSandbox,
    create_sandbox as _create_sync_sandbox,
    delete_drive as _delete_sync_drive,
    fork_sandbox as _fork_sync_sandbox,
    get_or_create_drive as _get_or_create_sync_drive,
    get_or_create_sandbox as _get_or_create_sync_sandbox,
    get_sandbox as _get_sync_sandbox,
    get_snapshot as _get_sync_snapshot,
    query_drives as _query_sync_drives,
    query_sandboxes as _query_sync_sandboxes,
    query_sessions as _query_sync_sessions,
    query_snapshots as _query_sync_snapshots,
    resume_sandbox as _resume_sync_sandbox,
)


class SyncSandboxClient:
    """Standalone synchronous client for Sandbox service operations.

    The client owns an SDK session without making it active. Construction is
    lazy and performs no I/O; call :meth:`close` when the client is no longer
    needed.
    """

    def __init__(self, *, _session: SyncSdkSession) -> None:
        self._session = _session

    @property
    def _service(self) -> SandboxService:
        return get_sync_sandbox_service(self._session)

    @classmethod
    def create(
        cls,
        *,
        options: SyncSandboxServiceOptions | None = None,
        httpx_client_factory: HttpxClientFactory | None = None,
    ) -> Self:
        """Create a standalone client with lazy transport initialization."""
        session = SyncSdkSession(
            service_options=collect_service_options([options] if options is not None else None),
            httpx_client_factory=httpx_client_factory,
        )
        return cls(_session=session)

    def close(self) -> None:
        """Close resources owned by this client."""
        self._session.close()

    def create_sandbox(
        self,
        *,
        project_id: str | None = None,
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
        return _create_sync_sandbox(
            self._service,
            project_id=project_id,
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
        self,
        *,
        source_sandbox: str,
        project_id: str | None = None,
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
        return _fork_sync_sandbox(
            self._service,
            source_sandbox=source_sandbox,
            project_id=project_id,
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
        self,
        *,
        name: str,
        project_id: str | None = None,
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
        return _get_or_create_sync_sandbox(
            self._service,
            name=name,
            project_id=project_id,
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
        self,
        *,
        name: str,
        project_id: str | None = None,
        include_system_routes: bool | None = None,
        **private_parameters: _JSONValue,
    ) -> SyncSandbox:
        return _get_sync_sandbox(
            self._service,
            name=name,
            project_id=project_id,
            include_system_routes=include_system_routes,
            private_parameters=_normalize_private_parameters("get_sandbox", private_parameters),
        )

    def resume_sandbox(
        self,
        *,
        name: str,
        project_id: str | None = None,
        include_system_routes: bool | None = None,
        **private_parameters: _JSONValue,
    ) -> _ManagedSyncSandbox:
        return _resume_sync_sandbox(
            self._service,
            name=name,
            project_id=project_id,
            include_system_routes=include_system_routes,
            private_parameters=_normalize_private_parameters("resume_sandbox", private_parameters),
        )

    def get_or_create_drive(
        self,
        *,
        name: str,
        project_id: str | None = None,
        max_size_bytes: int | None = None,
        region: str | None = None,
    ) -> SyncDrive:
        return _get_or_create_sync_drive(
            self._service,
            name=name,
            project_id=project_id,
            max_size_bytes=max_size_bytes,
            region=region,
        )

    def delete_drive(self, *, name: str, project_id: str | None = None) -> SyncDrive:
        return _delete_sync_drive(self._service, name=name, project_id=project_id)

    def query_drives(
        self,
        *,
        query: DriveQuery | None = None,
        project_id: str | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
    ) -> Iterator[SyncDrive]:
        return _query_sync_drives(
            self._service,
            query=query,
            project_id=project_id,
            page_size=page_size,
            cursor=cursor,
        )

    def query_sandboxes(
        self,
        *,
        query: SandboxQuery | None = None,
        project_id: str | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
    ) -> Iterator[SyncSandbox]:
        return _query_sync_sandboxes(
            self._service,
            query=query,
            project_id=project_id,
            page_size=page_size,
            cursor=cursor,
        )

    def query_sessions(
        self,
        *,
        project_id: str | None = None,
        name: str | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> Iterator[SyncSandboxRuntimeSession]:
        return _query_sync_sessions(
            self._service,
            project_id=project_id,
            name=name,
            page_size=page_size,
            cursor=cursor,
            sort_order=sort_order,
        )

    def query_snapshots(
        self,
        *,
        project_id: str | None = None,
        name: str | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> Iterator[SyncSnapshot]:
        return _query_sync_snapshots(
            self._service,
            project_id=project_id,
            name=name,
            page_size=page_size,
            cursor=cursor,
            sort_order=sort_order,
        )

    def get_snapshot(self, *, snapshot_id: str) -> SyncSnapshot:
        return _get_sync_snapshot(self._service, snapshot_id=snapshot_id)


__all__ = ["SyncSandboxClient"]
