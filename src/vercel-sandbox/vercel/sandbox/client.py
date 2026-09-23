"""Standalone Sandbox service clients."""

from collections.abc import AsyncIterator, Mapping

from vercel._internal.core.options import collect_service_options
from vercel._internal.core.polyfills import Self
from vercel._internal.core.session import HttpxClientFactory, SdkSession
from vercel.sandbox._internal.async_runtime import (
    CreateSandboxOperation,
    Drive,
    ForkSandboxOperation,
    ResumeSandboxOperation,
    Sandbox,
    SandboxRuntimeSession,
    Snapshot,
    create_sandbox_operation as _create_sandbox_operation,
    delete_drive as _delete_drive,
    fork_sandbox_operation as _fork_sandbox_operation,
    get_or_create_drive as _get_or_create_drive,
    get_or_create_sandbox as _get_or_create_sandbox,
    get_sandbox as _get_sandbox,
    get_snapshot as _get_snapshot,
    query_drives as _query_drives,
    query_sandboxes as _query_sandboxes,
    query_sessions as _query_sessions,
    query_snapshots as _query_snapshots,
    resume_sandbox_operation as _resume_sandbox_operation,
)
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
from vercel.sandbox._internal.options import SandboxServiceOptions
from vercel.sandbox._internal.service import SandboxService, get_sandbox_service


class SandboxClient:
    """Standalone asynchronous client for Sandbox service operations.

    The client owns an SDK session without making it active. Construction is
    synchronous and performs no I/O; call :meth:`aclose` when the client is no
    longer needed.
    """

    def __init__(self, *, _session: SdkSession) -> None:
        self._session = _session

    @property
    def _service(self) -> SandboxService:
        return get_sandbox_service(self._session)

    @classmethod
    def create(
        cls,
        *,
        options: SandboxServiceOptions | None = None,
        httpx_client_factory: HttpxClientFactory | None = None,
    ) -> Self:
        """Create a standalone client with lazy transport initialization."""
        session = SdkSession(
            service_options=collect_service_options([options] if options is not None else None),
            httpx_client_factory=httpx_client_factory,
        )
        return cls(_session=session)

    async def aclose(self) -> None:
        """Close resources owned by this client."""
        await self._session.aclose()

    def create_sandbox(
        self,
        *,
        name: str | None = None,
        image: str | None = None,
        source: SandboxSource | None = None,
        ports: list[int] | None = None,
        execution_time_limit: DurationInput = None,
        resources: SandboxResources | None = None,
        persistent: bool | None = None,
        network_policy: NetworkPolicy | None = None,
        network_id: str | None = None,
        env: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        mounts: DriveMountsInput[_RemotePathT] | None = None,
        snapshot_expiration: SnapshotExpirationInput = None,
        snapshot_retention: SnapshotRetention | None = None,
        region: str | None = None,
        failover_regions: FailoverRegionsInput = None,
        destroy: bool = True,
        **private_parameters: _JSONValue,
    ) -> CreateSandboxOperation:
        return _create_sandbox_operation(
            self._service,
            name=name,
            image=image,
            source=source,
            ports=ports,
            execution_time_limit=execution_time_limit,
            resources=resources,
            persistent=persistent,
            network_policy=network_policy,
            network_id=network_id,
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
        name: str | None = None,
        ports: list[int] | None = None,
        execution_time_limit: DurationInput = None,
        resources: SandboxResources | None = None,
        image: str | None = None,
        persistent: bool | None = None,
        network_policy: NetworkPolicy | None = None,
        network_id: str | None = None,
        env: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        mounts: DriveMountsInput[_RemotePathT] | None = None,
        snapshot_expiration: SnapshotExpirationInput = None,
        snapshot_retention: SnapshotRetention | None = None,
        region: str | None = None,
        failover_regions: FailoverRegionsInput = None,
        destroy: bool = True,
        **private_parameters: _JSONValue,
    ) -> ForkSandboxOperation:
        return _fork_sandbox_operation(
            self._service,
            source_sandbox=source_sandbox,
            name=name,
            ports=ports,
            execution_time_limit=execution_time_limit,
            resources=resources,
            image=image,
            persistent=persistent,
            network_policy=network_policy,
            network_id=network_id,
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

    async def get_or_create_sandbox(
        self,
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
        network_id: str | None = None,
        env: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        mounts: DriveMountsInput[_RemotePathT] | None = None,
        snapshot_expiration: SnapshotExpirationInput = None,
        snapshot_retention: SnapshotRetention | None = None,
        region: str | None = None,
        failover_regions: FailoverRegionsInput = None,
        **private_parameters: _JSONValue,
    ) -> tuple[Sandbox, bool]:
        return await _get_or_create_sandbox(
            self._service,
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
            network_id=network_id,
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

    async def get_sandbox(
        self,
        *,
        name: str,
        include_system_routes: bool | None = None,
        **private_parameters: _JSONValue,
    ) -> Sandbox:
        return await _get_sandbox(
            self._service,
            name=name,
            include_system_routes=include_system_routes,
            private_parameters=_normalize_private_parameters("get_sandbox", private_parameters),
        )

    def resume_sandbox(
        self,
        *,
        name: str,
        include_system_routes: bool | None = None,
        **private_parameters: _JSONValue,
    ) -> ResumeSandboxOperation:
        return _resume_sandbox_operation(
            self._service,
            name=name,
            include_system_routes=include_system_routes,
            private_parameters=_normalize_private_parameters("resume_sandbox", private_parameters),
        )

    async def get_or_create_drive(
        self,
        *,
        name: str,
        max_size_bytes: int | None = None,
        region: str | None = None,
    ) -> tuple[Drive, bool]:
        return await _get_or_create_drive(
            self._service,
            name=name,
            max_size_bytes=max_size_bytes,
            region=region,
        )

    async def delete_drive(self, *, name: str) -> Drive:
        return await _delete_drive(self._service, name=name)

    def query_drives(
        self,
        *,
        query: DriveQuery | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
    ) -> AsyncIterator[Drive]:
        return _query_drives(
            self._service,
            query=query,
            page_size=page_size,
            cursor=cursor,
        )

    def query_sandboxes(
        self,
        *,
        query: SandboxQuery | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
    ) -> AsyncIterator[Sandbox]:
        return _query_sandboxes(
            self._service,
            query=query,
            page_size=page_size,
            cursor=cursor,
        )

    def query_sessions(
        self,
        *,
        name: str | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> AsyncIterator[SandboxRuntimeSession]:
        return _query_sessions(
            self._service,
            name=name,
            page_size=page_size,
            cursor=cursor,
            sort_order=sort_order,
        )

    def query_snapshots(
        self,
        *,
        name: str | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> AsyncIterator[Snapshot]:
        return _query_snapshots(
            self._service,
            name=name,
            page_size=page_size,
            cursor=cursor,
            sort_order=sort_order,
        )

    async def get_snapshot(self, *, snapshot_id: str) -> Snapshot:
        return await _get_snapshot(self._service, snapshot_id=snapshot_id)


__all__ = ["SandboxClient"]
