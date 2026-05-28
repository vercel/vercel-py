"""Sandbox service layer."""

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import anyio
import httpx

from vercel._internal.unstable.sandbox.api_client import (
    SandboxApiClient,
    _CommandPayload,
    _RuntimeSessionPayload,
    _SandboxPayload,
    _SnapshotPayload,
)
from vercel._internal.unstable.sandbox.errors import (
    SandboxResponseError,
    SandboxTerminalStateError,
)
from vercel._internal.unstable.sandbox.handles import (
    Sandbox,
    SandboxCommand,
    SandboxRuntimeSession,
    Snapshot,
    SyncSandbox,
    SyncSandboxCommand,
    SyncSandboxRuntimeSession,
    SyncSnapshot,
)
from vercel._internal.unstable.sandbox.log_stream import _parse_command_log_record
from vercel._internal.unstable.sandbox.models import (
    DurationInput,
    JSONValue,
    SandboxCommandLog,
    SandboxQuery,
    SandboxQueryByCreatedAt,
    SandboxQueryByCurrentSnapshotId,
    SandboxQueryByName,
    SandboxQueryByStatusUpdatedAt,
    SandboxResources,
    SandboxSource,
    SandboxStatus,
    SnapshotRetention,
    TagFilter,
    WriteFile,
)
from vercel._internal.unstable.sandbox.options import SandboxServiceOptions
from vercel._internal.unstable.sandbox.pagination import (
    QuerySandboxesPage,
    QuerySandboxesParams,
    QuerySessionsPage,
    QuerySessionsParams,
    QuerySnapshotsPage,
    QuerySnapshotsParams,
)

if TYPE_CHECKING:
    from vercel._internal.unstable.session import SdkSession, SyncSdkSession

_READY_SANDBOX_STATUSES = frozenset({SandboxStatus.RUNNING})
_TERMINAL_SANDBOX_STATUSES = frozenset(
    {SandboxStatus.STOPPED, SandboxStatus.FAILED, SandboxStatus.ABORTED}
)
_TRANSITIONAL_SANDBOX_STATUSES = frozenset(
    {SandboxStatus.PENDING, SandboxStatus.STOPPING, SandboxStatus.SNAPSHOTTING}
)
_READY_POLL_INTERVAL_SECONDS = 0.5
AsyncSleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class _SandboxQueryCriteria:
    sort_by: str | None = None
    sort_order: str | None = None
    name_prefix: str | None = None
    tag: TagFilter | None = None


def _compile_sandbox_query(query: SandboxQuery | None) -> _SandboxQueryCriteria:
    if query is None:
        return _SandboxQueryCriteria()
    if isinstance(query, SandboxQueryByCreatedAt):
        return _SandboxQueryCriteria(
            sort_by="createdAt",
            sort_order=query.sort_order,
            tag=query.tag,
        )
    if isinstance(query, SandboxQueryByName):
        return _SandboxQueryCriteria(
            sort_by="name",
            sort_order=query.sort_order,
            name_prefix=query.name_prefix,
            tag=query.tag,
        )
    if isinstance(query, SandboxQueryByStatusUpdatedAt):
        return _SandboxQueryCriteria(sort_by="statusUpdatedAt", sort_order=query.sort_order)
    if isinstance(query, SandboxQueryByCurrentSnapshotId):
        return _SandboxQueryCriteria(sort_by="currentSnapshotId", sort_order=query.sort_order)
    raise TypeError(f"Unsupported sandbox query type: {type(query)!r}")


def _sandbox_status(sandbox: Sandbox) -> SandboxStatus | None:
    if sandbox.current_session is not None and sandbox.current_session.status is not None:
        return sandbox.current_session.status
    return sandbox.status


class SandboxService:
    """Endpoint-oriented Sandbox v2 operations for bound SDK sessions.

    Public modules and handles delegate here for endpoint composition, response
    binding, polling, and sync-handle conversion. Session option lookup happens
    before construction; this object always receives an explicit options object.
    """

    def __init__(
        self,
        *,
        api_client: SandboxApiClient,
        options: SandboxServiceOptions,
        sdk_session: "SdkSession | SyncSdkSession",
        sleep: AsyncSleep | None = None,
        sync_handles: bool = False,
    ) -> None:
        self._api_client = api_client
        self._options = options
        self._sdk_session = sdk_session
        self._sleep = sleep or anyio.sleep
        self._sync_handles = sync_handles

    @property
    def api_client(self) -> SandboxApiClient:
        return self._api_client

    @property
    def options(self) -> SandboxServiceOptions:
        return self._options

    def _bind_sandbox(self, payload: _SandboxPayload) -> Sandbox:
        cls = SyncSandbox if self._sync_handles else Sandbox
        return cast(Sandbox, cls(payload=payload, sdk_session=self._sdk_session))

    def _bind_runtime_session(self, payload: _RuntimeSessionPayload) -> SandboxRuntimeSession:
        cls = SyncSandboxRuntimeSession if self._sync_handles else SandboxRuntimeSession
        return cast(SandboxRuntimeSession, cls(payload=payload, sdk_session=self._sdk_session))

    def _bind_command(self, payload: _CommandPayload) -> SandboxCommand:
        cls = SyncSandboxCommand if self._sync_handles else SandboxCommand
        return cast(SandboxCommand, cls(payload=payload, sdk_session=self._sdk_session))

    def _bind_snapshot(self, payload: _SnapshotPayload) -> Snapshot:
        cls = SyncSnapshot if self._sync_handles else Snapshot
        return cast(Snapshot, cls(payload=payload, sdk_session=self._sdk_session))

    def _check_open(self) -> None:
        self._sdk_session.check_open()

    async def _wait_for_ready_sandbox(
        self,
        sandbox: Sandbox,
        *,
        project_id: str | None = None,
    ) -> Sandbox:
        while True:
            self._check_open()
            status = _sandbox_status(sandbox)

            if status in _READY_SANDBOX_STATUSES:
                return sandbox

            if status in _TERMINAL_SANDBOX_STATUSES:
                raise SandboxTerminalStateError(
                    f"Sandbox {sandbox.name!r} reached terminal state {status!r}",
                    status=status,
                    sandbox=sandbox,
                )

            if status not in _TRANSITIONAL_SANDBOX_STATUSES:
                raise SandboxResponseError(
                    "Sandbox API response did not include a recognized creation status",
                    data=sandbox.raw,
                )

            await self._sleep(_READY_POLL_INTERVAL_SECONDS)
            sandbox = await self.get_sandbox(
                name=sandbox.name,
                project_id=project_id or sandbox.project_id,
                resume=False,
            )

    async def create_sandbox(
        self,
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
    ) -> Sandbox:
        self._check_open()
        response = await self._api_client.create_sandbox(
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
        sandbox = self._bind_sandbox(response.to_sandbox())
        return await self._wait_for_ready_sandbox(sandbox, project_id=project_id)

    async def get_sandbox(
        self,
        *,
        name: str,
        project_id: str | None = None,
        resume: bool = True,
        include_system_routes: bool | None = None,
    ) -> Sandbox:
        self._check_open()
        response = await self._api_client.get_sandbox(
            name=name,
            project_id=project_id,
            resume=resume,
            include_system_routes=include_system_routes,
        )
        return self._bind_sandbox(response.to_sandbox())

    async def query_sandboxes_page(
        self,
        *,
        query: SandboxQuery | None = None,
        project_id: str | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
    ) -> QuerySandboxesPage:
        self._check_open()
        criteria = _compile_sandbox_query(query)
        result = await self._api_client.query_sandboxes(
            project_id=project_id,
            limit=page_size,
            cursor=cursor,
            sort_by=criteria.sort_by,
            sort_order=criteria.sort_order,
            name_prefix=criteria.name_prefix,
            tag=criteria.tag,
        )
        response = result.response
        return QuerySandboxesPage(
            sandboxes=[
                self._bind_sandbox(
                    sandbox
                    if sandbox.project_id is not None
                    else sandbox.model_copy(update={"project_id": result.project_id})
                )
                for sandbox in response.sandboxes
            ],
            next_cursor=response.pagination.next if response.pagination is not None else None,
        )

    def query_sandboxes(
        self,
        *,
        query: SandboxQuery | None = None,
        project_id: str | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
    ) -> AsyncIterator[Sandbox]:
        async def iter_sandboxes() -> AsyncIterator[Sandbox]:
            current_params = QuerySandboxesParams(
                page_size=page_size,
                cursor=cursor,
            )
            while True:
                page = await self.query_sandboxes_page(
                    query=query,
                    project_id=project_id,
                    page_size=current_params.page_size,
                    cursor=current_params.cursor,
                )
                for sandbox in page.sandboxes:
                    yield sandbox
                if page.next_cursor is None:
                    return
                if not page.sandboxes:
                    return
                current_params = current_params.with_cursor(page.next_cursor)

        return iter_sandboxes()

    async def destroy_sandbox(self, *, name: str, project_id: str | None = None) -> Sandbox:
        return self._bind_sandbox(
            await self.destroy_sandbox_payload(name=name, project_id=project_id)
        )

    async def destroy_sandbox_payload(
        self, *, name: str, project_id: str | None = None
    ) -> _SandboxPayload:
        self._check_open()
        response = await self._api_client.destroy_sandbox(name=name, project_id=project_id)
        return response.to_sandbox()

    async def update_sandbox(
        self,
        *,
        name: str,
        project_id: str | None = None,
        runtime: str | None = None,
        ports: list[int] | None = None,
        execution_time_limit: DurationInput = None,
        resources: SandboxResources | None = None,
        persistent: bool | None = None,
        network_policy: JSONValue | None = None,
        env: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        snapshot_expiration: DurationInput = None,
        snapshot_retention: SnapshotRetention | None = None,
        current_snapshot_id: str | None = None,
    ) -> Sandbox:
        return self._bind_sandbox(
            await self.update_sandbox_payload(
                name=name,
                project_id=project_id,
                runtime=runtime,
                ports=ports,
                execution_time_limit=execution_time_limit,
                resources=resources,
                persistent=persistent,
                network_policy=network_policy,
                env=env,
                tags=tags,
                snapshot_expiration=snapshot_expiration,
                snapshot_retention=snapshot_retention,
                current_snapshot_id=current_snapshot_id,
            )
        )

    async def update_sandbox_payload(
        self,
        *,
        name: str,
        project_id: str | None = None,
        runtime: str | None = None,
        ports: list[int] | None = None,
        execution_time_limit: DurationInput = None,
        resources: SandboxResources | None = None,
        persistent: bool | None = None,
        network_policy: JSONValue | None = None,
        env: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        snapshot_expiration: DurationInput = None,
        snapshot_retention: SnapshotRetention | None = None,
        current_snapshot_id: str | None = None,
    ) -> _SandboxPayload:
        self._check_open()
        response = await self._api_client.update_sandbox(
            name=name,
            project_id=project_id,
            runtime=runtime,
            ports=ports,
            execution_time_limit=execution_time_limit,
            resources=resources,
            persistent=persistent,
            network_policy=network_policy,
            env=env,
            tags=tags,
            snapshot_expiration=snapshot_expiration,
            snapshot_retention=snapshot_retention,
            current_snapshot_id=current_snapshot_id,
        )
        return response.to_sandbox()

    async def create_runtime_session(
        self,
        *,
        name: str,
        project_id: str | None = None,
        resume: bool = True,
        include_system_routes: bool | None = None,
    ) -> SandboxRuntimeSession:
        self._check_open()
        response = await self._api_client.create_runtime_session(
            name=name,
            project_id=project_id,
            resume=resume,
            include_system_routes=include_system_routes,
        )
        payload = response.to_sandbox()
        if payload.current_session is None:
            raise SandboxResponseError(
                "Sandbox API response is missing object field 'session'",
                data=response.model_dump(by_alias=True),
            )
        return self._bind_runtime_session(payload.current_session)

    async def destroy_runtime_session(self, *, session_id: str) -> Sandbox:
        return await self.stop_runtime_session_sandbox(session_id=session_id)

    async def stop_runtime_session_sandbox(self, *, session_id: str) -> Sandbox:
        self._check_open()
        response = await self._api_client.stop_runtime_session(session_id=session_id)
        return self._bind_sandbox(response.to_sandbox())

    async def stop_runtime_session(self, *, session_id: str) -> SandboxRuntimeSession:
        return self._bind_runtime_session(
            await self.stop_runtime_session_payload(session_id=session_id)
        )

    async def stop_runtime_session_payload(self, *, session_id: str) -> _RuntimeSessionPayload:
        self._check_open()
        response = await self._api_client.stop_runtime_session(session_id=session_id)
        payload = response.to_sandbox()
        if payload.current_session is None:
            raise SandboxResponseError(
                "Sandbox API response is missing object field 'session'",
                data=response.model_dump(by_alias=True),
            )
        if payload.current_session.id != session_id:
            raise SandboxResponseError(
                "Sandbox current-session operation returned a different session identity",
                data=response.model_dump(by_alias=True),
            )
        return payload.current_session

    async def get_runtime_session(
        self,
        *,
        session_id: str,
        include_system_routes: bool | None = None,
    ) -> SandboxRuntimeSession:
        return self._bind_runtime_session(
            await self.get_runtime_session_payload(
                session_id=session_id, include_system_routes=include_system_routes
            )
        )

    async def get_runtime_session_payload(
        self,
        *,
        session_id: str,
        include_system_routes: bool | None = None,
    ) -> _RuntimeSessionPayload:
        self._check_open()
        response = await self._api_client.get_runtime_session(
            session_id=session_id,
            include_system_routes=include_system_routes,
        )
        return response.to_runtime_session()

    async def query_sessions_page(
        self,
        *,
        project_id: str | None = None,
        name: str | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> QuerySessionsPage:
        self._check_open()
        response = await self._api_client.query_runtime_sessions(
            project_id=project_id,
            name=name,
            limit=page_size,
            cursor=cursor,
            sort_order=sort_order,
        )
        return QuerySessionsPage(
            sessions=[self._bind_runtime_session(session) for session in response.sessions],
            next_cursor=response.pagination.next if response.pagination is not None else None,
        )

    def query_sessions(
        self,
        *,
        project_id: str | None = None,
        name: str | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> AsyncIterator[SandboxRuntimeSession]:
        async def iter_sessions() -> AsyncIterator[SandboxRuntimeSession]:
            current_params = QuerySessionsParams(
                page_size=page_size,
                cursor=cursor,
            )
            while True:
                page = await self.query_sessions_page(
                    project_id=project_id,
                    name=name,
                    page_size=current_params.page_size,
                    cursor=current_params.cursor,
                    sort_order=sort_order,
                )
                for session in page.sessions:
                    yield session
                if page.next_cursor is None:
                    return
                if not page.sessions:
                    return
                current_params = current_params.with_cursor(page.next_cursor)

        return iter_sessions()

    async def extend_runtime_session_timeout(
        self,
        *,
        session_id: str,
        duration: DurationInput,
    ) -> SandboxRuntimeSession:
        return self._bind_runtime_session(
            await self.extend_runtime_session_timeout_payload(
                session_id=session_id, duration=duration
            )
        )

    async def extend_runtime_session_timeout_payload(
        self, *, session_id: str, duration: DurationInput
    ) -> _RuntimeSessionPayload:
        self._check_open()
        response = await self._api_client.extend_runtime_session_timeout(
            session_id=session_id,
            duration=duration,
        )
        return response.to_runtime_session()

    async def update_runtime_session_network_policy(
        self,
        *,
        session_id: str,
        network_policy: JSONValue,
    ) -> SandboxRuntimeSession:
        return self._bind_runtime_session(
            await self.update_runtime_session_network_policy_payload(
                session_id=session_id, network_policy=network_policy
            )
        )

    async def update_runtime_session_network_policy_payload(
        self, *, session_id: str, network_policy: JSONValue
    ) -> _RuntimeSessionPayload:
        self._check_open()
        response = await self._api_client.update_runtime_session_network_policy(
            session_id=session_id,
            network_policy=network_policy,
        )
        return response.to_runtime_session()

    async def create_snapshot(
        self,
        *,
        session_id: str,
        expiration: DurationInput = None,
    ) -> tuple[Snapshot, SandboxRuntimeSession]:
        snapshot, payload = await self.create_snapshot_for_session(
            session_id=session_id, expiration=expiration
        )
        return snapshot, self._bind_runtime_session(payload)

    async def create_snapshot_for_session(
        self,
        *,
        session_id: str,
        expiration: DurationInput = None,
    ) -> tuple[Snapshot, _RuntimeSessionPayload]:
        self._check_open()
        response = await self._api_client.create_snapshot(
            session_id=session_id,
            expiration=expiration,
        )
        snapshot, session = response.to_snapshot_and_session()
        return self._bind_snapshot(snapshot), session

    async def query_snapshots_page(
        self,
        *,
        project_id: str | None = None,
        name: str | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> QuerySnapshotsPage:
        self._check_open()
        response = await self._api_client.query_snapshots(
            project_id=project_id,
            name=name,
            limit=page_size,
            cursor=cursor,
            sort_order=sort_order,
        )
        return QuerySnapshotsPage(
            snapshots=[self._bind_snapshot(snapshot) for snapshot in response.snapshots],
            next_cursor=response.pagination.next if response.pagination is not None else None,
        )

    def query_snapshots(
        self,
        *,
        project_id: str | None = None,
        name: str | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> AsyncIterator[Snapshot]:
        async def iter_snapshots() -> AsyncIterator[Snapshot]:
            current_params = QuerySnapshotsParams(
                page_size=page_size,
                cursor=cursor,
            )
            while True:
                page = await self.query_snapshots_page(
                    project_id=project_id,
                    name=name,
                    page_size=current_params.page_size,
                    cursor=current_params.cursor,
                    sort_order=sort_order,
                )
                for snapshot in page.snapshots:
                    yield snapshot
                if page.next_cursor is None:
                    return
                if not page.snapshots:
                    return
                current_params = current_params.with_cursor(page.next_cursor)

        return iter_snapshots()

    async def get_snapshot(self, *, snapshot_id: str) -> Snapshot:
        self._check_open()
        response = await self._api_client.get_snapshot(snapshot_id=snapshot_id)
        return self._bind_snapshot(response.to_snapshot())

    async def delete_snapshot(self, *, snapshot_id: str) -> Snapshot:
        return self._bind_snapshot(await self.delete_snapshot_payload(snapshot_id=snapshot_id))

    async def delete_snapshot_payload(self, *, snapshot_id: str) -> _SnapshotPayload:
        self._check_open()
        response = await self._api_client.delete_snapshot(snapshot_id=snapshot_id)
        return response.to_snapshot()

    async def _run_command(
        self,
        *,
        session_id: str,
        command: str,
        args: list[str] | None = None,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        wait: bool,
    ) -> SandboxCommand:
        self._check_open()
        started_response = await self._api_client.run_command(
            session_id=session_id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
        )
        started = started_response.to_command()
        if not wait:
            return self._bind_command(started)
        finished_response = await self._api_client.get_command(
            session_id=session_id,
            command_id=started.id,
            wait=True,
        )
        return self._bind_command(finished_response.to_command())

    async def run_command(
        self,
        *,
        session_id: str,
        command: str,
        args: list[str] | None = None,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
    ) -> SandboxCommand:
        return await self._run_command(
            session_id=session_id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
            wait=True,
        )

    async def start_command(
        self,
        *,
        session_id: str,
        command: str,
        args: list[str] | None = None,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
    ) -> SandboxCommand:
        return await self._run_command(
            session_id=session_id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
            wait=False,
        )

    async def get_command(
        self,
        *,
        session_id: str,
        command_id: str,
        wait: bool = False,
    ) -> SandboxCommand:
        return self._bind_command(
            await self.get_command_payload(session_id=session_id, command_id=command_id, wait=wait)
        )

    async def get_command_payload(
        self, *, session_id: str, command_id: str, wait: bool = False
    ) -> _CommandPayload:
        self._check_open()
        response = await self._api_client.get_command(
            session_id=session_id,
            command_id=command_id,
            wait=wait,
        )
        return response.to_command()

    async def query_commands(self, *, session_id: str) -> list[SandboxCommand]:
        self._check_open()
        response = await self._api_client.query_commands(session_id=session_id)
        return [self._bind_command(command) for command in response.commands]

    async def mkdir(
        self,
        *,
        session_id: str,
        path: str,
        cwd: str | None = None,
        recursive: bool = True,
    ) -> None:
        self._check_open()
        await self._api_client.mkdir(
            session_id=session_id,
            path=path,
            cwd=cwd,
            recursive=recursive,
        )

    async def read_file(
        self,
        *,
        session_id: str,
        path: str,
        cwd: str | None = None,
    ) -> bytes:
        self._check_open()
        return await self._api_client.read_file(session_id=session_id, path=path, cwd=cwd)

    async def write_files(
        self,
        *,
        session_id: str,
        files: Sequence[WriteFile],
        cwd: str,
        encoding: str = "utf-8",
    ) -> None:
        self._check_open()
        await self._api_client.write_files(
            session_id=session_id,
            files=files,
            cwd=cwd,
            encoding=encoding,
        )

    async def kill_command(
        self,
        *,
        session_id: str,
        command_id: str,
        signal: int,
    ) -> SandboxCommand:
        return self._bind_command(
            await self.kill_command_payload(
                session_id=session_id, command_id=command_id, signal=signal
            )
        )

    async def kill_command_payload(
        self, *, session_id: str, command_id: str, signal: int
    ) -> _CommandPayload:
        self._check_open()
        response = await self._api_client.kill_command(
            session_id=session_id,
            command_id=command_id,
            signal=signal,
        )
        return response.to_command()

    async def command_logs_response(
        self,
        *,
        session_id: str,
        command_id: str,
    ) -> httpx.Response:
        self._check_open()
        return await self._api_client.command_logs_response(
            session_id=session_id,
            command_id=command_id,
        )

    def command_logs(
        self,
        *,
        session_id: str,
        command_id: str,
    ) -> AsyncIterator[SandboxCommandLog]:
        async def iter_logs() -> AsyncIterator[SandboxCommandLog]:
            response = await self.command_logs_response(
                session_id=session_id,
                command_id=command_id,
            )
            try:
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    event = _parse_command_log_record(line)
                    if event is not None:
                        yield event
            finally:
                await response.aclose()

        return iter_logs()
