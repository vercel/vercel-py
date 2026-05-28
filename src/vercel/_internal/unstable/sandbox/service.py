"""Neutral orchestration for unstable Sandbox operations."""

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass

import anyio
import httpx

from vercel._internal.unstable.sandbox.api_client import SandboxApiClient
from vercel._internal.unstable.sandbox.errors import SandboxResponseError
from vercel._internal.unstable.sandbox.models import (
    DurationInput,
    JSONValue,
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
from vercel._internal.unstable.sandbox.state import (
    RuntimeSessionsPageState,
    SandboxCommandState,
    SandboxesPageState,
    SandboxRuntimeSessionState,
    SandboxState,
    SnapshotSessionState,
    SnapshotsPageState,
    SnapshotState,
)

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


class _SandboxTerminalState(Exception):
    def __init__(self, *, status: SandboxStatus, sandbox: SandboxState) -> None:
        self.status = status
        self.sandbox = sandbox


def _compile_sandbox_query(query: SandboxQuery | None) -> _SandboxQueryCriteria:
    if query is None:
        return _SandboxQueryCriteria()
    if isinstance(query, SandboxQueryByCreatedAt):
        return _SandboxQueryCriteria(
            sort_by="createdAt", sort_order=query.sort_order, tag=query.tag
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


def _sandbox_status(sandbox: SandboxState) -> SandboxStatus | None:
    if sandbox.current_session is not None and sandbox.current_session.status is not None:
        return sandbox.current_session.status
    return sandbox.status


class SandboxService:
    """Async-only Sandbox domain orchestration returning neutral state."""

    def __init__(
        self,
        *,
        api_client: SandboxApiClient,
        options: SandboxServiceOptions,
        ensure_open: Callable[[], None],
        sleep: AsyncSleep | None = None,
    ) -> None:
        self._api_client = api_client
        self._options = options
        self._ensure_open = ensure_open
        self._sleep = sleep or anyio.sleep

    @property
    def api_client(self) -> SandboxApiClient:
        return self._api_client

    @property
    def options(self) -> SandboxServiceOptions:
        return self._options

    async def _wait_for_ready_sandbox(
        self, sandbox: SandboxState, *, project_id: str | None = None
    ) -> SandboxState:
        while True:
            self._ensure_open()
            status = _sandbox_status(sandbox)
            if status in _READY_SANDBOX_STATUSES:
                return sandbox
            if status in _TERMINAL_SANDBOX_STATUSES:
                raise _SandboxTerminalState(status=status, sandbox=sandbox)
            if status not in _TRANSITIONAL_SANDBOX_STATUSES:
                raise SandboxResponseError(
                    "Sandbox API response did not include a recognized creation status",
                    data=sandbox.raw,
                )
            await self._sleep(_READY_POLL_INTERVAL_SECONDS)
            self._ensure_open()
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
    ) -> SandboxState:
        self._ensure_open()
        sandbox = await self._api_client.create_sandbox(
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
        return await self._wait_for_ready_sandbox(sandbox, project_id=project_id)

    async def get_sandbox(
        self,
        *,
        name: str,
        project_id: str | None = None,
        resume: bool = True,
        include_system_routes: bool | None = None,
    ) -> SandboxState:
        self._ensure_open()
        return await self._api_client.get_sandbox(
            name=name,
            project_id=project_id,
            resume=resume,
            include_system_routes=include_system_routes,
        )

    async def query_sandboxes_page(
        self,
        *,
        query: SandboxQuery | None = None,
        project_id: str | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
    ) -> SandboxesPageState:
        self._ensure_open()
        criteria = _compile_sandbox_query(query)
        return await self._api_client.query_sandboxes(
            project_id=project_id,
            limit=page_size,
            cursor=cursor,
            sort_by=criteria.sort_by,
            sort_order=criteria.sort_order,
            name_prefix=criteria.name_prefix,
            tag=criteria.tag,
        )

    async def destroy_sandbox(self, *, name: str, project_id: str | None = None) -> SandboxState:
        self._ensure_open()
        return await self._api_client.destroy_sandbox(name=name, project_id=project_id)

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
    ) -> SandboxState:
        self._ensure_open()
        return await self._api_client.update_sandbox(
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

    async def create_runtime_session(
        self,
        *,
        name: str,
        project_id: str | None = None,
        resume: bool = True,
        include_system_routes: bool | None = None,
    ) -> SandboxRuntimeSessionState:
        self._ensure_open()
        sandbox = await self._api_client.create_runtime_session(
            name=name,
            project_id=project_id,
            resume=resume,
            include_system_routes=include_system_routes,
        )
        if sandbox.current_session is None:
            raise SandboxResponseError(
                "Sandbox API response is missing object field 'session'", data=sandbox.raw
            )
        return sandbox.current_session

    async def stop_runtime_session(self, *, session_id: str) -> SandboxRuntimeSessionState:
        self._ensure_open()
        sandbox = await self._api_client.stop_runtime_session(session_id=session_id)
        session = sandbox.current_session
        if session is None:
            raise SandboxResponseError(
                "Sandbox API response is missing object field 'session'", data=sandbox.raw
            )
        if session.id != session_id:
            raise SandboxResponseError(
                "Sandbox current-session operation returned a different session identity",
                data=sandbox.raw,
            )
        return session

    async def get_runtime_session(
        self, *, session_id: str, include_system_routes: bool | None = None
    ) -> SandboxRuntimeSessionState:
        self._ensure_open()
        return await self._api_client.get_runtime_session(
            session_id=session_id, include_system_routes=include_system_routes
        )

    async def query_sessions_page(
        self,
        *,
        project_id: str | None = None,
        name: str | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> RuntimeSessionsPageState:
        self._ensure_open()
        return await self._api_client.query_runtime_sessions(
            project_id=project_id,
            name=name,
            limit=page_size,
            cursor=cursor,
            sort_order=sort_order,
        )

    async def extend_runtime_session_timeout(
        self, *, session_id: str, duration: DurationInput
    ) -> SandboxRuntimeSessionState:
        self._ensure_open()
        return await self._api_client.extend_runtime_session_timeout(
            session_id=session_id, duration=duration
        )

    async def update_runtime_session_network_policy(
        self, *, session_id: str, network_policy: JSONValue
    ) -> SandboxRuntimeSessionState:
        self._ensure_open()
        return await self._api_client.update_runtime_session_network_policy(
            session_id=session_id, network_policy=network_policy
        )

    async def create_snapshot(
        self, *, session_id: str, expiration: DurationInput = None
    ) -> SnapshotSessionState:
        self._ensure_open()
        return await self._api_client.create_snapshot(session_id=session_id, expiration=expiration)

    async def query_snapshots_page(
        self,
        *,
        project_id: str | None = None,
        name: str | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> SnapshotsPageState:
        self._ensure_open()
        return await self._api_client.query_snapshots(
            project_id=project_id,
            name=name,
            limit=page_size,
            cursor=cursor,
            sort_order=sort_order,
        )

    async def get_snapshot(self, *, snapshot_id: str) -> SnapshotState:
        self._ensure_open()
        return await self._api_client.get_snapshot(snapshot_id=snapshot_id)

    async def delete_snapshot(self, *, snapshot_id: str) -> SnapshotState:
        self._ensure_open()
        return await self._api_client.delete_snapshot(snapshot_id=snapshot_id)

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
    ) -> SandboxCommandState:
        self._ensure_open()
        started = await self._api_client.run_command(
            session_id=session_id, command=command, args=args, cwd=cwd, env=env, sudo=sudo
        )
        if not wait:
            return started
        self._ensure_open()
        return await self._api_client.get_command(
            session_id=session_id, command_id=started.id, wait=True
        )

    async def run_command(
        self,
        *,
        session_id: str,
        command: str,
        args: list[str] | None = None,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
    ) -> SandboxCommandState:
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
    ) -> SandboxCommandState:
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
        self, *, session_id: str, command_id: str, wait: bool = False
    ) -> SandboxCommandState:
        self._ensure_open()
        return await self._api_client.get_command(
            session_id=session_id, command_id=command_id, wait=wait
        )

    async def query_commands(self, *, session_id: str) -> list[SandboxCommandState]:
        self._ensure_open()
        return await self._api_client.query_commands(session_id=session_id)

    async def mkdir(
        self, *, session_id: str, path: str, cwd: str | None = None, recursive: bool = True
    ) -> None:
        self._ensure_open()
        await self._api_client.mkdir(session_id=session_id, path=path, cwd=cwd, recursive=recursive)

    async def read_file(self, *, session_id: str, path: str, cwd: str | None = None) -> bytes:
        self._ensure_open()
        return await self._api_client.read_file(session_id=session_id, path=path, cwd=cwd)

    async def write_files(
        self,
        *,
        session_id: str,
        files: Sequence[WriteFile],
        cwd: str,
        encoding: str = "utf-8",
    ) -> None:
        self._ensure_open()
        await self._api_client.write_files(
            session_id=session_id, files=files, cwd=cwd, encoding=encoding
        )

    async def kill_command(
        self, *, session_id: str, command_id: str, signal: int
    ) -> SandboxCommandState:
        self._ensure_open()
        return await self._api_client.kill_command(
            session_id=session_id, command_id=command_id, signal=signal
        )

    async def command_logs_response(self, *, session_id: str, command_id: str) -> httpx.Response:
        self._ensure_open()
        return await self._api_client.command_logs_response(
            session_id=session_id, command_id=command_id
        )
