"""Neutral orchestration for unstable Sandbox operations."""

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Literal, cast

import httpx

from vercel._internal.unstable.sandbox.api_client import SandboxApiClient
from vercel._internal.unstable.sandbox.errors import (
    SandboxApiError,
    SandboxFilesystemCommandError,
    SandboxFilesystemWriteError,
    SandboxPathNotFoundError,
    SandboxResponseError,
)
from vercel._internal.unstable.sandbox.models import (
    _OMITTED,
    DirectoryEntry,
    NetworkPolicy,
    SandboxQuery,
    SandboxQueryByCreatedAt,
    SandboxQueryByCurrentSnapshotId,
    SandboxQueryByName,
    SandboxQueryByStatusUpdatedAt,
    SandboxResources,
    SandboxSource,
    SandboxStatus,
    SnapshotExpiration,
    SnapshotRetention,
    SnapshotRetentionUpdate,
    TagFilter,
    _WriteFile,
)
from vercel._internal.unstable.sandbox.options import SandboxServiceOptions
from vercel._internal.unstable.sandbox.process_output import ProcessOutputRouter
from vercel._internal.unstable.sandbox.state import (
    CompletedProcessState,
    ProcessState,
    RuntimeSessionsPageState,
    SandboxesPageState,
    SandboxRuntimeSessionState,
    SandboxState,
    SnapshotSessionState,
    SnapshotsPageState,
    SnapshotState,
)

if TYPE_CHECKING:
    from vercel._internal.unstable.session import _BaseSdkSession

_READY_SANDBOX_STATUSES = frozenset({SandboxStatus.RUNNING})
_TERMINAL_SANDBOX_STATUSES = frozenset(
    {SandboxStatus.STOPPED, SandboxStatus.FAILED, SandboxStatus.ABORTED}
)
_TRANSITIONAL_SANDBOX_STATUSES = frozenset(
    {SandboxStatus.PENDING, SandboxStatus.STOPPING, SandboxStatus.SNAPSHOTTING}
)
_READY_POLL_INTERVAL_SECONDS = 0.5
AsyncSleep = Callable[[float], Awaitable[None]]
ProcessOutputCollector = Callable[[ProcessState], Awaitable[tuple[str, str]]]
_MISSING_PATH_ERROR_CODES = frozenset({"not_found", "path_not_found", "file_not_found", "ENOENT"})
_PREDICATE_SCRIPT = """\
case "$1" in
  /*) path=$1 ;;
  *) path=./$1 ;;
esac
test "$2" "$path"
"""
_LISTDIR_SCRIPT = """\
case "$1" in
  /*) path=$1 ;;
  *) path=./$1 ;;
esac
test -d "$path" || exit 1
for entry in "$path"/* "$path"/.[!.]* "$path"/..?*; do
  if test ! -e "$entry" && test ! -L "$entry"; then
    continue
  fi
  if test -L "$entry"; then
    kind=symlink
  elif test -d "$entry"; then
    kind=directory
  elif test -f "$entry"; then
    kind=file
  else
    kind=other
  fi
  name=${entry#"$path"/}
  printf '%s\\0%s\\0' "$name" "$kind"
done
"""
_REMOVE_SCRIPT = """\
case "$1" in
  /*) path=$1 ;;
  *) path=./$1 ;;
esac
if test ! -e "$path" && test ! -L "$path"; then
  test "$3" = true && exit 0
  exit 1
fi
if test "$2" = true; then
  rm -rf "$path"
else
  rm -f "$path"
fi
"""
_RENAME_SCRIPT = """\
case "$1" in
  /*) source=$1 ;;
  *) source=./$1 ;;
esac
case "$2" in
  /*) destination=$2 ;;
  *) destination=./$2 ;;
esac
mv "$source" "$destination"
"""


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


def _listdir_entries(output: str) -> list[DirectoryEntry]:
    fields = output.split("\0")
    if fields[-1:] != [""] or (len(fields) - 1) % 2 != 0:
        raise SandboxResponseError("Sandbox filesystem listdir output was malformed", data=output)
    entries: list[DirectoryEntry] = []
    for index in range(0, len(fields) - 1, 2):
        kind = fields[index + 1]
        if kind not in {"file", "directory", "symlink", "other"}:
            raise SandboxResponseError(
                "Sandbox filesystem listdir returned an invalid entry kind", data=output
            )
        entries.append(
            DirectoryEntry(
                path=fields[index],
                kind=cast(Literal["file", "directory", "symlink", "other"], kind),
            )
        )
    return sorted(entries, key=lambda entry: entry.path)


class SandboxService:
    """Async-only Sandbox domain orchestration returning neutral state."""

    def __init__(
        self,
        *,
        api_client: SandboxApiClient,
        options: SandboxServiceOptions,
        ensure_open: Callable[[], None],
        sleep: AsyncSleep,
    ) -> None:
        self._api_client = api_client
        self._options = options
        self._ensure_open = ensure_open
        self._sleep = sleep

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
        execution_time_limit: timedelta | None = None,
        resources: SandboxResources | None = None,
        persistent: bool | None = None,
        network_policy: NetworkPolicy | None = None,
        env: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        snapshot_expiration: SnapshotExpiration | None = None,
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
        execution_time_limit: timedelta | None = None,
        resources: SandboxResources | None = None,
        persistent: bool | None = None,
        network_policy: NetworkPolicy | None = None,
        env: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        snapshot_expiration: SnapshotExpiration | None = None,
        snapshot_retention: SnapshotRetentionUpdate = _OMITTED,
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
        session = await self._api_client.stop_runtime_session(session_id=session_id)
        if session.id != session_id:
            raise SandboxResponseError(
                "Sandbox current-session operation returned a different session identity",
                data=session,
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
        self, *, session_id: str, duration: timedelta
    ) -> SandboxRuntimeSessionState:
        self._ensure_open()
        return await self._api_client.extend_runtime_session_timeout(
            session_id=session_id, duration=duration
        )

    async def update_runtime_session_network_policy(
        self, *, session_id: str, network_policy: NetworkPolicy
    ) -> SandboxRuntimeSessionState:
        self._ensure_open()
        return await self._api_client.update_runtime_session_network_policy(
            session_id=session_id, network_policy=network_policy
        )

    async def create_snapshot(
        self, *, session_id: str, expiration: SnapshotExpiration | None = None
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

    async def _run_process(
        self,
        *,
        session_id: str,
        command: str,
        args: list[str] | None = None,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: timedelta | None = None,
        wait: bool,
    ) -> ProcessState:
        self._ensure_open()
        started = await self._api_client.create_process(
            session_id=session_id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
            kill_after=kill_after,
        )
        if not wait:
            return started
        self._ensure_open()
        return await self._api_client.get_command(
            session_id=session_id, command_id=started.id, wait=True
        )

    async def _wait_process(
        self,
        *,
        session_id: str,
        command: str,
        args: list[str] | None = None,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: timedelta | None = None,
    ) -> ProcessState:
        return await self._run_process(
            session_id=session_id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
            kill_after=kill_after,
            wait=True,
        )

    async def run_process(
        self,
        *,
        session_id: str,
        command: str,
        args: Sequence[str] | None = None,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: timedelta | None = None,
        output_router: ProcessOutputRouter,
    ) -> CompletedProcessState:
        self._ensure_open()
        return await self._api_client.run_process(
            session_id=session_id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
            kill_after=kill_after,
            output_router=output_router,
        )

    async def create_process(
        self,
        *,
        session_id: str,
        command: str,
        args: list[str] | None = None,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: timedelta | None = None,
    ) -> ProcessState:
        return await self._run_process(
            session_id=session_id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
            kill_after=kill_after,
            wait=False,
        )

    async def get_process(
        self, *, session_id: str, process_id: str, wait: bool = False
    ) -> ProcessState:
        self._ensure_open()
        return await self._api_client.get_command(
            session_id=session_id, command_id=process_id, wait=wait
        )

    async def query_processes(self, *, session_id: str) -> list[ProcessState]:
        self._ensure_open()
        return await self._api_client.query_commands(session_id=session_id)

    async def mkdir(
        self, *, session_id: str, path: str, cwd: str | None = None, recursive: bool = True
    ) -> None:
        self._ensure_open()
        try:
            await self._api_client.mkdir(
                session_id=session_id, path=path, cwd=cwd, recursive=recursive
            )
        except SandboxApiError as error:
            if error.code in _MISSING_PATH_ERROR_CODES:
                raise SandboxPathNotFoundError(
                    path, operation="mkdir", cwd=cwd, cause=error
                ) from error
            raise

    async def read_bytes(self, *, session_id: str, path: str, cwd: str | None = None) -> bytes:
        self._ensure_open()
        try:
            return await self._api_client.read_bytes(session_id=session_id, path=path, cwd=cwd)
        except SandboxApiError as error:
            if error.code in _MISSING_PATH_ERROR_CODES:
                raise SandboxPathNotFoundError(
                    path, operation="read_bytes", cwd=cwd, cause=error
                ) from error
            raise

    async def write_files(
        self,
        *,
        session_id: str,
        files: Sequence[_WriteFile],
        cwd: str,
    ) -> None:
        self._ensure_open()
        try:
            await self._api_client.write_files(session_id=session_id, files=files, cwd=cwd)
        except SandboxApiError as error:
            raise SandboxFilesystemWriteError(
                paths=tuple(file.path for file in files), cwd=cwd, cause=error
            ) from error

    async def _filesystem_command(
        self,
        *,
        operation: str,
        session_id: str,
        script: str,
        args: list[str],
        cwd: str | None,
        collect_output: ProcessOutputCollector,
    ) -> tuple[ProcessState, str, str]:
        command = await self._wait_process(
            session_id=session_id,
            command="sh",
            args=["-c", script, f"vercel-fs-{operation}", *args],
            cwd=cwd,
        )
        stdout, stderr = await collect_output(command)
        return command, stdout, stderr

    async def _predicate(
        self,
        *,
        operation: str,
        operator: str,
        session_id: str,
        path: str,
        cwd: str | None,
        collect_output: ProcessOutputCollector,
    ) -> bool:
        command, stdout, stderr = await self._filesystem_command(
            operation=operation,
            session_id=session_id,
            script=_PREDICATE_SCRIPT,
            args=[path, operator],
            cwd=cwd,
            collect_output=collect_output,
        )
        if command.returncode == 0:
            return True
        if command.returncode == 1:
            return False
        raise SandboxFilesystemCommandError(
            operation,
            paths=(path,),
            exit_code=command.returncode,
            stdout=stdout,
            stderr=stderr,
        )

    async def exists(
        self,
        *,
        session_id: str,
        path: str,
        cwd: str | None,
        collect_output: ProcessOutputCollector,
    ) -> bool:
        return await self._predicate(
            operation="exists",
            operator="-e",
            session_id=session_id,
            path=path,
            cwd=cwd,
            collect_output=collect_output,
        )

    async def is_file(
        self,
        *,
        session_id: str,
        path: str,
        cwd: str | None,
        collect_output: ProcessOutputCollector,
    ) -> bool:
        return await self._predicate(
            operation="is_file",
            operator="-f",
            session_id=session_id,
            path=path,
            cwd=cwd,
            collect_output=collect_output,
        )

    async def is_dir(
        self,
        *,
        session_id: str,
        path: str,
        cwd: str | None,
        collect_output: ProcessOutputCollector,
    ) -> bool:
        return await self._predicate(
            operation="is_dir",
            operator="-d",
            session_id=session_id,
            path=path,
            cwd=cwd,
            collect_output=collect_output,
        )

    async def listdir(
        self,
        *,
        session_id: str,
        path: str,
        cwd: str | None,
        collect_output: ProcessOutputCollector,
    ) -> list[DirectoryEntry]:
        command, stdout, stderr = await self._filesystem_command(
            operation="listdir",
            session_id=session_id,
            script=_LISTDIR_SCRIPT,
            args=[path],
            cwd=cwd,
            collect_output=collect_output,
        )
        if command.returncode != 0:
            raise SandboxFilesystemCommandError(
                "listdir",
                paths=(path,),
                exit_code=command.returncode,
                stdout=stdout,
                stderr=stderr,
            )
        return _listdir_entries(stdout)

    async def remove(
        self,
        *,
        session_id: str,
        path: str,
        cwd: str | None,
        recursive: bool,
        missing_ok: bool,
        collect_output: ProcessOutputCollector,
    ) -> None:
        command, stdout, stderr = await self._filesystem_command(
            operation="remove",
            session_id=session_id,
            script=_REMOVE_SCRIPT,
            args=[path, str(recursive).lower(), str(missing_ok).lower()],
            cwd=cwd,
            collect_output=collect_output,
        )
        if command.returncode != 0:
            raise SandboxFilesystemCommandError(
                "remove",
                paths=(path,),
                exit_code=command.returncode,
                stdout=stdout,
                stderr=stderr,
            )

    async def rename(
        self,
        *,
        session_id: str,
        source: str,
        destination: str,
        cwd: str | None,
        collect_output: ProcessOutputCollector,
    ) -> None:
        command, stdout, stderr = await self._filesystem_command(
            operation="rename",
            session_id=session_id,
            script=_RENAME_SCRIPT,
            args=[source, destination],
            cwd=cwd,
            collect_output=collect_output,
        )
        if command.returncode != 0:
            raise SandboxFilesystemCommandError(
                "rename",
                paths=(source, destination),
                exit_code=command.returncode,
                stdout=stdout,
                stderr=stderr,
            )

    async def send_process_signal(
        self, *, session_id: str, process_id: str, signal: int
    ) -> ProcessState:
        self._ensure_open()
        return await self._api_client.kill_command(
            session_id=session_id, command_id=process_id, signal=signal
        )

    async def process_logs_response(self, *, session_id: str, process_id: str) -> httpx.Response:
        self._ensure_open()
        return await self._api_client.command_logs_response(
            session_id=session_id, command_id=process_id
        )


def get_sandbox_service(session: "_BaseSdkSession") -> SandboxService:
    def factory() -> SandboxService:
        options = session.get_service_option(SandboxServiceOptions) or SandboxServiceOptions()
        return SandboxService(
            api_client=SandboxApiClient(
                base_url=options.base_url,
                credentials_factory=options.credentials_factory,
                transport=session.get_transport(),
            ),
            options=options,
            ensure_open=session.check_open,
            sleep=session.sleep,
        )

    return session.get_or_create_service(SandboxService, factory)
