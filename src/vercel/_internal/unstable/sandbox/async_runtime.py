"""Async runtime handles and entry points for unstable Sandbox operations."""

import signal as signal_module
import warnings
from collections.abc import AsyncIterator, Callable, Generator, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from types import TracebackType
from typing import Any, TextIO

from vercel._internal.polyfills import Self
from vercel._internal.time import parse_duration_seconds, parse_required_duration_seconds
from vercel._internal.unstable.sandbox.errors import (
    SandboxCleanupError,
    SandboxResponseError,
    SandboxTerminalStateError,
)
from vercel._internal.unstable.sandbox.log_stream import _parse_command_log_record
from vercel._internal.unstable.sandbox.models import (
    _OMITTED,
    CompletedProcess,
    DirectoryEntry,
    DurationInput,
    JSONValue,
    ProcessLog,
    SandboxQuery,
    SandboxResources,
    SandboxSource,
    SnapshotExpiration,
    SnapshotExpirationInput,
    SnapshotRetention,
    SnapshotRetentionUpdate,
    _parse_snapshot_expiration,
    _WriteFile,
)
from vercel._internal.unstable.sandbox.pagination import (
    QuerySandboxesPage,
    QuerySandboxesParams,
    QuerySessionsPage,
    QuerySessionsParams,
    QuerySnapshotsPage,
    QuerySnapshotsParams,
)
from vercel._internal.unstable.sandbox.process_output import ProcessOutputRouter
from vercel._internal.unstable.sandbox.runtime_common import (
    RemotePath,
    RuntimeSessionHandleBase,
    SandboxHandleBase,
    SnapshotHandleBase,
    _coerce_remote_path,
    _ProcessHandleState,
    _signal_number,
)
from vercel._internal.unstable.sandbox.service import SandboxService, _SandboxTerminalState
from vercel._internal.unstable.sandbox.state import (
    ProcessState,
    SandboxRuntimeSessionState,
    SandboxState,
    SnapshotState,
)
from vercel._internal.unstable.sandbox.text_reader import _text_readers


def _terminal_error(error: _SandboxTerminalState, sandbox: object) -> SandboxTerminalStateError:
    return SandboxTerminalStateError(
        f"Sandbox {error.sandbox.name!r} reached terminal state {error.status!r}",
        status=error.status,
        sandbox=sandbox,
    )


class Process(_ProcessHandleState):
    __slots__ = ("_service", "stderr", "stdout")

    def __init__(self, *, payload: ProcessState, service: SandboxService) -> None:
        super().__init__(payload)
        self._service = service
        self.stdout, self.stderr = _text_readers(
            lambda: service.process_logs_response(session_id=self._session_id, process_id=self.id)
        )

    async def refresh(self) -> Self:
        payload = await self._service.get_process(session_id=self._session_id, process_id=self.id)
        self._apply_payload(payload)
        return self

    async def wait(self) -> int:
        payload = await self._service.get_process(
            session_id=self._session_id, process_id=self.id, wait=True
        )
        self._apply_payload(payload)
        if self.returncode is None:
            raise SandboxResponseError("Wait response did not include a process return code")
        return self.returncode

    async def communicate(self, input: None = None) -> tuple[str, str]:
        if input is not None:
            raise NotImplementedError("process stdin is not supported")
        stdout, stderr = await self.stdout.read(), await self.stderr.read()
        await self.wait()
        return stdout, stderr

    async def send_signal(self, signal: int | str | signal_module.Signals) -> None:
        payload = await self._service.send_process_signal(
            session_id=self._session_id,
            process_id=self.id,
            signal=_signal_number(signal),
        )
        self._apply_payload(payload)

    async def terminate(self) -> None:
        await self.send_signal(signal_module.SIGTERM)

    async def kill(self) -> None:
        await self.send_signal(signal_module.SIGKILL)

    def logs(self) -> AsyncIterator[ProcessLog]:
        return _process_logs(self._service, session_id=self._session_id, process_id=self.id)


class Snapshot(SnapshotHandleBase):
    __slots__ = ("_service",)

    def __init__(self, *, payload: SnapshotState, service: SandboxService) -> None:
        super().__init__(payload)
        self._service = service

    async def delete(self) -> Self:
        payload = await self._service.delete_snapshot(snapshot_id=self.id)
        self._apply_payload(payload)
        return self


class SandboxFilesystem:
    __slots__ = ("_service", "_session_id", "_write_files_cwd")

    def __init__(
        self,
        *,
        service: SandboxService,
        session_id: Callable[[], str],
        write_files_cwd: Callable[[RemotePath | None], str],
    ) -> None:
        self._service = service
        self._session_id = session_id
        self._write_files_cwd = write_files_cwd

    async def _collect_output(self, command: ProcessState) -> tuple[str, str]:
        stdout: list[str] = []
        stderr: list[str] = []
        async for event in _process_logs(
            self._service, session_id=command.session_id, process_id=command.id
        ):
            if event.stream == "stdout":
                stdout.append(event.data)
            else:
                stderr.append(event.data)
        return "".join(stdout), "".join(stderr)

    async def mkdir(
        self, path: RemotePath, *, cwd: RemotePath | None = None, recursive: bool = True
    ) -> None:
        await self._service.mkdir(
            session_id=self._session_id(),
            path=_coerce_remote_path(path),
            cwd=None if cwd is None else _coerce_remote_path(cwd),
            recursive=recursive,
        )

    async def read_bytes(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bytes:
        return await self._service.read_bytes(
            session_id=self._session_id(),
            path=_coerce_remote_path(path),
            cwd=None if cwd is None else _coerce_remote_path(cwd),
        )

    async def read_text(
        self,
        path: RemotePath,
        *,
        cwd: RemotePath | None = None,
        encoding: str = "utf-8",
        errors: str = "strict",
    ) -> str:
        return (await self.read_bytes(path, cwd=cwd)).decode(encoding, errors=errors)

    async def write_bytes(
        self,
        path: RemotePath,
        data: bytes,
        *,
        cwd: RemotePath | None = None,
        mode: int | None = None,
    ) -> None:
        await self._write_files(
            [_WriteFile(path=_coerce_remote_path(path), content=data, mode=mode)], cwd=cwd
        )

    async def write_text(
        self,
        path: RemotePath,
        text: str,
        *,
        cwd: RemotePath | None = None,
        encoding: str = "utf-8",
        errors: str = "strict",
        mode: int | None = None,
    ) -> None:
        await self._write_files(
            [
                _WriteFile(
                    path=_coerce_remote_path(path),
                    content=text.encode(encoding, errors=errors),
                    mode=mode,
                )
            ],
            cwd=cwd,
        )

    async def _write_files(
        self, files: Sequence[_WriteFile], *, cwd: RemotePath | None = None
    ) -> None:
        await self._service.write_files(
            session_id=self._session_id(),
            files=files,
            cwd=self._write_files_cwd(cwd),
        )

    def batch(self, *, cwd: RemotePath | None = None) -> "SandboxFilesystemBatch":
        return SandboxFilesystemBatch(filesystem=self, cwd=cwd)

    async def exists(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bool:
        return await self._service.exists(
            session_id=self._session_id(),
            path=_coerce_remote_path(path),
            cwd=None if cwd is None else _coerce_remote_path(cwd),
            collect_output=self._collect_output,
        )

    async def is_file(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bool:
        return await self._service.is_file(
            session_id=self._session_id(),
            path=_coerce_remote_path(path),
            cwd=None if cwd is None else _coerce_remote_path(cwd),
            collect_output=self._collect_output,
        )

    async def is_dir(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bool:
        return await self._service.is_dir(
            session_id=self._session_id(),
            path=_coerce_remote_path(path),
            cwd=None if cwd is None else _coerce_remote_path(cwd),
            collect_output=self._collect_output,
        )

    async def listdir(
        self, path: RemotePath = ".", *, cwd: RemotePath | None = None
    ) -> list[DirectoryEntry]:
        return await self._service.listdir(
            session_id=self._session_id(),
            path=_coerce_remote_path(path),
            cwd=None if cwd is None else _coerce_remote_path(cwd),
            collect_output=self._collect_output,
        )

    async def remove(
        self,
        path: RemotePath,
        *,
        cwd: RemotePath | None = None,
        recursive: bool = False,
        missing_ok: bool = False,
    ) -> None:
        await self._service.remove(
            session_id=self._session_id(),
            path=_coerce_remote_path(path),
            cwd=None if cwd is None else _coerce_remote_path(cwd),
            recursive=recursive,
            missing_ok=missing_ok,
            collect_output=self._collect_output,
        )

    async def rename(
        self,
        source: RemotePath,
        destination: RemotePath,
        *,
        cwd: RemotePath | None = None,
    ) -> None:
        await self._service.rename(
            session_id=self._session_id(),
            source=_coerce_remote_path(source),
            destination=_coerce_remote_path(destination),
            cwd=None if cwd is None else _coerce_remote_path(cwd),
            collect_output=self._collect_output,
        )


class SandboxFilesystemBatch:
    __slots__ = ("_active", "_cwd", "_files", "_filesystem", "_used")

    def __init__(self, *, filesystem: SandboxFilesystem, cwd: RemotePath | None) -> None:
        self._filesystem = filesystem
        self._cwd = cwd
        self._files: list[_WriteFile] = []
        self._active = False
        self._used = False

    def _stage(self, file: _WriteFile) -> None:
        if not self._active:
            raise RuntimeError("filesystem batch staging is only allowed inside its context")
        self._files.append(file)

    def write_bytes(self, path: RemotePath, data: bytes, *, mode: int | None = None) -> None:
        self._stage(_WriteFile(path=_coerce_remote_path(path), content=data, mode=mode))

    def write_text(
        self,
        path: RemotePath,
        text: str,
        *,
        encoding: str = "utf-8",
        errors: str = "strict",
        mode: int | None = None,
    ) -> None:
        self._stage(
            _WriteFile(
                path=_coerce_remote_path(path),
                content=text.encode(encoding, errors=errors),
                mode=mode,
            )
        )

    async def __aenter__(self) -> "SandboxFilesystemBatch":
        if self._used:
            raise RuntimeError("filesystem batch contexts can only be entered once")
        self._used = True
        self._active = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._active = False
        if exc_type is None and self._files:
            await self._filesystem._write_files(self._files, cwd=self._cwd)


class SandboxRuntimeSession(RuntimeSessionHandleBase):
    __slots__ = ("_service", "fs")

    def __init__(self, *, payload: SandboxRuntimeSessionState, service: SandboxService) -> None:
        super().__init__(payload)
        self._service = service
        self.fs = SandboxFilesystem(
            service=service,
            session_id=lambda: self.id,
            write_files_cwd=self._write_files_cwd,
        )

    async def run_process(
        self,
        command: str,
        args: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
        check: bool = False,
        stdout: TextIO | int | None = None,
        stderr: TextIO | int | None = None,
        capture_output: bool = False,
    ) -> CompletedProcess:
        output_router = ProcessOutputRouter(
            stdout=stdout, stderr=stderr, capture_output=capture_output
        )
        state = await self._service.run_process(
            session_id=self.id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
            kill_after=parse_duration_seconds(kill_after),
            output_router=output_router,
        )
        assert state.process.returncode is not None
        result = CompletedProcess(
            id=state.process.id,
            name=state.process.name,
            args=(state.process.name, *state.process.args),
            cwd=state.process.cwd,
            session_id=state.process.session_id,
            started_at=state.process.started_at,
            returncode=state.process.returncode,
            stdout=state.stdout,
            stderr=state.stderr,
        )
        if check:
            result.check_returncode()
        return result

    async def create_process(
        self,
        command: str,
        args: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
    ) -> Process:
        state = await self._service.create_process(
            session_id=self.id,
            command=command,
            args=list(args) if args is not None else None,
            cwd=cwd,
            env=env,
            sudo=sudo,
            kill_after=parse_duration_seconds(kill_after),
        )
        return Process(payload=state, service=self._service)

    async def get_process(self, process_id: str, *, wait: bool = False) -> Process:
        state = await self._service.get_process(
            session_id=self.id, process_id=process_id, wait=wait
        )
        return Process(payload=state, service=self._service)

    async def query_processes(self) -> list[Process]:
        states = await self._service.query_processes(session_id=self.id)
        return [Process(payload=state, service=self._service) for state in states]

    async def refresh(self, *, include_system_routes: bool | None = None) -> Self:
        payload = await self._service.get_runtime_session(
            session_id=self.id, include_system_routes=include_system_routes
        )
        self._apply_payload(payload)
        return self

    async def extend_execution_time_limit(self, duration: DurationInput) -> Self:
        payload = await self._service.extend_runtime_session_timeout(
            session_id=self.id, duration=parse_required_duration_seconds(duration)
        )
        self._apply_payload(payload)
        return self

    async def update_network_policy(self, network_policy: JSONValue) -> Self:
        payload = await self._service.update_runtime_session_network_policy(
            session_id=self.id, network_policy=network_policy
        )
        self._apply_payload(payload)
        return self

    async def snapshot(self, *, expiration: SnapshotExpirationInput = None) -> Snapshot:
        result = await self._service.create_snapshot(
            session_id=self.id, expiration=_parse_snapshot_expiration(expiration)
        )
        self._apply_payload(result.session)
        return Snapshot(payload=result.snapshot, service=self._service)

    async def stop(self) -> Self:
        payload = await self._service.stop_runtime_session(session_id=self.id)
        self._apply_payload(payload)
        return self


class Sandbox(SandboxHandleBase[SandboxRuntimeSession]):
    __slots__ = ("_service", "fs")

    def __init__(self, *, payload: SandboxState, service: SandboxService) -> None:
        super().__init__(
            payload,
            session_factory=lambda session: SandboxRuntimeSession(payload=session, service=service),
        )
        self._service = service
        self.fs = SandboxFilesystem(
            service=service,
            session_id=lambda: self.current_session_id,
            write_files_cwd=self._write_files_cwd,
        )

    def session(self) -> "CreateRuntimeSessionOperation":
        return CreateRuntimeSessionOperation(
            service=self._service,
            sandbox_name=self.name,
            project_id=self.project_id,
        )

    async def run_process(
        self,
        command: str,
        args: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
        check: bool = False,
        stdout: TextIO | int | None = None,
        stderr: TextIO | int | None = None,
        capture_output: bool = False,
    ) -> CompletedProcess:
        output_router = ProcessOutputRouter(
            stdout=stdout, stderr=stderr, capture_output=capture_output
        )
        state = await self._service.run_process(
            session_id=self.current_session_id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
            kill_after=parse_duration_seconds(kill_after),
            output_router=output_router,
        )
        assert state.process.returncode is not None
        result = CompletedProcess(
            id=state.process.id,
            name=state.process.name,
            args=(state.process.name, *state.process.args),
            cwd=state.process.cwd,
            session_id=state.process.session_id,
            started_at=state.process.started_at,
            returncode=state.process.returncode,
            stdout=state.stdout,
            stderr=state.stderr,
        )
        if check:
            result.check_returncode()
        return result

    async def create_process(
        self,
        command: str,
        args: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
    ) -> Process:
        state = await self._service.create_process(
            session_id=self.current_session_id,
            command=command,
            args=list(args) if args is not None else None,
            cwd=cwd,
            env=env,
            sudo=sudo,
            kill_after=parse_duration_seconds(kill_after),
        )
        return Process(payload=state, service=self._service)

    async def get_process(self, process_id: str, *, wait: bool = False) -> Process:
        state = await self._service.get_process(
            session_id=self.current_session_id, process_id=process_id, wait=wait
        )
        return Process(payload=state, service=self._service)

    async def query_processes(self) -> list[Process]:
        states = await self._service.query_processes(session_id=self.current_session_id)
        return [Process(payload=state, service=self._service) for state in states]

    async def list_sessions(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> list[SandboxRuntimeSession]:
        page = await query_sessions_page(
            self._service,
            project_id=self.project_id,
            name=self.name,
            page_size=page_size,
            cursor=cursor,
            sort_order=sort_order,
        )
        return page.sessions

    async def list_snapshots(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> list[Snapshot]:
        page = await query_snapshots_page(
            self._service,
            project_id=self.project_id,
            name=self.name,
            page_size=page_size,
            cursor=cursor,
            sort_order=sort_order,
        )
        return page.snapshots

    async def extend_execution_time_limit(self, duration: DurationInput) -> SandboxRuntimeSession:
        payload = await self._service.extend_runtime_session_timeout(
            session_id=self.current_session_id,
            duration=parse_required_duration_seconds(duration),
        )
        return self._apply_current_session_payload(payload)

    async def update_network_policy(self, network_policy: JSONValue) -> SandboxRuntimeSession:
        payload = await self._service.update_runtime_session_network_policy(
            session_id=self.current_session_id, network_policy=network_policy
        )
        return self._apply_current_session_payload(payload)

    async def snapshot(self, *, expiration: SnapshotExpirationInput = None) -> Snapshot:
        result = await self._service.create_snapshot(
            session_id=self.current_session_id,
            expiration=_parse_snapshot_expiration(expiration),
        )
        self._apply_current_session_payload(result.session)
        return Snapshot(payload=result.snapshot, service=self._service)

    async def destroy(self) -> Self:
        payload = await self._service.destroy_sandbox(name=self.name, project_id=self.project_id)
        self._apply_payload(payload)
        return self

    async def update(
        self,
        *,
        runtime: str | None = None,
        ports: list[int] | None = None,
        execution_time_limit: DurationInput = None,
        resources: SandboxResources | None = None,
        persistent: bool | None = None,
        network_policy: JSONValue | None = None,
        env: dict[str, str] | None = None,
        tags: dict[str, str] | None = None,
        snapshot_expiration: SnapshotExpirationInput = None,
        snapshot_retention: SnapshotRetentionUpdate = _OMITTED,
        current_snapshot_id: str | None = None,
    ) -> Self:
        payload = await self._service.update_sandbox(
            name=self.name,
            project_id=self.project_id,
            runtime=runtime,
            ports=ports,
            execution_time_limit=parse_duration_seconds(execution_time_limit),
            resources=resources,
            persistent=persistent,
            network_policy=network_policy,
            env=env,
            tags=tags,
            snapshot_expiration=_parse_snapshot_expiration(snapshot_expiration),
            snapshot_retention=snapshot_retention,
            current_snapshot_id=current_snapshot_id,
        )
        self._apply_payload(payload)
        return self


@dataclass(frozen=True, slots=True)
class _CreateSandboxParams:
    project_id: str | None = None
    name: str | None = None
    runtime: str | None = None
    source: SandboxSource | None = None
    ports: list[int] | None = None
    execution_time_limit: timedelta | None = None
    resources: SandboxResources | None = None
    persistent: bool | None = None
    network_policy: JSONValue | None = None
    env: Mapping[str, str] | None = None
    tags: Mapping[str, str] | None = None
    snapshot_expiration: SnapshotExpiration | None = None
    snapshot_retention: SnapshotRetention | None = None


class CreateSandboxOperation:
    def __init__(self, *, service: SandboxService, params: _CreateSandboxParams) -> None:
        self._service = service
        self._params = params
        self._consumed = False
        self._handle: Sandbox | None = None

    def _mark_consumed(self) -> None:
        if self._consumed:
            raise RuntimeError("sandbox.create_sandbox(...) operations can only be used once")
        self._consumed = True

    async def _run_once(self) -> Sandbox:
        self._mark_consumed()
        return await _create_sandbox(
            self._service,
            project_id=self._params.project_id,
            name=self._params.name,
            runtime=self._params.runtime,
            source=self._params.source,
            ports=self._params.ports,
            execution_time_limit=self._params.execution_time_limit,
            resources=self._params.resources,
            persistent=self._params.persistent,
            network_policy=self._params.network_policy,
            env=self._params.env,
            tags=self._params.tags,
            snapshot_expiration=self._params.snapshot_expiration,
            snapshot_retention=self._params.snapshot_retention,
        )

    def __await__(self) -> Generator[Any, None, Sandbox]:
        return self._run_once().__await__()

    async def __aenter__(self) -> Sandbox:
        handle = await self._run_once()
        self._handle = handle
        return handle

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._handle is None:
            return None
        try:
            payload = await self._service.destroy_sandbox(
                name=self._handle.name, project_id=self._handle.project_id
            )
            self._handle._apply_payload(payload)
        except Exception as cleanup_exc:
            raise SandboxCleanupError(
                f"Failed to clean up sandbox {self._handle.name!r}",
                resource_type="sandbox",
                resource_id=self._handle.name,
                cause=cleanup_exc,
            ) from cleanup_exc
        return None

    def __del__(self) -> None:
        if self._consumed:
            return
        warnings.warn(
            "sandbox.create_sandbox(...) operation was never awaited or entered",
            RuntimeWarning,
            stacklevel=2,
        )


class CreateRuntimeSessionOperation:
    def __init__(
        self, *, service: SandboxService, sandbox_name: str, project_id: str | None
    ) -> None:
        self._service = service
        self._sandbox_name = sandbox_name
        self._project_id = project_id
        self._consumed = False
        self._handle: SandboxRuntimeSession | None = None

    def _mark_consumed(self) -> None:
        if self._consumed:
            raise RuntimeError("sandbox runtime-session operations can only be used once")
        self._consumed = True

    async def _run_once(self) -> SandboxRuntimeSession:
        self._mark_consumed()
        payload = await self._service.create_runtime_session(
            name=self._sandbox_name, project_id=self._project_id
        )
        return SandboxRuntimeSession(payload=payload, service=self._service)

    def __await__(self) -> Generator[Any, None, SandboxRuntimeSession]:
        return self._run_once().__await__()

    async def __aenter__(self) -> SandboxRuntimeSession:
        handle = await self._run_once()
        self._handle = handle
        return handle

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._handle is None:
            return None
        try:
            payload = await self._service.stop_runtime_session(session_id=self._handle.id)
            self._handle._apply_payload(payload)
        except Exception as cleanup_exc:
            raise SandboxCleanupError(
                f"Failed to clean up sandbox runtime session {self._handle.id!r}",
                resource_type="sandbox_runtime_session",
                resource_id=self._handle.id,
                cause=cleanup_exc,
            ) from cleanup_exc
        return None


async def _create_sandbox(service: SandboxService, **kwargs: Any) -> Sandbox:
    try:
        return Sandbox(payload=await service.create_sandbox(**kwargs), service=service)
    except _SandboxTerminalState as error:
        raise _terminal_error(error, Sandbox(payload=error.sandbox, service=service)) from error


def create_sandbox_operation(
    service: SandboxService,
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
) -> CreateSandboxOperation:
    return CreateSandboxOperation(
        service=service,
        params=_CreateSandboxParams(
            project_id=project_id,
            name=name,
            runtime=runtime,
            source=source,
            ports=ports,
            execution_time_limit=parse_duration_seconds(execution_time_limit),
            resources=resources,
            persistent=persistent,
            network_policy=network_policy,
            env=env,
            tags=tags,
            snapshot_expiration=_parse_snapshot_expiration(snapshot_expiration),
            snapshot_retention=snapshot_retention,
        ),
    )


async def get_sandbox(service: SandboxService, **kwargs: Any) -> Sandbox:
    return Sandbox(payload=await service.get_sandbox(**kwargs), service=service)


async def query_sandboxes_page(
    service: SandboxService, **kwargs: Any
) -> QuerySandboxesPage[Sandbox]:
    page = await service.query_sandboxes_page(**kwargs)
    return QuerySandboxesPage(
        sandboxes=[Sandbox(payload=state, service=service) for state in page.sandboxes],
        next_cursor=page.next_cursor,
    )


def query_sandboxes(
    service: SandboxService,
    *,
    query: SandboxQuery | None = None,
    project_id: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> AsyncIterator[Sandbox]:
    async def iterate() -> AsyncIterator[Sandbox]:
        params = QuerySandboxesParams(page_size=page_size, cursor=cursor)
        while True:
            page = await query_sandboxes_page(
                service,
                query=query,
                project_id=project_id,
                page_size=params.page_size,
                cursor=params.cursor,
            )
            for sandbox in page.sandboxes:
                yield sandbox
            if page.next_cursor is None or not page.sandboxes:
                return
            params = params.with_cursor(page.next_cursor)

    return iterate()


async def query_sessions_page(
    service: SandboxService, **kwargs: Any
) -> QuerySessionsPage[SandboxRuntimeSession]:
    page = await service.query_sessions_page(**kwargs)
    return QuerySessionsPage(
        sessions=[SandboxRuntimeSession(payload=state, service=service) for state in page.sessions],
        next_cursor=page.next_cursor,
    )


def query_sessions(
    service: SandboxService,
    *,
    project_id: str | None = None,
    name: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
    sort_order: str | None = None,
) -> AsyncIterator[SandboxRuntimeSession]:
    async def iterate() -> AsyncIterator[SandboxRuntimeSession]:
        params = QuerySessionsParams(page_size=page_size, cursor=cursor)
        while True:
            page = await query_sessions_page(
                service,
                project_id=project_id,
                name=name,
                page_size=params.page_size,
                cursor=params.cursor,
                sort_order=sort_order,
            )
            for session in page.sessions:
                yield session
            if page.next_cursor is None or not page.sessions:
                return
            params = params.with_cursor(page.next_cursor)

    return iterate()


async def query_snapshots_page(
    service: SandboxService, **kwargs: Any
) -> QuerySnapshotsPage[Snapshot]:
    page = await service.query_snapshots_page(**kwargs)
    return QuerySnapshotsPage(
        snapshots=[Snapshot(payload=state, service=service) for state in page.snapshots],
        next_cursor=page.next_cursor,
    )


def query_snapshots(
    service: SandboxService,
    *,
    project_id: str | None = None,
    name: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
    sort_order: str | None = None,
) -> AsyncIterator[Snapshot]:
    async def iterate() -> AsyncIterator[Snapshot]:
        params = QuerySnapshotsParams(page_size=page_size, cursor=cursor)
        while True:
            page = await query_snapshots_page(
                service,
                project_id=project_id,
                name=name,
                page_size=params.page_size,
                cursor=params.cursor,
                sort_order=sort_order,
            )
            for snapshot in page.snapshots:
                yield snapshot
            if page.next_cursor is None or not page.snapshots:
                return
            params = params.with_cursor(page.next_cursor)

    return iterate()


async def get_snapshot(service: SandboxService, *, snapshot_id: str) -> Snapshot:
    return Snapshot(payload=await service.get_snapshot(snapshot_id=snapshot_id), service=service)


def _process_logs(
    service: SandboxService, *, session_id: str, process_id: str
) -> AsyncIterator[ProcessLog]:
    async def iterate() -> AsyncIterator[ProcessLog]:
        response = await service.process_logs_response(session_id=session_id, process_id=process_id)
        try:
            async for line in response.aiter_lines():
                if line:
                    event = _parse_command_log_record(line)
                    if event is not None:
                        yield event
        finally:
            await response.aclose()

    return iterate()
