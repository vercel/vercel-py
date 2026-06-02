"""Sync runtime handles and entry points for unstable Sandbox operations."""

import signal as signal_module
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import timedelta
from types import TracebackType
from typing import Any, Literal

from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.polyfills import Self
from vercel._internal.time import parse_duration_seconds, parse_required_duration_seconds
from vercel._internal.unstable.sandbox.errors import (
    SandboxCleanupError,
    SandboxTerminalStateError,
)
from vercel._internal.unstable.sandbox.log_stream import _parse_command_log_record
from vercel._internal.unstable.sandbox.models import (
    _OMITTED,
    DirectoryEntry,
    DurationInput,
    JSONValue,
    SandboxCommandLog,
    SandboxQuery,
    SandboxResources,
    SandboxSource,
    SnapshotExpirationInput,
    SnapshotRetention,
    SnapshotRetentionUpdate,
    WriteFile,
    _parse_snapshot_expiration,
)
from vercel._internal.unstable.sandbox.pagination import (
    QuerySandboxesPage,
    QuerySandboxesParams,
    QuerySessionsPage,
    QuerySessionsParams,
    QuerySnapshotsPage,
    QuerySnapshotsParams,
)
from vercel._internal.unstable.sandbox.runtime_common import (
    RuntimeSessionHandleBase,
    SandboxHandleBase,
    SnapshotHandleBase,
    _CommandHandleState,
    _log_from_snapshot,
    _LogSnapshot,
    _select_output,
    _signal_number,
)
from vercel._internal.unstable.sandbox.service import SandboxService, _SandboxTerminalState
from vercel._internal.unstable.sandbox.state import (
    SandboxCommandState,
    SandboxRuntimeSessionState,
    SandboxState,
    SnapshotState,
)


def _terminal_error(error: _SandboxTerminalState, sandbox: object) -> SandboxTerminalStateError:
    return SandboxTerminalStateError(
        f"Sandbox {error.sandbox.name!r} reached terminal state {error.status!r}",
        status=error.status,
        sandbox=sandbox,
    )


class SyncSandboxCommand(_CommandHandleState):
    __slots__ = ("_service",)

    def __init__(self, *, payload: SandboxCommandState, service: SandboxService) -> None:
        super().__init__(payload)
        self._service = service

    def refresh(self, *, wait: bool = False) -> Self:
        payload = iter_coroutine(
            self._service.get_command(session_id=self.session_id, command_id=self.id, wait=wait)
        )
        self._apply_payload(payload)
        return self

    def wait(self) -> Self:
        return self.refresh(wait=True)

    def kill(self, signal: int | str | signal_module.Signals | None = None) -> Self:
        payload = iter_coroutine(
            self._service.kill_command(
                session_id=self.session_id,
                command_id=self.id,
                signal=_signal_number(signal),
            )
        )
        self._apply_payload(payload)
        return self

    def logs(self, *, refresh: bool = False) -> Iterator[SandboxCommandLog]:
        def iter_logs() -> Iterator[SandboxCommandLog]:
            if not refresh and self._log_cache is not None:
                for snapshot in self._log_cache:
                    yield _log_from_snapshot(snapshot)
                return

            if refresh:
                self._log_cache = None
                self._log_cache_generation += 1
            generation = self._log_cache_generation
            staged: list[_LogSnapshot] = []
            for event in _command_logs(
                self._service, session_id=self.session_id, command_id=self.id
            ):
                staged.append((event.stream, event.data))
                yield event
            if generation == self._log_cache_generation:
                self._log_cache = tuple(staged)

        return iter_logs()

    def output(self, stream: Literal["stdout", "stderr", "both"] = "both") -> str:
        snapshots = [(line.stream, line.data) for line in self.logs()]
        return _select_output(snapshots, stream)

    def stdout(self) -> str:
        return self.output("stdout")

    def stderr(self) -> str:
        return self.output("stderr")


class SyncSnapshot(SnapshotHandleBase):
    __slots__ = ("_service",)

    def __init__(self, *, payload: SnapshotState, service: SandboxService) -> None:
        super().__init__(payload)
        self._service = service

    def delete(self) -> Self:
        payload = iter_coroutine(self._service.delete_snapshot(snapshot_id=self.id))
        self._apply_payload(payload)
        return self


class SyncSandboxFilesystem:
    __slots__ = ("_service", "_session_id", "_write_files_cwd")

    def __init__(
        self,
        *,
        service: SandboxService,
        session_id: Callable[[], str],
        write_files_cwd: Callable[[str | None], str],
    ) -> None:
        self._service = service
        self._session_id = session_id
        self._write_files_cwd = write_files_cwd

    async def _collect_output(self, command: SandboxCommandState) -> tuple[str, str]:
        stdout: list[str] = []
        stderr: list[str] = []
        for event in _command_logs(
            self._service, session_id=command.session_id, command_id=command.id
        ):
            if event.stream == "stdout":
                stdout.append(event.data)
            else:
                stderr.append(event.data)
        return "".join(stdout), "".join(stderr)

    def mkdir(self, path: str, *, cwd: str | None = None, recursive: bool = True) -> None:
        iter_coroutine(
            self._service.mkdir(
                session_id=self._session_id(), path=path, cwd=cwd, recursive=recursive
            )
        )

    def read_bytes(self, path: str, *, cwd: str | None = None) -> bytes:
        return iter_coroutine(
            self._service.read_bytes(session_id=self._session_id(), path=path, cwd=cwd)
        )

    def read_text(
        self, path: str, *, cwd: str | None = None, encoding: str = "utf-8", errors: str = "strict"
    ) -> str:
        return self.read_bytes(path, cwd=cwd).decode(encoding, errors=errors)

    def write_bytes(
        self, path: str, data: bytes, *, cwd: str | None = None, mode: int | None = None
    ) -> None:
        self.write_files([WriteFile(path=path, content=data, mode=mode)], cwd=cwd)

    def write_text(
        self,
        path: str,
        text: str,
        *,
        cwd: str | None = None,
        encoding: str = "utf-8",
        mode: int | None = None,
    ) -> None:
        self.write_files(
            [WriteFile(path=path, content=text, mode=mode)], cwd=cwd, encoding=encoding
        )

    def write_files(
        self, files: Sequence[WriteFile], *, cwd: str | None = None, encoding: str = "utf-8"
    ) -> None:
        iter_coroutine(
            self._service.write_files(
                session_id=self._session_id(),
                files=files,
                cwd=self._write_files_cwd(cwd),
                encoding=encoding,
            )
        )

    def exists(self, path: str, *, cwd: str | None = None) -> bool:
        return iter_coroutine(
            self._service.exists(
                session_id=self._session_id(),
                path=path,
                cwd=cwd,
                collect_output=self._collect_output,
            )
        )

    def is_file(self, path: str, *, cwd: str | None = None) -> bool:
        return iter_coroutine(
            self._service.is_file(
                session_id=self._session_id(),
                path=path,
                cwd=cwd,
                collect_output=self._collect_output,
            )
        )

    def is_dir(self, path: str, *, cwd: str | None = None) -> bool:
        return iter_coroutine(
            self._service.is_dir(
                session_id=self._session_id(),
                path=path,
                cwd=cwd,
                collect_output=self._collect_output,
            )
        )

    def listdir(self, path: str = ".", *, cwd: str | None = None) -> list[DirectoryEntry]:
        return iter_coroutine(
            self._service.listdir(
                session_id=self._session_id(),
                path=path,
                cwd=cwd,
                collect_output=self._collect_output,
            )
        )

    def remove(
        self,
        path: str,
        *,
        cwd: str | None = None,
        recursive: bool = False,
        missing_ok: bool = False,
    ) -> None:
        iter_coroutine(
            self._service.remove(
                session_id=self._session_id(),
                path=path,
                cwd=cwd,
                recursive=recursive,
                missing_ok=missing_ok,
                collect_output=self._collect_output,
            )
        )

    def rename(self, source: str, destination: str, *, cwd: str | None = None) -> None:
        iter_coroutine(
            self._service.rename(
                session_id=self._session_id(),
                source=source,
                destination=destination,
                cwd=cwd,
                collect_output=self._collect_output,
            )
        )


class SyncSandboxRuntimeSession(RuntimeSessionHandleBase):
    __slots__ = ("_service", "fs")

    def __init__(self, *, payload: SandboxRuntimeSessionState, service: SandboxService) -> None:
        super().__init__(payload)
        self._service = service
        self.fs = SyncSandboxFilesystem(
            service=service,
            session_id=lambda: self.id,
            write_files_cwd=self._write_files_cwd,
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            payload = iter_coroutine(self._service.stop_runtime_session(session_id=self.id))
            self._apply_payload(payload)
        except Exception as cleanup_exc:
            raise SandboxCleanupError(
                f"Failed to clean up sandbox runtime session {self.id!r}",
                resource_type="sandbox_runtime_session",
                resource_id=self.id,
                cause=cleanup_exc,
            ) from cleanup_exc

    def run_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
    ) -> SyncSandboxCommand:
        state = iter_coroutine(
            self._service.run_command(
                session_id=self.id,
                command=command,
                args=args,
                cwd=cwd,
                env=env,
                sudo=sudo,
                kill_after=parse_duration_seconds(kill_after),
            )
        )
        return SyncSandboxCommand(payload=state, service=self._service)

    def start_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
    ) -> SyncSandboxCommand:
        state = iter_coroutine(
            self._service.start_command(
                session_id=self.id,
                command=command,
                args=args,
                cwd=cwd,
                env=env,
                sudo=sudo,
                kill_after=parse_duration_seconds(kill_after),
            )
        )
        return SyncSandboxCommand(payload=state, service=self._service)

    def get_command(self, command_id: str, *, wait: bool = False) -> SyncSandboxCommand:
        state = iter_coroutine(
            self._service.get_command(session_id=self.id, command_id=command_id, wait=wait)
        )
        return SyncSandboxCommand(payload=state, service=self._service)

    def query_commands(self) -> list[SyncSandboxCommand]:
        states = iter_coroutine(self._service.query_commands(session_id=self.id))
        return [SyncSandboxCommand(payload=state, service=self._service) for state in states]

    def refresh(self, *, include_system_routes: bool | None = None) -> Self:
        payload = iter_coroutine(
            self._service.get_runtime_session(
                session_id=self.id, include_system_routes=include_system_routes
            )
        )
        self._apply_payload(payload)
        return self

    def extend_execution_time_limit(self, duration: DurationInput) -> Self:
        payload = iter_coroutine(
            self._service.extend_runtime_session_timeout(
                session_id=self.id, duration=parse_required_duration_seconds(duration)
            )
        )
        self._apply_payload(payload)
        return self

    def update_network_policy(self, network_policy: JSONValue) -> Self:
        payload = iter_coroutine(
            self._service.update_runtime_session_network_policy(
                session_id=self.id, network_policy=network_policy
            )
        )
        self._apply_payload(payload)
        return self

    def snapshot(self, *, expiration: SnapshotExpirationInput = None) -> SyncSnapshot:
        result = iter_coroutine(
            self._service.create_snapshot(
                session_id=self.id, expiration=_parse_snapshot_expiration(expiration)
            )
        )
        self._apply_payload(result.session)
        return SyncSnapshot(payload=result.snapshot, service=self._service)

    def command_logs(self, command_id: str) -> Iterator[SandboxCommandLog]:
        return _command_logs(self._service, session_id=self.id, command_id=command_id)

    def stop(self) -> Self:
        payload = iter_coroutine(self._service.stop_runtime_session(session_id=self.id))
        self._apply_payload(payload)
        return self


class SyncSandbox(SandboxHandleBase[SyncSandboxRuntimeSession]):
    __slots__ = ("_service", "fs")

    def __init__(self, *, payload: SandboxState, service: SandboxService) -> None:
        super().__init__(
            payload,
            session_factory=lambda session: SyncSandboxRuntimeSession(
                payload=session, service=service
            ),
        )
        self._service = service
        self.fs = SyncSandboxFilesystem(
            service=service,
            session_id=lambda: self.current_session_id,
            write_files_cwd=self._write_files_cwd,
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            payload = iter_coroutine(
                self._service.destroy_sandbox(name=self.name, project_id=self.project_id)
            )
            self._apply_payload(payload)
        except Exception as cleanup_exc:
            raise SandboxCleanupError(
                f"Failed to clean up sandbox {self.name!r}",
                resource_type="sandbox",
                resource_id=self.name,
                cause=cleanup_exc,
            ) from cleanup_exc

    def session(self) -> SyncSandboxRuntimeSession:
        payload = iter_coroutine(
            self._service.create_runtime_session(name=self.name, project_id=self.project_id)
        )
        return SyncSandboxRuntimeSession(payload=payload, service=self._service)

    def run_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
    ) -> SyncSandboxCommand:
        state = iter_coroutine(
            self._service.run_command(
                session_id=self.current_session_id,
                command=command,
                args=args,
                cwd=cwd,
                env=env,
                sudo=sudo,
                kill_after=parse_duration_seconds(kill_after),
            )
        )
        return SyncSandboxCommand(payload=state, service=self._service)

    def start_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
    ) -> SyncSandboxCommand:
        state = iter_coroutine(
            self._service.start_command(
                session_id=self.current_session_id,
                command=command,
                args=args,
                cwd=cwd,
                env=env,
                sudo=sudo,
                kill_after=parse_duration_seconds(kill_after),
            )
        )
        return SyncSandboxCommand(payload=state, service=self._service)

    def get_command(self, command_id: str, *, wait: bool = False) -> SyncSandboxCommand:
        state = iter_coroutine(
            self._service.get_command(
                session_id=self.current_session_id, command_id=command_id, wait=wait
            )
        )
        return SyncSandboxCommand(payload=state, service=self._service)

    def query_commands(self) -> list[SyncSandboxCommand]:
        states = iter_coroutine(self._service.query_commands(session_id=self.current_session_id))
        return [SyncSandboxCommand(payload=state, service=self._service) for state in states]

    def list_sessions(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> list[SyncSandboxRuntimeSession]:
        return query_sessions_page(
            self._service,
            project_id=self.project_id,
            name=self.name,
            page_size=page_size,
            cursor=cursor,
            sort_order=sort_order,
        ).sessions

    def list_snapshots(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> list[SyncSnapshot]:
        return query_snapshots_page(
            self._service,
            project_id=self.project_id,
            name=self.name,
            page_size=page_size,
            cursor=cursor,
            sort_order=sort_order,
        ).snapshots

    def extend_execution_time_limit(self, duration: DurationInput) -> SyncSandboxRuntimeSession:
        payload = iter_coroutine(
            self._service.extend_runtime_session_timeout(
                session_id=self.current_session_id,
                duration=parse_required_duration_seconds(duration),
            )
        )
        return self._apply_current_session_payload(payload)

    def update_network_policy(self, network_policy: JSONValue) -> SyncSandboxRuntimeSession:
        payload = iter_coroutine(
            self._service.update_runtime_session_network_policy(
                session_id=self.current_session_id, network_policy=network_policy
            )
        )
        return self._apply_current_session_payload(payload)

    def snapshot(self, *, expiration: SnapshotExpirationInput = None) -> SyncSnapshot:
        result = iter_coroutine(
            self._service.create_snapshot(
                session_id=self.current_session_id,
                expiration=_parse_snapshot_expiration(expiration),
            )
        )
        self._apply_current_session_payload(result.session)
        return SyncSnapshot(payload=result.snapshot, service=self._service)

    def destroy(self) -> Self:
        payload = iter_coroutine(
            self._service.destroy_sandbox(name=self.name, project_id=self.project_id)
        )
        self._apply_payload(payload)
        return self

    def update(
        self,
        *,
        runtime: str | None = None,
        ports: list[int] | None = None,
        execution_time_limit: DurationInput = None,
        resources: SandboxResources | None = None,
        persistent: bool | None = None,
        network_policy: JSONValue | None = None,
        env: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        snapshot_expiration: SnapshotExpirationInput = None,
        snapshot_retention: SnapshotRetentionUpdate = _OMITTED,
        current_snapshot_id: str | None = None,
    ) -> Self:
        payload = iter_coroutine(
            self._service.update_sandbox(
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
        )
        self._apply_payload(payload)
        return self


def create_sandbox(
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
) -> SyncSandbox:
    try:
        state = iter_coroutine(
            service.create_sandbox(
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
            )
        )
        return SyncSandbox(payload=state, service=service)
    except _SandboxTerminalState as error:
        raise _terminal_error(error, SyncSandbox(payload=error.sandbox, service=service)) from error


def get_sandbox(service: SandboxService, **kwargs: Any) -> SyncSandbox:
    return SyncSandbox(payload=iter_coroutine(service.get_sandbox(**kwargs)), service=service)


def query_sandboxes_page(service: SandboxService, **kwargs: Any) -> QuerySandboxesPage[SyncSandbox]:
    page = iter_coroutine(service.query_sandboxes_page(**kwargs))
    return QuerySandboxesPage(
        sandboxes=[SyncSandbox(payload=state, service=service) for state in page.sandboxes],
        next_cursor=page.next_cursor,
    )


def query_sandboxes(
    service: SandboxService,
    *,
    query: SandboxQuery | None = None,
    project_id: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> Iterator[SyncSandbox]:
    params = QuerySandboxesParams(page_size=page_size, cursor=cursor)
    while True:
        page = query_sandboxes_page(
            service,
            query=query,
            project_id=project_id,
            page_size=params.page_size,
            cursor=params.cursor,
        )
        yield from page.sandboxes
        if page.next_cursor is None or not page.sandboxes:
            return
        params = params.with_cursor(page.next_cursor)


def query_sessions_page(
    service: SandboxService, **kwargs: Any
) -> QuerySessionsPage[SyncSandboxRuntimeSession]:
    page = iter_coroutine(service.query_sessions_page(**kwargs))
    return QuerySessionsPage(
        sessions=[
            SyncSandboxRuntimeSession(payload=state, service=service) for state in page.sessions
        ],
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
) -> Iterator[SyncSandboxRuntimeSession]:
    params = QuerySessionsParams(page_size=page_size, cursor=cursor)
    while True:
        page = query_sessions_page(
            service,
            project_id=project_id,
            name=name,
            page_size=params.page_size,
            cursor=params.cursor,
            sort_order=sort_order,
        )
        yield from page.sessions
        if page.next_cursor is None or not page.sessions:
            return
        params = params.with_cursor(page.next_cursor)


def query_snapshots_page(
    service: SandboxService, **kwargs: Any
) -> QuerySnapshotsPage[SyncSnapshot]:
    page = iter_coroutine(service.query_snapshots_page(**kwargs))
    return QuerySnapshotsPage(
        snapshots=[SyncSnapshot(payload=state, service=service) for state in page.snapshots],
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
) -> Iterator[SyncSnapshot]:
    params = QuerySnapshotsParams(page_size=page_size, cursor=cursor)
    while True:
        page = query_snapshots_page(
            service,
            project_id=project_id,
            name=name,
            page_size=params.page_size,
            cursor=params.cursor,
            sort_order=sort_order,
        )
        yield from page.snapshots
        if page.next_cursor is None or not page.snapshots:
            return
        params = params.with_cursor(page.next_cursor)


def get_snapshot(service: SandboxService, *, snapshot_id: str) -> SyncSnapshot:
    return SyncSnapshot(
        payload=iter_coroutine(service.get_snapshot(snapshot_id=snapshot_id)), service=service
    )


def _command_logs(
    service: SandboxService, *, session_id: str, command_id: str
) -> Iterator[SandboxCommandLog]:
    response = iter_coroutine(
        service.command_logs_response(session_id=session_id, command_id=command_id)
    )
    try:
        for line in response.iter_lines():
            if line:
                event = _parse_command_log_record(line)
                if event is not None:
                    yield event
    finally:
        response.close()
