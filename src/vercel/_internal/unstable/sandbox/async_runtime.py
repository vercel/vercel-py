"""Async runtime handles and entry points for unstable Sandbox operations."""

import signal as signal_module
import warnings
from collections.abc import AsyncIterator, Awaitable, Callable, Generator, Mapping, Sequence
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
    _SandboxFilesystemBatchBase,
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
    """Control and inspect an asynchronously running sandbox process.

    The ``stdout`` and ``stderr`` readers consume the process log stream and
    may each be read only once.
    """

    __slots__ = ("_service", "stderr", "stdout")

    def __init__(self, *, payload: ProcessState, service: SandboxService) -> None:
        super().__init__(payload)
        self._service = service
        self.stdout, self.stderr = _text_readers(
            lambda: service.process_logs_response(session_id=self._session_id, process_id=self.id)
        )

    async def refresh(self) -> Self:
        """Refresh the process state and return this handle."""
        payload = await self._service.get_process(session_id=self._session_id, process_id=self.id)
        self._apply_payload(payload)
        return self

    async def wait(self) -> int:
        """Wait for the process to exit and return its exit code."""
        payload = await self._service.get_process(
            session_id=self._session_id, process_id=self.id, wait=True
        )
        self._apply_payload(payload)
        if self.returncode is None:
            raise SandboxResponseError("Wait response did not include a process return code")
        return self.returncode

    async def communicate(self, input: None = None) -> tuple[str, str]:
        """Read all output and wait for the process to exit.

        Args:
            input: Reserved for subprocess compatibility. Process standard
                input is not supported and must be ``None``.

        Returns:
            A ``(stdout, stderr)`` tuple.

        Raises:
            NotImplementedError: If ``input`` is not ``None``.
        """
        if input is not None:
            raise NotImplementedError("process stdin is not supported")
        stdout, stderr = await self.stdout.read(), await self.stderr.read()
        await self.wait()
        return stdout, stderr

    async def send_signal(self, signal: int | str | signal_module.Signals) -> None:
        """Send a signal to the running process.

        Args:
            signal: Numeric signal, ``Signals`` member, or name such as
                ``"TERM"`` or ``"SIGTERM"``.
        """
        payload = await self._service.send_process_signal(
            session_id=self._session_id,
            process_id=self.id,
            signal=_signal_number(signal),
        )
        self._apply_payload(payload)

    async def terminate(self) -> None:
        """Request graceful process termination with ``SIGTERM``."""
        await self.send_signal(signal_module.SIGTERM)

    async def kill(self) -> None:
        """Terminate the process immediately with ``SIGKILL``."""
        await self.send_signal(signal_module.SIGKILL)

    def logs(self) -> AsyncIterator[ProcessLog]:
        """Iterate over interleaved stdout and stderr log events."""
        return _process_logs(self._service, session_id=self._session_id, process_id=self.id)


class Snapshot(SnapshotHandleBase):
    """Represent a sandbox filesystem snapshot."""

    __slots__ = ("_service",)

    def __init__(self, *, payload: SnapshotState, service: SandboxService) -> None:
        super().__init__(payload)
        self._service = service

    async def delete(self) -> Self:
        """Delete the snapshot and refresh this handle."""
        payload = await self._service.delete_snapshot(snapshot_id=self.id)
        self._apply_payload(payload)
        return self


class SandboxFilesystem:
    """Perform filesystem operations in a sandbox runtime session."""

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
        """Create a directory.

        Args:
            path: Absolute path or path relative to ``cwd``.
            cwd: Base directory for a relative path.
            recursive: Whether to create missing parent directories.
        """
        await self._service.mkdir(
            session_id=self._session_id(),
            path=_coerce_remote_path(path),
            cwd=None if cwd is None else _coerce_remote_path(cwd),
            recursive=recursive,
        )

    async def read_bytes(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bytes:
        """Read a file as bytes.

        Args:
            path: Absolute path or path relative to ``cwd``.
            cwd: Base directory for a relative path.

        Returns:
            The complete file contents.
        """
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
        """Read and decode a text file.

        Args:
            path: Absolute path or path relative to ``cwd``.
            cwd: Base directory for a relative path.
            encoding: Text encoding used to decode the file.
            errors: Decoding error policy.

        Returns:
            The decoded file contents.
        """
        return (await self.read_bytes(path, cwd=cwd)).decode(encoding, errors=errors)

    async def write_bytes(
        self,
        path: RemotePath,
        data: bytes,
        *,
        cwd: RemotePath | None = None,
        mode: int | None = None,
    ) -> None:
        """Write bytes to a file, replacing any existing contents.

        Args:
            path: Absolute path or path relative to ``cwd``.
            data: File contents.
            cwd: Base directory for a relative path.
            mode: Optional POSIX permission bits for the file.
        """
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
        """Encode and write text to a file.

        Args:
            path: Absolute path or path relative to ``cwd``.
            text: Text to write.
            cwd: Base directory for a relative path.
            encoding: Text encoding used to encode ``text``.
            errors: Encoding error policy.
            mode: Optional POSIX permission bits for the file.
        """
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
        """Create an async context manager for one atomic write request.

        Args:
            cwd: Base directory shared by staged relative paths.

        Returns:
            A batch that uploads its staged files on successful context exit.
        """
        return SandboxFilesystemBatch(write_files=lambda files: self._write_files(files, cwd=cwd))

    async def exists(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bool:
        """Return whether a filesystem entry exists."""
        return await self._service.exists(
            session_id=self._session_id(),
            path=_coerce_remote_path(path),
            cwd=None if cwd is None else _coerce_remote_path(cwd),
            collect_output=self._collect_output,
        )

    async def is_file(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bool:
        """Return whether a path exists and is a regular file."""
        return await self._service.is_file(
            session_id=self._session_id(),
            path=_coerce_remote_path(path),
            cwd=None if cwd is None else _coerce_remote_path(cwd),
            collect_output=self._collect_output,
        )

    async def is_dir(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bool:
        """Return whether a path exists and is a directory."""
        return await self._service.is_dir(
            session_id=self._session_id(),
            path=_coerce_remote_path(path),
            cwd=None if cwd is None else _coerce_remote_path(cwd),
            collect_output=self._collect_output,
        )

    async def listdir(
        self, path: RemotePath = ".", *, cwd: RemotePath | None = None
    ) -> list[DirectoryEntry]:
        """List the direct children of a directory.

        Args:
            path: Directory to list.
            cwd: Base directory for a relative path.

        Returns:
            The directory entries returned by the remote filesystem.
        """
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
        """Remove a file or directory.

        Args:
            path: Absolute path or path relative to ``cwd``.
            cwd: Base directory for a relative path.
            recursive: Whether to recursively remove a directory.
            missing_ok: Whether a missing path should be ignored.
        """
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
        """Rename or move a filesystem entry.

        Args:
            source: Existing absolute or relative path.
            destination: New absolute or relative path.
            cwd: Base directory for relative paths.
        """
        await self._service.rename(
            session_id=self._session_id(),
            source=_coerce_remote_path(source),
            destination=_coerce_remote_path(destination),
            cwd=None if cwd is None else _coerce_remote_path(cwd),
            collect_output=self._collect_output,
        )


class SandboxFilesystemBatch(_SandboxFilesystemBatchBase):
    """Stage multiple file writes for one async filesystem request.

    Create batches with ``SandboxFilesystem.batch`` and use them only inside
    their async context.
    """

    __slots__ = ("_write_files",)

    def __init__(self, *, write_files: Callable[[Sequence[_WriteFile]], Awaitable[None]]) -> None:
        super().__init__()
        self._write_files = write_files

    async def __aenter__(self) -> "SandboxFilesystemBatch":
        self._enter()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        files = self._close()
        if exc_type is None and files:
            await self._write_files(files)


class SandboxRuntimeSession(RuntimeSessionHandleBase):
    """Represent one execution session in a sandbox's session history.

    A sandbox has at most one active current session. A session owns its process
    namespace and filesystem access. Calling ``stop`` ends the session without
    destroying the parent sandbox; a later resume creates a replacement current
    session from the sandbox's latest snapshot.
    """

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
        """Run a process to completion.

        Args:
            command: Executable or command name.
            args: Command arguments, excluding the executable.
            cwd: Process working directory.
            env: Environment variables added to the process.
            sudo: Whether to run with elevated privileges.
            kill_after: Duration after which the service kills the process.
            check: Whether to raise for a nonzero exit code.
            stdout: Writable text stream or subprocess output sentinel for
                stdout. ``None`` inherits the local stdout stream.
            stderr: Writable text stream or subprocess output sentinel for
                stderr. ``None`` inherits the local stderr stream; ``STDOUT``
                merges stderr into the stdout destination.
            capture_output: Whether to capture stdout and stderr in the result.

        Returns:
            The completed process result.

        Raises:
            subprocess.CalledProcessError: If ``check`` is true and the process
                exits unsuccessfully.
        """
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
        """Start a process without waiting for it to exit.

        Returns:
            A handle for monitoring and controlling the process.
        """
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
        """Get a process in this session.

        Args:
            process_id: Process identifier.
            wait: Whether the API call should wait for process completion.

        Returns:
            A process handle populated with the latest state.
        """
        state = await self._service.get_process(
            session_id=self.id, process_id=process_id, wait=wait
        )
        return Process(payload=state, service=self._service)

    async def query_processes(self) -> list[Process]:
        """Return handles for the processes in this session."""
        states = await self._service.query_processes(session_id=self.id)
        return [Process(payload=state, service=self._service) for state in states]

    async def refresh(self, *, include_system_routes: bool | None = None) -> Self:
        """Refresh this session's state and return the same handle."""
        payload = await self._service.get_runtime_session(
            session_id=self.id, include_system_routes=include_system_routes
        )
        self._apply_payload(payload)
        return self

    async def extend_execution_time_limit(self, duration: DurationInput) -> Self:
        """Increase the session execution time limit by a duration."""
        payload = await self._service.extend_runtime_session_timeout(
            session_id=self.id, duration=parse_required_duration_seconds(duration)
        )
        self._apply_payload(payload)
        return self

    async def update_network_policy(self, network_policy: JSONValue) -> Self:
        """Replace the session network policy."""
        payload = await self._service.update_runtime_session_network_policy(
            session_id=self.id, network_policy=network_policy
        )
        self._apply_payload(payload)
        return self

    async def snapshot(self, *, expiration: SnapshotExpirationInput = None) -> Snapshot:
        """Create a filesystem snapshot from this session.

        Args:
            expiration: Snapshot lifetime, or zero to disable expiration.

        Returns:
            A handle for the created snapshot.
        """
        result = await self._service.create_snapshot(
            session_id=self.id, expiration=_parse_snapshot_expiration(expiration)
        )
        self._apply_payload(result.session)
        return Snapshot(payload=result.snapshot, service=self._service)

    async def stop(self) -> Self:
        """Stop this runtime session and refresh the handle."""
        payload = await self._service.stop_runtime_session(session_id=self.id)
        self._apply_payload(payload)
        return self


class Sandbox(SandboxHandleBase[SandboxRuntimeSession]):
    """Control an asynchronous Vercel Sandbox.

    A sandbox has at most one active current session. Process and filesystem
    operations target the session recorded by this handle. Use ``session`` to
    resolve that session, resuming the sandbox into a replacement session when
    needed, and ``destroy`` to permanently remove the sandbox.
    """

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
        """Prepare a single-use current-session operation.

        Awaiting returns the sandbox's active current session. If the current
        session is stopped or otherwise unusable, the backend resumes the
        sandbox from its latest snapshot and returns the replacement session.
        Using the operation as an async context manager stops the returned
        session on exit.

        The returned session handle is independent. If resuming replaces the
        backend's current session, this existing ``Sandbox`` handle is not
        refreshed automatically.
        """
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
        """Run a process to completion in the current session.

        See ``SandboxRuntimeSession.run_process`` for argument behavior.
        """
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
        """Start a process in the current session without waiting for it."""
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
        """Get a process from the current session."""
        state = await self._service.get_process(
            session_id=self.current_session_id, process_id=process_id, wait=wait
        )
        return Process(payload=state, service=self._service)

    async def query_processes(self) -> list[Process]:
        """Return handles for processes in the current session."""
        states = await self._service.query_processes(session_id=self.current_session_id)
        return [Process(payload=state, service=self._service) for state in states]

    async def list_sessions(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> list[SandboxRuntimeSession]:
        """Return one page of runtime sessions belonging to this sandbox."""
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
        """Return one page of snapshots belonging to this sandbox."""
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
        """Increase the current session's execution time limit."""
        payload = await self._service.extend_runtime_session_timeout(
            session_id=self.current_session_id,
            duration=parse_required_duration_seconds(duration),
        )
        return self._apply_current_session_payload(payload)

    async def update_network_policy(self, network_policy: JSONValue) -> SandboxRuntimeSession:
        """Replace the current session's network policy."""
        payload = await self._service.update_runtime_session_network_policy(
            session_id=self.current_session_id, network_policy=network_policy
        )
        return self._apply_current_session_payload(payload)

    async def snapshot(self, *, expiration: SnapshotExpirationInput = None) -> Snapshot:
        """Create a filesystem snapshot from the current session."""
        result = await self._service.create_snapshot(
            session_id=self.current_session_id,
            expiration=_parse_snapshot_expiration(expiration),
        )
        self._apply_current_session_payload(result.session)
        return Snapshot(payload=result.snapshot, service=self._service)

    async def destroy(self) -> Self:
        """Permanently destroy the sandbox and refresh this handle."""
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
        """Update mutable sandbox configuration.

        Only non-``None`` values are sent, except ``snapshot_retention`` where
        explicitly passing ``None`` removes the retention policy.

        Returns:
            This handle refreshed with the updated sandbox state.
        """
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
    """Manage one asynchronous sandbox creation request.

    Await the operation to create a sandbox that remains alive, or use it as an
    async context manager to destroy the created sandbox on exit. An operation
    can be consumed only once.
    """

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
    """Manage one asynchronous current-session request.

    Await the operation to return the active current session, resuming the
    sandbox into a replacement session when necessary. Use it as an async
    context manager to stop the returned session on exit. An operation can be
    consumed only once.
    """

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
