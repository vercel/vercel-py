"""Sync runtime handles and entry points for Sandbox operations."""

import io
import signal as signal_module
import subprocess
import threading
import warnings
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import timedelta
from threading import Condition
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, TextIO, overload

from vercel._internal.core.byte_stream import SyncByteStreamRuntime
from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel._internal.core.polyfills import Self
from vercel._internal.core.time import parse_duration_seconds, parse_required_duration_seconds
from vercel.sandbox._internal.api_client import ProcessStream
from vercel.sandbox._internal.errors import (
    SandboxApiError,
    SandboxCleanupError,
    SandboxResponseError,
    SandboxTerminalStateError,
)
from vercel.sandbox._internal.filesystem_handle_common import _validate_open_options
from vercel.sandbox._internal.filesystem_handle_core import (
    BinaryReaderCore,
    BinaryWriterCore,
    FilesystemHandleBinding,
    FilesystemOperationBinding,
    TextReaderCore,
    TextWriterCore,
)
from vercel.sandbox._internal.interactive_session import (
    DEFAULT_COLS,
    DEFAULT_ROWS,
    SyncInteractiveTransport,
    resize_message,
    start_message,
)
from vercel.sandbox._internal.models import (
    _OMITTED,
    NO_PRIVATE_PARAMETERS,
    CompletedProcess,
    DirectoryEntry,
    DriveMount,
    DriveMountsInput,
    DriveQuery,
    DurationInput,
    FailoverRegionsInput,
    NetworkIdUpdate,
    NetworkPolicy,
    PrivateSandboxParameters,
    ProcessLog,
    ProcessSignal,
    RemotePath,
    SandboxQuery,
    SandboxResources,
    SandboxSource,
    SandboxStatus,
    SnapshotExpirationInput,
    SnapshotRetention,
    SnapshotRetentionUpdate,
    _parse_snapshot_expiration,
    _RemotePathT,
    _WriteFile,
    normalize_failover_regions,
)
from vercel.sandbox._internal.pagination import (
    QueryDrivesPage,
    QueryDrivesParams,
    QuerySandboxesPage,
    QuerySandboxesParams,
    QuerySessionsPage,
    QuerySessionsParams,
    QuerySnapshotsPage,
    QuerySnapshotsParams,
)
from vercel.sandbox._internal.process_output import (
    ProcessOutputRouter,
    _validate_reader_destination,
)
from vercel.sandbox._internal.recovery import (
    SandboxLifecycle,
    classify_sandbox_lifecycle_error,
    execute_with_sandbox_recovery,
)
from vercel.sandbox._internal.runtime_common import (
    DriveHandleBase,
    RuntimeSessionHandleBase,
    SandboxHandleBase,
    SnapshotHandleBase,
    _coerce_remote_path,
    _completed_process,
    _normalize_tar_path,
    _ProcessHandleState,
    _SandboxFilesystemBatchBase,
    _signal_number,
    _UploadFileEntry,
    _validate_file_mode,
)
from vercel.sandbox._internal.service import SandboxService, _SandboxTerminalState
from vercel.sandbox._internal.state import (
    CompletedProcessState,
    DriveState,
    InteractiveSessionState,
    ProcessState,
    RuntimeSessionStopState,
    SandboxRuntimeSessionState,
    SandboxState,
    SnapshotSessionState,
    SnapshotState,
)
from vercel.sandbox._internal.sync_filesystem_handle import (
    SyncSandboxBinaryReader,
    SyncSandboxBinaryWriter,
    SyncSandboxTextReader,
    SyncSandboxTextWriter,
)
from vercel.sandbox._internal.text_reader import SyncTextReader, _sync_text_readers

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer, WriteableBuffer


def _terminal_error(error: _SandboxTerminalState, sandbox: object) -> SandboxTerminalStateError:
    return SandboxTerminalStateError(
        f"Sandbox {error.sandbox.name!r} reached terminal state {error.status!r}",
        status=error.status,
        sandbox=sandbox,
    )


class SyncProcess(_ProcessHandleState):
    """Control and inspect a synchronously running sandbox process.

    The ``stdout`` and ``stderr`` readers each consume their process log
    stream once; reads make forward progress through the stream and cannot
    rewind. A reader is ``None`` when its stream was dropped with
    ``subprocess.DEVNULL`` or merged with ``subprocess.STDOUT``.
    """

    __slots__ = ("_service", "stderr", "stdout")

    stdout: SyncTextReader | None
    stderr: SyncTextReader | None

    def __init__(
        self,
        *,
        payload: ProcessState,
        service: SandboxService,
        stdout: int = subprocess.PIPE,
        stderr: int = subprocess.PIPE,
    ) -> None:
        super().__init__(payload)
        self._service = service
        self.stdout, self.stderr = _sync_text_readers(
            lambda: service.process_logs_response(session_id=self._session_id, process_id=self.id),
            stdout=stdout,
            stderr=stderr,
        )

    def refresh(self) -> Self:
        """Refresh the process state and return this handle."""
        payload = iter_coroutine(
            self._service.get_process(session_id=self._session_id, process_id=self.id)
        )
        self._apply_payload(payload)
        return self

    def wait(self) -> int:
        """Wait for the process to exit and return its exit code.

        Raises:
            SandboxResponseError: If the service response omits the process
                return code.
        """
        payload = iter_coroutine(
            self._service.get_process(session_id=self._session_id, process_id=self.id, wait=True)
        )
        self._apply_payload(payload)
        if self.returncode is None:
            raise SandboxResponseError("Wait response did not include a process return code")
        return self.returncode

    def communicate(self, input: None = None) -> tuple[str | None, str | None]:
        """Read all output and wait for the process to exit.

        Args:
            input: Reserved for subprocess compatibility. Process standard
                input is not supported and must be ``None``.

        Returns:
            A ``(stdout, stderr)`` tuple. A stream without a reader is
            ``None``, so merging with ``stderr=subprocess.STDOUT`` returns
            ``(merged, None)``.

        Raises:
            NotImplementedError: If ``input`` is not ``None``.
        """
        if input is not None:
            raise NotImplementedError("process stdin is not supported")
        stdout = None if self.stdout is None else self.stdout.read()
        stderr = None if self.stderr is None else self.stderr.read()
        self.wait()
        return stdout, stderr

    def send_signal(self, signal: int | str | signal_module.Signals | ProcessSignal) -> None:
        """Send a signal to the running process.

        Args:
            signal: Numeric signal, ``ProcessSignal`` member, or name such as
                ``"TERM"`` or ``"SIGTERM"``.

        Note:
            Passing ``signal.Signals`` directly is deprecated. Use
            ``ProcessSignal`` so signal availability does not depend on the
            SDK host platform.
        """
        payload = iter_coroutine(
            self._service.send_process_signal(
                session_id=self._session_id,
                process_id=self.id,
                signal=_signal_number(signal),
            )
        )
        self._apply_payload(payload)

    def terminate(self) -> None:
        """Request graceful process termination with ``SIGTERM``."""
        self.send_signal(ProcessSignal.SIGTERM)

    def kill(self) -> None:
        """Terminate the process immediately with ``SIGKILL``."""
        self.send_signal(ProcessSignal.SIGKILL)


_BLOCK_EXITED_MESSAGE = "open_process() block exited before the process completed"


class SyncRunningProcess(_ProcessHandleState):
    """Observe and control a process whose output the caller reads.

    The handle owns no thread. Output is read from the connection only while
    the caller iterates the handle or calls ``wait``. Each event is written to
    its destination and then yielded, so unread output stays in the connection
    rather than in a buffer. Iteration can be broken off and resumed. It ends
    when the final metadata arrives, and ``wait`` returns the completed result.

    Only one thread reads at a time. ``send_signal``, ``terminate``, and
    ``kill`` never wait for a reader, so they can be called from another
    thread while ``wait`` is pumping.
    """

    __slots__ = (
        "_failure",
        "_output",
        "_pump_lock",
        "_result",
        "_service",
        "_state_lock",
        "_stream",
    )

    def __init__(
        self,
        *,
        stream: ProcessStream,
        output: ProcessOutputRouter,
        service: SandboxService,
    ) -> None:
        super().__init__(stream.initial)
        self._stream = stream
        self._output = output
        self._service = service
        self._pump_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._result: CompletedProcess | None = None
        self._failure: BaseException | None = None

    @property
    def result(self) -> CompletedProcess:
        """The completed process result.

        The result is available once the process has completed, either by
        ``wait`` or by iterating to the end, and before the block exits.

        Raises:
            RuntimeError: If the process has not completed, or the block exited
                before it did.
        """
        if self._result is None:
            raise RuntimeError(
                "process has not completed; the result is available after wait() "
                "returns or the open_process() block exits normally"
            )
        return self._result

    def __iter__(self) -> "SyncRunningProcess":
        return self

    def __next__(self) -> ProcessLog:
        """Read, route, and return the next output event.

        The event is written to its destination before it is returned. The
        iteration ends when the final metadata arrives. The process result is
        complete by then, so ``wait`` returns it without reading more.

        Raises:
            Exception: The output-stream failure that ended routing, if any.
        """
        with self._pump_lock:
            if self._result is not None:
                raise StopIteration
            if self._failure is not None:
                raise self._failure
            event = self._pump_once()
            if event is None:
                raise StopIteration
            return event

    def wait(self) -> CompletedProcess:
        """Read the rest of the output and return the completed result.

        Output is routed to its destinations while this runs. Waiting does not
        check the exit code; ``check`` applies when the block exits. Calling it
        again returns the same result.

        Raises:
            Exception: The output-stream failure that ended routing, if any.
            RuntimeError: If the block exited before the process completed.
        """
        for _ in self:
            pass
        return self._settled_result()

    def send_signal(self, signal: int | str | signal_module.Signals | ProcessSignal) -> None:
        """Send a signal to the running process.

        Safe to call from another thread while ``wait`` or iteration is
        reading output.

        Args:
            signal: Numeric signal, ``ProcessSignal`` member, or name such as
                ``"TERM"`` or ``"SIGTERM"``.
        """
        payload = iter_coroutine(
            self._service.send_process_signal(
                session_id=self._session_id,
                process_id=self.id,
                signal=_signal_number(signal),
            )
        )
        with self._state_lock:
            # The reader's final metadata is authoritative. A signal response
            # that raced with process exit must not reset ``returncode``.
            if self.returncode is None:
                self._apply_payload(payload)

    def terminate(self) -> None:
        """Request graceful process termination with ``SIGTERM``."""
        self.send_signal(ProcessSignal.SIGTERM)

    def kill(self) -> None:
        """Terminate the process immediately with ``SIGKILL``."""
        self.send_signal(ProcessSignal.SIGKILL)

    def _pump_once(self) -> ProcessLog | None:
        """Advance the stream by one event. Must hold the pump lock."""
        stream = self._stream
        try:
            event = iter_coroutine(stream.next_event())
            if event is None:
                final = stream.final
                assert final is not None
                with self._state_lock:
                    self._apply_payload(final)
                completed = iter_coroutine(stream.finish())
                iter_coroutine(stream.aclose())
                stdout, stderr = self._output.captured()
                self._complete(
                    _completed_process(
                        CompletedProcessState(process=completed, stdout=stdout, stderr=stderr)
                    )
                )
                return None
            self._output.route(event)
        except Exception as error:
            self._fail(error)
            iter_coroutine(stream.aclose())
            raise
        except BaseException:
            self._fail(RuntimeError(_BLOCK_EXITED_MESSAGE))
            iter_coroutine(stream.aclose())
            raise
        return event

    def _settled_result(self) -> CompletedProcess:
        if self._failure is not None:
            raise self._failure
        if self._result is None:
            raise RuntimeError(_BLOCK_EXITED_MESSAGE)
        return self._result

    def _complete(self, result: CompletedProcess) -> None:
        with self._state_lock:
            self._result = result

    def _fail(self, error: BaseException) -> None:
        """Record a failure unless the process already completed or failed."""
        with self._state_lock:
            if self._result is None and self._failure is None:
                self._failure = error

    def _abandon(self) -> None:
        """Stop reading and close the stream without waiting for the process."""
        self._fail(RuntimeError(_BLOCK_EXITED_MESSAGE))
        iter_coroutine(self._stream.aclose())


class SyncOpenProcessOperation:
    """Scope one process whose output the caller reads inside a block.

    Entering starts the process and yields a ``SyncRunningProcess`` without
    reading any output. Output is read only when the caller iterates the handle
    or calls ``wait``. Leaving the block normally drains the remaining output,
    waits for exit, and then applies ``check``. Leaving it through an exception
    closes the stream without waiting, and does not kill the process; call
    ``kill`` inside the block for that.

    The operation can be entered once and has no direct call form.
    """

    __slots__ = ("_check", "_consumed", "_handle", "_open_stream", "_output", "_service")

    def __init__(
        self,
        *,
        open_stream: Callable[[], ProcessStream],
        output: ProcessOutputRouter,
        service: SandboxService,
        check: bool,
    ) -> None:
        self._consumed = False
        self._open_stream = open_stream
        self._output = output
        self._service = service
        self._check = check
        self._handle: SyncRunningProcess | None = None

    def _mark_consumed(self) -> None:
        if self._consumed:
            raise RuntimeError("open_process() operations can only be used once")
        self._consumed = True

    def __enter__(self) -> SyncRunningProcess:
        self._mark_consumed()
        stream = self._open_stream()
        handle = SyncRunningProcess(stream=stream, output=self._output, service=self._service)
        self._handle = handle
        return handle

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        handle = self._handle
        assert handle is not None
        if exc_type is not None:
            handle._abandon()
            return None
        try:
            result = handle.wait()
        except BaseException:
            # An interrupt while draining must close the stream too, so output
            # does not keep arriving after the block has raised.
            handle._abandon()
            raise
        if self._check:
            result.check_returncode()
        return None

    def __del__(self) -> None:
        if self._consumed:
            return
        warnings.warn(
            "open_process() operation was never entered; use "
            "`with box.open_process(...)` to run the process",
            RuntimeWarning,
            stacklevel=2,
        )


class SyncInteractiveStream(io.RawIOBase):
    """Terminal I/O for an interactive session, as a plain raw binary stream."""

    def __init__(self, session: "SyncInteractiveSession") -> None:
        super().__init__()
        self._session = session
        self._buffer = b""

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return True

    def readinto(self, buffer: "WriteableBuffer", /) -> int:
        self._checkClosed()
        view = memoryview(buffer).cast("B")
        if not len(view):
            return 0
        if not self._buffer:
            data = self._session._read()
            if data is None:
                self._session._raise_for_failure()
                return 0
            self._buffer = data
        count = min(len(view), len(self._buffer))
        view[:count] = self._buffer[:count]
        self._buffer = self._buffer[count:]
        return count

    def write(self, data: "ReadableBuffer", /) -> int:
        self._checkClosed()
        payload = bytes(memoryview(data).cast("B"))
        self._session._write(payload)
        return len(payload)

    def close(self) -> None:
        if self.closed:
            return
        try:
            super().close()
        finally:
            self._session._aclose_transport()


class SyncInteractiveSession:
    """Control an interactive sandbox process attached to a PTY.

    Terminal I/O goes through :attr:`stream`; this object carries the controls
    that are not part of the byte stream. The session cannot be reattached
    after it closes.
    """

    __slots__ = ("_transport", "stream")

    stream: SyncInteractiveStream
    """Raw terminal output and input."""

    def __init__(self, *, transport: SyncInteractiveTransport) -> None:
        self._transport = transport
        self.stream = SyncInteractiveStream(self)

    @property
    def returncode(self) -> int | None:
        """Exit code of the remote process, or ``None`` while it runs."""
        return self._transport.returncode

    def resize(self, cols: int, rows: int) -> None:
        """Tell the remote terminal its new size in characters."""
        self.stream._checkClosed()
        iter_coroutine(self._transport.send_json(resize_message(cols, rows)))

    def wait(self, timeout: float | None = None) -> int | None:
        """Wait for the remote process to exit and return its exit code.

        Output that arrives meanwhile is buffered for :attr:`stream` rather
        than discarded. The buffer is bounded, so with more output pending
        than it holds this blocks until a reader makes room; sequential code
        should read to the end of the stream first. Returns ``None`` if the
        session was closed locally first, or if the service reported an exit
        code that is not an integer.

        Args:
            timeout: Seconds to wait before giving up. Waits indefinitely
                when ``None``.

        Raises:
            TimeoutError: If the process is still running at the deadline.
            ConnectionError: If the connection ended, including a clean
                disconnect, before the process reported its exit.
        """
        return iter_coroutine(self._transport.wait(timeout))

    def close(self) -> None:
        """Close the session and release the connection."""
        self.stream.close()

    def _read(self, timeout: float | None = None) -> bytes | None:
        return iter_coroutine(self._transport.receive(timeout=timeout)).data

    def _raise_for_failure(self) -> None:
        failure = self._transport.failure
        if failure is not None:
            raise ConnectionError("Interactive session connection failed") from failure

    def _write(self, data: bytes) -> None:
        iter_coroutine(self._transport.send_bytes(data))

    def _aclose_transport(self) -> None:
        iter_coroutine(self._transport.close())


class SyncInteractiveSessionOperation:
    """Scope one interactive session.

    Entering opens the connection and exiting closes it. Unlike the other
    single-use operations in this SDK this one has no direct call form: the
    connection is owned by the scope that opened it.
    """

    def __init__(self, scope: AbstractContextManager["SyncInteractiveSession"]) -> None:
        self._scope = scope
        self._consumed = False

    def __enter__(self) -> "SyncInteractiveSession":
        if self._consumed:
            raise RuntimeError("open_interactive() operations can only be used once")
        self._consumed = True
        return self._scope.__enter__()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        return self._scope.__exit__(exc_type, exc, traceback)


@contextmanager
def _open_interactive_session(
    open_state: Callable[[], InteractiveSessionState],
    *,
    command: str,
    args: Sequence[str] | None,
    cwd: str | None,
    env: Mapping[str, str] | None,
    sudo: bool,
    cols: int,
    rows: int,
) -> Iterator[SyncInteractiveSession]:
    transport = SyncInteractiveTransport(open_state())
    iter_coroutine(transport.connect())
    try:
        iter_coroutine(
            transport.send_json(
                start_message(
                    command=command,
                    args=args,
                    cwd=cwd,
                    env=env,
                    sudo=sudo,
                    cols=cols,
                    rows=rows,
                )
            )
        )
        pty = SyncInteractiveSession(transport=transport)
        try:
            yield pty
        finally:
            pty.close()
    finally:
        iter_coroutine(transport.close())


class SyncSnapshot(SnapshotHandleBase):
    """Represent a sandbox filesystem snapshot."""

    __slots__ = ("_service",)

    def __init__(self, *, payload: SnapshotState, service: SandboxService) -> None:
        super().__init__(payload)
        self._service = service

    def delete(self) -> Self:
        """Delete the snapshot and refresh this handle."""
        payload = iter_coroutine(self._service.delete_snapshot(snapshot_id=self.id))
        self._apply_payload(payload)
        return self


class SyncDrive(DriveHandleBase):
    """Operate synchronously on a project-local persistent Drive."""

    __slots__ = ("_service",)

    def __init__(self, *, payload: DriveState, service: SandboxService) -> None:
        super().__init__(payload)
        self._service = service

    def snapshot(self) -> DriveMount:
        """Return a read-only snapshot mount for this Drive."""
        return DriveMount(self, mode="snapshot")

    def delete(self) -> Self:
        """Delete by project and name, then refresh this handle from the response.

        Deletion does not use the Drive ID. If the name now refers to a replacement
        Drive, this method deletes that replacement and updates this handle to it.
        """
        payload = iter_coroutine(self._service.delete_drive(name=self.name))
        self._apply_payload(payload)
        return self


class SyncSandboxFilesystem:
    """Perform synchronous filesystem operations in a sandbox session."""

    __slots__ = ("_execution", "_service", "_write_files_cwd")

    def __init__(
        self,
        *,
        service: SandboxService,
        execution: FilesystemOperationBinding,
        write_files_cwd: Callable[[RemotePath | None], str],
    ) -> None:
        self._service = service
        self._execution = execution
        self._write_files_cwd = write_files_cwd

    @overload
    def open(
        self,
        path: RemotePath,
        mode: Literal["r"] = "r",
        *,
        cwd: RemotePath | None = None,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        size: None = None,
        permissions: None = None,
    ) -> SyncSandboxTextReader: ...

    @overload
    def open(
        self,
        path: RemotePath,
        mode: Literal["rb"],
        *,
        cwd: RemotePath | None = None,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        size: None = None,
        permissions: None = None,
    ) -> SyncSandboxBinaryReader: ...

    @overload
    def open(
        self,
        path: RemotePath,
        mode: Literal["w"],
        *,
        cwd: RemotePath | None = None,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        size: None = None,
        permissions: int | None = None,
    ) -> SyncSandboxTextWriter: ...

    @overload
    def open(
        self,
        path: RemotePath,
        mode: Literal["wb"],
        *,
        cwd: RemotePath | None = None,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        size: int | None = None,
        permissions: int | None = None,
    ) -> SyncSandboxBinaryWriter: ...

    def open(
        self,
        path: RemotePath,
        mode: str = "r",
        *,
        cwd: RemotePath | None = None,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        size: int | None = None,
        permissions: int | None = None,
    ) -> (
        SyncSandboxBinaryReader
        | SyncSandboxTextReader
        | SyncSandboxBinaryWriter
        | SyncSandboxTextWriter
    ):
        """Create a lazy, single-use sequential file handle."""
        path, mode, encoding, errors, newline, size, permissions = _validate_open_options(
            path,
            mode,
            encoding=encoding,
            errors=errors,
            newline=newline,
            size=size,
            permissions=permissions,
        )
        normalized_cwd = None if cwd is None else _coerce_remote_path(cwd)
        binding = FilesystemHandleBinding(
            service=self._service,
            runtime=self._service.staging_file_runtime,
            execution=self._execution,
            write_files_cwd=self._write_files_cwd,
            path=path,
            cwd=normalized_cwd,
        )
        if mode == "rb":
            return SyncSandboxBinaryReader(BinaryReaderCore(binding))
        if mode == "r":
            return SyncSandboxTextReader(TextReaderCore(binding, encoding, errors, newline))
        if mode == "wb":
            return SyncSandboxBinaryWriter(
                BinaryWriterCore(binding.write_target_source(size=size, permissions=permissions))
            )
        return SyncSandboxTextWriter(
            TextWriterCore(binding, encoding, errors, newline, permissions)
        )

    async def _collect_output(self, command: ProcessState) -> tuple[str, str]:
        stdout: list[str] = []
        stderr: list[str] = []
        async for event in self._service.process_logs(
            session_id=command.session_id, process_id=command.id
        ):
            if event.stream == "stdout":
                stdout.append(event.data)
            else:
                stderr.append(event.data)
        return "".join(stdout), "".join(stderr)

    def mkdir(
        self, path: RemotePath, *, cwd: RemotePath | None = None, recursive: bool = True
    ) -> None:
        """Create a directory.

        Args:
            path: Absolute path or path relative to ``cwd``.
            cwd: Base directory for a relative path.
            recursive: Whether to create missing parent directories.

        Raises:
            SandboxPathNotFoundError: If a parent directory is missing and
                ``recursive`` is false.
        """
        remote_path = _coerce_remote_path(path)
        remote_cwd = None if cwd is None else _coerce_remote_path(cwd)
        iter_coroutine(
            self._execution.execute(
                lambda session_id: self._service.mkdir(
                    session_id=session_id,
                    path=remote_path,
                    cwd=remote_cwd,
                    recursive=recursive,
                )
            )
        )

    def read_bytes(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bytes:
        """Read a file as bytes.

        Args:
            path: Absolute path or path relative to ``cwd``.
            cwd: Base directory for a relative path.

        Returns:
            The complete file contents.

        Raises:
            SandboxPathNotFoundError: If the file does not exist.
        """
        remote_path = _coerce_remote_path(path)
        remote_cwd = None if cwd is None else _coerce_remote_path(cwd)
        return iter_coroutine(
            self._execution.execute(
                lambda session_id: self._service.read_bytes(
                    operation="read_bytes",
                    session_id=session_id,
                    path=remote_path,
                    cwd=remote_cwd,
                )
            )
        )

    def read_text(
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

        Raises:
            SandboxPathNotFoundError: If the file does not exist.
        """
        return self.read_bytes(path, cwd=cwd).decode(encoding, errors=errors)

    def write_bytes(
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

        Raises:
            SandboxFilesystemWriteError: If the write request fails.
        """
        self._write_files(
            [_WriteFile(path=_coerce_remote_path(path), content=data, mode=mode)], cwd=cwd
        )

    def write_text(
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

        Raises:
            SandboxFilesystemWriteError: If the write request fails.
        """
        self._write_files(
            [
                _WriteFile(
                    path=_coerce_remote_path(path),
                    content=text.encode(encoding, errors=errors),
                    mode=mode,
                )
            ],
            cwd=cwd,
        )

    def _write_files(self, files: Sequence[_WriteFile], *, cwd: RemotePath | None = None) -> None:
        """Upload an in-memory write set with deliberate at-least-once replay.

        Sandbox-level lifecycle recovery may replay the full archive once. The
        first non-atomic upload may already have written some paths when it
        reports a recognized lifecycle failure.
        """
        for file in files:
            _validate_file_mode(file.mode)

        async def write(session_id: str) -> None:
            resolved_cwd = self._write_files_cwd(cwd)
            entries = [
                _UploadFileEntry(
                    path=file.path,
                    size=len(file.content),
                    source=SyncByteStreamRuntime.reader(file.content),
                    mode=file.mode,
                    archive_path=_normalize_tar_path(file.path, cwd=resolved_cwd),
                )
                for file in files
            ]
            await self._service.write_stream_archive(
                session_id=session_id,
                entries=entries,
                paths=tuple(entry.path for entry in entries),
                cwd=resolved_cwd,
            )

        iter_coroutine(self._execution.execute(write))

    def batch(self, *, cwd: RemotePath | None = None) -> "SyncSandboxFilesystemBatch":
        """Create a context manager that stages files for one write request.

        The staged files are uploaded together, but the upload is not
        all-or-nothing: a failure partway through can leave some files written.

        Args:
            cwd: Base directory shared by staged relative paths.

        Returns:
            A batch that uploads its staged files on successful context exit.
        """
        return SyncSandboxFilesystemBatch(
            write_files=lambda files: self._write_files(files, cwd=cwd)
        )

    def exists(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bool:
        """Return whether a filesystem entry exists.

        Raises:
            SandboxFilesystemCommandError: If the remote check fails.
        """
        remote_path = _coerce_remote_path(path)
        remote_cwd = None if cwd is None else _coerce_remote_path(cwd)
        return iter_coroutine(
            self._execution.execute(
                lambda session_id: self._service.exists(
                    session_id=session_id,
                    path=remote_path,
                    cwd=remote_cwd,
                    collect_output=self._collect_output,
                )
            )
        )

    def is_file(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bool:
        """Return whether a path exists and is a regular file.

        Raises:
            SandboxFilesystemCommandError: If the remote check fails.
        """
        remote_path = _coerce_remote_path(path)
        remote_cwd = None if cwd is None else _coerce_remote_path(cwd)
        return iter_coroutine(
            self._execution.execute(
                lambda session_id: self._service.is_file(
                    session_id=session_id,
                    path=remote_path,
                    cwd=remote_cwd,
                    collect_output=self._collect_output,
                )
            )
        )

    def is_dir(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bool:
        """Return whether a path exists and is a directory.

        Raises:
            SandboxFilesystemCommandError: If the remote check fails.
        """
        remote_path = _coerce_remote_path(path)
        remote_cwd = None if cwd is None else _coerce_remote_path(cwd)
        return iter_coroutine(
            self._execution.execute(
                lambda session_id: self._service.is_dir(
                    session_id=session_id,
                    path=remote_path,
                    cwd=remote_cwd,
                    collect_output=self._collect_output,
                )
            )
        )

    def listdir(
        self, path: RemotePath = ".", *, cwd: RemotePath | None = None
    ) -> list[DirectoryEntry]:
        """List the direct children of a directory.

        Args:
            path: Directory to list.
            cwd: Base directory for a relative path.

        Returns:
            The directory entries returned by the remote filesystem.

        Raises:
            SandboxFilesystemCommandError: If the listing fails, including
                when the directory does not exist.
        """
        remote_path = _coerce_remote_path(path)
        remote_cwd = None if cwd is None else _coerce_remote_path(cwd)
        return iter_coroutine(
            self._execution.execute(
                lambda session_id: self._service.listdir(
                    session_id=session_id,
                    path=remote_path,
                    cwd=remote_cwd,
                    collect_output=self._collect_output,
                )
            )
        )

    def remove(
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

        Raises:
            SandboxFilesystemCommandError: If removal fails, including when
                the path is missing and ``missing_ok`` is false.
        """
        remote_path = _coerce_remote_path(path)
        remote_cwd = None if cwd is None else _coerce_remote_path(cwd)
        iter_coroutine(
            self._execution.execute(
                lambda session_id: self._service.remove(
                    session_id=session_id,
                    path=remote_path,
                    cwd=remote_cwd,
                    recursive=recursive,
                    missing_ok=missing_ok,
                    collect_output=self._collect_output,
                )
            )
        )

    def rename(
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

        Raises:
            SandboxFilesystemCommandError: If the rename fails.
        """
        remote_source = _coerce_remote_path(source)
        remote_destination = _coerce_remote_path(destination)
        remote_cwd = None if cwd is None else _coerce_remote_path(cwd)
        iter_coroutine(
            self._execution.execute(
                lambda session_id: self._service.rename(
                    session_id=session_id,
                    source=remote_source,
                    destination=remote_destination,
                    cwd=remote_cwd,
                    collect_output=self._collect_output,
                )
            )
        )


class SyncSandboxFilesystemBatch(_SandboxFilesystemBatchBase):
    """Stage multiple file writes for one synchronous filesystem request.

    Create batches with ``SyncSandboxFilesystem.batch`` and use them only
    inside their context. Exiting the context uploads the staged files and
    raises ``SandboxFilesystemWriteError`` if the write request fails.
    """

    __slots__ = ("_write_files",)

    def __init__(self, *, write_files: Callable[[Sequence[_WriteFile]], None]) -> None:
        super().__init__()
        self._write_files = write_files

    def __enter__(self) -> "SyncSandboxFilesystemBatch":
        self._enter()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        files = self._close()
        if exc_type is None and files:
            self._write_files(files)


class SyncSandboxRuntimeSession(RuntimeSessionHandleBase):
    """Represent one session in a sandbox's execution history.

    A sandbox has at most one active current session. The handle is a context
    manager that stops this session on exit; exiting raises
    ``SandboxCleanupError`` if stopping the session fails.
    """

    __slots__ = ("_service", "fs")

    def __init__(self, *, payload: SandboxRuntimeSessionState, service: SandboxService) -> None:
        super().__init__(payload)
        self._service = service
        self.fs = SyncSandboxFilesystem(
            service=service,
            execution=FilesystemOperationBinding.direct(self.id),
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
            _cleanup_exact_sync_session(self)
        except SandboxCleanupError as cleanup_error:
            if exc is None:
                raise
            warnings.warn(
                str(cleanup_error),
                RuntimeWarning,
                source=cleanup_error,
                stacklevel=2,
            )

    def run_process(
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
            kill_after: Duration after which the service kills the process
                with ``SIGKILL``.
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
        state = iter_coroutine(
            self._service.run_process(
                session_id=self.id,
                command=command,
                args=args,
                cwd=cwd,
                env=env,
                sudo=sudo,
                kill_after=parse_duration_seconds(kill_after),
                output_router=output_router,
            )
        )
        result = _completed_process(state)
        if check:
            result.check_returncode()
        return result

    def open_process(
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
    ) -> SyncOpenProcessOperation:
        """Start a process and read its output inside a ``with`` block.

        The process starts on entry and yields a ``SyncRunningProcess`` whose
        ``id`` is available immediately, before any output is read. The caller
        drives the output: iterating the handle reads and routes one event at a
        time, and ``wait`` reads the rest. Output the caller has not read stays
        in the connection, so nothing is buffered beyond what ``run_process``
        already keeps for captured output. Leaving the block normally drains
        the remaining output and waits for exit. Leaving it through an exception
        closes the output stream without waiting, and does not kill the process.

        Args:
            command: Executable or command name.
            args: Command arguments, excluding the executable.
            cwd: Process working directory.
            env: Environment variables added to the process.
            sudo: Whether to run with elevated privileges.
            kill_after: Duration after which the service kills the process
                with ``SIGKILL``.
            check: Whether to raise for a nonzero exit code when the block exits
                normally.
            stdout: Writable text stream or subprocess output sentinel for
                stdout. ``None`` inherits the local stdout stream.
            stderr: Writable text stream or subprocess output sentinel for
                stderr. ``None`` inherits the local stderr stream; ``STDOUT``
                merges stderr into the stdout destination.
            capture_output: Whether to capture stdout and stderr in the result.

        Returns:
            A single-use operation to enter with ``with``.

        Raises:
            ValueError: If the output options are invalid. Raised before any
                request is sent.

        Example::

            with session.open_process("pytest", capture_output=True) as proc:
                print(proc.id)
                result = proc.wait()
        """
        output = ProcessOutputRouter(stdout=stdout, stderr=stderr, capture_output=capture_output)
        parsed_kill_after = parse_duration_seconds(kill_after)
        return SyncOpenProcessOperation(
            open_stream=lambda: iter_coroutine(
                self._service.open_process(
                    session_id=self.id,
                    command=command,
                    args=args,
                    cwd=cwd,
                    env=env,
                    sudo=sudo,
                    kill_after=parsed_kill_after,
                )
            ),
            output=output,
            service=self._service,
            check=check,
        )

    def create_process(
        self,
        command: str,
        args: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
        stdout: int = subprocess.PIPE,
        stderr: int = subprocess.PIPE,
    ) -> SyncProcess:
        """Start a process without waiting for it to exit.

        Args:
            command: Executable or command name.
            args: Command arguments, excluding the executable.
            cwd: Process working directory.
            env: Environment variables added to the process.
            sudo: Whether to run with elevated privileges.
            kill_after: Duration after which the service kills the process
                with ``SIGKILL``.
            stdout: ``subprocess.PIPE`` (default) for a live reader or
                ``subprocess.DEVNULL`` to drop the stream.
            stderr: ``subprocess.PIPE`` (default), ``subprocess.DEVNULL``, or
                ``subprocess.STDOUT`` to merge stderr into the stdout reader
                in arrival order.

        Returns:
            A handle for monitoring and controlling the process.
        """
        stdout = _validate_reader_destination(stdout, name="stdout")
        stderr = _validate_reader_destination(stderr, name="stderr", allow_stdout_merge=True)
        state = iter_coroutine(
            self._service.create_process(
                session_id=self.id,
                command=command,
                args=list(args) if args is not None else None,
                cwd=cwd,
                env=env,
                sudo=sudo,
                kill_after=parse_duration_seconds(kill_after),
            )
        )
        return SyncProcess(payload=state, service=self._service, stdout=stdout, stderr=stderr)

    def open_interactive(
        self,
        command: str = "/bin/bash",
        args: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        cols: int = DEFAULT_COLS,
        rows: int = DEFAULT_ROWS,
    ) -> SyncInteractiveSessionOperation:
        """Start a process attached to a PTY and scope its session.

        The connection is opened on entry and closed on exit, so the session
        must be used as a context manager::

            with session.open_interactive() as pty:
                pty.stream.write(b"echo hi\n")

        Args:
            command: Executable or command name.
            args: Command arguments, excluding the executable.
            cwd: Process working directory.
            env: Environment variables added to the process. ``TERM`` defaults
                to ``xterm-256color`` so full-screen programs render correctly.
            sudo: Whether to run with elevated privileges.
            cols: Terminal width in characters.
            rows: Terminal height in characters.

        Returns:
            A context manager yielding the terminal session.
        """
        return SyncInteractiveSessionOperation(
            _open_interactive_session(
                lambda: iter_coroutine(self._service.open_interactive(session_id=self.id)),
                command=command,
                args=args,
                cwd=cwd if cwd is not None else self.cwd,
                env=env,
                sudo=sudo,
                cols=cols,
                rows=rows,
            )
        )

    def get_process(self, process_id: str, *, wait: bool = False) -> SyncProcess:
        """Get a process in this session."""
        state = iter_coroutine(
            self._service.get_process(session_id=self.id, process_id=process_id, wait=wait)
        )
        return SyncProcess(payload=state, service=self._service)

    def query_processes(self) -> list[SyncProcess]:
        """Return handles for the processes in this session."""
        states = iter_coroutine(self._service.query_processes(session_id=self.id))
        return [SyncProcess(payload=state, service=self._service) for state in states]

    def refresh(self, *, include_system_routes: bool | None = None) -> Self:
        """Refresh this session's state and return the same handle."""
        payload = iter_coroutine(
            self._service.get_runtime_session(
                session_id=self.id, include_system_routes=include_system_routes
            )
        )
        self._apply_payload_with_parent(payload)
        return self

    def extend_execution_time_limit(self, duration: DurationInput) -> Self:
        """Increase the session execution time limit by a duration.

        The service rejects durations shorter than one second.
        """
        payload = iter_coroutine(
            self._service.extend_runtime_session_timeout(
                session_id=self.id, duration=parse_required_duration_seconds(duration)
            )
        )
        self._apply_payload(payload)
        return self

    def update_network_policy(self, network_policy: NetworkPolicy) -> Self:
        """Replace the session network policy."""
        payload = iter_coroutine(
            self._service.update_runtime_session_network_policy(
                session_id=self.id, network_policy=network_policy
            )
        )
        self._apply_payload(payload)
        return self

    def snapshot(self, *, expiration: SnapshotExpirationInput = None) -> SyncSnapshot:
        """Create a filesystem snapshot from this session."""
        result = iter_coroutine(
            self._service.create_snapshot(
                session_id=self.id, expiration=_parse_snapshot_expiration(expiration)
            )
        )
        self._apply_payload(result.session)
        return SyncSnapshot(payload=result.snapshot, service=self._service)

    def stop(self) -> Self:
        """Stop this runtime session and refresh the handle."""
        result = iter_coroutine(self._service.stop_runtime_session(session_id=self.id))
        self._apply_stop_result(result)
        return self


def _cleanup_exact_sync_session(handle: SyncSandboxRuntimeSession) -> None:
    if handle.status == SandboxStatus.STOPPED:
        return
    try:
        result = iter_coroutine(handle._service.stop_runtime_session(session_id=handle.id))
        handle._apply_stop_result(result)
    except SandboxApiError as error:
        if classify_sandbox_lifecycle_error(error) is SandboxLifecycle.STOPPED:
            handle._mark_stopped()
            return
        raise SandboxCleanupError(
            f"Failed to clean up sandbox runtime session {handle.id!r}",
            resource_type="sandbox_runtime_session",
            resource_id=handle.id,
            cause=error,
        ) from error
    except Exception as error:
        raise SandboxCleanupError(
            f"Failed to clean up sandbox runtime session {handle.id!r}",
            resource_type="sandbox_runtime_session",
            resource_id=handle.id,
            cause=error,
        ) from error


@dataclass(slots=True)
class _SyncRecoveryAttempt:
    status: Literal["in_flight", "succeeded", "failed", "abandoned"] = "in_flight"
    error: BaseException | None = None


class SyncSandbox(SandboxHandleBase[SyncSandboxRuntimeSession]):
    """Control a synchronous Vercel Sandbox.

    A sandbox has at most one active current session. Process and filesystem
    session-bound operations lazily resume when needed and update this handle's
    canonical current session. Use ``session`` for an explicit exact-session
    lifecycle, ``stop`` to stop the current session, and ``destroy`` to
    permanently remove the sandbox.
    """

    __slots__ = ("_recovery_attempt", "_recovery_condition", "_service", "fs")

    def __init__(
        self,
        *,
        payload: SandboxState,
        service: SandboxService,
        include_system_routes: bool | None = None,
    ) -> None:
        self._recovery_condition = Condition()
        super().__init__(
            payload,
            session_factory=lambda session: SyncSandboxRuntimeSession(
                payload=session, service=service
            ),
            include_system_routes=include_system_routes,
        )
        self._service = service
        self._recovery_attempt: _SyncRecoveryAttempt | None = None
        self.fs = SyncSandboxFilesystem(
            service=service,
            execution=FilesystemOperationBinding(
                execute=lambda operation: execute_with_sandbox_recovery(
                    operation, coordinator=self
                ),
                bind=self._ensure_active,
            ),
            write_files_cwd=self._write_files_cwd,
        )

    def _apply_payload(self, payload: SandboxState) -> None:
        with self._recovery_condition:
            super()._apply_payload(payload)

    def _apply_current_session_payload(
        self, payload: SandboxRuntimeSessionState
    ) -> SyncSandboxRuntimeSession:
        with self._recovery_condition:
            return super()._apply_current_session_payload(payload)

    def _apply_session_payload_from_child(
        self,
        child: RuntimeSessionHandleBase,
        payload: SandboxRuntimeSessionState,
    ) -> None:
        with self._recovery_condition:
            super()._apply_session_payload_from_child(child, payload)

    def _apply_session_stop_from_child(
        self,
        child: RuntimeSessionHandleBase,
        result: RuntimeSessionStopState,
    ) -> None:
        with self._recovery_condition:
            super()._apply_session_stop_from_child(child, result)

    def _capture_recovery_session_id(self) -> str:
        with self._recovery_condition:
            return super()._capture_recovery_session_id()

    async def _await_shared_resume(self) -> None:
        while True:
            with self._recovery_condition:
                attempt = self._recovery_attempt
                if attempt is None:
                    attempt = _SyncRecoveryAttempt()
                    self._recovery_attempt = attempt
                    owns_attempt = True
                else:
                    owns_attempt = False

                if not owns_attempt:
                    while attempt.status == "in_flight":
                        self._recovery_condition.wait()
                    if attempt.status == "succeeded":
                        return
                    if attempt.status == "failed":
                        assert attempt.error is not None
                        raise attempt.error
                    continue

            outcome: Literal["succeeded", "failed", "abandoned"] = "abandoned"
            attempt_error: BaseException | None = None
            try:
                payload = await self._service.resume_sandbox(
                    name=self.name,
                    project_id=self.project_id,
                    include_system_routes=self._include_system_routes,
                )
                with self._recovery_condition:
                    self._apply_payload(payload)
            except Exception as error:
                if not isinstance(error, InterruptedError):
                    outcome = "failed"
                    attempt_error = error
                raise
            else:
                outcome = "succeeded"
            finally:
                with self._recovery_condition:
                    attempt.status = outcome
                    attempt.error = attempt_error
                    if self._recovery_attempt is attempt:
                        self._recovery_attempt = None
                    self._recovery_condition.notify_all()
            return

    async def _ensure_active(self) -> str:
        """Ensure an active current session before binding a lazy stream."""
        return (await self._acquire_session()).id

    async def _acquire_session(self) -> SyncSandboxRuntimeSession:
        try:
            await self._await_shared_resume()
        except Exception as error:
            if classify_sandbox_lifecycle_error(error) not in {
                SandboxLifecycle.STOPPING,
                SandboxLifecycle.SNAPSHOTTING,
            }:
                raise
            await self._await_shared_resume()
        session = self.current_session
        if session is None:
            raise SandboxResponseError(
                "Sandbox resume response is missing the current-session attachment",
                data=self.raw,
            )
        return session

    def session(self) -> SyncSandboxRuntimeSession:
        """Acquire and return the canonical active runtime session eagerly.

        Entering the returned handle does not reacquire it and stops only that
        exact session identity on exit.
        """
        return iter_coroutine(self._acquire_session())

    def run_process(
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

        See ``SyncSandboxRuntimeSession.run_process`` for argument behavior.
        """
        output_router = ProcessOutputRouter(
            stdout=stdout, stderr=stderr, capture_output=capture_output
        )
        parsed_kill_after = parse_duration_seconds(kill_after)
        state = iter_coroutine(
            execute_with_sandbox_recovery(
                lambda session_id: self._service.run_process(
                    session_id=session_id,
                    command=command,
                    args=args,
                    cwd=cwd,
                    env=env,
                    sudo=sudo,
                    kill_after=parsed_kill_after,
                    output_router=output_router,
                ),
                coordinator=self,
            )
        )
        result = _completed_process(state)
        if check:
            result.check_returncode()
        return result

    def open_process(
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
    ) -> SyncOpenProcessOperation:
        """Start a process in the current session and route its output live.

        See ``SyncSandboxRuntimeSession.open_process`` for argument behavior.
        Only the open step, which starts the process and reads its initial
        metadata, is retried after a lifecycle recovery. The block body is never
        replayed.
        """
        output = ProcessOutputRouter(stdout=stdout, stderr=stderr, capture_output=capture_output)
        parsed_kill_after = parse_duration_seconds(kill_after)
        return SyncOpenProcessOperation(
            open_stream=lambda: iter_coroutine(
                execute_with_sandbox_recovery(
                    lambda session_id: self._service.open_process(
                        session_id=session_id,
                        command=command,
                        args=args,
                        cwd=cwd,
                        env=env,
                        sudo=sudo,
                        kill_after=parsed_kill_after,
                    ),
                    coordinator=self,
                )
            ),
            output=output,
            service=self._service,
            check=check,
        )

    def create_process(
        self,
        command: str,
        args: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
        stdout: int = subprocess.PIPE,
        stderr: int = subprocess.PIPE,
    ) -> SyncProcess:
        """Start a process in the current session without waiting for it.

        See ``SyncSandboxRuntimeSession.create_process`` for argument behavior.

        Returns:
            A handle for monitoring and controlling the process.
        """
        stdout = _validate_reader_destination(stdout, name="stdout")
        stderr = _validate_reader_destination(stderr, name="stderr", allow_stdout_merge=True)
        parsed_args = list(args) if args is not None else None
        parsed_kill_after = parse_duration_seconds(kill_after)
        state = iter_coroutine(
            execute_with_sandbox_recovery(
                lambda session_id: self._service.create_process(
                    session_id=session_id,
                    command=command,
                    args=parsed_args,
                    cwd=cwd,
                    env=env,
                    sudo=sudo,
                    kill_after=parsed_kill_after,
                ),
                coordinator=self,
            )
        )
        return SyncProcess(payload=state, service=self._service, stdout=stdout, stderr=stderr)

    def open_interactive(
        self,
        command: str = "/bin/bash",
        args: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        cols: int = DEFAULT_COLS,
        rows: int = DEFAULT_ROWS,
    ) -> SyncInteractiveSessionOperation:
        """Start a PTY-attached process in the current session.

        See ``SyncSandboxRuntimeSession.open_interactive`` for argument
        behavior.

        Returns:
            A context manager yielding the terminal session.
        """
        return SyncInteractiveSessionOperation(
            _open_interactive_session(
                lambda: iter_coroutine(
                    execute_with_sandbox_recovery(
                        lambda session_id: self._service.open_interactive(session_id=session_id),
                        coordinator=self,
                    )
                ),
                command=command,
                args=args,
                cwd=cwd if cwd is not None else self.cwd,
                env=env,
                sudo=sudo,
                cols=cols,
                rows=rows,
            )
        )

    def get_process(self, process_id: str, *, wait: bool = False) -> SyncProcess:
        """Get a process from the current session."""
        state = iter_coroutine(
            execute_with_sandbox_recovery(
                lambda session_id: self._service.get_process(
                    session_id=session_id,
                    process_id=process_id,
                    wait=wait,
                ),
                coordinator=self,
            )
        )
        return SyncProcess(payload=state, service=self._service)

    def query_processes(self) -> list[SyncProcess]:
        """Return handles for processes in the current session."""
        states = iter_coroutine(
            execute_with_sandbox_recovery(
                lambda session_id: self._service.query_processes(session_id=session_id),
                coordinator=self,
            )
        )
        return [SyncProcess(payload=state, service=self._service) for state in states]

    def list_sessions(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> list[SyncSandboxRuntimeSession]:
        """Return one page of runtime sessions belonging to this sandbox."""
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
        """Return one page of snapshots belonging to this sandbox."""
        return query_snapshots_page(
            self._service,
            project_id=self.project_id,
            name=self.name,
            page_size=page_size,
            cursor=cursor,
            sort_order=sort_order,
        ).snapshots

    def extend_execution_time_limit(self, duration: DurationInput) -> SyncSandboxRuntimeSession:
        """Increase the current session's execution time limit.

        The service rejects durations shorter than one second.
        """
        parsed_duration = parse_required_duration_seconds(duration)

        async def extend(
            session_id: str,
        ) -> tuple[str, SandboxRuntimeSessionState]:
            payload = await self._service.extend_runtime_session_timeout(
                session_id=session_id,
                duration=parsed_duration,
            )
            return session_id, payload

        target_id, payload = iter_coroutine(execute_with_sandbox_recovery(extend, coordinator=self))
        return self._apply_current_session_payload_if_current(payload, target_id)

    def update_network_policy(self, network_policy: NetworkPolicy) -> SyncSandboxRuntimeSession:
        """Replace the current session's network policy."""

        async def update(
            session_id: str,
        ) -> tuple[str, SandboxRuntimeSessionState]:
            payload = await self._service.update_runtime_session_network_policy(
                session_id=session_id,
                network_policy=network_policy,
            )
            return session_id, payload

        target_id, payload = iter_coroutine(execute_with_sandbox_recovery(update, coordinator=self))
        return self._apply_current_session_payload_if_current(payload, target_id)

    def snapshot(self, *, expiration: SnapshotExpirationInput = None) -> SyncSnapshot:
        """Create a filesystem snapshot from the current session."""
        parsed_expiration = _parse_snapshot_expiration(expiration)

        async def create(session_id: str) -> tuple[str, SnapshotSessionState]:
            result = await self._service.create_snapshot(
                session_id=session_id,
                expiration=parsed_expiration,
            )
            return session_id, result

        target_id, result = iter_coroutine(execute_with_sandbox_recovery(create, coordinator=self))
        self._apply_current_session_payload_if_current(result.session, target_id)
        return SyncSnapshot(payload=result.snapshot, service=self._service)

    def _apply_current_session_payload_if_current(
        self, payload: SandboxRuntimeSessionState, target_id: str
    ) -> SyncSandboxRuntimeSession:
        """Ignore an operation reply superseded by a concurrent recovery."""
        with self._recovery_condition:
            if target_id == self.current_session_id:
                return self._apply_current_session_payload(payload)
            session = self.current_session
            if session is None:
                raise SandboxResponseError(
                    "Sandbox current-session operation returned a different session identity",
                    data=payload,
                )
            return session

    def stop(self) -> Self:
        """Stop the current session and return this sandbox handle."""
        with self._recovery_condition:
            session = self.current_session
            target_id = self.current_session_id
        result = iter_coroutine(self._service.stop_runtime_session(session_id=target_id))
        if session is None:
            with self._recovery_condition:
                if self.current_session_id != target_id:
                    return self
                self._apply_current_session_payload(result.session)
                if (
                    result._sandbox_attached
                    and result.sandbox is not None
                    and result.sandbox.current_session_id == result.session.id
                ):
                    self._apply_payload(result.sandbox)
        else:
            session._apply_stop_result(result)
        return self

    def destroy(self, *, delete_orphan_snapshots: bool = False) -> Self:
        """Permanently destroy the sandbox and refresh this handle.

        Set ``delete_orphan_snapshots`` to also delete snapshots that no other
        sandbox uses. By default, snapshots are kept until they expire.
        """
        payload = iter_coroutine(
            self._service.destroy_sandbox(
                name=self.name,
                project_id=self.project_id,
                delete_orphan_snapshots=delete_orphan_snapshots,
            )
        )
        self._apply_payload(payload)
        return self

    def update(
        self,
        *,
        ports: list[int] | None = None,
        execution_time_limit: DurationInput = None,
        resources: SandboxResources | None = None,
        persistent: bool | None = None,
        network_policy: NetworkPolicy | None = None,
        network_id: NetworkIdUpdate = _OMITTED,
        env: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        mounts: DriveMountsInput[_RemotePathT] | None = None,
        snapshot_expiration: SnapshotExpirationInput = None,
        snapshot_retention: SnapshotRetentionUpdate = _OMITTED,
        current_snapshot_id: str | None = None,
        region: str | None = None,
        failover_regions: FailoverRegionsInput = None,
    ) -> Self:
        """Update mutable sandbox configuration.

        Only non-``None`` values are sent, except ``snapshot_retention`` where
        explicitly passing ``None`` removes the retention policy.

        Args:
            mounts: Complete Drive mount replacement for the next session.
                Pass ``None`` to leave mounts unchanged or ``{}`` to remove all
                mounts.
            current_snapshot_id: Snapshot the sandbox restores from on its
                next resume.

        Returns:
            This handle refreshed with the updated sandbox state.
        """
        payload = iter_coroutine(
            self._service.update_sandbox(
                name=self.name,
                project_id=self.project_id,
                ports=ports,
                execution_time_limit=parse_duration_seconds(execution_time_limit),
                resources=resources,
                persistent=persistent,
                network_policy=network_policy,
                network_id=network_id,
                env=env,
                tags=tags,
                mounts=mounts,
                snapshot_expiration=_parse_snapshot_expiration(snapshot_expiration),
                snapshot_retention=snapshot_retention,
                region=region,
                failover_regions=normalize_failover_regions(failover_regions),
                current_snapshot_id=current_snapshot_id,
            )
        )
        self._apply_payload(payload)
        return self


def _cleanup_managed_sandbox(handle: SyncSandbox, *, destroy: bool) -> None:
    cleanup_error: Exception | None = None
    try:
        handle.stop()
    except Exception as exc:
        cleanup_error = exc

    if destroy:
        try:
            handle.destroy(delete_orphan_snapshots=True)
        except Exception as exc:
            if cleanup_error is None:
                cleanup_error = exc

    if cleanup_error is not None:
        raise SandboxCleanupError(
            f"Failed to clean up sandbox {handle.name!r}",
            resource_type="sandbox",
            resource_id=handle.name,
            cause=cleanup_error,
        ) from cleanup_error


class _ManagedSyncSandbox(SyncSandbox):
    __slots__ = ("_destroy_on_exit",)

    def __init__(
        self,
        *,
        payload: SandboxState,
        service: SandboxService,
        destroy_on_exit: bool,
        include_system_routes: bool | None = None,
    ) -> None:
        super().__init__(
            payload=payload,
            service=service,
            include_system_routes=include_system_routes,
        )
        self._destroy_on_exit = destroy_on_exit

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        _cleanup_managed_sandbox(self, destroy=self._destroy_on_exit)


def create_sandbox(
    service: SandboxService,
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
    network_id: str | None = None,
    env: Mapping[str, str] | None = None,
    tags: Mapping[str, str] | None = None,
    mounts: DriveMountsInput[_RemotePathT] | None = None,
    snapshot_expiration: SnapshotExpirationInput = None,
    snapshot_retention: SnapshotRetention | None = None,
    region: str | None = None,
    failover_regions: FailoverRegionsInput = None,
    destroy: bool = True,
    private_parameters: PrivateSandboxParameters = NO_PRIVATE_PARAMETERS,
) -> _ManagedSyncSandbox:
    try:
        state = iter_coroutine(
            service.create_sandbox(
                project_id=project_id,
                name=name,
                image=image,
                source=source,
                ports=ports,
                execution_time_limit=parse_duration_seconds(execution_time_limit),
                resources=resources,
                persistent=persistent,
                network_policy=network_policy,
                network_id=network_id,
                env=env,
                tags=tags,
                mounts=mounts,
                snapshot_expiration=_parse_snapshot_expiration(snapshot_expiration),
                snapshot_retention=snapshot_retention,
                region=region,
                failover_regions=normalize_failover_regions(failover_regions),
                private_parameters=private_parameters,
            )
        )
        return _ManagedSyncSandbox(
            payload=state,
            service=service,
            destroy_on_exit=destroy,
        )
    except _SandboxTerminalState as error:
        raise _terminal_error(error, SyncSandbox(payload=error.sandbox, service=service)) from error


def fork_sandbox(
    service: SandboxService,
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
    network_id: str | None = None,
    env: Mapping[str, str] | None = None,
    tags: Mapping[str, str] | None = None,
    mounts: DriveMountsInput[_RemotePathT] | None = None,
    snapshot_expiration: SnapshotExpirationInput = None,
    snapshot_retention: SnapshotRetention | None = None,
    region: str | None = None,
    failover_regions: FailoverRegionsInput = None,
    destroy: bool = True,
    private_parameters: PrivateSandboxParameters = NO_PRIVATE_PARAMETERS,
) -> _ManagedSyncSandbox:
    try:
        state = iter_coroutine(
            service.fork_sandbox(
                source_sandbox=source_sandbox,
                project_id=project_id,
                name=name,
                ports=ports,
                execution_time_limit=parse_duration_seconds(execution_time_limit),
                resources=resources,
                image=image,
                persistent=persistent,
                network_policy=network_policy,
                network_id=network_id,
                env=env,
                tags=tags,
                mounts=mounts,
                snapshot_expiration=_parse_snapshot_expiration(snapshot_expiration),
                snapshot_retention=snapshot_retention,
                region=region,
                failover_regions=normalize_failover_regions(failover_regions),
                private_parameters=private_parameters,
            )
        )
        return _ManagedSyncSandbox(
            payload=state,
            service=service,
            destroy_on_exit=destroy,
        )
    except _SandboxTerminalState as error:
        raise _terminal_error(error, SyncSandbox(payload=error.sandbox, service=service)) from error


def get_sandbox(
    service: SandboxService,
    *,
    name: str,
    project_id: str | None = None,
    resume: bool = False,
    include_system_routes: bool | None = None,
    private_parameters: PrivateSandboxParameters = NO_PRIVATE_PARAMETERS,
) -> SyncSandbox:
    return SyncSandbox(
        payload=iter_coroutine(
            service.get_sandbox(
                name=name,
                project_id=project_id,
                resume=resume,
                include_system_routes=include_system_routes,
                private_parameters=private_parameters,
            )
        ),
        service=service,
        include_system_routes=include_system_routes,
    )


def get_or_create_sandbox(
    service: SandboxService,
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
    network_id: str | None = None,
    env: Mapping[str, str] | None = None,
    tags: Mapping[str, str] | None = None,
    mounts: DriveMountsInput[_RemotePathT] | None = None,
    snapshot_expiration: SnapshotExpirationInput = None,
    snapshot_retention: SnapshotRetention | None = None,
    region: str | None = None,
    failover_regions: FailoverRegionsInput = None,
    private_parameters: PrivateSandboxParameters = NO_PRIVATE_PARAMETERS,
) -> tuple[SyncSandbox, bool]:
    try:
        state, created = iter_coroutine(
            service.get_or_create_sandbox(
                name=name,
                project_id=project_id,
                resume=resume,
                include_system_routes=include_system_routes,
                image=image,
                source=source,
                ports=ports,
                execution_time_limit=parse_duration_seconds(execution_time_limit),
                resources=resources,
                persistent=persistent,
                network_policy=network_policy,
                network_id=network_id,
                env=env,
                tags=tags,
                mounts=mounts,
                snapshot_expiration=_parse_snapshot_expiration(snapshot_expiration),
                snapshot_retention=snapshot_retention,
                region=region,
                failover_regions=normalize_failover_regions(failover_regions),
                private_parameters=private_parameters,
            )
        )
        return (
            SyncSandbox(
                payload=state,
                service=service,
                include_system_routes=include_system_routes,
            ),
            created,
        )
    except _SandboxTerminalState as error:
        raise _terminal_error(
            error,
            SyncSandbox(
                payload=error.sandbox,
                service=service,
                include_system_routes=include_system_routes,
            ),
        ) from error


def resume_sandbox(
    service: SandboxService,
    *,
    name: str,
    project_id: str | None = None,
    include_system_routes: bool | None = None,
    private_parameters: PrivateSandboxParameters = NO_PRIVATE_PARAMETERS,
) -> _ManagedSyncSandbox:
    return _ManagedSyncSandbox(
        payload=iter_coroutine(
            service.resume_sandbox(
                name=name,
                project_id=project_id,
                include_system_routes=include_system_routes,
                private_parameters=private_parameters,
            )
        ),
        service=service,
        destroy_on_exit=False,
        include_system_routes=include_system_routes,
    )


def get_drive(service: SandboxService, *, name_or_id: str) -> SyncDrive:
    return SyncDrive(
        payload=iter_coroutine(service.get_drive(name_or_id=name_or_id)),
        service=service,
    )


def get_or_create_drive(
    service: SandboxService,
    *,
    name: str,
    max_size_bytes: int | None = None,
    region: str | None = None,
) -> tuple[SyncDrive, bool]:
    state, created = iter_coroutine(
        service.get_or_create_drive(
            name=name,
            max_size_bytes=max_size_bytes,
            region=region,
        )
    )
    return SyncDrive(payload=state, service=service), created


def delete_drive(service: SandboxService, *, name: str) -> SyncDrive:
    return SyncDrive(
        payload=iter_coroutine(service.delete_drive(name=name)),
        service=service,
    )


def query_drives_page(service: SandboxService, **kwargs: Any) -> QueryDrivesPage[SyncDrive]:
    page = iter_coroutine(service.query_drives_page(**kwargs))
    return QueryDrivesPage(
        drives=[SyncDrive(payload=state, service=service) for state in page.drives],
        next_cursor=page.next_cursor,
    )


def query_drives(
    service: SandboxService,
    *,
    query: DriveQuery | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> Iterator[SyncDrive]:
    params = QueryDrivesParams(page_size=page_size, cursor=cursor)
    while True:
        page = query_drives_page(
            service,
            query=query,
            page_size=params.page_size,
            cursor=params.cursor,
        )
        yield from page.drives
        if page.next_cursor is None or not page.drives:
            return
        params = params.with_cursor(page.next_cursor)


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


def _process_logs(
    service: SandboxService, *, session_id: str, process_id: str
) -> Iterator[ProcessLog]:
    stream = service.process_logs(session_id=session_id, process_id=process_id)

    async def next_log() -> ProcessLog:
        return await anext(stream)

    try:
        while True:
            try:
                yield iter_coroutine(next_log())
            except StopAsyncIteration:
                return
    finally:
        iter_coroutine(stream.aclose())
