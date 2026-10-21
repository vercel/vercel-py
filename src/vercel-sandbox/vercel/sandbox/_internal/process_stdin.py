"""Process standard input backed by one async-shaped write core.

Every write is a request to the service, sent with the number of bytes the
service has confirmed so far. A resent request carries the same offset, so the
service skips whatever an earlier attempt already delivered and a retry can't
duplicate input.
"""

import io
import subprocess
import threading
from contextlib import suppress
from typing import TYPE_CHECKING, Literal, Protocol, TypeAlias

import anyio

from vercel._internal.core.http._compat import transport_errors
from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel._internal.core.polyfills import Buffer
from vercel.sandbox._internal.errors import (
    SandboxApiError,
    SandboxResponseError,
    SandboxStdinError,
)
from vercel.sandbox._internal.models import ProcessSignal
from vercel.sandbox._internal.state import ProcessState

if TYPE_CHECKING:
    from vercel.sandbox._internal.service import SandboxService

STDIN_CHUNK_SIZE = 512 * 1024
"""Bytes per request, keeping the base64 JSON body under the API's 1 MB limit."""

_MAX_RETRIES = 5
_MAX_STALLED_TIMEOUTS = 2
_MAX_RETRY_AFTER_SECONDS = 20.0

ProcessInput = str | bytes | bytearray | memoryview

StdinMode: TypeAlias = Literal["attached", "fetched"] | None
"""How a process handle relates to its stdin.

``"attached"`` means this SDK started the process with stdin attached.
``"fetched"`` means the handle was looked up, so whether the process has stdin
is unknown until the first request. ``None`` means the process has none.
"""


def _backoff(attempt: int) -> float:
    return 0.4 * 2**attempt


def _validate_stdin(stdin: object) -> bool:
    """Return whether a Popen-style ``stdin`` asks for a writable stdin.

    The service has no inherit mode: ``None`` and ``DEVNULL`` both give the
    process an empty stdin, and ``PIPE`` attaches a writer.
    """
    if stdin is None:
        return False
    if isinstance(stdin, int) and not isinstance(stdin, bool):
        if stdin == subprocess.PIPE:
            return True
        if stdin == subprocess.DEVNULL:
            return False
        raise ValueError(f"unsupported stdin value: {stdin}")
    raise TypeError("stdin must be None, subprocess.PIPE, or subprocess.DEVNULL")


def _coerce_bytes(data: object) -> bytes:
    if isinstance(data, str):
        raise TypeError("stdin accepts bytes, not str; encode text or wrap the stream")
    try:
        return bytes(memoryview(data).cast("B"))  # type: ignore[arg-type]
    except TypeError:
        raise TypeError(f"a bytes-like object is required, not {type(data).__name__}") from None


def _coerce_input(input: ProcessInput) -> bytes:
    """Encode ``communicate`` and ``run_process`` input; text becomes UTF-8."""
    if isinstance(input, str):
        return input.encode("utf-8")
    return _coerce_bytes(input)


def _is_stdin_unavailable(error: BaseException) -> bool:
    return (
        isinstance(error, SandboxApiError)
        and error.status_code == 400
        and error.code == "command_stdin_unavailable"
    )


def _is_input_not_accepted(error: BaseException) -> bool:
    """Whether the process exited or stopped reading, like a closed pipe."""
    return isinstance(error, SandboxApiError) and (
        error.status_code == 404
        or (error.status_code == 400 and error.code == "command_stdin_unavailable")
    )


def _retry_after(error: SandboxApiError) -> float | None:
    try:
        return float(error.response.headers.get("retry-after", ""))
    except ValueError:
        return None


def _timeout_bytes_written(error: SandboxApiError) -> int | None:
    data = error.data
    if not isinstance(data, dict):
        return None
    detail = data.get("error")
    if not isinstance(detail, dict):
        return None
    value = detail.get("bytesWritten")
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


class _StdinUnavailable(Exception):
    """The stdin stream can't accept input; ``__cause__`` says why."""


class _InputNotAccepted(_StdinUnavailable):
    """The process exited or closed its stdin."""


class _RequestFailed(Exception):
    def __init__(self, error: BaseException, *, maybe_applied: bool) -> None:
        super().__init__(error)
        self.error = error
        self.maybe_applied = maybe_applied


class _WriteLock(Protocol):
    async def acquire(self) -> None: ...

    def release(self) -> None: ...


class _SyncWriteLock:
    def __init__(self) -> None:
        self._lock = threading.Lock()

    async def acquire(self) -> None:
        self._lock.acquire()

    def release(self) -> None:
        self._lock.release()


class _UnsharedLock:
    """Lock for a core that only one caller ever drives."""

    async def acquire(self) -> None:
        pass

    def release(self) -> None:
        pass


class StdinCore:
    """Ordered, resumable writes to one process's stdin."""

    def __init__(
        self,
        *,
        service: "SandboxService",
        session_id: str,
        process_id: str,
        lock: _WriteLock,
        attached: bool = True,
    ) -> None:
        self._service = service
        self._session_id = session_id
        self._process_id = process_id
        self._lock = lock
        # The service reports "never attached" and "stopped reading" the same
        # way. That is only ambiguous until a request succeeds, and only for a
        # handle that didn't start the process itself.
        self.attached = attached
        self._verified = attached
        self.written = False
        # Confirmed bytes, or ``None`` until the service is asked.
        self._offset: int | None = None
        self._failure: BaseException | None = None
        self._not_accepted: BaseException | None = None
        self.closed = False

    async def write(self, data: bytes) -> None:
        await self._lock.acquire()
        try:
            if self.closed:
                raise anyio.ClosedResourceError
            if self._not_accepted is not None:
                raise _InputNotAccepted from self._not_accepted
            if self._failure is not None:
                raise _StdinUnavailable(
                    "stdin is in an unknown state after an earlier write failed"
                ) from self._failure
            if not data:
                return
            self.written = True
            await self._resolve_offset()
            await self._write_chunks(data)
        finally:
            self._lock.release()

    async def close(self) -> None:
        await self._lock.acquire()
        try:
            if self.closed:
                return
            # Only a close that went through, or found nothing to close, marks
            # the stream closed; a failed one can be retried.
            if self._not_accepted is None:
                try:
                    await self._resolve_offset()
                    await self._request(offset=self._confirmed(), close=True)
                except _InputNotAccepted:
                    pass
                except SandboxStdinError as error:
                    # A looked-up process without stdin has nothing to close.
                    if not _is_stdin_unavailable(error.cause):
                        raise
                except _RequestFailed as failure:
                    if not _is_input_not_accepted(failure.error):
                        raise SandboxStdinError(
                            "Failed to close process stdin", cause=failure.error
                        ) from failure.error
            self.closed = True
        finally:
            self._lock.release()

    async def communicate(self, data: bytes | None) -> None:
        """Write ``data`` and close stdin, ignoring a process that stopped reading.

        With ``data=None``, a looked-up handle that hasn't written leaves stdin
        open.
        """
        if data is None and not self.attached and not self.written:
            return
        if data:
            with suppress(_InputNotAccepted):
                await self.write(data)
        await self.close()

    async def _write_chunks(self, data: bytes) -> None:
        view = memoryview(data)
        sent = 0
        try:
            for start in range(0, len(view), STDIN_CHUNK_SIZE):
                chunk = bytes(view[start : start + STDIN_CHUNK_SIZE])
                offset = self._confirmed()
                written = await self._request(data=chunk, offset=offset)
                self._offset = offset + len(chunk) if written is None else written
                sent += 1
        except _RequestFailed as failure:
            error = failure.error
            if _is_input_not_accepted(error):
                self._not_accepted = error
                raise _InputNotAccepted from error
            rejected_before_write = (
                isinstance(error, SandboxApiError)
                and 400 <= error.status_code < 500
                and not failure.maybe_applied
            )
            if sent or not rejected_before_write:
                self._fail(error)
            raise SandboxStdinError("Failed to write to process stdin", cause=error) from error
        except BaseException as error:
            # Cancelled mid-request: the service may or may not have applied it.
            self._fail(error)
            raise

    def _fail(self, error: BaseException) -> None:
        self._failure = error
        self._offset = None

    async def _resolve_offset(self) -> None:
        if self._offset is not None:
            return
        try:
            written = await self._request()
        except _RequestFailed as failure:
            if not self._verified and _is_stdin_unavailable(failure.error):
                # Unlike a process that stops reading, input sent here may be
                # lost, so ``communicate`` doesn't ignore it.
                raise SandboxStdinError(
                    "Process stdin is unavailable: the process was not started with "
                    "stdin=subprocess.PIPE, or it has stopped reading stdin",
                    cause=failure.error,
                ) from failure.error
            if _is_input_not_accepted(failure.error):
                self._not_accepted = failure.error
                raise _InputNotAccepted from failure.error
            raise SandboxStdinError(
                "Failed to read process stdin state", cause=failure.error
            ) from failure.error
        self._verified = True
        if written is None:
            # Without the current offset a write could duplicate or skip input.
            raise SandboxStdinError(
                "Failed to read process stdin state",
                cause=SandboxResponseError(
                    "Sandbox stdin response did not report the bytes written"
                ),
            )
        self._offset = written

    def _confirmed(self) -> int:
        assert self._offset is not None
        return self._offset

    async def _send(
        self, *, data: bytes = b"", offset: int | None = None, close: bool = False
    ) -> int | None:
        return await self._service.write_process_stdin(
            session_id=self._session_id,
            process_id=self._process_id,
            data=data,
            offset=offset,
            close=close,
        )

    async def _request(
        self, *, data: bytes = b"", offset: int | None = None, close: bool = False
    ) -> int | None:
        """Send one logical request, resending it while that is safe.

        Every write carries an offset, and the service skips bytes it already
        has, so a request is resent across connection failures, 429, 502, 503,
        and timeouts that report progress. Requests without data only read the
        offset, so they are always safe to resend.
        """
        maybe_applied = False
        stalled_timeouts = 0
        last_written = offset or 0
        attempt = 0
        while True:
            can_retry = attempt < _MAX_RETRIES
            attempt += 1
            try:
                return await self._send(data=data, offset=offset, close=close)
            except transport_errors() as error:
                if not can_retry:
                    raise _RequestFailed(error, maybe_applied=True) from error
                maybe_applied = True
                await self._service.sleep(_backoff(attempt - 1))
            except SandboxApiError as error:
                status = error.status_code
                if status == 404 and maybe_applied and close and not data:
                    # The process exited after a close whose response was lost.
                    return None
                if status == 429 and can_retry:
                    retry_after = _retry_after(error)
                    if retry_after is None or retry_after <= _MAX_RETRY_AFTER_SECONDS:
                        await self._service.sleep(
                            retry_after
                            if retry_after is not None and retry_after > 0
                            else _backoff(attempt - 1)
                        )
                        continue
                elif can_retry and status in (502, 503):
                    maybe_applied = True
                    await self._service.sleep(_backoff(attempt - 1))
                    continue
                elif can_retry and status == 504:
                    # The process isn't reading fast enough. Keep resending
                    # while it makes progress; a timeout that doesn't report
                    # the bytes written counts as stalled.
                    maybe_applied = True
                    written = _timeout_bytes_written(error)
                    if written is not None and written > last_written:
                        last_written = written
                        stalled_timeouts = 0
                        continue
                    stalled_timeouts += 1
                    if stalled_timeouts < _MAX_STALLED_TIMEOUTS:
                        continue
                raise _RequestFailed(error, maybe_applied=maybe_applied) from error


async def feed_process_input(
    service: "SandboxService", process: ProcessState, input: bytes
) -> None:
    """Write ``input`` to a process the caller is waiting on, then close stdin.

    The caller has no handle to stop the process, so it is killed if writing
    fails, rather than left waiting for input that will never arrive.
    """
    core = StdinCore(
        service=service,
        session_id=process.session_id,
        process_id=process.id,
        lock=_UnsharedLock(),
    )
    try:
        await core.communicate(input)
    except Exception:
        with suppress(Exception):
            await service.send_process_signal(
                session_id=process.session_id,
                process_id=process.id,
                signal=int(ProcessSignal.SIGKILL),
            )
        raise


class ProcessStdin(anyio.abc.ByteSendStream):
    """Standard input of an asynchronously running sandbox process.

    Each ``send`` returns once the process has accepted the bytes, and
    concurrent sends reach the process in the order they were called.
    ``aclose`` closes stdin, so the process reads end of file after any
    pending data. Wrap the stream in :class:`anyio.streams.text.TextSendStream`
    to send text.

    ``send`` raises :class:`anyio.BrokenResourceError` once the process has
    exited or stopped reading stdin, and :class:`SandboxStdinError` if a
    request fails. A looked-up process always has a writer; if the process
    was started without stdin, the first ``send`` raises
    :class:`SandboxStdinError`. After a failure that may have delivered part
    of the data, further sends raise :class:`anyio.BrokenResourceError`; the
    stream can still be closed. If ``aclose`` fails, calling it again retries.
    """

    def __init__(self, core: StdinCore) -> None:
        self._core = core

    async def send(self, item: Buffer) -> None:
        """Write bytes to the process's stdin."""
        try:
            await self._core.write(_coerce_bytes(item))
        except _StdinUnavailable as error:
            raise anyio.BrokenResourceError(*error.args) from error.__cause__

    async def aclose(self) -> None:
        """Close stdin. A process that already exited is not an error."""
        with anyio.CancelScope(shield=True):
            await self._core.close()

    async def _communicate(self, input: bytes | None) -> None:
        if input and self._core.closed:
            raise anyio.ClosedResourceError
        try:
            await self._core.communicate(input)
        except _StdinUnavailable as error:
            raise anyio.BrokenResourceError(*error.args) from error.__cause__


class SyncProcessStdin(io.RawIOBase):
    """Standard input of a synchronously running sandbox process.

    A raw binary stream: each ``write`` writes every byte and returns once the
    process has accepted them, and writes from several threads reach the
    process in the order they acquire the stream. ``close`` closes stdin, so
    the process reads end of file after any pending data. Wrap the stream in
    :class:`io.TextIOWrapper` to write text.

    ``write`` raises :class:`BrokenPipeError` once the process has exited or
    stopped reading stdin, and :class:`SandboxStdinError` if a request fails.
    A looked-up process always has a writer; if the process was started
    without stdin, the first ``write`` raises :class:`SandboxStdinError`.
    After a failure that may have delivered part of the data, further writes
    raise :class:`BrokenPipeError`; the stream can still be closed. If
    ``close`` fails, the stream stays open and calling it again retries.

    Unlike a local pipe, garbage collection does not close the stream, since
    that would need a request to the service. Close it explicitly or use it as
    a context manager.
    """

    def __init__(self, core: StdinCore) -> None:
        super().__init__()
        self._core = core

    def writable(self) -> bool:
        return True

    def write(self, data: Buffer, /) -> int:
        """Write bytes to the process's stdin and return how many were written."""
        self._checkClosed()
        payload = _coerce_bytes(data)
        try:
            iter_coroutine(self._core.write(payload))
        except _StdinUnavailable as error:
            raise _broken_pipe(error) from error.__cause__
        return len(payload)

    def close(self) -> None:
        """Close stdin. A process that already exited is not an error."""
        if self.closed:
            return
        iter_coroutine(self._core.close())
        super().close()

    def __del__(self) -> None:
        pass

    def _communicate(self, input: bytes | None) -> None:
        if input:
            self._checkClosed()
        try:
            iter_coroutine(self._core.communicate(input))
        except _StdinUnavailable as error:
            raise _broken_pipe(error) from error.__cause__
        finally:
            if self._core.closed:
                super().close()


def _broken_pipe(error: _StdinUnavailable) -> BrokenPipeError:
    return BrokenPipeError(str(error) or "the process is no longer reading stdin")


def _async_stdin(
    service: "SandboxService", process: ProcessState, mode: StdinMode
) -> ProcessStdin | None:
    if mode is None:
        return None
    return ProcessStdin(
        StdinCore(
            service=service,
            session_id=process.session_id,
            process_id=process.id,
            lock=anyio.Lock(),
            attached=mode == "attached",
        )
    )


def _sync_stdin(
    service: "SandboxService", process: ProcessState, mode: StdinMode
) -> SyncProcessStdin | None:
    if mode is None:
        return None
    return SyncProcessStdin(
        StdinCore(
            service=service,
            session_id=process.session_id,
            process_id=process.id,
            lock=_SyncWriteLock(),
            attached=mode == "attached",
        )
    )
