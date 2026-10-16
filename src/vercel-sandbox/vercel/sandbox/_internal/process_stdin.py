"""Process standard input backed by one async-shaped write core.

Every write is a request to the service, sent with the number of bytes the
service has confirmed so far. A resent request carries the same offset, so the
service skips whatever an earlier attempt already delivered and a retry can't
duplicate input.
"""

import io
import subprocess
from contextlib import suppress
from typing import TYPE_CHECKING

import anyio

from vercel._internal.core.http._compat import transport_errors
from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel._internal.core.polyfills import Buffer
from vercel.sandbox._internal.errors import SandboxApiError, SandboxStdinError
from vercel.sandbox._internal.models import ProcessSignal
from vercel.sandbox._internal.state import ProcessState
from vercel.sandbox._internal.text_reader import _AsyncPumpLock, _PumpLock, _SyncPumpLock

if TYPE_CHECKING:
    from vercel.sandbox._internal.service import SandboxService

STDIN_CHUNK_SIZE = 512 * 1024
"""Bytes per request, keeping the base64 JSON body under the API's 1 MB limit."""

_MAX_RETRIES = 5
_MAX_STALLED_TIMEOUTS = 2
_MAX_RETRY_AFTER_SECONDS = 20.0

ProcessInput = str | bytes | bytearray | memoryview


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
        lock: _PumpLock,
    ) -> None:
        self._service = service
        self._session_id = session_id
        self._process_id = process_id
        self._lock = lock
        # Confirmed bytes, or ``None`` until the service is asked. Asking also
        # reveals sandboxes that predate offsets, where resending is unsafe.
        self._offset: int | None = None
        self._offsets_supported = True
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
            await self._resolve_offset()
            await self._write_chunks(data)
        finally:
            self._lock.release()

    async def close(self) -> None:
        await self._lock.acquire()
        try:
            if self.closed:
                return
            self.closed = True
            if self._not_accepted is not None:
                return
            try:
                await self._resolve_offset()
                await self._request(offset=self._request_offset(), close=True)
            except _InputNotAccepted:
                return
            except _RequestFailed as failure:
                if _is_input_not_accepted(failure.error):
                    return
                raise SandboxStdinError(
                    "Failed to close process stdin", cause=failure.error
                ) from failure.error
        finally:
            self._lock.release()

    async def communicate(self, data: bytes | None) -> None:
        """Write ``data`` and close stdin, ignoring a process that stopped reading."""
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
                written = await self._request(data=chunk, offset=self._request_offset())
                self._confirm(written, len(chunk))
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
            if _is_input_not_accepted(failure.error):
                self._not_accepted = failure.error
                raise _InputNotAccepted from failure.error
            raise SandboxStdinError(
                "Failed to read process stdin state", cause=failure.error
            ) from failure.error
        self._offsets_supported = written is not None
        self._offset = written or 0

    def _request_offset(self) -> int | None:
        return self._offset if self._offsets_supported else None

    def _confirm(self, written: int | None, length: int) -> None:
        if written is None:
            self._offsets_supported = False
            self._offset = (self._offset or 0) + length
        else:
            self._offset = written

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

    async def _probe(self) -> int | None:
        try:
            return await self._send()
        except Exception:
            return None

    async def _request(
        self, *, data: bytes = b"", offset: int | None = None, close: bool = False
    ) -> int | None:
        """Send one logical request, resending it while that is safe.

        With an offset the service skips bytes it already has, so the request
        is resent across connection failures, 502, 503, and timeouts that
        still make progress. Without one, only rate-limited requests are
        resent, since any other failure may have written the bytes already.
        """
        resendable = offset is not None or (not data and not close)
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
                if not resendable or not can_retry:
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
                elif resendable and can_retry and status in (502, 503):
                    maybe_applied = True
                    await self._service.sleep(_backoff(attempt - 1))
                    continue
                elif resendable and can_retry and status == 504:
                    # The process isn't reading fast enough. Keep resending
                    # while it makes progress.
                    maybe_applied = True
                    written = _timeout_bytes_written(error)
                    if written is None:
                        written = await self._probe()
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
    request fails. After a failure that may have delivered part of the data,
    further sends raise :class:`anyio.BrokenResourceError`; the stream can
    still be closed.
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
    After a failure that may have delivered part of the data, further writes
    raise :class:`BrokenPipeError`; the stream can still be closed.

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
        try:
            iter_coroutine(self._core.close())
        finally:
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
            super().close()


def _broken_pipe(error: _StdinUnavailable) -> BrokenPipeError:
    return BrokenPipeError(str(error) or "the process is no longer reading stdin")


def _async_stdin(
    service: "SandboxService", process: ProcessState, *, attached: bool
) -> ProcessStdin | None:
    if not attached:
        return None
    return ProcessStdin(
        StdinCore(
            service=service,
            session_id=process.session_id,
            process_id=process.id,
            lock=_AsyncPumpLock(),
        )
    )


def _sync_stdin(
    service: "SandboxService", process: ProcessState, *, attached: bool
) -> SyncProcessStdin | None:
    if not attached:
        return None
    return SyncProcessStdin(
        StdinCore(
            service=service,
            session_id=process.session_id,
            process_id=process.id,
            lock=_SyncPumpLock(),
        )
    )
