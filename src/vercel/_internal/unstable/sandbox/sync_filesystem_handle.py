"""Synchronous sequential file handles for sandbox filesystems."""

import queue
import tempfile
import threading
from collections.abc import Callable, Iterable, Iterator
from types import TracebackType
from typing import Any

import httpx

from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.unstable.sandbox.errors import SandboxUploadSizeMismatchError
from vercel._internal.unstable.sandbox.filesystem_handle_common import (
    _HandleInfo,
    _TextEncoder,
    _TextReadBuffer,
    _validate_read_size,
)
from vercel._internal.unstable.sandbox.runtime_common import (
    RemotePath,
    _normalize_tar_path,
    _UploadFileEntry,
)
from vercel._internal.unstable.sandbox.service import SandboxService
from vercel._internal.unstable.sandbox.streaming_archive import sync_archive_body

_CHUNK_SIZE = 64 * 1024


class SyncSandboxBinaryReader(_HandleInfo):
    __slots__ = ("_buffer", "_eof", "_iterator", "_open_response", "_response")

    def __init__(self, name: str, open_response: Callable[[], httpx.Response]) -> None:
        super().__init__(name, "rb")
        self._open_response = open_response
        self._response: httpx.Response | None = None
        self._iterator: Iterator[bytes] | None = None
        self._buffer = bytearray()
        self._eof = False

    def __enter__(self) -> "SyncSandboxBinaryReader":
        self._enter()
        try:
            self._response = self._open_response()
            self._iterator = self._response.iter_bytes(_CHUNK_SIZE)
        except BaseException:
            self._mark_closed()
            raise
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _pump(self) -> None:
        if self._eof:
            return
        assert self._iterator is not None
        try:
            chunk = next(self._iterator)
        except StopIteration:
            self._eof = True
            self._close_response()
        except BaseException:
            self._eof = True
            self._close_response()
            raise
        else:
            self._buffer.extend(chunk)

    def _read(self, size: int = -1) -> bytes:
        self._ensure_active()
        _validate_read_size(size)
        while not self._eof and (size < 0 or len(self._buffer) < size):
            self._pump()
        if size < 0:
            result = bytes(self._buffer)
            self._buffer.clear()
            return result
        result = bytes(self._buffer[:size])
        del self._buffer[:size]
        return result

    def read(self, size: int = -1) -> bytes:
        return self._read(size)

    def readline(self, size: int = -1) -> bytes:
        self._ensure_active()
        _validate_read_size(size)
        while True:
            limit = len(self._buffer) if size < 0 else min(size, len(self._buffer))
            newline = self._buffer.find(b"\n", 0, limit)
            if newline >= 0:
                return self._read(newline + 1)
            if (size >= 0 and len(self._buffer) >= size) or self._eof:
                return self._read(limit)
            self._pump()

    def readinto(self, buffer: object) -> int:
        self._ensure_active()
        view = memoryview(buffer)  # type: ignore[arg-type]
        if view.readonly:
            raise TypeError("readinto() argument must be read-write bytes-like object")
        data = self._read(view.nbytes)
        view.cast("B")[: len(data)] = data
        return len(data)

    def __iter__(self) -> "SyncSandboxBinaryReader":
        return self

    def __next__(self) -> bytes:
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def _close_response(self) -> None:
        response, self._response = self._response, None
        self._iterator = None
        if response is not None:
            response.close()

    def close(self) -> None:
        if not self.closed:
            self._buffer.clear()
            self._eof = True
            self._close_response()
            self._mark_closed()


class SyncSandboxTextReader(_HandleInfo):
    __slots__ = ("_binary", "_text")

    def __init__(
        self,
        name: str,
        open_response: Callable[[], httpx.Response],
        encoding: str,
        errors: str,
        newline: str | None,
    ) -> None:
        super().__init__(name, "r")
        self._binary = SyncSandboxBinaryReader(name, open_response)
        self._text = _TextReadBuffer(encoding, errors, newline)

    def __enter__(self) -> "SyncSandboxTextReader":
        self._enter()
        try:
            self._binary.__enter__()
        except BaseException:
            self._mark_closed()
            raise
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _pump(self) -> None:
        data = self._binary._read(_CHUNK_SIZE)
        self._text.feed(data, final=not data)

    def read(self, size: int = -1) -> str:
        self._ensure_active()
        _validate_read_size(size)
        while not self._text._eof and (size < 0 or len(self._text._buffer) < size):
            self._pump()
        return self._text.take(size)

    def readline(self, size: int = -1) -> str:
        self._ensure_active()
        _validate_read_size(size)
        while True:
            end = self._text.line_end(size)
            if end is not None:
                return self._text.take(end)
            if (size >= 0 and len(self._text._buffer) >= size) or self._text._eof:
                limit = len(self._text._buffer) if size < 0 else min(size, len(self._text._buffer))
                return self._text.take(limit)
            self._pump()

    def __iter__(self) -> "SyncSandboxTextReader":
        return self

    def __next__(self) -> str:
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def close(self) -> None:
        if not self.closed:
            self._binary.close()
            self._mark_closed()


class _UploadAborted(Exception):
    pass


_EOF = object()
_ABORT = object()


class _QueueReader:
    __slots__ = ("_buffer", "_queue")

    def __init__(self, chunks: "queue.Queue[bytes | object]") -> None:
        self._queue = chunks
        self._buffer = bytearray()

    def read(self, size: int = -1) -> bytes:
        while size < 0 or len(self._buffer) < size:
            item = self._queue.get()
            if item is _EOF:
                break
            if item is _ABORT:
                raise _UploadAborted
            self._buffer.extend(item)  # type: ignore[arg-type]
        if size < 0:
            result = bytes(self._buffer)
            self._buffer.clear()
        else:
            result = bytes(self._buffer[:size])
            del self._buffer[:size]
        return result


class SyncSandboxBinaryWriter(_HandleInfo):
    __slots__ = (
        "_bind_publish",
        "_chunks",
        "_error",
        "_permissions",
        "_publish",
        "_size",
        "_spool",
        "_thread",
        "_written",
    )

    def __init__(
        self,
        name: str,
        bind_publish: Callable[[], Callable[[object, int, int | None], None]],
        *,
        size: int | None,
        permissions: int | None,
    ) -> None:
        super().__init__(name, "wb")
        self._bind_publish = bind_publish
        self._publish: Callable[[object, int, int | None], None] | None = None
        self._size = size
        self._permissions = permissions
        self._spool: Any = None
        self._chunks: queue.Queue[bytes | object] | None = None
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None
        self._written = 0

    def __enter__(self) -> "SyncSandboxBinaryWriter":
        self._enter()
        try:
            self._publish = self._bind_publish()
            if self._size is None:
                self._spool = tempfile.TemporaryFile("w+b")
            else:
                self._chunks = queue.Queue(maxsize=1)
                reader = _QueueReader(self._chunks)

                def worker() -> None:
                    try:
                        assert self._publish is not None
                        self._publish(reader, self._size or 0, self._permissions)
                    except BaseException as exc:
                        self._error = exc

                self._thread = threading.Thread(target=worker, daemon=True)
                self._thread.start()
        except BaseException:
            self._mark_closed()
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self.close()
        else:
            self._abort()

    def _put(self, item: bytes | object) -> None:
        assert self._chunks is not None
        while True:
            if self._error is not None:
                raise self._error
            try:
                self._chunks.put(item, timeout=0.05)
                return
            except queue.Full:
                continue

    def write(self, data: bytes, /) -> int:
        self._ensure_active()
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"a bytes-like object is required, not {type(data).__name__}")
        chunk = bytes(data)
        if self._size is not None and self._written + len(chunk) > self._size:
            raise SandboxUploadSizeMismatchError(
                self.name,
                declared=self._size,
                consumed=self._written + len(chunk),
                early_end=False,
            )
        if self._size is None:
            self._spool.write(chunk)
        elif chunk:
            self._put(chunk)
        self._written += len(chunk)
        return len(chunk)

    def writelines(self, lines: Iterable[bytes], /) -> None:
        for line in lines:
            self.write(line)

    def flush(self) -> None:
        self._ensure_active()
        if self._spool is not None:
            self._spool.flush()
        if self._error is not None:
            raise self._error

    def close(self) -> None:
        if self.closed:
            return
        self._ensure_active()
        try:
            if self._size is None:
                self._spool.flush()
                size = self._spool.tell()
                self._spool.seek(0)
                assert self._publish is not None
                self._publish(self._spool, size, self._permissions)
            else:
                self._put(_EOF)
                assert self._thread is not None
                self._thread.join()
                if self._error is not None:
                    raise self._error
        finally:
            if self._spool is not None:
                self._spool.close()
            self._mark_closed()

    def _abort(self) -> None:
        if self.closed:
            return
        try:
            if self._chunks is not None:
                try:
                    self._put(_ABORT)
                except BaseException:
                    pass
                if self._thread is not None:
                    self._thread.join()
        finally:
            if self._spool is not None:
                self._spool.close()
            self._mark_closed()


class SyncSandboxTextWriter(_HandleInfo):
    __slots__ = ("_binary", "_encoder")

    def __init__(
        self,
        name: str,
        bind_publish: Callable[[], Callable[[object, int, int | None], None]],
        encoding: str,
        errors: str,
        newline: str | None,
        permissions: int | None,
    ) -> None:
        super().__init__(name, "w")
        self._binary = SyncSandboxBinaryWriter(
            name, bind_publish, size=None, permissions=permissions
        )
        self._encoder = _TextEncoder(encoding, errors, newline)

    def __enter__(self) -> "SyncSandboxTextWriter":
        self._enter()
        try:
            self._binary.__enter__()
        except BaseException:
            self._mark_closed()
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self.close()
        else:
            self._binary._abort()
            self._mark_closed()

    def write(self, text: str, /) -> int:
        self._ensure_active()
        self._binary.write(self._encoder.encode(text))
        return len(text)

    def writelines(self, lines: Iterable[str], /) -> None:
        for line in lines:
            self.write(line)

    def flush(self) -> None:
        self._ensure_active()
        self._binary.flush()

    def close(self) -> None:
        if self.closed:
            return
        self._ensure_active()
        try:
            suffix = self._encoder.encode("", final=True)
            if suffix:
                self._binary.write(suffix)
            self._binary.close()
        except BaseException:
            self._binary._abort()
            raise
        finally:
            self._mark_closed()


def _sync_open_response(
    service: SandboxService,
    session_id: Callable[[], str],
    path: str,
    cwd: str | None,
) -> Callable[[], httpx.Response]:
    def open_response() -> httpx.Response:
        bound_session = session_id()
        return iter_coroutine(
            service.open_read_response(
                operation="open", session_id=bound_session, path=path, cwd=cwd
            )
        )

    return open_response


def _sync_publish(
    service: SandboxService,
    session_id: Callable[[], str],
    write_files_cwd: Callable[[RemotePath | None], str],
    path: str,
    cwd: RemotePath | None,
) -> Callable[[], Callable[[object, int, int | None], None]]:
    def bind() -> Callable[[object, int, int | None], None]:
        bound_session = session_id()
        resolved_cwd = write_files_cwd(cwd)
        archive_path = _normalize_tar_path(path, cwd=resolved_cwd)

        def publish(source: object, size: int, permissions: int | None) -> None:
            entry = _UploadFileEntry(
                path=path,
                size=size,
                source=source,  # type: ignore[arg-type]
                mode=permissions,
                archive_path=archive_path,
            )
            iter_coroutine(
                service.write_archive(
                    session_id=bound_session,
                    body=sync_archive_body([entry], _CHUNK_SIZE),
                    paths=(path,),
                    cwd=resolved_cwd,
                )
            )

        return publish

    return bind
