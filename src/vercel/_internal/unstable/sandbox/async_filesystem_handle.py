"""Asynchronous sequential file handles for sandbox filesystems."""

from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from types import TracebackType
from typing import Any

import anyio
import httpx

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
from vercel._internal.unstable.sandbox.streaming_archive import async_archive_body

_CHUNK_SIZE = 64 * 1024


class SandboxBinaryReader(_HandleInfo):
    __slots__ = ("_buffer", "_eof", "_guard", "_iterator", "_open_response", "_response")

    def __init__(self, name: str, open_response: Callable[[], Awaitable[httpx.Response]]) -> None:
        super().__init__(name, "rb")
        self._open_response = open_response
        self._response: httpx.Response | None = None
        self._iterator: AsyncIterator[bytes] | None = None
        self._buffer = bytearray()
        self._eof = False
        self._guard = anyio.ResourceGuard("reading from")

    async def __aenter__(self) -> "SandboxBinaryReader":
        self._enter()
        try:
            self._response = await self._open_response()
            self._iterator = self._response.aiter_bytes(_CHUNK_SIZE)
        except BaseException:
            self._mark_closed()
            raise
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def _pump(self) -> None:
        if self._eof:
            return
        assert self._iterator is not None
        try:
            chunk = await anext(self._iterator)
        except StopAsyncIteration:
            self._eof = True
            await self._close_response()
        except BaseException:
            self._eof = True
            await self._close_response()
            raise
        else:
            self._buffer.extend(chunk)

    async def _read(self, size: int = -1) -> bytes:
        self._ensure_active()
        _validate_read_size(size)
        while not self._eof and (size < 0 or len(self._buffer) < size):
            await self._pump()
        if size < 0:
            result = bytes(self._buffer)
            self._buffer.clear()
            return result
        result = bytes(self._buffer[:size])
        del self._buffer[:size]
        return result

    async def read(self, size: int = -1) -> bytes:
        with self._guard:
            return await self._read(size)

    async def readline(self, size: int = -1) -> bytes:
        self._ensure_active()
        _validate_read_size(size)
        with self._guard:
            while True:
                limit = len(self._buffer) if size < 0 else min(size, len(self._buffer))
                newline = self._buffer.find(b"\n", 0, limit)
                if newline >= 0:
                    return await self._read(newline + 1)
                if (size >= 0 and len(self._buffer) >= size) or self._eof:
                    return await self._read(limit)
                await self._pump()

    async def readinto(self, buffer: object) -> int:
        self._ensure_active()
        view = memoryview(buffer)  # type: ignore[arg-type]
        if view.readonly:
            raise TypeError("readinto() argument must be read-write bytes-like object")
        with self._guard:
            data = await self._read(view.nbytes)
        view.cast("B")[: len(data)] = data
        return len(data)

    def __aiter__(self) -> "SandboxBinaryReader":
        return self

    async def __anext__(self) -> bytes:
        line = await self.readline()
        if not line:
            raise StopAsyncIteration
        return line

    async def _close_response(self) -> None:
        response, self._response = self._response, None
        self._iterator = None
        if response is not None:
            with anyio.CancelScope(shield=True):
                await response.aclose()

    async def aclose(self) -> None:
        if not self.closed:
            self._buffer.clear()
            self._eof = True
            await self._close_response()
            self._mark_closed()


class SandboxTextReader(_HandleInfo):
    __slots__ = ("_binary", "_guard", "_text")

    def __init__(
        self,
        name: str,
        open_response: Callable[[], Awaitable[httpx.Response]],
        encoding: str,
        errors: str,
        newline: str | None,
    ) -> None:
        super().__init__(name, "r")
        self._binary = SandboxBinaryReader(name, open_response)
        self._text = _TextReadBuffer(encoding, errors, newline)
        self._guard = anyio.ResourceGuard("reading from")

    async def __aenter__(self) -> "SandboxTextReader":
        self._enter()
        try:
            await self._binary.__aenter__()
        except BaseException:
            self._mark_closed()
            raise
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def _pump(self) -> None:
        data = await self._binary._read(_CHUNK_SIZE)
        self._text.feed(data, final=not data)

    async def read(self, size: int = -1) -> str:
        self._ensure_active()
        _validate_read_size(size)
        with self._guard:
            while not self._text._eof and (size < 0 or len(self._text._buffer) < size):
                await self._pump()
            return self._text.take(size)

    async def readline(self, size: int = -1) -> str:
        self._ensure_active()
        _validate_read_size(size)
        with self._guard:
            while True:
                end = self._text.line_end(size)
                if end is not None:
                    return self._text.take(end)
                if (size >= 0 and len(self._text._buffer) >= size) or self._text._eof:
                    limit = (
                        len(self._text._buffer) if size < 0 else min(size, len(self._text._buffer))
                    )
                    return self._text.take(limit)
                await self._pump()

    def __aiter__(self) -> "SandboxTextReader":
        return self

    async def __anext__(self) -> str:
        line = await self.readline()
        if not line:
            raise StopAsyncIteration
        return line

    async def aclose(self) -> None:
        if not self.closed:
            await self._binary.aclose()
            self._mark_closed()


_EOF = object()


class _ChannelReader:
    __slots__ = ("_buffer", "_receive")

    def __init__(self, receive: anyio.abc.ObjectReceiveStream[bytes | object]) -> None:
        self._receive = receive
        self._buffer = bytearray()

    async def read(self, size: int = -1) -> bytes:
        while size < 0 or len(self._buffer) < size:
            item = await self._receive.receive()
            if item is _EOF:
                break
            self._buffer.extend(item)  # type: ignore[arg-type]
        if size < 0:
            result = bytes(self._buffer)
            self._buffer.clear()
        else:
            result = bytes(self._buffer[:size])
            del self._buffer[:size]
        return result


class SandboxBinaryWriter(_HandleInfo):
    __slots__ = (
        "_bind_publish",
        "_error",
        "_guard",
        "_permissions",
        "_publish",
        "_receive",
        "_send",
        "_size",
        "_spool",
        "_spool_context",
        "_task_group",
        "_written",
    )

    def __init__(
        self,
        name: str,
        bind_publish: Callable[[], Callable[[object, int, int | None], Awaitable[None]]],
        *,
        size: int | None,
        permissions: int | None,
    ) -> None:
        super().__init__(name, "wb")
        self._bind_publish = bind_publish
        self._publish: Callable[[object, int, int | None], Awaitable[None]] | None = None
        self._size = size
        self._permissions = permissions
        self._spool: Any = None
        self._spool_context: Any = None
        self._send: anyio.abc.ObjectSendStream[bytes | object] | None = None
        self._receive: anyio.abc.ObjectReceiveStream[bytes | object] | None = None
        self._task_group: anyio.abc.TaskGroup | None = None
        self._error: BaseException | None = None
        self._written = 0
        self._guard = anyio.ResourceGuard("writing to")

    async def __aenter__(self) -> "SandboxBinaryWriter":
        self._enter()
        try:
            self._publish = self._bind_publish()
            if self._size is None:
                self._spool_context = anyio.TemporaryFile("w+b")
                self._spool = await self._spool_context.__aenter__()
            else:
                self._send, self._receive = anyio.create_memory_object_stream(1)
                reader = _ChannelReader(self._receive)
                self._task_group = anyio.create_task_group()
                await self._task_group.__aenter__()

                async def worker() -> None:
                    try:
                        assert self._publish is not None
                        await self._publish(reader, self._size or 0, self._permissions)
                    except BaseException as exc:
                        self._error = exc
                    finally:
                        assert self._receive is not None
                        await self._receive.aclose()

                self._task_group.start_soon(worker)
        except BaseException:
            self._mark_closed()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is None:
            await self.aclose()
        else:
            with anyio.CancelScope(shield=True):
                await self._abort()

    async def write(self, data: bytes, /) -> int:
        self._ensure_active()
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"a bytes-like object is required, not {type(data).__name__}")
        chunk = bytes(data)
        with self._guard:
            if self._size is not None and self._written + len(chunk) > self._size:
                raise SandboxUploadSizeMismatchError(
                    self.name,
                    declared=self._size,
                    consumed=self._written + len(chunk),
                    early_end=False,
                )
            if self._size is None:
                await self._spool.write(chunk)
            elif chunk:
                if self._error is not None:
                    raise self._error
                assert self._send is not None
                await self._send.send(chunk)
                if self._error is not None:
                    raise self._error
            self._written += len(chunk)
            return len(chunk)

    async def writelines(self, lines: Iterable[bytes], /) -> None:
        for line in lines:
            await self.write(line)

    async def flush(self) -> None:
        self._ensure_active()
        with self._guard:
            if self._spool is not None:
                await self._spool.flush()
            if self._error is not None:
                raise self._error

    async def aclose(self) -> None:
        if self.closed:
            return
        self._ensure_active()
        try:
            if self._size is None:
                await self._spool.flush()
                size = await self._spool.tell()
                await self._spool.seek(0)
                assert self._publish is not None
                await self._publish(self._spool, size, self._permissions)
            else:
                if self._written != self._size:
                    error = SandboxUploadSizeMismatchError(
                        self.name,
                        declared=self._size,
                        consumed=self._written,
                        early_end=True,
                    )
                    await self._abort()
                    raise error
                assert self._send is not None
                try:
                    await self._send.send(_EOF)
                except (anyio.BrokenResourceError, anyio.ClosedResourceError):
                    pass
                await self._finish_tasks()
                if self._error is not None:
                    raise self._error
        finally:
            if self._spool is not None:
                await self._spool_context.__aexit__(None, None, None)
            self._mark_closed()

    async def _finish_tasks(self) -> None:
        if self._task_group is not None:
            task_group, self._task_group = self._task_group, None
            await task_group.__aexit__(None, None, None)
        if self._send is not None:
            await self._send.aclose()
        if self._receive is not None:
            await self._receive.aclose()

    async def _abort(self) -> None:
        if self.closed:
            return
        try:
            if self._task_group is not None:
                self._task_group.cancel_scope.cancel()
                await self._finish_tasks()
        finally:
            if self._spool is not None:
                await self._spool_context.__aexit__(None, None, None)
            self._mark_closed()


class SandboxTextWriter(_HandleInfo):
    __slots__ = ("_binary", "_encoder", "_guard")

    def __init__(
        self,
        name: str,
        bind_publish: Callable[[], Callable[[object, int, int | None], Awaitable[None]]],
        encoding: str,
        errors: str,
        newline: str | None,
        permissions: int | None,
    ) -> None:
        super().__init__(name, "w")
        self._binary = SandboxBinaryWriter(name, bind_publish, size=None, permissions=permissions)
        self._encoder = _TextEncoder(encoding, errors, newline)
        self._guard = anyio.ResourceGuard("writing to")

    async def __aenter__(self) -> "SandboxTextWriter":
        self._enter()
        try:
            await self._binary.__aenter__()
        except BaseException:
            self._mark_closed()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is None:
            await self.aclose()
        else:
            with anyio.CancelScope(shield=True):
                await self._binary._abort()
            self._mark_closed()

    async def write(self, text: str, /) -> int:
        self._ensure_active()
        with self._guard:
            await self._binary.write(self._encoder.encode(text))
        return len(text)

    async def writelines(self, lines: Iterable[str], /) -> None:
        for line in lines:
            await self.write(line)

    async def flush(self) -> None:
        self._ensure_active()
        with self._guard:
            await self._binary.flush()

    async def aclose(self) -> None:
        if self.closed:
            return
        self._ensure_active()
        try:
            suffix = self._encoder.encode("", final=True)
            if suffix:
                await self._binary.write(suffix)
            await self._binary.aclose()
        except BaseException:
            with anyio.CancelScope(shield=True):
                await self._binary._abort()
            raise
        finally:
            self._mark_closed()


def _async_open_response(
    service: SandboxService,
    session_id: Callable[[], str],
    path: str,
    cwd: str | None,
) -> Callable[[], Awaitable[httpx.Response]]:
    async def open_response() -> httpx.Response:
        bound_session = session_id()
        return await service.open_read_response(
            operation="open", session_id=bound_session, path=path, cwd=cwd
        )

    return open_response


def _async_publish(
    service: SandboxService,
    session_id: Callable[[], str],
    write_files_cwd: Callable[[RemotePath | None], str],
    path: str,
    cwd: RemotePath | None,
) -> Callable[[], Callable[[object, int, int | None], Awaitable[None]]]:
    def bind() -> Callable[[object, int, int | None], Awaitable[None]]:
        bound_session = session_id()
        resolved_cwd = write_files_cwd(cwd)
        archive_path = _normalize_tar_path(path, cwd=resolved_cwd)

        async def publish(source: object, size: int, permissions: int | None) -> None:
            entry = _UploadFileEntry(
                path=path,
                size=size,
                source=source,  # type: ignore[arg-type]
                mode=permissions,
                archive_path=archive_path,
            )
            await service.write_archive(
                session_id=bound_session,
                body=async_archive_body([entry], _CHUNK_SIZE),
                paths=(path,),
                cwd=resolved_cwd,
            )

        return publish

    return bind
