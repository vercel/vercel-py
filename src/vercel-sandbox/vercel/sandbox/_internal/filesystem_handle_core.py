"""Shared state machines for sync and async Sandbox file handles."""

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from vercel.internal.core.byte_stream import (
    StagingFileRuntime,
)
from vercel.internal.core.http import StreamingResponse
from vercel.sandbox._internal.filesystem_handle_common import (
    _HandleInfo,
    _TextEncoder,
    _TextReadBuffer,
    _validate_read_size,
)
from vercel.sandbox._internal.filesystem_write import (
    _BoundWrite,
    _FilesystemWriteTargetSource,
    _WriteTarget,
    _WriteTargetSource,
)
from vercel.sandbox._internal.runtime_common import (
    RemotePath,
    _normalize_tar_path,
)
from vercel.sandbox._internal.service import SandboxService

_CHUNK_SIZE = 64 * 1024


class FilesystemHandleBinding:
    def __init__(
        self,
        *,
        service: SandboxService,
        runtime: StagingFileRuntime,
        session_id: Callable[[], str],
        write_files_cwd: Callable[[RemotePath | None], str],
        path: str,
        cwd: RemotePath | None,
    ) -> None:
        self.service = service
        self.runtime = runtime
        self._session_id = session_id
        self._write_files_cwd = write_files_cwd
        self.path = path
        self.cwd = None if cwd is None else str(cwd)

    async def open_response(self) -> StreamingResponse:
        return await self.service.open_read_response(
            operation="open",
            session_id=self._session_id(),
            path=self.path,
            cwd=self.cwd,
        )

    def bind_write(self) -> _BoundWrite:
        cwd = self._write_files_cwd(self.cwd)
        return _BoundWrite(
            service=self.service,
            session_id=self._session_id(),
            path=self.path,
            cwd=cwd,
            archive_path=_normalize_tar_path(self.path, cwd=cwd),
        )

    def write_target_source(
        self, *, size: int | None, permissions: int | None
    ) -> _WriteTargetSource:
        return _FilesystemWriteTargetSource(
            name=self.path,
            runtime=self.runtime,
            bind=self.bind_write,
            size=size,
            permissions=permissions,
        )


class BinaryReaderCore(_HandleInfo):
    def __init__(self, binding: FilesystemHandleBinding) -> None:
        super().__init__(binding.path, "rb")
        self._binding = binding
        self._response: StreamingResponse | None = None
        self._buffer = bytearray()
        self._eof = False

    async def enter(self) -> None:
        self._enter()
        try:
            self._response = await self._binding.open_response()
        except BaseException:
            self._mark_closed()
            raise

    async def _pump(self) -> None:
        if self._eof:
            return
        assert self._response is not None
        try:
            chunk = await anext(self._response)
        except StopAsyncIteration:
            self._eof = True
            await self._close_response()
        except BaseException:
            self._eof = True
            await self._close_response()
            raise
        else:
            self._buffer.extend(chunk)

    async def read(self, size: int = -1) -> bytes:
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

    async def readline(self, size: int = -1) -> bytes:
        self._ensure_active()
        _validate_read_size(size)
        while True:
            limit = len(self._buffer) if size < 0 else min(size, len(self._buffer))
            newline = self._buffer.find(b"\n", 0, limit)
            if newline >= 0:
                return await self.read(newline + 1)
            if (size >= 0 and len(self._buffer) >= size) or self._eof:
                return await self.read(limit)
            await self._pump()

    async def readinto(self, buffer: object) -> int:
        self._ensure_active()
        view = memoryview(buffer)  # type: ignore[arg-type]
        if view.readonly:
            raise TypeError("readinto() argument must be read-write bytes-like object")
        data = await self.read(view.nbytes)
        view.cast("B")[: len(data)] = data
        return len(data)

    async def _close_response(self) -> None:
        response, self._response = self._response, None
        if response is not None:
            await response.aclose()

    async def close(self) -> None:
        if not self.closed:
            self._buffer.clear()
            self._eof = True
            await self._close_response()
            self._mark_closed()


class TextReaderCore(_HandleInfo):
    def __init__(
        self,
        binding: FilesystemHandleBinding,
        encoding: str,
        errors: str,
        newline: str | None,
    ) -> None:
        super().__init__(binding.path, "r")
        self._binary = BinaryReaderCore(binding)
        self._text = _TextReadBuffer(encoding, errors, newline)

    async def enter(self) -> None:
        self._enter()
        try:
            await self._binary.enter()
        except BaseException:
            self._mark_closed()
            raise

    async def _pump(self) -> None:
        data = await self._binary.read(_CHUNK_SIZE)
        self._text.feed(data, final=not data)

    async def read(self, size: int = -1) -> str:
        self._ensure_active()
        _validate_read_size(size)
        while not self._text._eof and (size < 0 or len(self._text._buffer) < size):
            await self._pump()
        return self._text.take(size)

    async def readline(self, size: int = -1) -> str:
        self._ensure_active()
        _validate_read_size(size)
        while True:
            end = self._text.line_end(size)
            if end is not None:
                return self._text.take(end)
            if (size >= 0 and len(self._text._buffer) >= size) or self._text._eof:
                limit = len(self._text._buffer) if size < 0 else min(size, len(self._text._buffer))
                return self._text.take(limit)
            await self._pump()

    async def close(self) -> None:
        if not self.closed:
            await self._binary.close()
            self._mark_closed()


class BinaryWriterCore(_HandleInfo):
    def __init__(self, source: _WriteTargetSource) -> None:
        super().__init__(source.name, "wb")
        self._source = source
        self._target: _WriteTarget | None = None

    @asynccontextmanager
    async def lifecycle(self) -> AsyncIterator[None]:
        self._enter()
        try:
            async with self._source.acquire() as target:
                self._target = target
                try:
                    yield
                except BaseException:
                    try:
                        await self.abort()
                    except BaseException:
                        pass
                    raise
                else:
                    await self.close()
                finally:
                    self._target = None
        except BaseException:
            if not self.closed:
                try:
                    await self.abort()
                except BaseException:
                    pass
            raise

    async def write(self, data: bytes) -> int:
        self._ensure_active()
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"a bytes-like object is required, not {type(data).__name__}")
        chunk = bytes(data)
        assert self._target is not None
        await self._target.write(chunk)
        return len(chunk)

    async def flush(self) -> None:
        self._ensure_active()
        assert self._target is not None
        await self._target.flush()

    async def close(self) -> None:
        if self.closed:
            return
        self._ensure_active()
        try:
            assert self._target is not None
            await self._target.finish()
        except BaseException:
            try:
                await self.abort()
            except BaseException:
                pass
            raise
        finally:
            self._mark_closed()

    async def abort(self) -> None:
        if self.closed:
            return
        try:
            if self._target is not None:
                await self._target.abort()
        finally:
            self._mark_closed()


class TextWriterCore(_HandleInfo):
    def __init__(
        self,
        binding: FilesystemHandleBinding,
        encoding: str,
        errors: str,
        newline: str | None,
        permissions: int | None,
    ) -> None:
        super().__init__(binding.path, "w")
        self._binary = BinaryWriterCore(
            binding.write_target_source(size=None, permissions=permissions)
        )
        self._encoder = _TextEncoder(encoding, errors, newline)

    @asynccontextmanager
    async def lifecycle(self) -> AsyncIterator[None]:
        self._enter()
        try:
            async with self._binary.lifecycle():
                try:
                    yield
                except BaseException:
                    self._mark_closed()
                    raise
                else:
                    await self.close()
        except BaseException:
            if not self.closed:
                self._mark_closed()
            raise

    async def write(self, text: str) -> int:
        self._ensure_active()
        await self._binary.write(self._encoder.encode(text))
        return len(text)

    async def flush(self) -> None:
        self._ensure_active()
        await self._binary.flush()

    async def close(self) -> None:
        if self.closed:
            return
        self._ensure_active()
        try:
            suffix = self._encoder.encode("", final=True)
            if suffix:
                await self._binary.write(suffix)
            await self._binary.close()
        except BaseException:
            try:
                await self._binary.abort()
            except BaseException:
                pass
            raise
        finally:
            self._mark_closed()

    async def abort(self) -> None:
        if not self.closed:
            await self._binary.abort()
            self._mark_closed()
