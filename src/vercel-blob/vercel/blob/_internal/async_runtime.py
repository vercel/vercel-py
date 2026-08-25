"""Asynchronous Blob stream facades."""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, Generic, Protocol, TypeVar, cast

import anyio

from .models import Access, BlobStatResult
from .service import BlobService
from .streams import BinaryReaderCore, BinaryWriterCore, TextReaderCore


class _AsyncStreamProtocol(Protocol):
    async def close(self) -> None: ...


_T = TypeVar("_T", bound=_AsyncStreamProtocol)
_R = TypeVar("_R")


class OpenBlobOperation(Generic[_T]):
    """Deferred, single-use Blob open operation."""

    def __init__(self, operation: Callable[[], Awaitable[_T]]) -> None:
        self._operation = operation
        self._used = False
        self._stream: _T | None = None

    async def _open(self) -> _T:
        if self._used:
            raise RuntimeError("OpenBlobOperation is single-use")
        self._used = True
        self._stream = await self._operation()
        return self._stream

    def __await__(self):
        return self._open().__await__()

    async def __aenter__(self) -> _T:
        return await self._open()

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        assert self._stream is not None
        if exc_type is not None and hasattr(self._stream, "abort"):
            try:
                await cast(Any, self._stream).abort()
            except BaseException as cleanup_error:
                if exc is None:
                    raise
                add_note = getattr(exc, "add_note", None)
                if callable(add_note):
                    add_note(f"Blob staging cleanup also failed: {cleanup_error!r}")
        else:
            await self._stream.close()


class AsyncBlobBinaryStream:
    def __init__(self, core: BinaryReaderCore) -> None:
        self._core = core

    @property
    def closed(self) -> bool:
        return self._core.closed

    @property
    def name(self) -> str:
        return self._core.stat.pathname

    @property
    def mode(self) -> str:
        return "rb"

    @property
    def stat(self) -> BlobStatResult:
        return self._core.stat

    def tell(self) -> int:
        self._core.check()
        return self._core.position

    async def read(self, size: int | None = -1) -> bytes:
        return await self._core.read(size)

    async def readinto(self, buffer: Any) -> int:
        return await self._core.readinto(buffer)

    async def readline(self, size: int | None = -1) -> bytes:
        return await self._core.readline(size)

    async def close(self) -> None:
        await self._core.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        line = await self.readline()
        if not line:
            raise StopAsyncIteration
        return line


class AsyncBlobBinaryWriter:
    def __init__(self, core: BinaryWriterCore) -> None:
        self._core = core
        self._lock = anyio.Lock()

    async def _run(self, operation: Callable[[], Awaitable[_R]]) -> _R:
        async with self._lock:
            try:
                return await operation()
            except BaseException as error:
                broken = self._core.broken
                if broken is not None:
                    retry = isinstance(error, anyio.get_cancelled_exc_class())
                    with anyio.CancelScope(shield=True):
                        await self._core.cleanup_after_failure(retry=retry)
                if broken is not None and broken is not error:
                    raise broken from error
                raise

    @property
    def closed(self) -> bool:
        return self._core.closed

    @property
    def name(self) -> str:
        return self._core.pathname

    @property
    def mode(self) -> str:
        return "wb"

    def tell(self) -> int:
        self._core.check()
        return self._core.position

    async def write(self, data: Any) -> int:
        return await self._run(lambda: self._core.write(data))

    async def flush(self) -> None:
        await self._run(self._core.flush)

    async def close(self) -> None:
        await self._run(self._core.close)

    async def abort(self) -> None:
        with anyio.CancelScope(shield=True):
            async with self._lock:
                await self._core.abort()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is None:
            await self.close()
        else:
            await self.abort()


class AsyncBlobTextStream:
    def __init__(
        self,
        binary: AsyncBlobBinaryStream,
        *,
        encoding: str,
        errors: str,
        newline: str | None,
    ) -> None:
        self.buffer = binary
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self._text = TextReaderCore(
            binary._core,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    @property
    def closed(self) -> bool:
        return self.buffer.closed

    @property
    def name(self) -> str:
        return self.buffer.name

    @property
    def mode(self) -> str:
        return "r"

    @property
    def stat(self) -> BlobStatResult:
        return self.buffer.stat

    @property
    def newlines(self):
        return self._text.newlines

    def tell(self) -> int:
        return self._text.tell()

    async def read(self, size: int = -1) -> str:
        return self._text.read(size)

    async def readline(self, size: int = -1) -> str:
        return self._text.readline(size)

    async def close(self) -> None:
        await self.buffer.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        line = await self.readline()
        if not line:
            raise StopAsyncIteration
        return line


class AsyncBlobTextWriter:
    def __init__(
        self,
        binary: AsyncBlobBinaryWriter,
        *,
        encoding: str,
        errors: str,
        newline: str | None,
    ) -> None:
        self.buffer = binary
        self.encoding = encoding
        self.errors = errors
        self.newline = newline

    @property
    def closed(self) -> bool:
        return self.buffer.closed

    @property
    def name(self) -> str:
        return self.buffer.name

    @property
    def mode(self) -> str:
        return "w"

    def tell(self) -> int:
        self.buffer._core.check()
        return self.buffer._core.position

    async def write(self, text: str) -> int:
        if not isinstance(text, str):
            raise TypeError("write() argument must be str")
        translated = text
        newline = self.newline
        target_newline = os.linesep if newline is None else newline
        if target_newline not in ("", "\n"):
            translated = text.replace("\n", target_newline)
        await self.buffer.write(translated.encode(self.encoding, self.errors))
        return len(text)

    async def flush(self) -> None:
        await self.buffer.flush()

    async def close(self) -> None:
        await self.buffer.close()

    async def abort(self) -> None:
        await self.buffer.abort()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is None:
            await self.close()
        else:
            await self.abort()


async def open_async_stream(
    service: BlobService,
    pathname: str,
    mode: str,
    *,
    access: Access,
    encoding: str,
    errors: str,
    newline: str | None,
    content_type: str | None,
    cache_control_max_age: timedelta | None,
) -> _AsyncStreamProtocol:
    if mode.startswith("r"):
        stat, data = await service.read_all(pathname, access=access)
        binary = AsyncBlobBinaryStream(BinaryReaderCore(service=service, stat=stat, data=data))
        if "b" in mode:
            return binary
        return AsyncBlobTextStream(binary, encoding=encoding, errors=errors, newline=newline)
    binary_writer = AsyncBlobBinaryWriter(
        await BinaryWriterCore.create(
            service=service,
            pathname=pathname,
            access=access,
            content_type=content_type,
            cache_control_max_age=cache_control_max_age,
        )
    )
    if "b" in mode:
        return binary_writer
    return AsyncBlobTextWriter(binary_writer, encoding=encoding, errors=errors, newline=newline)
