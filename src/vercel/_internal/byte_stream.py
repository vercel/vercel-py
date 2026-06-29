"""Adapt byte sources for business logic shared by sync and async APIs.

Shared code consumes the async-shaped ``ReadableByteStream`` and
``StagingByteFile`` protocols. Callers select the runtime matching their public
API, then use its factories instead of constructing the private adapters directly.
The sync runtime never suspends, while the async runtime awaits or offloads I/O as
appropriate.
"""

import inspect
import io
import tempfile
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Protocol, TypeAlias, cast

import anyio
from typing_extensions import Buffer


class SyncByteReader(Protocol):
    """Caller-provided byte source with a blocking ``read`` method."""

    def read(self, size: int = -1, /) -> bytes: ...


class AsyncByteReader(Protocol):
    """Caller-provided byte source with an asynchronous ``read`` method.

    This is structurally identical to ``ReadableByteStream`` but describes an
    input that has not yet been normalized by a byte-stream runtime.
    """

    async def read(self, size: int = -1, /) -> bytes: ...


BytesLike: TypeAlias = bytes | bytearray | memoryview
SyncByteSource: TypeAlias = BytesLike | SyncByteReader
AsyncByteSource: TypeAlias = AsyncByteReader
RawByteSource: TypeAlias = SyncByteSource | AsyncByteSource


class ReadableByteStream(Protocol):
    """Normalized readable stream consumed by shared internal workflows.

    Its async shape hides whether a runtime performs the read inline, awaits an
    async source, or moves a blocking read to a worker thread.
    """

    async def read(self, size: int = -1, /) -> bytes: ...


class StagingByteFile(ReadableByteStream, Protocol):
    """SDK-owned temporary byte file used by shared staging workflows.

    The runtime's temporary-file context manager owns the lifetime of streams
    implementing this protocol.
    """

    async def write(self, data: bytes, /) -> int: ...

    async def readinto(self, buffer: Buffer, /) -> int:
        """Read bytes into a writable buffer.

        ``Buffer`` cannot express mutability, so read-only buffers are rejected
        at runtime.
        """
        ...

    async def flush(self) -> None: ...

    async def tell(self) -> int: ...

    async def seek(self, offset: int, whence: int = 0, /) -> int: ...

    async def truncate(self, size: int | None = None, /) -> int: ...


class StagingFileRuntime(Protocol):
    """Runtime-specific temporary-file capability for shared business logic."""

    def temporary_file(self) -> AbstractAsyncContextManager[StagingByteFile]: ...


def _bytes_result(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    raise TypeError(f"byte stream returned {type(value).__name__}, expected bytes")


class _SyncReader:
    """Expose a blocking reader through an async-shaped, non-suspending method."""

    def __init__(self, source: SyncByteReader) -> None:
        self._source = source

    async def read(self, size: int = -1, /) -> bytes:
        return _bytes_result(self._source.read(size))


class _MemoryReader:
    """Give an immutable bytes snapshot a stateful, non-suspending read cursor."""

    def __init__(self, data: BytesLike) -> None:
        self._data = memoryview(bytes(data))
        self._offset = 0

    async def read(self, size: int = -1, /) -> bytes:
        remaining = self._data[self._offset :]
        if size < 0:
            self._offset = len(self._data)
            return bytes(remaining)
        chunk = bytes(remaining[:size])
        self._offset += len(chunk)
        return chunk


class _AsyncReader:
    """Normalize a genuinely asynchronous reader and validate its results."""

    def __init__(self, source: AsyncByteReader) -> None:
        self._source = source

    async def read(self, size: int = -1, /) -> bytes:
        return _bytes_result(await self._source.read(size))


class _ThreadedSyncReader:
    """Run a blocking reader on a worker thread for use by async workflows."""

    def __init__(self, source: SyncByteReader) -> None:
        self._source = source

    async def read(self, size: int = -1, /) -> bytes:
        return _bytes_result(await anyio.to_thread.run_sync(self._source.read, size))


class _SyncTemporaryFile:
    """Expose a blocking temporary file through non-suspending async methods."""

    def __init__(self) -> None:
        self._file = cast(io.BufferedRandom, tempfile.TemporaryFile("w+b"))

    def _ensure_open(self) -> None:
        if self._file.closed:
            raise anyio.ClosedResourceError

    async def read(self, size: int = -1, /) -> bytes:
        self._ensure_open()
        return self._file.read(size)

    async def write(self, data: bytes, /) -> int:
        self._ensure_open()
        return self._file.write(data)

    async def readinto(self, buffer: Buffer, /) -> int:
        """Read bytes into a writable buffer.

        ``Buffer`` cannot express mutability, so read-only buffers are rejected
        at runtime.
        """
        self._ensure_open()
        return self._file.readinto(buffer)

    async def flush(self) -> None:
        self._ensure_open()
        self._file.flush()

    async def tell(self) -> int:
        self._ensure_open()
        return self._file.tell()

    async def seek(self, offset: int, whence: int = 0, /) -> int:
        self._ensure_open()
        return self._file.seek(offset, whence)

    async def truncate(self, size: int | None = None, /) -> int:
        self._ensure_open()
        return self._file.truncate(size)

    def close(self) -> None:
        self._file.close()


@asynccontextmanager
async def _sync_temporary_file() -> AsyncIterator[StagingByteFile]:
    file = _SyncTemporaryFile()
    try:
        yield file
    finally:
        file.close()


class SyncByteStreamRuntime:
    """Adapt blocking byte primitives for shared async-shaped workflows.

    Every operation completes without suspending so sync entry points can drive
    the shared coroutine with ``iter_coroutine`` and no event loop.
    """

    @staticmethod
    def reader(source: SyncByteSource) -> ReadableByteStream:
        if isinstance(source, (bytes, bytearray, memoryview)):
            return _MemoryReader(source)
        read = getattr(source, "read", None)
        if not callable(read):
            raise TypeError("byte source must provide a callable read method")
        if inspect.iscoroutinefunction(read):
            raise TypeError("sync byte stream runtime does not support async readers")
        return _SyncReader(cast(SyncByteReader, source))

    def temporary_file(self) -> AbstractAsyncContextManager[StagingByteFile]:
        return _sync_temporary_file()


class AsyncByteStreamRuntime:
    """Adapt byte primitives for execution under AnyIO.

    Async readers are awaited directly, while blocking readers run on a worker
    thread so they do not block the event loop.
    """

    @staticmethod
    def reader(source: RawByteSource) -> ReadableByteStream:
        if isinstance(source, (bytes, bytearray, memoryview)):
            return _MemoryReader(source)
        read = getattr(source, "read", None)
        if not callable(read):
            raise TypeError("byte source must provide a callable read method")
        if inspect.iscoroutinefunction(read):
            return _AsyncReader(cast(AsyncByteReader, source))
        return _ThreadedSyncReader(cast(SyncByteReader, source))

    def temporary_file(self) -> AbstractAsyncContextManager[StagingByteFile]:
        return cast(
            AbstractAsyncContextManager[StagingByteFile],
            anyio.TemporaryFile("w+b"),
        )


__all__ = [
    "AsyncByteReader",
    "AsyncByteSource",
    "AsyncByteStreamRuntime",
    "BytesLike",
    "RawByteSource",
    "ReadableByteStream",
    "StagingByteFile",
    "StagingFileRuntime",
    "SyncByteReader",
    "SyncByteSource",
    "SyncByteStreamRuntime",
]
