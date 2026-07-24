import io
import threading

import anyio
import pytest

from vercel.internal.core.byte_stream import (
    AsyncByteStreamRuntime,
    StagingFileRuntime,
    SyncByteStreamRuntime,
)
from vercel.internal.core.iter_coroutine import iter_coroutine


class _SyncReader:
    def __init__(self, data: bytes) -> None:
        self._source = io.BytesIO(data)

    def read(self, size: int = -1, /) -> bytes:
        return self._source.read(size)


class _AsyncReader:
    def __init__(self, data: bytes) -> None:
        self._source = io.BytesIO(data)

    async def read(self, size: int = -1, /) -> bytes:
        return self._source.read(size)


async def _assert_bytes_like_readers(runtime: SyncByteStreamRuntime) -> None:
    for value in (b"bytes", bytearray(b"bytearray"), memoryview(b"memoryview")):
        source = runtime.reader(value)
        assert await source.read(4) == bytes(value)[:4]
        assert await source.read() == bytes(value)[4:]


def test_sync_runtime_reader_operations_never_suspend() -> None:
    runtime = SyncByteStreamRuntime()

    async def operation() -> None:
        await _assert_bytes_like_readers(runtime)
        sync_source = runtime.reader(_SyncReader(b"sync"))
        assert await sync_source.read(2) == b"sy"
        assert await sync_source.read() == b"nc"

    iter_coroutine(operation())


def test_sync_runtime_rejects_async_reader() -> None:
    with pytest.raises(TypeError, match="does not support async readers"):
        SyncByteStreamRuntime().reader(_AsyncReader(b"async"))  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_async_runtime_adapts_async_readers() -> None:
    runtime = AsyncByteStreamRuntime()
    async_source = runtime.reader(_AsyncReader(b"async"))
    assert await async_source.read(2) == b"as"
    assert await async_source.read() == b"ync"


@pytest.mark.anyio
async def test_async_runtime_runs_sync_reader_on_worker_thread() -> None:
    caller_thread = threading.get_ident()
    reader_thread: int | None = None

    class Reader(_SyncReader):
        def read(self, size: int = -1, /) -> bytes:
            nonlocal reader_thread
            reader_thread = threading.get_ident()
            return super().read(size)

    runtime = AsyncByteStreamRuntime()
    sync_source = runtime.reader(Reader(b"sync"))
    assert await sync_source.read(2) == b"sy"
    assert await sync_source.read() == b"nc"
    assert reader_thread is not None
    assert reader_thread != caller_thread


def test_sync_runtime_rejects_invalid_and_non_bytes_readers() -> None:
    class MissingReader:
        pass

    class NonCallableReader:
        read = b"not callable"

    class BadSyncReader:
        def read(self, size: int = -1, /) -> str:
            return "not bytes"

    runtime = SyncByteStreamRuntime()
    for missing in (MissingReader(), NonCallableReader()):
        with pytest.raises(TypeError, match="callable read method"):
            runtime.reader(missing)  # type: ignore[arg-type]

    source = runtime.reader(BadSyncReader())  # type: ignore[arg-type]

    async def operation() -> None:
        with pytest.raises(TypeError, match="returned str, expected bytes"):
            await source.read()

    iter_coroutine(operation())


@pytest.mark.anyio
async def test_async_runtime_rejects_invalid_and_non_bytes_readers() -> None:
    class MissingReader:
        pass

    class NonCallableReader:
        read = b"not callable"

    class BadSyncReader:
        def read(self, size: int = -1, /) -> str:
            return "not bytes"

    class BadAsyncReader:
        async def read(self, size: int = -1, /) -> str:
            return "not bytes"

    runtime = AsyncByteStreamRuntime()
    for missing in (MissingReader(), NonCallableReader()):
        with pytest.raises(TypeError, match="callable read method"):
            runtime.reader(missing)  # type: ignore[arg-type]

    for bad in (BadSyncReader(), BadAsyncReader()):
        source = runtime.reader(bad)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="returned str, expected bytes"):
            await source.read()


def test_sync_temporary_file_context_never_suspends_and_owns_cleanup() -> None:
    runtime: StagingFileRuntime = SyncByteStreamRuntime()

    async def operation() -> None:
        async with runtime.temporary_file() as temporary:
            await temporary.write(b"temporary")

        with pytest.raises(anyio.ClosedResourceError):
            await temporary.read()

        with pytest.raises(ValueError, match="stop"):
            async with runtime.temporary_file() as failed:
                raise ValueError("stop")

        with pytest.raises(anyio.ClosedResourceError):
            await failed.read()

    iter_coroutine(operation())


@pytest.mark.anyio
async def test_async_temporary_file_context_owns_cleanup() -> None:
    runtime: StagingFileRuntime = AsyncByteStreamRuntime()

    async with runtime.temporary_file() as temporary:
        await temporary.write(b"temporary")

    with pytest.raises((anyio.ClosedResourceError, ValueError)):
        await temporary.read()

    with pytest.raises(ValueError, match="stop"):
        async with runtime.temporary_file() as failed:
            raise ValueError("stop")

    with pytest.raises((anyio.ClosedResourceError, ValueError)):
        await failed.read()
