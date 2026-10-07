import io
from collections.abc import Callable

import anyio
import pytest

from vercel._internal.core.byte_stream import (
    AsyncByteStreamRuntime,
    BytesLike,
    StagingFileRuntime,
    SyncByteStreamRuntime,
)
from vercel._internal.core.iter_coroutine import iter_coroutine


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


async def _assert_bytes_like_readers(
    runtime: SyncByteStreamRuntime | AsyncByteStreamRuntime,
) -> None:
    # The bytearray and memoryview cases alias ``backing``, so mutating it after
    # ``reader()`` proves the runtime reads from a snapshot.
    views: tuple[Callable[[bytearray], BytesLike], ...] = (bytes, lambda b: b, memoryview)
    for view in views:
        backing = bytearray(b"buffer")
        source = runtime.reader(view(backing))
        backing[:] = b"XXXXXX"
        assert await source.read(4) == b"buff"
        assert await source.read() == b"er"
        assert await source.read() == b""
        assert await source.read(1) == b""


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
async def test_async_runtime_reader_operations() -> None:
    runtime = AsyncByteStreamRuntime()
    await _assert_bytes_like_readers(runtime)
    async_source = runtime.reader(_AsyncReader(b"async"))
    assert await async_source.read(2) == b"as"
    assert await async_source.read() == b"ync"


class _LoggedSyncReader(_SyncReader):
    def __init__(self, data: bytes) -> None:
        super().__init__(data)
        self.calls: list[str] = []

    def read(self, size: int = -1, /) -> bytes:
        self.calls.append("read")
        return super().read(size)


class _LoggedBytesIO(io.BytesIO):
    def __init__(self, data: bytes) -> None:
        self.calls: list[str] = []
        super().__init__(data)

    def read(self, size: int | None = -1, /) -> bytes:
        self.calls.append("read")
        return super().read(size)


@pytest.mark.parametrize(
    "make_reader",
    [_LoggedSyncReader, _LoggedBytesIO],
    ids=["sync-reader", "bytesio"],
)
def test_async_runtime_rejects_sync_readers_without_reading(
    make_reader: Callable[[bytes], _LoggedSyncReader | _LoggedBytesIO],
) -> None:
    reader = make_reader(b"sync")
    with pytest.raises(TypeError, match="does not support sync readers"):
        AsyncByteStreamRuntime().reader(reader)  # type: ignore[arg-type]
    assert reader.calls == []


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

    class BadAsyncReader:
        async def read(self, size: int = -1, /) -> str:
            return "not bytes"

    runtime = AsyncByteStreamRuntime()
    for missing in (MissingReader(), NonCallableReader()):
        with pytest.raises(TypeError, match="callable read method"):
            runtime.reader(missing)  # type: ignore[arg-type]

    source = runtime.reader(BadAsyncReader())  # type: ignore[arg-type]
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
