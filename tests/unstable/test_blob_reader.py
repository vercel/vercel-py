import io
import threading
from datetime import datetime, timezone
from typing import Any, cast

import anyio
import pytest

from vercel._internal.byte_stream import StagingFileRuntime
from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.unstable.blob.async_runtime import (
    AsyncBlobBinaryStream,
    AsyncBlobTextStream,
)
from vercel._internal.unstable.blob.errors import (
    BlobPreconditionFailedError,
)
from vercel._internal.unstable.blob.models import BlobRangeResponse, BlobStatResult
from vercel._internal.unstable.blob.options import BlobServiceOptions
from vercel._internal.unstable.blob.service import BlobService
from vercel._internal.unstable.blob.sync_runtime import SyncBlobBinaryStream, SyncBlobTextStream

UTC = timezone.utc


class _Chunks:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        if not self._chunks:
            raise StopAsyncIteration
        return self._chunks.pop(0)

    async def aclose(self) -> None:
        self.closed = True


class _BlockingChunks(_Chunks):
    def __init__(self) -> None:
        super().__init__([])
        self.started = anyio.Event()
        self.released = anyio.Event()
        self.close_count = 0

    async def __anext__(self) -> bytes:
        self.started.set()
        await self.released.wait()
        if self._chunks:
            return self._chunks.pop(0)
        raise StopAsyncIteration

    async def aclose(self) -> None:
        self.close_count += 1
        self.closed = True
        self.released.set()


class _ThreadBlockingChunks(_Chunks):
    def __init__(self) -> None:
        super().__init__([])
        self.started = threading.Event()
        self.released = threading.Event()
        self.close_count = 0

    async def __anext__(self) -> bytes:
        self.started.set()
        assert self.released.wait(2)
        raise StopAsyncIteration

    async def aclose(self) -> None:
        self.close_count += 1
        self.closed = True
        self.released.set()


class _Api:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.calls: list[tuple[int, int, str]] = []

    async def stat(self, pathname: str) -> BlobStatResult:
        return BlobStatResult(
            pathname=pathname,
            url="https://store.public.blob.vercel-storage.com/object",
            download_url="https://store.public.blob.vercel-storage.com/object?download=1",
            size=len(self.body),
            etag='"v1"',
            uploaded_at=datetime(2026, 1, 1, tzinfo=UTC),
            content_type="application/octet-stream",
            content_disposition="inline",
            cache_control="",
        )

    async def read_range(self, stat, *, access, start: int, end: int):
        self.calls.append((start, end, stat.etag))
        chunks = [self.body[start : end + 1]]
        return BlobRangeResponse(
            cast(Any, _Chunks(chunks)), start=start, end=end, total=len(self.body)
        )


def _service(body: bytes, *, buffer_size: int = 4, ensure_open=lambda: None):
    api = _Api(body)
    service = BlobService(
        api_client=cast(Any, api),
        options=BlobServiceOptions(read_buffer_size=buffer_size),
        ensure_open=ensure_open,
        staging_file_runtime=cast(StagingFileRuntime, object()),
    )
    return service, api


@pytest.mark.anyio
async def test_async_binary_reader_buffers_and_seeks() -> None:
    service, api = _service(b"abc\ndef\n")
    stream = AsyncBlobBinaryStream(await service.open_reader("object", access="public"))

    assert await stream.read(2) == b"ab"
    assert await stream.readline() == b"c\n"
    assert api.calls == [(0, 3, '"v1"')]
    assert await stream.seek(-4, io.SEEK_END) == 4
    target = bytearray(3)
    assert await stream.readinto(target) == 3
    assert bytes(target) == b"def"
    assert stream.stat.etag == '"v1"'
    assert stream.name == "object"
    await stream.close()
    with pytest.raises(ValueError, match="closed"):
        await stream.read()


@pytest.mark.anyio
async def test_large_read_consumes_existing_read_ahead_before_fetching() -> None:
    service, api = _service(b"abcdefgh", buffer_size=4)
    stream = AsyncBlobBinaryStream(await service.open_reader("object", access="public"))

    assert await stream.read(2) == b"ab"
    assert await stream.read(5) == b"cdefg"
    assert api.calls == [(0, 3, '"v1"'), (4, 7, '"v1"')]


@pytest.mark.anyio
async def test_large_read_does_not_retain_more_than_read_ahead_window() -> None:
    service, api = _service(b"abcdefghijkl", buffer_size=3)
    state = await service.open_reader("object", access="public")
    stream = AsyncBlobBinaryStream(state)

    assert await stream.read(10) == b"abcdefghij"
    assert api.calls == [(0, 9, '"v1"')]
    assert len(state._buffer) <= 3


def test_sync_binary_reader_is_buffered_io() -> None:
    service, _ = _service(b"one\ntwo\n")
    stream = SyncBlobBinaryStream(iter_coroutine(service.open_reader("object", access="public")))

    assert isinstance(stream, io.BufferedIOBase)
    assert list(stream) == [b"one\n", b"two\n"]
    stream.seek(0)
    assert stream.read() == b"one\ntwo\n"
    stream.close()
    assert stream.closed
    with pytest.raises(ValueError):
        stream.readable()


@pytest.mark.anyio
async def test_async_binary_complete_io_and_seek_matrix() -> None:
    service, _ = _service(b"a\nbc\ndef", buffer_size=3)
    stream = AsyncBlobBinaryStream(await service.open_reader("object", access="public"))

    assert await stream.read() == b"a\nbc\ndef"
    assert await stream.seek(0) == 0
    assert await stream.read(2) == b"a\n"
    assert await stream.seek(1, io.SEEK_CUR) == 3
    assert await stream.readline() == b"c\n"
    assert await stream.seek(-3, io.SEEK_END) == 5
    target = bytearray(2)
    assert await stream.readinto(target) == 2
    assert target == b"de"
    assert await stream.seek(1, io.SEEK_SET) == 1
    assert [line async for line in stream] == [b"\n", b"bc\n", b"def"]
    with pytest.raises(ValueError, match="negative"):
        await stream.seek(-1)
    assert await stream.seek(20) == 20
    assert await stream.read() == b""

    empty_service, empty_api = _service(b"", buffer_size=3)
    empty = AsyncBlobBinaryStream(await empty_service.open_reader("empty", access="public"))
    assert await empty.read() == b""
    assert empty_api.calls == []


@pytest.mark.anyio
async def test_text_reader_decodes_boundaries_and_newlines() -> None:
    service, _ = _service("a€\r\nb\rc\n".encode(), buffer_size=2)
    stream = AsyncBlobTextStream(
        AsyncBlobBinaryStream(await service.open_reader("object", access="public")),
        encoding="utf-8",
        errors="strict",
        newline=None,
    )

    assert await stream.readline() == "a€\n"
    cookie = stream.tell()
    assert stream.tell() == cookie
    assert await stream.read() == "b\nc\n"
    assert await stream.seek(cookie) == cookie
    assert await stream.read() == "b\nc\n"


@pytest.mark.anyio
async def test_async_text_zero_relative_seeks_and_newlines_match_stdlib() -> None:
    service, _ = _service(b"a\r\nb\rc\n", buffer_size=2)
    stream = AsyncBlobTextStream(
        AsyncBlobBinaryStream(await service.open_reader("object", access="public")),
        newline=None,
    )

    assert stream.newlines is None
    assert await stream.read(1) == "a"
    cookie = stream.tell()
    assert await stream.seek(0, io.SEEK_CUR) == cookie
    with pytest.raises(io.UnsupportedOperation, match="cur-relative"):
        await stream.seek(1, io.SEEK_CUR)
    assert await stream.read() == "\nb\nc\n"
    assert stream.newlines == ("\r", "\n", "\r\n")
    natural_end_cookie = stream.tell()
    assert natural_end_cookie != stream.stat.size
    assert await stream.seek(0) == 0
    assert await stream.seek(natural_end_cookie) == natural_end_cookie
    end_cookie = await stream.seek(0, io.SEEK_END)
    assert end_cookie != stream.stat.size
    assert end_cookie == stream.tell()
    with pytest.raises(io.UnsupportedOperation, match="end-relative"):
        await stream.seek(-1, io.SEEK_END)
    assert await stream.seek(cookie) == cookie
    assert await stream.read() == "\nb\nc\n"


def test_sync_text_reader_is_text_io() -> None:
    service, _ = _service(b"a\r\nb\r")
    state = iter_coroutine(service.open_reader("object", access="public"))
    stream = SyncBlobTextStream(
        SyncBlobBinaryStream(state), encoding="utf-8", errors="strict", newline=None
    )
    assert isinstance(stream, io.TextIOBase)
    assert stream.readlines() == ["a\n", "b\n"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("newline", "expected"),
    [
        (None, ["a\n", "b\n", "c\n"]),
        ("", ["a\r\n", "b\r", "c\n"]),
        ("\n", ["a\r\n", "b\rc\n"]),
        ("\r", ["a\r", "\nb\r", "c\n"]),
        ("\r\n", ["a\r\n", "b\rc\n"]),
    ],
)
async def test_text_newline_modes(newline: str | None, expected: list[str]) -> None:
    service, _ = _service(b"a\r\nb\rc\n", buffer_size=1)
    stream = AsyncBlobTextStream(
        AsyncBlobBinaryStream(await service.open_reader("object", access="public")),
        newline=newline,
    )
    lines = [line async for line in stream]
    assert lines == expected


@pytest.mark.anyio
async def test_newlines_reports_single_then_multiple_forms_in_both_runtimes() -> None:
    service, _ = _service(b"a\nb\r\n", buffer_size=2)
    async_stream = AsyncBlobTextStream(
        AsyncBlobBinaryStream(await service.open_reader("object", access="public")),
        newline=None,
    )
    assert await async_stream.read(1) == "a"
    assert async_stream.newlines == "\n"
    assert await async_stream.read() == "\nb\n"
    assert async_stream.newlines == ("\n", "\r\n")

    sync_state = iter_coroutine(service.open_reader("object", access="public"))
    sync_stream = SyncBlobTextStream(SyncBlobBinaryStream(sync_state), newline=None)
    assert sync_stream.read(1) == "a"
    assert sync_stream.newlines == "\n"
    assert sync_stream.read() == "\nb\n"
    assert sync_stream.newlines == ("\n", "\r\n")


@pytest.mark.anyio
async def test_text_defaults_validation_and_split_multibyte_parity() -> None:
    service, _ = _service("x€y".encode(), buffer_size=1)
    async_stream = AsyncBlobTextStream(
        AsyncBlobBinaryStream(await service.open_reader("object", access="public"))
    )
    assert (async_stream.encoding, async_stream.errors) == ("utf-8", "strict")
    assert await async_stream.read() == "x€y"

    sync_state = iter_coroutine(service.open_reader("object", access="public"))
    sync_stream = SyncBlobTextStream(SyncBlobBinaryStream(sync_state))
    assert (sync_stream.encoding, sync_stream.errors) == ("utf-8", "strict")
    assert sync_stream.read() == "x€y"

    invalid_state = await service.open_reader("object", access="public")
    async_binary = AsyncBlobBinaryStream(invalid_state)
    with pytest.raises(LookupError, match="unknown encoding"):
        AsyncBlobTextStream(async_binary, encoding="not-an-encoding")
    with pytest.raises(ValueError, match="newline"):
        AsyncBlobTextStream(async_binary, newline="bad")

    invalid_state = await service.open_reader("object", access="public")
    sync_binary = SyncBlobBinaryStream(invalid_state)
    with pytest.raises(LookupError, match="unknown encoding"):
        SyncBlobTextStream(sync_binary, encoding="not-an-encoding")
    with pytest.raises(ValueError, match="newline"):
        SyncBlobTextStream(sync_binary, newline="bad")


@pytest.mark.anyio
async def test_invalid_error_handler_raises_when_decoding_fails() -> None:
    service, _ = _service(b"\xff", buffer_size=1)
    stream = AsyncBlobTextStream(
        AsyncBlobBinaryStream(await service.open_reader("object", access="public")),
        errors="not-an-error-handler",
    )
    with pytest.raises(LookupError, match="error handler"):
        await stream.read()

    sync_state = iter_coroutine(service.open_reader("object", access="public"))
    sync_stream = SyncBlobTextStream(
        SyncBlobBinaryStream(sync_state), errors="not-an-error-handler"
    )
    with pytest.raises(LookupError, match="error handler"):
        sync_stream.read()


@pytest.mark.anyio
async def test_precondition_failure_breaks_reader() -> None:
    service, api = _service(b"abcdef", buffer_size=2)
    stream = AsyncBlobBinaryStream(await service.open_reader("object", access="public"))
    assert await stream.read(2) == b"ab"

    async def replaced(*args, **kwargs):
        raise BlobPreconditionFailedError("replaced")

    api.read_range = replaced  # type: ignore[method-assign]
    with pytest.raises(BlobPreconditionFailedError, match="replaced"):
        await stream.read(2)
    with pytest.raises(BlobPreconditionFailedError, match="replaced"):
        await stream.seek(0)


@pytest.mark.anyio
async def test_async_session_close_blocks_buffered_binary_and_text_io() -> None:
    open_ = True

    def ensure_open() -> None:
        if not open_:
            raise RuntimeError("session closed")

    service, _ = _service(b"abcdef", buffer_size=4, ensure_open=ensure_open)
    binary = AsyncBlobBinaryStream(await service.open_reader("object", access="public"))
    assert await binary.read(1) == b"a"
    open_ = False
    for operation in (
        lambda: binary.read(1),
        lambda: binary.readline(),
        lambda: binary.readinto(bytearray(1)),
        lambda: binary.seek(0),
    ):
        with pytest.raises(RuntimeError, match="session closed"):
            await operation()
    assert binary.tell() == 1

    open_ = True
    text = AsyncBlobTextStream(
        AsyncBlobBinaryStream(await service.open_reader("object", access="public"))
    )
    assert await text.read(1) == "a"
    open_ = False
    with pytest.raises(RuntimeError, match="session closed"):
        await text.read(1)


@pytest.mark.anyio
async def test_async_closed_binary_and_text_contracts() -> None:
    service, _ = _service(b"a\n", buffer_size=2)
    binary = AsyncBlobBinaryStream(await service.open_reader("object", access="public"))
    await binary.close()
    await binary.close()
    assert binary.closed
    assert not binary.readable()
    assert not binary.writable()
    assert not binary.seekable()
    assert binary.name == "object"
    assert binary.stat.size == 2
    for operation in (binary.read, binary.readline, lambda: binary.seek(0), binary.tell):
        with pytest.raises(ValueError, match="closed"):
            result = operation()
            if hasattr(result, "__await__"):
                await result

    text_state = await service.open_reader("object", access="public")
    text = AsyncBlobTextStream(AsyncBlobBinaryStream(text_state))
    await text.close()
    await text.close()
    assert text.closed
    assert not text.readable()
    assert not text.writable()
    assert not text.seekable()
    assert text.encoding == "utf-8"
    assert text.stat.size == 2
    for text_operation in (text.read, text.readline, lambda: text.seek(0), text.tell):
        with pytest.raises(ValueError, match="closed"):
            text_result = text_operation()
            if hasattr(text_result, "__await__"):
                await text_result
