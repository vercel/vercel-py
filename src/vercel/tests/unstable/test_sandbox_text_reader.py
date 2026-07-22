import json
import subprocess
from collections.abc import AsyncIterator, Iterator

import anyio
import httpx
import pytest

from vercel._internal.http import StreamingResponse
from vercel._internal.unstable.sandbox.errors import SandboxStreamError
from vercel._internal.unstable.sandbox.text_reader import _sync_text_readers, _text_readers


class _TestStreamingResponse(StreamingResponse):
    def __init__(self, response: httpx.Response) -> None:
        self.response = response
        self._sync_iterator: Iterator[bytes] | None = None
        self._async_iterator: AsyncIterator[bytes] | None = None
        if isinstance(response.stream, httpx.AsyncByteStream):
            self._async_iterator = response.aiter_bytes()
        else:
            self._sync_iterator = response.iter_bytes()

    async def __anext__(self) -> bytes:
        if self._async_iterator is not None:
            return await anext(self._async_iterator)
        assert self._sync_iterator is not None
        try:
            return next(self._sync_iterator)
        except StopIteration:
            raise StopAsyncIteration from None

    async def aiter_lines(self) -> AsyncIterator[str]:
        if isinstance(self.response.stream, httpx.AsyncByteStream):
            async for line in self.response.aiter_lines():
                yield line
        else:
            for line in self.response.iter_lines():
                yield line

    async def aclose(self) -> None:
        if isinstance(self.response.stream, httpx.AsyncByteStream):
            await self.response.aclose()
        else:
            self.response.close()


def _streaming(response: httpx.Response) -> StreamingResponse:
    return _TestStreamingResponse(response)


def _logs_response(*records: object) -> StreamingResponse:
    return _streaming(
        httpx.Response(200, text="\n".join(json.dumps(record) for record in records) + "\n")
    )


def _logs_body(*records: object) -> bytes:
    return ("\n".join(json.dumps(record) for record in records) + "\n").encode()


class _TrackingAsyncStream(httpx.AsyncByteStream):
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self.content

    async def aclose(self) -> None:
        self.closed = True


class _TrackingSyncStream(httpx.SyncByteStream):
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        yield self.content

    def close(self) -> None:
        self.closed = True


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_text_reader_lines_shared_cursor_eof_and_close(anyio_backend: str) -> None:
    async def open_response() -> StreamingResponse:
        return _logs_response(
            {"stream": "stdout", "data": "first\nsecond"},
            {"stream": "stderr", "data": "ignored\n"},
            {"stream": "stdout", "data": " line\nlast"},
        )

    reader, peer = _text_readers(open_response)
    assert reader is not None and peer is not None

    assert await reader.read(2) == "fi"
    assert await reader.readline() == "rst\n"
    assert await reader.receive() == "second line\n"
    assert [line async for line in reader] == ["last"]
    assert await reader.readline() == ""
    with pytest.raises(anyio.EndOfStream):
        await reader.receive()

    await reader.aclose()
    assert reader.closed
    assert await peer.read() == "ignored\n"
    with pytest.raises(anyio.ClosedResourceError):
        await reader.read()


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_text_reader_rejects_concurrent_reads(anyio_backend: str) -> None:
    started = anyio.Event()
    release = anyio.Event()

    class PendingStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            started.set()
            await release.wait()
            yield b'{"stream":"stdout","data":"done\\n"}\n'

    async def open_response() -> StreamingResponse:
        return _streaming(httpx.Response(200, stream=PendingStream()))

    reader, _ = _text_readers(open_response)
    assert reader is not None
    async with anyio.create_task_group() as group:
        group.start_soon(reader.readline)
        await started.wait()
        with pytest.raises(anyio.BusyResourceError):
            await reader.readline()
        release.set()


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_text_reader_propagates_in_band_errors(anyio_backend: str) -> None:
    async def open_response() -> StreamingResponse:
        return _logs_response(
            {"stream": "stdout", "data": "before\n"},
            {"stream": "error", "data": {"code": "stopped", "message": "process stopped"}},
        )

    reader, peer = _text_readers(open_response)
    assert reader is not None and peer is not None
    assert await reader.readline() == "before\n"
    with pytest.raises(SandboxStreamError, match="process stopped") as exc_info:
        await reader.readline()
    assert exc_info.value.code == "stopped"
    with pytest.raises(anyio.BrokenResourceError):
        await peer.read()


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_text_reader_breaks_peer_after_transport_failure(
    anyio_backend: str,
) -> None:
    class FailedStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            raise httpx.ReadError("connection failed")
            yield b""  # pragma: no cover

    async def open_response() -> StreamingResponse:
        return _streaming(httpx.Response(200, stream=FailedStream()))

    reader, peer = _text_readers(open_response)
    assert reader is not None and peer is not None
    with pytest.raises(httpx.ReadError, match="connection failed"):
        await reader.read()
    with pytest.raises(anyio.BrokenResourceError):
        await peer.read()


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_text_reader_breaks_peer_after_cancellation(anyio_backend: str) -> None:
    class PendingStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            await anyio.sleep_forever()
            yield b""  # pragma: no cover

    async def open_response() -> StreamingResponse:
        return _streaming(httpx.Response(200, stream=PendingStream()))

    reader, peer = _text_readers(open_response)
    assert reader is not None and peer is not None
    with anyio.move_on_after(0.01) as scope:
        await reader.read()
    assert scope.cancel_called
    with pytest.raises(anyio.BrokenResourceError):
        await peer.read()


def test_sync_text_reader_lines_shared_cursor_eof_and_close() -> None:
    reader, peer = _sync_text_readers(
        lambda: _logs_response(
            {"stream": "stdout", "data": "first\nsecond"},
            {"stream": "stderr", "data": "ignored\n"},
            {"stream": "stdout", "data": " line\nlast"},
        ),
    )

    assert reader is not None and peer is not None

    assert reader.read(2) == "fi"
    assert reader.readline() == "rst\n"
    assert list(reader) == ["second line\n", "last"]
    assert reader.readline() == ""

    reader.close()
    assert reader.closed
    assert peer.read() == "ignored\n"
    with pytest.raises(anyio.ClosedResourceError):
        reader.read()


def test_sync_text_reader_propagates_in_band_errors() -> None:
    reader, peer = _sync_text_readers(
        lambda: _logs_response(
            {"stream": "error", "data": {"code": "stopped", "message": "process stopped"}},
        ),
    )

    assert reader is not None and peer is not None
    with pytest.raises(SandboxStreamError, match="process stopped"):
        reader.readline()
    with pytest.raises(anyio.BrokenResourceError):
        peer.read()


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_text_reader_merges_stderr_in_arrival_order(anyio_backend: str) -> None:
    stream = _TrackingAsyncStream(
        _logs_body(
            {"stream": "stdout", "data": "out-1\n"},
            {"stream": "stderr", "data": "err\n"},
            {"stream": "stdout", "data": "out-2\n"},
        )
    )

    async def open_response() -> StreamingResponse:
        return _streaming(httpx.Response(200, stream=stream))

    reader, peer = _text_readers(open_response, stderr=subprocess.STDOUT)
    assert peer is None
    assert reader is not None
    assert await reader.readline() == "out-1\n"
    assert await reader.read() == "err\nout-2\n"

    await reader.aclose()
    assert stream.closed


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_text_reader_drops_devnull_stream(anyio_backend: str) -> None:
    async def open_response() -> StreamingResponse:
        return _logs_response(
            {"stream": "stdout", "data": "dropped\n"},
            {"stream": "stderr", "data": "kept\n"},
        )

    stdout, stderr = _text_readers(open_response, stdout=subprocess.DEVNULL)
    assert stdout is None
    assert stderr is not None
    assert await stderr.read() == "kept\n"


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
@pytest.mark.parametrize("stderr", [subprocess.DEVNULL, subprocess.STDOUT])
async def test_async_text_readers_with_no_streams_never_open_response(
    anyio_backend: str, stderr: int
) -> None:
    opened = 0

    async def open_response() -> StreamingResponse:
        nonlocal opened
        opened += 1
        return _logs_response()

    readers = _text_readers(open_response, stdout=subprocess.DEVNULL, stderr=stderr)
    assert readers == (None, None)
    assert opened == 0


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_merged_reader_propagates_in_band_errors(anyio_backend: str) -> None:
    async def open_response() -> StreamingResponse:
        return _logs_response(
            {"stream": "stderr", "data": "before\n"},
            {"stream": "error", "data": {"code": "stopped", "message": "process stopped"}},
        )

    reader, _ = _text_readers(open_response, stderr=subprocess.STDOUT)
    assert reader is not None
    assert await reader.readline() == "before\n"
    with pytest.raises(SandboxStreamError, match="process stopped"):
        await reader.readline()
    with pytest.raises(anyio.BrokenResourceError):
        await reader.read()


def test_sync_text_reader_merges_stderr_in_arrival_order() -> None:
    stream = _TrackingSyncStream(
        _logs_body(
            {"stream": "stdout", "data": "out-1\n"},
            {"stream": "stderr", "data": "err\n"},
            {"stream": "stdout", "data": "out-2\n"},
        )
    )

    reader, peer = _sync_text_readers(
        lambda: _streaming(httpx.Response(200, stream=stream)), stderr=subprocess.STDOUT
    )
    assert peer is None
    assert reader is not None
    assert reader.readline() == "out-1\n"
    assert reader.read() == "err\nout-2\n"

    reader.close()
    assert stream.closed


def test_sync_text_reader_drops_devnull_stream() -> None:
    stdout, stderr = _sync_text_readers(
        lambda: _logs_response(
            {"stream": "stdout", "data": "kept\n"},
            {"stream": "stderr", "data": "dropped\n"},
        ),
        stderr=subprocess.DEVNULL,
    )
    assert stderr is None
    assert stdout is not None
    assert stdout.read() == "kept\n"


@pytest.mark.parametrize("stderr", [subprocess.DEVNULL, subprocess.STDOUT])
def test_sync_text_readers_with_no_streams_never_open_response(stderr: int) -> None:
    opened = 0

    def open_response() -> StreamingResponse:
        nonlocal opened
        opened += 1
        return _logs_response()

    readers = _sync_text_readers(open_response, stdout=subprocess.DEVNULL, stderr=stderr)
    assert readers == (None, None)
    assert opened == 0
