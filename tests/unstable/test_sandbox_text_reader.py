import json
from collections.abc import AsyncIterator

import anyio
import httpx
import pytest

from vercel._internal.unstable.sandbox.errors import SandboxStreamError
from vercel._internal.unstable.sandbox.text_reader import _sync_text_readers, _text_readers


def _logs_response(*records: object) -> httpx.Response:
    return httpx.Response(200, text="\n".join(json.dumps(record) for record in records) + "\n")


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_text_reader_lines_shared_cursor_eof_and_close(anyio_backend: str) -> None:
    async def open_response() -> httpx.Response:
        return _logs_response(
            {"stream": "stdout", "data": "first\nsecond"},
            {"stream": "stderr", "data": "ignored\n"},
            {"stream": "stdout", "data": " line\nlast"},
        )

    reader, peer = _text_readers(open_response)

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

    async def open_response() -> httpx.Response:
        return httpx.Response(200, stream=PendingStream())

    reader, _ = _text_readers(open_response)
    async with anyio.create_task_group() as group:
        group.start_soon(reader.readline)
        await started.wait()
        with pytest.raises(anyio.BusyResourceError):
            await reader.readline()
        release.set()


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_text_reader_propagates_in_band_errors(anyio_backend: str) -> None:
    async def open_response() -> httpx.Response:
        return _logs_response(
            {"stream": "stdout", "data": "before\n"},
            {"stream": "error", "data": {"code": "stopped", "message": "process stopped"}},
        )

    reader, peer = _text_readers(open_response)
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

    async def open_response() -> httpx.Response:
        return httpx.Response(200, stream=FailedStream())

    reader, peer = _text_readers(open_response)
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

    async def open_response() -> httpx.Response:
        return httpx.Response(200, stream=PendingStream())

    reader, peer = _text_readers(open_response)
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

    with pytest.raises(SandboxStreamError, match="process stopped"):
        reader.readline()
    with pytest.raises(anyio.BrokenResourceError):
        peer.read()
