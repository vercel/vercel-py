import inspect
import json
import math
import queue
import subprocess
import threading
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Iterator

import anyio
import httpx2 as httpx
import pytest
from hypothesis import example, given, settings, strategies as st

from vercel._internal.core.http import StreamingResponse
from vercel.sandbox._internal import byte_reader
from vercel.sandbox._internal.byte_reader import SyncByteReader
from vercel.sandbox._internal.errors import SandboxStreamError
from vercel.sandbox._internal.log_stream import AsyncSleep, _iter_command_logs
from vercel.sandbox._internal.models import ProcessLog


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


async def _no_sleep(seconds: float) -> None:
    pass


def _wire_logs(
    open_response: Callable[[], StreamingResponse | Awaitable[StreamingResponse]],
    sleep: AsyncSleep,
) -> AsyncGenerator[ProcessLog, None]:
    async def open_stream() -> StreamingResponse:
        response = open_response()
        return await response if inspect.isawaitable(response) else response

    return _iter_command_logs(open_stream, sleep=sleep)


def _byte_readers(
    open_response: Callable[[], StreamingResponse | Awaitable[StreamingResponse]],
    *,
    stdout: int = subprocess.PIPE,
    stderr: int = subprocess.PIPE,
    sleep: AsyncSleep = _no_sleep,
) -> tuple[byte_reader.ByteReader | None, byte_reader.ByteReader | None]:
    return byte_reader._byte_readers(
        lambda: _wire_logs(open_response, sleep), stdout=stdout, stderr=stderr
    )


def _sync_byte_readers(
    open_response: Callable[[], StreamingResponse | Awaitable[StreamingResponse]],
    *,
    stdout: int = subprocess.PIPE,
    stderr: int = subprocess.PIPE,
    sleep: AsyncSleep = _no_sleep,
) -> tuple[SyncByteReader | None, SyncByteReader | None]:
    return byte_reader._sync_byte_readers(
        lambda: _wire_logs(open_response, sleep), stdout=stdout, stderr=stderr
    )


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
async def test_async_byte_reader_lines_shared_cursor_eof_and_close(anyio_backend: str) -> None:
    async def open_response() -> StreamingResponse:
        return _logs_response(
            {"stream": "stdout", "data": "first\nsecond"},
            {"stream": "stderr", "data": "ignored\n"},
            {"stream": "stdout", "data": " line\nlast"},
        )

    reader, peer = _byte_readers(open_response)
    assert reader is not None and peer is not None

    assert await reader.read(2) == b"fi"
    assert await reader.readline() == b"rst\n"
    assert await reader.readline() == b"second line\n"
    assert [line async for line in reader] == [b"last"]
    assert await reader.readline() == b""
    with pytest.raises(anyio.EndOfStream):
        await reader.receive()

    await reader.aclose()
    assert reader.closed
    assert await peer.read() == b"ignored\n"
    with pytest.raises(anyio.ClosedResourceError):
        await reader.read()


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_byte_reader_rejects_concurrent_reads(anyio_backend: str) -> None:
    started = anyio.Event()
    release = anyio.Event()

    class PendingStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            started.set()
            await release.wait()
            yield b'{"stream":"stdout","data":"done\\n"}\n'

    async def open_response() -> StreamingResponse:
        return _streaming(httpx.Response(200, stream=PendingStream()))

    reader, _ = _byte_readers(open_response)
    assert reader is not None
    async with anyio.create_task_group() as group:
        group.start_soon(reader.readline)
        await started.wait()
        with pytest.raises(anyio.BusyResourceError):
            await reader.readline()
        release.set()


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_byte_reader_propagates_in_band_errors(anyio_backend: str) -> None:
    async def open_response() -> StreamingResponse:
        return _logs_response(
            {"stream": "stdout", "data": "before\n"},
            {"stream": "error", "data": {"code": "stopped", "message": "process stopped"}},
        )

    reader, peer = _byte_readers(open_response)
    assert reader is not None and peer is not None
    assert await reader.readline() == b"before\n"
    with pytest.raises(SandboxStreamError, match="process stopped") as exc_info:
        await reader.readline()
    assert exc_info.value.code == "stopped"
    with pytest.raises(anyio.BrokenResourceError):
        await peer.read()


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_byte_reader_breaks_peer_after_transport_failure(
    anyio_backend: str,
) -> None:
    class FailedStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            raise httpx.ReadError("connection failed")
            yield b""  # pragma: no cover

    opened = 0

    async def open_response() -> StreamingResponse:
        nonlocal opened
        opened += 1
        return _streaming(httpx.Response(200, stream=FailedStream()))

    reader, peer = _byte_readers(open_response, sleep=_no_sleep)
    assert reader is not None and peer is not None
    with pytest.raises(httpx.ReadError, match="connection failed"):
        await reader.read()
    with pytest.raises(anyio.BrokenResourceError):
        await peer.read()
    assert opened == 3


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_byte_reader_breaks_peer_after_cancellation(anyio_backend: str) -> None:
    reading = anyio.Event()

    class PendingStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            reading.set()
            await anyio.sleep_forever()
            yield b""  # pragma: no cover

    async def open_response() -> StreamingResponse:
        return _streaming(httpx.Response(200, stream=PendingStream()))

    reader, peer = _byte_readers(open_response)
    assert reader is not None and peer is not None

    async def cancel_once_reading(scope: anyio.CancelScope) -> None:
        await reading.wait()
        scope.cancel()

    # Cancel only once the read is blocked on the stream. A short deadline
    # instead can expire before the read reaches the transport, which leaves
    # the peer usable and its read waiting on this stream forever.
    with anyio.CancelScope() as scope:
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(cancel_once_reading, scope)
            await reader.read()
    assert scope.cancel_called
    with anyio.fail_after(10), pytest.raises(anyio.BrokenResourceError):
        await peer.read()


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_byte_reader_cancelled_before_reading_leaves_peer_usable(
    anyio_backend: str,
) -> None:
    async def open_response() -> StreamingResponse:
        return _logs_response({"stream": "stderr", "data": "still here\n"})

    reader, peer = _byte_readers(open_response)
    assert reader is not None and peer is not None
    with anyio.CancelScope() as scope:
        scope.cancel()
        await reader.read()

    assert await peer.read() == b"still here\n"


def test_sync_byte_reader_lines_shared_cursor_eof_and_close() -> None:
    reader, peer = _sync_byte_readers(
        lambda: _logs_response(
            {"stream": "stdout", "data": "first\nsecond"},
            {"stream": "stderr", "data": "ignored\n"},
            {"stream": "stdout", "data": " line\nlast"},
        ),
    )

    assert reader is not None and peer is not None

    assert reader.read(2) == b"fi"
    assert reader.readline() == b"rst\n"
    assert list(reader) == [b"second line\n", b"last"]
    assert reader.readline() == b""

    reader.close()
    assert reader.closed
    assert peer.read() == b"ignored\n"
    with pytest.raises(anyio.ClosedResourceError):
        reader.read()


def test_sync_byte_reader_propagates_in_band_errors() -> None:
    reader, peer = _sync_byte_readers(
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
async def test_async_byte_reader_merges_stderr_in_arrival_order(anyio_backend: str) -> None:
    stream = _TrackingAsyncStream(
        _logs_body(
            {"stream": "stdout", "data": "out-1\n"},
            {"stream": "stderr", "data": "err\n"},
            {"stream": "stdout", "data": "out-2\n"},
        )
    )

    async def open_response() -> StreamingResponse:
        return _streaming(httpx.Response(200, stream=stream))

    reader, peer = _byte_readers(open_response, stderr=subprocess.STDOUT)
    assert peer is None
    assert reader is not None
    assert await reader.readline() == b"out-1\n"
    assert await reader.read() == b"err\nout-2\n"

    await reader.aclose()
    assert stream.closed


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_byte_reader_drops_devnull_stream(anyio_backend: str) -> None:
    async def open_response() -> StreamingResponse:
        return _logs_response(
            {"stream": "stdout", "data": "dropped\n"},
            {"stream": "stderr", "data": "kept\n"},
        )

    stdout, stderr = _byte_readers(open_response, stdout=subprocess.DEVNULL)
    assert stdout is None
    assert stderr is not None
    assert await stderr.read() == b"kept\n"


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
@pytest.mark.parametrize("stderr", [subprocess.DEVNULL, subprocess.STDOUT])
async def test_async_byte_readers_with_no_streams_never_open_response(
    anyio_backend: str, stderr: int
) -> None:
    opened = 0

    async def open_response() -> StreamingResponse:
        nonlocal opened
        opened += 1
        return _logs_response()

    readers = _byte_readers(open_response, stdout=subprocess.DEVNULL, stderr=stderr)
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

    reader, _ = _byte_readers(open_response, stderr=subprocess.STDOUT)
    assert reader is not None
    assert await reader.readline() == b"before\n"
    with pytest.raises(SandboxStreamError, match="process stopped"):
        await reader.readline()
    with pytest.raises(anyio.BrokenResourceError):
        await reader.read()


def test_sync_byte_reader_merges_stderr_in_arrival_order() -> None:
    stream = _TrackingSyncStream(
        _logs_body(
            {"stream": "stdout", "data": "out-1\n"},
            {"stream": "stderr", "data": "err\n"},
            {"stream": "stdout", "data": "out-2\n"},
        )
    )

    reader, peer = _sync_byte_readers(
        lambda: _streaming(httpx.Response(200, stream=stream)), stderr=subprocess.STDOUT
    )
    assert peer is None
    assert reader is not None
    assert reader.readline() == b"out-1\n"
    assert reader.read() == b"err\nout-2\n"

    reader.close()
    assert stream.closed


def test_sync_byte_reader_drops_devnull_stream() -> None:
    stdout, stderr = _sync_byte_readers(
        lambda: _logs_response(
            {"stream": "stdout", "data": "kept\n"},
            {"stream": "stderr", "data": "dropped\n"},
        ),
        stderr=subprocess.DEVNULL,
    )
    assert stderr is None
    assert stdout is not None
    assert stdout.read() == b"kept\n"


@pytest.mark.parametrize("stderr", [subprocess.DEVNULL, subprocess.STDOUT])
def test_sync_byte_readers_with_no_streams_never_open_response(stderr: int) -> None:
    opened = 0

    def open_response() -> StreamingResponse:
        nonlocal opened
        opened += 1
        return _logs_response()

    readers = _sync_byte_readers(open_response, stdout=subprocess.DEVNULL, stderr=stderr)
    assert readers == (None, None)
    assert opened == 0


class _CutStream(httpx.SyncByteStream):
    def __init__(self, content: bytes, cut: int | None) -> None:
        self.content = content
        self.cut = cut

    def __iter__(self) -> Iterator[bytes]:
        if self.cut is None:
            yield self.content
            return
        yield self.content[: self.cut]
        raise httpx.ReadError("connection reset")


_records = st.lists(
    st.tuples(st.sampled_from(["stdout", "stderr"]), st.text(min_size=1)), max_size=6
)


@given(output=_records, replays=st.lists(st.tuples(_records, st.integers(0, 200)), max_size=2))
def test_sync_readers_deliver_interrupted_output_exactly_once(
    output: list[tuple[str, str]], replays: list[tuple[list[tuple[str, str]], int]]
) -> None:
    """Every connection replays the same output under arbitrary record boundaries."""
    expected = {
        stream: "".join(data for name, data in output if name == stream)
        for stream in ("stdout", "stderr")
    }

    def body(records: list[tuple[str, str]]) -> bytes:
        return "".join(
            json.dumps({"stream": stream, "data": data}, ensure_ascii=False) + "\n"
            for stream, data in records
        ).encode()

    def rechunk(records: list[tuple[str, str]]) -> list[tuple[str, str]]:
        # Re-split the expected output at the replay's own record boundaries.
        remaining = dict(expected)
        chunks: list[tuple[str, str]] = []
        for stream, data in records:
            size = len(data)
            chunk, remaining[stream] = remaining[stream][:size], remaining[stream][size:]
            if chunk:
                chunks.append((stream, chunk))
        chunks.extend((stream, rest) for stream, rest in remaining.items() if rest)
        return chunks

    connections = [_CutStream(body(rechunk(records)), cut) for records, cut in replays] + [
        _CutStream(body(output), None)
    ]
    responses = iter(connections)

    stdout, stderr = _sync_byte_readers(
        lambda: _streaming(httpx.Response(200, stream=next(responses))), sleep=_no_sleep
    )
    assert stdout is not None and stderr is not None
    assert (stdout.read(), stderr.read()) == (
        expected["stdout"].encode(),
        expected["stderr"].encode(),
    )


# Each turn is one server reply: a run of stdout/stderr records containing at
# least one stdout line, like a stdio JSON-RPC server logging to stderr while it
# answers on stdout. The server only replies once the client waits for it.
_TURNS = st.lists(
    st.lists(st.sampled_from(["stdout", "stderr"]), min_size=1, max_size=4).filter(
        lambda streams: "stdout" in streams
    ),
    min_size=1,
    max_size=4,
)


def _conversation(turns: list[list[str]]) -> list[list[dict[str, str]]]:
    return [
        [
            {"stream": stream, "data": f"{stream}-{turn}-{index}\n"}
            for index, stream in enumerate(streams)
        ]
        for turn, streams in enumerate(turns)
    ]


def _lines(conversation: list[list[dict[str, str]]], stream: str) -> list[str]:
    return [
        record["data"]
        for records in conversation
        for record in records
        if record["stream"] == stream
    ]


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
@settings(max_examples=25, deadline=None)
@given(turns=_TURNS)
@example(turns=[["stdout"], ["stdout"]])
@example(turns=[["stderr", "stdout"], ["stdout", "stderr"]])
def test_async_byte_readers_read_stdout_while_another_task_reads_stderr(
    backend: str, turns: list[list[str]]
) -> None:
    conversation = _conversation(turns)

    async def converse() -> list[bytes]:
        send, receive = anyio.create_memory_object_stream[bytes](math.inf)

        class ServerStream(httpx.AsyncByteStream):
            async def __aiter__(self) -> AsyncIterator[bytes]:
                async for chunk in receive:
                    yield chunk

        async def open_response() -> StreamingResponse:
            return _streaming(httpx.Response(200, stream=ServerStream()))

        stdout, stderr = _byte_readers(open_response)
        assert stdout is not None and stderr is not None
        forwarded: list[bytes] = []

        async def forward_stderr() -> None:
            while line := await stderr.readline():
                forwarded.append(line)

        async def reply(records: list[dict[str, str]]) -> None:
            # Let both readers settle into waiting before the server answers.
            for _ in range(10):
                await anyio.lowlevel.checkpoint()
            await send.send(_logs_body(*records))

        async with anyio.create_task_group() as group:
            group.start_soon(forward_stderr)
            for records in conversation:
                group.start_soon(reply, records)
                for record in records:
                    if record["stream"] == "stdout":
                        with anyio.fail_after(1):
                            assert await stdout.readline() == record["data"].encode()
            send.close()
        return forwarded

    assert anyio.run(converse, backend=backend) == [
        line.encode() for line in _lines(conversation, "stderr")
    ]


def _readline_within(reader: SyncByteReader, timeout: float = 1) -> bytes:
    result: queue.Queue[bytes] = queue.Queue()
    threading.Thread(target=lambda: result.put(reader.readline()), daemon=True).start()
    try:
        return result.get(timeout=timeout)
    except queue.Empty:
        raise TimeoutError("readline did not return") from None


@settings(max_examples=25, deadline=None)
@given(turns=_TURNS)
@example(turns=[["stdout"], ["stdout"]])
@example(turns=[["stderr", "stdout"], ["stdout", "stderr"]])
def test_sync_byte_readers_read_stdout_while_another_thread_reads_stderr(
    turns: list[list[str]],
) -> None:
    conversation = _conversation(turns)
    chunks: queue.Queue[bytes | None] = queue.Queue()

    class ServerStream(httpx.SyncByteStream):
        def __iter__(self) -> Iterator[bytes]:
            while (chunk := chunks.get()) is not None:
                yield chunk

    stdout, stderr = _sync_byte_readers(
        lambda: _streaming(httpx.Response(200, stream=ServerStream()))
    )
    assert stdout is not None and stderr is not None
    forwarded: list[bytes] = []
    forwarder = threading.Thread(target=lambda: forwarded.extend(stderr), daemon=True)
    forwarder.start()
    try:
        for records in conversation:
            threading.Timer(0.02, chunks.put, (_logs_body(*records),)).start()
            for record in records:
                if record["stream"] == "stdout":
                    assert _readline_within(stdout) == record["data"].encode()
    finally:
        chunks.put(None)
    forwarder.join(timeout=1)
    assert not forwarder.is_alive()
    assert forwarded == [line.encode() for line in _lines(conversation, "stderr")]


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_byte_reader_receives_available_bytes_without_newlines(
    anyio_backend: str,
) -> None:
    from anyio.streams.text import TextReceiveStream

    release = anyio.Event()

    class ServerStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield _logs_body({"stream": "stdout", "data": ""})
            yield _logs_body({"stream": "stdout", "data": "é雪"})
            await release.wait()
            yield _logs_body({"stream": "stdout", "data": "café\n"})

    async def open_response() -> StreamingResponse:
        return _streaming(httpx.Response(200, stream=ServerStream()))

    reader, _ = _byte_readers(open_response)
    assert isinstance(reader, anyio.abc.ByteReceiveStream)
    with pytest.raises(ValueError, match="max_bytes"):
        await reader.receive(0)
    with anyio.fail_after(1):
        # receive() returns before EOF or a newline; one byte can split UTF-8.
        assert await reader.receive(1) == b"\xc3"
        assert await reader.read(1) == b"\xa9"
        assert await reader.receive(2) == b"\xe9\x9b"
        assert await reader.receive(2) == b"\xaa"
    release.set()
    assert [chunk async for chunk in TextReceiveStream(reader)] == ["café\n"]
    assert await reader.read() == b""
    with pytest.raises(anyio.EndOfStream):
        await reader.receive()
    await reader.aclose()
    with pytest.raises(anyio.ClosedResourceError):
        await reader.receive()


def test_sync_byte_reader_sizes_count_bytes() -> None:
    reader, _ = _sync_byte_readers(
        lambda: _logs_response({"stream": "stdout", "data": "é雪\nlast"})
    )
    assert reader is not None
    assert reader.read(0) == b""
    assert reader.read(1) == b"\xc3"
    assert reader.read(1) == b"\xa9"
    assert reader.readline() == "雪\n".encode()
    assert list(reader) == [b"last"]
    assert reader.read() == b""


@given(
    records=st.lists(
        st.tuples(st.sampled_from(["stdout", "stderr"]), st.binary(max_size=32)), max_size=8
    ),
    sync=st.booleans(),
)
@settings(max_examples=25, deadline=None)
async def test_command_output_preserves_bytes(records: list[tuple[str, bytes]], sync: bool) -> None:
    import io

    from vercel.sandbox._internal.process_output import ProcessOutputRouter

    async def logs() -> AsyncGenerator[ProcessLog, None]:
        for stream, data in records:
            yield ProcessLog(stream=stream, data=data)

    stdout_sink, stderr_sink = io.BytesIO(), io.BytesIO()
    captured = ProcessOutputRouter(stdout=None, stderr=None, capture_output=True)
    routed = ProcessOutputRouter(stdout=stdout_sink, stderr=stderr_sink, capture_output=False)
    async for event in logs():
        captured.route(event)
        routed.route(event)
    expected = tuple(
        b"".join(data for stream, data in records if stream == target)
        for target in ("stdout", "stderr")
    )
    assert captured.captured() == expected
    assert (stdout_sink.getvalue(), stderr_sink.getvalue()) == expected

    if sync:
        sync_stdout, sync_stderr = byte_reader._sync_byte_readers(logs)
        assert sync_stdout is not None and sync_stderr is not None
        assert (sync_stdout.read(), sync_stderr.read()) == expected
        sync_stdout.close()
        sync_stderr.close()
    else:
        stdout, stderr = byte_reader._byte_readers(logs)
        assert stdout is not None and stderr is not None
        assert (await stdout.read(), await stderr.read()) == expected
        await stdout.aclose()
        await stderr.aclose()
