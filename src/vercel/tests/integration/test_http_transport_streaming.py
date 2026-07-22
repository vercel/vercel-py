import threading
from typing import cast

import anyio
import httpx
import pytest

from vercel._internal.http import (
    AsyncTransport,
    ReadResponsePolicy,
    StreamingResponse,
    SyncTransport,
)
from vercel._internal.iter_coroutine import iter_coroutine


class _SyncChunks(httpx.SyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.closed = False

    def __iter__(self):  # type: ignore[no-untyped-def]
        yield from self.chunks

    def close(self) -> None:
        self.closed = True


class _AsyncChunks(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self):  # type: ignore[no-untyped-def]
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


class _FailingSyncChunks(httpx.SyncByteStream):
    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.closed = False

    def __iter__(self):  # type: ignore[no-untyped-def]
        yield b"first\n"
        raise self.error

    def close(self) -> None:
        self.closed = True


class _FailingAsyncChunks(httpx.AsyncByteStream):
    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.closed = False

    async def __aiter__(self):  # type: ignore[no-untyped-def]
        yield b"first\n"
        raise self.error

    async def aclose(self) -> None:
        self.closed = True


def _encoded_line_chunks() -> list[bytes]:
    content = "café\r\nsecond\rthird\nlast".encode("utf-16-le")
    return [content[offset : offset + 3] for offset in range(0, len(content), 3)]


@pytest.mark.parametrize(
    ("policy", "status", "is_consumed"),
    [
        (ReadResponsePolicy.NON_SUCCESS_ONLY, 400, True),
        (ReadResponsePolicy.NEVER, 201, False),
    ],
)
def test_sync_request_stream_finishes_under_one_iter_coroutine(
    policy: ReadResponsePolicy, status: int, is_consumed: bool
) -> None:
    received: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received.extend(cast(httpx.SyncByteStream, request.stream))
        return httpx.Response(status, stream=_SyncChunks([b"response"]))

    transport = SyncTransport(httpx.Client(transport=httpx.MockTransport(handler)))

    async def operation() -> StreamingResponse:
        async with transport.request_stream(
            "POST",
            "https://example.com/upload",
            read_response=policy,
            response_chunk_size=3,
        ) as request:
            await request.write(b"first")
            await request.write(memoryview(b"second"))  # type: ignore[arg-type]
            response = await request.finish()
            assert response.response.status_code == status
            with pytest.raises(anyio.ClosedResourceError):
                await request.write(b"after finish")
            return response

    response = iter_coroutine(operation())
    assert isinstance(response, StreamingResponse)
    assert b"".join(received) == b"firstsecond"
    assert response.response.is_stream_consumed is is_consumed

    async def consume() -> list[bytes]:
        return [chunk async for chunk in response]

    assert iter_coroutine(consume()) == [b"res", b"pon", b"se"]
    assert response.response.is_closed


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("policy", "status", "is_consumed"),
    [
        (ReadResponsePolicy.NON_SUCCESS_ONLY, 400, True),
        (ReadResponsePolicy.NEVER, 201, False),
    ],
)
async def test_async_request_stream_finishes(
    policy: ReadResponsePolicy, status: int, is_consumed: bool
) -> None:
    received: list[bytes] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        async for chunk in cast(httpx.AsyncByteStream, request.stream):
            received.append(chunk)
        return httpx.Response(status, stream=_AsyncChunks([b"response"]))

    transport = AsyncTransport(httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    async with transport.request_stream(
        "POST",
        "https://example.com/upload",
        read_response=policy,
        response_chunk_size=3,
    ) as request:
        await request.write(b"first")
        await request.write(bytearray(b"second"))  # type: ignore[arg-type]
        response = await request.finish()
        assert response.response.status_code == status
        with pytest.raises(anyio.ClosedResourceError):
            await request.write(b"after finish")

    assert isinstance(response, StreamingResponse)
    assert b"".join(received) == b"firstsecond"
    assert response.response.is_stream_consumed is is_consumed
    assert [chunk async for chunk in response] == [b"res", b"pon", b"se"]
    assert response.response.is_closed


@pytest.mark.parametrize(
    ("chunks", "preconsume", "expected"),
    [
        ([], 0, b""),
        ([b"single"], 0, b"single"),
        ([b"one", b"two", b"three"], 0, b"onetwothree"),
        ([b"one", b"two", b"three"], 1, b"twothree"),
        ([b"one", b"two"], 2, b""),
    ],
)
def test_sync_response_read_consumes_remaining_body_and_closes(
    chunks: list[bytes], preconsume: int, expected: bytes
) -> None:
    body = _SyncChunks(chunks)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(202, headers={"x-result": "ok"}, stream=body)

    transport = SyncTransport(httpx.Client(transport=httpx.MockTransport(handler)))

    async def operation() -> bytes:
        stream = await transport.open_response_stream("GET", "https://example.com/result")
        for _ in range(preconsume):
            await anext(stream)
        result = await stream.read()
        assert stream.response.status_code == 202
        assert stream.response.headers["x-result"] == "ok"
        return result

    assert iter_coroutine(operation()) == expected
    assert body.closed


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("chunks", "preconsume", "expected"),
    [
        ([], 0, b""),
        ([b"single"], 0, b"single"),
        ([b"one", b"two", b"three"], 0, b"onetwothree"),
        ([b"one", b"two", b"three"], 1, b"twothree"),
        ([b"one", b"two"], 2, b""),
    ],
)
async def test_async_response_read_consumes_remaining_body_and_closes(
    chunks: list[bytes], preconsume: int, expected: bytes
) -> None:
    body = _AsyncChunks(chunks)

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(202, headers={"x-result": "ok"}, stream=body)

    transport = AsyncTransport(httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    stream = await transport.open_response_stream("GET", "https://example.com/result")
    for _ in range(preconsume):
        await anext(stream)
    assert await stream.read() == expected
    assert stream.response.status_code == 202
    assert stream.response.headers["x-result"] == "ok"
    assert body.closed


def test_sync_response_read_closes_on_failure() -> None:
    error = RuntimeError("stream failed")
    body = _FailingSyncChunks(error)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=body)

    transport = SyncTransport(httpx.Client(transport=httpx.MockTransport(handler)))

    async def operation() -> None:
        stream = await transport.open_response_stream("GET", "https://example.com/result")
        with pytest.raises(RuntimeError) as exc_info:
            await stream.read()
        assert exc_info.value is error

    iter_coroutine(operation())
    assert body.closed


@pytest.mark.anyio
async def test_async_response_read_closes_on_failure() -> None:
    error = RuntimeError("stream failed")
    body = _FailingAsyncChunks(error)

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=body)

    transport = AsyncTransport(httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    stream = await transport.open_response_stream("GET", "https://example.com/result")
    with pytest.raises(RuntimeError) as exc_info:
        await stream.read()
    assert exc_info.value is error
    assert body.closed


def test_sync_request_scope_implicitly_aborts() -> None:
    body_closed = threading.Event()

    class BlockingClient(httpx.Client):
        def send(self, request: httpx.Request, **kwargs):  # type: ignore[no-untyped-def, override]
            try:
                list(cast(httpx.SyncByteStream, request.stream))
            finally:
                body_closed.set()
            return httpx.Response(204)

    transport = SyncTransport(BlockingClient())
    retained = None

    async def operation() -> None:
        nonlocal retained
        async with transport.request_stream("POST", "https://example.com/upload") as request:
            retained = request

    iter_coroutine(operation())
    assert body_closed.is_set()
    assert retained is not None
    with pytest.raises(anyio.ClosedResourceError):
        iter_coroutine(retained.write(b"after context"))


@pytest.mark.anyio
@pytest.mark.parametrize("raise_in_body", [False, True])
async def test_async_request_scope_implicitly_aborts(raise_in_body: bool) -> None:
    body_closed = anyio.Event()
    body_error = RuntimeError("body failed")
    retained = None

    class BlockingClient(httpx.AsyncClient):
        async def send(
            self,
            request: httpx.Request,
            **kwargs,  # type: ignore[no-untyped-def, override]
        ) -> httpx.Response:
            try:
                async for _ in cast(httpx.AsyncByteStream, request.stream):
                    pass
            finally:
                body_closed.set()
            return httpx.Response(204)

    transport = AsyncTransport(BlockingClient())

    async def operation() -> None:
        nonlocal retained
        async with transport.request_stream("POST", "https://example.com/upload") as request:
            retained = request
            await request.write(b"first")
            if raise_in_body:
                raise body_error

    if raise_in_body:
        with pytest.raises(RuntimeError) as exc_info:
            await operation()
        assert exc_info.value is body_error
    else:
        await operation()
    assert body_closed.is_set()
    assert retained is not None
    with pytest.raises(anyio.ClosedResourceError):
        await retained.write(b"after context")


@pytest.mark.anyio
async def test_async_request_entry_cancellation_cleans_up_worker() -> None:
    stopped = anyio.Event()

    class BlockingTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            try:
                await anyio.sleep_forever()
            finally:
                stopped.set()
            raise AssertionError("unreachable")

    transport = AsyncTransport(httpx.AsyncClient(transport=BlockingTransport()))
    entered = False
    with anyio.move_on_after(0) as scope:
        async with transport.request_stream("POST", "https://example.com/upload"):
            entered = True
    assert scope.cancel_called
    assert not entered
    assert stopped.is_set()


@pytest.mark.anyio
async def test_async_request_cancellation_during_blocked_write_aborts() -> None:
    class BlockingTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            await anyio.sleep_forever()
            raise AssertionError("unreachable")

    transport = AsyncTransport(httpx.AsyncClient(transport=BlockingTransport()))
    async with transport.request_stream("POST", "https://example.com/upload") as request:
        await request.write(b"first")
        with anyio.move_on_after(0.01) as scope:
            await request.write(b"second")
        assert scope.cancel_called
        with pytest.raises(anyio.ClosedResourceError):
            await request.write(b"after cancellation")


@pytest.mark.anyio
async def test_async_request_stream_has_one_chunk_backpressure() -> None:
    consume = anyio.Event()
    second_started = anyio.Event()
    second_finished = anyio.Event()

    class BlockingClient(httpx.AsyncClient):
        async def send(
            self,
            request: httpx.Request,
            **kwargs,  # type: ignore[no-untyped-def, override]
        ) -> httpx.Response:
            await consume.wait()
            async for _ in cast(httpx.AsyncByteStream, request.stream):
                pass
            return httpx.Response(204)

    transport = AsyncTransport(BlockingClient())
    async with transport.request_stream("POST", "https://example.com/upload") as request:
        await request.write(b"first")

        async def write_second() -> None:
            second_started.set()
            await request.write(b"second")
            second_finished.set()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(write_second)
            await second_started.wait()
            await anyio.lowlevel.checkpoint()
            assert not second_finished.is_set()
            consume.set()
        response = await request.finish()
        await response.aclose()


def test_sync_request_stream_has_one_chunk_backpressure() -> None:
    consume = threading.Event()
    second_started = threading.Event()
    second_finished = threading.Event()
    errors: list[BaseException] = []

    class BlockingClient(httpx.Client):
        def send(self, request: httpx.Request, **kwargs):  # type: ignore[no-untyped-def, override]
            consume.wait()
            list(cast(httpx.SyncByteStream, request.stream))
            return httpx.Response(204)

    transport = SyncTransport(BlockingClient())

    async def operation() -> None:
        async with transport.request_stream("POST", "https://example.com/upload") as request:
            await request.write(b"first")
            second_started.set()
            await request.write(b"second")
            second_finished.set()
            response = await request.finish()
            await response.aclose()

    def run() -> None:
        try:
            iter_coroutine(operation())
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=run)
    worker.start()
    assert second_started.wait(timeout=1)
    assert not second_finished.is_set()
    consume.set()
    worker.join(timeout=1)
    assert not worker.is_alive()
    assert second_finished.is_set()
    assert errors == []


def test_sync_request_stream_preserves_worker_error_identity() -> None:
    error = RuntimeError("worker failed")

    def handler(request: httpx.Request) -> httpx.Response:
        raise error

    transport = SyncTransport(httpx.Client(transport=httpx.MockTransport(handler)))

    async def operation() -> None:
        async with transport.request_stream("POST", "https://example.com/upload") as request:
            with pytest.raises(RuntimeError) as exc_info:
                await request.finish()
            assert exc_info.value is error
            await request.abort()
            await request.abort()

    iter_coroutine(operation())


@pytest.mark.anyio
async def test_async_request_stream_preserves_worker_error_identity() -> None:
    error = RuntimeError("worker failed")

    async def handler(request: httpx.Request) -> httpx.Response:
        raise error

    transport = AsyncTransport(httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    async with transport.request_stream("POST", "https://example.com/upload") as request:
        with pytest.raises(RuntimeError) as exc_info:
            await request.finish()
        assert exc_info.value is error
        await request.abort()
        await request.abort()
        with pytest.raises(anyio.ClosedResourceError):
            await request.write(b"after abort")


def test_sync_response_line_stream_uses_httpx_decoding_and_closes() -> None:
    body = _SyncChunks(_encoded_line_chunks())

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/plain; charset=utf-16-le"},
            stream=body,
        )

    transport = SyncTransport(httpx.Client(transport=httpx.MockTransport(handler)))

    async def operation() -> list[str]:
        stream = await transport.open_response_stream("GET", "https://example.com/logs")
        return [line async for line in stream.aiter_lines()]

    assert iter_coroutine(operation()) == ["café", "second", "third", "last"]
    assert body.closed


@pytest.mark.anyio
async def test_async_response_line_stream_uses_httpx_decoding_and_closes() -> None:
    body = _AsyncChunks(_encoded_line_chunks())

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/plain; charset=utf-16-le"},
            stream=body,
        )

    transport = AsyncTransport(httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    stream = await transport.open_response_stream("GET", "https://example.com/logs")
    assert [line async for line in stream.aiter_lines()] == [
        "café",
        "second",
        "third",
        "last",
    ]
    assert body.closed


def test_sync_response_line_stream_closes_on_failure() -> None:
    error = RuntimeError("stream failed")
    body = _FailingSyncChunks(error)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=body)

    transport = SyncTransport(httpx.Client(transport=httpx.MockTransport(handler)))

    async def operation() -> None:
        stream = await transport.open_response_stream("GET", "https://example.com/logs")
        with pytest.raises(RuntimeError) as exc_info:
            [line async for line in stream.aiter_lines()]
        assert exc_info.value is error

    iter_coroutine(operation())
    assert body.closed


@pytest.mark.anyio
async def test_async_response_line_stream_closes_on_failure() -> None:
    error = RuntimeError("stream failed")
    body = _FailingAsyncChunks(error)

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=body)

    transport = AsyncTransport(httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    stream = await transport.open_response_stream("GET", "https://example.com/logs")
    with pytest.raises(RuntimeError) as exc_info:
        [line async for line in stream.aiter_lines()]
    assert exc_info.value is error
    assert body.closed
