"""Download lifecycle behavior through public APIs and faulting HTTP streams."""

from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import AsyncExitStack, ExitStack
from typing import Literal

import anyio
import httpx2 as httpx
import pytest

from vercel import blob
from vercel.api import session
from vercel.blob import BlobStreamError, DownloadMetadata
from vercel.errors import VercelSessionClosedError

_URL = "https://localstore.public.blob.vercel-storage.com/core.bin"
_CHUNK = b"a" * (64 * 1024)
_HEADERS = {"content-type": "application/octet-stream", "etag": "lifecycle-etag"}
_Scenario = Literal["eof", "explicit", "eof-close-error", "explicit-close-error", "read", "session"]
_CLOSE_ERROR_XFAIL = pytest.mark.xfail(
    strict=True,
    reason="Core streaming-response adapters suppress explicit close errors",
)
_CANCEL_CLEANUP_XFAIL = pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="httpx2 automatic response cleanup is interrupted by cancellation",
)
_SCENARIOS = (
    "eof",
    "explicit",
    "eof-close-error",
    pytest.param("explicit-close-error", marks=_CLOSE_ERROR_XFAIL),
    pytest.param(
        "read",
        marks=pytest.mark.xfail(
            strict=True,
            raises=RuntimeError,
            reason="httpx2 automatic response cleanup replaces the original read error",
        ),
    ),
    "session",
)


class _ReadFailure(BaseException):
    pass


class _FaultingStream(httpx.SyncByteStream, httpx.AsyncByteStream):
    def __init__(
        self,
        *,
        read_error: BaseException | None = None,
        close_error: bool = False,
        checkpoint: bool = False,
        tail: bytes = b"b",
    ) -> None:
        self.tail = tail
        self.read_error = read_error
        self.close_error = close_error
        self.checkpoint = checkpoint
        self.close_calls = 0
        self.read_calls = 0
        self.cleanup_completed = False
        self.on_close: Callable[[], None] | None = None
        self.on_read: Callable[[], None] | None = None

    def __iter__(self) -> Iterator[bytes]:
        self.read_calls += 1
        if self.on_read is not None:
            self.on_read()
        if self.read_error is not None:
            raise self.read_error
        yield _CHUNK
        self.read_calls += 1
        yield self.tail
        self.read_calls += 1

    async def __aiter__(self) -> AsyncIterator[bytes]:
        if self.checkpoint:
            await anyio.lowlevel.checkpoint()
        for chunk in self:
            yield chunk

    def close(self) -> None:
        self.close_calls += 1
        if self.on_close is not None:
            self.on_close()
        self.cleanup_completed = True
        if self.close_error:
            raise RuntimeError("close failure")

    async def aclose(self) -> None:
        if self.checkpoint:
            await anyio.lowlevel.checkpoint()
        self.close()


def _response(stream: _FaultingStream) -> httpx.Response:
    return httpx.Response(
        200,
        headers=_HEADERS,
        stream=stream,
    )


@pytest.mark.parametrize("scenario", _SCENARIOS)
def test_sync_lifecycle_outside_event_loop(scenario: _Scenario) -> None:
    primary = _ReadFailure("read failure")
    stream = _FaultingStream(
        read_error=primary if scenario == "read" else None,
        close_error=scenario not in ("eof", "explicit"),
        tail=b"b" * (64 * 1024),
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _response(stream)

    with ExitStack() as sessions:
        sessions.enter_context(
            session(
                httpx_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler))
            )
        )
        with blob.sync.stream(_URL, access="public") as download:
            metadata = download.metadata
            assert metadata == DownloadMetadata(
                url=_URL,
                status_code=200,
                content_type="application/octet-stream",
                etag="lifecycle-etag",
                headers=_HEADERS,
            )
            assert not download.is_closed
            assert stream.read_calls == 0
            iterator = iter(download)
            assert iterator is not download
            assert iter(iterator) is iterator
            with pytest.raises(BlobStreamError, match="consumed once"):
                iter(download)

            def assert_closed() -> None:
                assert download.is_closed

            if scenario.startswith("explicit"):
                stream.on_close = assert_closed
                if scenario == "explicit-close-error":
                    with pytest.raises(RuntimeError, match="close failure"):
                        download.close()
                else:
                    download.close()
                assert stream.read_calls == 0
            elif scenario in ("read", "session"):
                if scenario == "session":
                    sessions.close()
                    stream.on_close = assert_closed
                    with pytest.raises(VercelSessionClosedError, match="session is closed"):
                        next(download)
                    assert stream.read_calls == 0
                else:
                    with pytest.raises(_ReadFailure) as caught:
                        next(download)
                    assert caught.value is primary
                    assert stream.read_calls == 1
            else:
                assert next(download) == _CHUNK
                assert next(download) == stream.tail
                if scenario == "eof-close-error":
                    with pytest.raises(RuntimeError, match="close failure"):
                        next(download)
                else:
                    with pytest.raises(StopIteration):
                        next(download)
                assert stream.read_calls == 3

            assert download.is_closed
            assert download.metadata is metadata
            download.close()
            download.close()
            assert stream.close_calls == 1
            with pytest.raises(BlobStreamError, match="closed download"):
                next(download)
            with pytest.raises(BlobStreamError, match="closed download"):
                iter(download)
    assert stream.close_calls == 1
    assert len(requests) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("scenario", _SCENARIOS)
async def test_async_lifecycle(scenario: _Scenario) -> None:
    primary = _ReadFailure("read failure")
    stream = _FaultingStream(
        read_error=primary if scenario == "read" else None,
        close_error=scenario not in ("eof", "explicit"),
        tail=b"b" * (64 * 1024),
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _response(stream)

    async with AsyncExitStack() as sessions:
        await sessions.enter_async_context(
            session(
                httpx_client_factory=lambda: httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                )
            )
        )
        async with blob.stream(_URL, access="public") as download:
            metadata = download.metadata
            assert metadata == DownloadMetadata(
                url=_URL,
                status_code=200,
                content_type="application/octet-stream",
                etag="lifecycle-etag",
                headers=_HEADERS,
            )
            assert not download.is_closed
            assert stream.read_calls == 0
            iterator = aiter(download)
            assert iterator is not download
            assert aiter(iterator) is iterator
            with pytest.raises(BlobStreamError, match="consumed once"):
                aiter(download)

            def assert_closed() -> None:
                assert download.is_closed

            if scenario.startswith("explicit"):
                stream.on_close = assert_closed
                if scenario == "explicit-close-error":
                    with pytest.raises(RuntimeError, match="close failure"):
                        await download.aclose()
                else:
                    await download.aclose()
                assert stream.read_calls == 0
            elif scenario in ("read", "session"):
                if scenario == "session":
                    await sessions.aclose()
                    stream.on_close = assert_closed
                    with pytest.raises(VercelSessionClosedError, match="session is closed"):
                        await anext(download)
                    assert stream.read_calls == 0
                else:
                    with pytest.raises(_ReadFailure) as caught:
                        await anext(download)
                    assert caught.value is primary
                    assert stream.read_calls == 1
            else:
                assert await anext(download) == _CHUNK
                assert await anext(download) == stream.tail
                if scenario == "eof-close-error":
                    with pytest.raises(RuntimeError, match="close failure"):
                        await anext(download)
                else:
                    with pytest.raises(StopAsyncIteration):
                        await anext(download)
                assert stream.read_calls == 3

            assert download.is_closed
            assert download.metadata is metadata
            await download.aclose()
            await download.aclose()
            assert stream.close_calls == 1
            with pytest.raises(BlobStreamError, match="closed download"):
                await anext(download)
            with pytest.raises(BlobStreamError, match="closed download"):
                aiter(download)
    assert stream.close_calls == 1
    assert len(requests) == 1


def test_public_sync_handle_eof_and_idempotency() -> None:
    stream = _FaultingStream()
    with session(
        httpx_client_factory=lambda: httpx.Client(
            transport=httpx.MockTransport(lambda _: _response(stream))
        )
    ):
        with blob.sync.stream(_URL, access="public") as download:
            iterator = iter(download)
            assert iter(iterator) is iterator
            assert list(iterator) == [_CHUNK, b"b"]
            assert download.is_closed
            download.close()
    assert stream.close_calls == 1


@pytest.mark.anyio
async def test_public_async_handle_eof_and_idempotency() -> None:
    stream = _FaultingStream()
    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: _response(stream))
        )
    ):
        async with blob.stream(_URL, access="public") as download:
            iterator = aiter(download)
            assert aiter(iterator) is iterator
            assert [chunk async for chunk in iterator] == [_CHUNK, b"b"]
            assert download.is_closed
            await download.aclose()
    assert stream.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("explicit", "close_error"),
    [
        pytest.param(False, False, marks=_CANCEL_CLEANUP_XFAIL, id="read"),
        pytest.param(False, True, marks=_CANCEL_CLEANUP_XFAIL, id="read-close-error"),
        pytest.param(True, False, id="explicit"),
        pytest.param(True, True, marks=_CLOSE_ERROR_XFAIL, id="explicit-close-error"),
    ],
)
async def test_async_cleanup_checkpoints_under_cancellation(
    close_error: bool, explicit: bool
) -> None:
    stream = _FaultingStream(close_error=close_error, checkpoint=True)
    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: _response(stream))
        )
    ):
        async with blob.stream(_URL, access="public") as download:

            def assert_closed() -> None:
                assert download.is_closed

            if explicit:
                stream.on_close = assert_closed
            with anyio.CancelScope() as scope:
                scope.cancel()
                if explicit:
                    if close_error:
                        with pytest.raises(RuntimeError, match="close failure"):
                            await download.aclose()
                    else:
                        await download.aclose()
                else:
                    with pytest.raises(anyio.get_cancelled_exc_class()):
                        await anext(download)

            assert stream.cleanup_completed
            assert download.is_closed
            await download.aclose()
            assert stream.close_calls == 1
            with pytest.raises(BlobStreamError, match="closed download"):
                await anext(download)
    assert stream.close_calls == 1


def test_sync_overlap_rejection_preserves_first_read() -> None:
    stream = _FaultingStream()
    with session(
        httpx_client_factory=lambda: httpx.Client(
            transport=httpx.MockTransport(lambda _: _response(stream))
        )
    ):
        with blob.sync.stream(_URL, access="public") as download:

            def overlap() -> None:
                with pytest.raises(BlobStreamError, match="Concurrent reads"):
                    next(download)
                assert not download.is_closed
                assert stream.close_calls == 0

            stream.on_read = overlap
            assert next(download) == _CHUNK
            assert not download.is_closed
            stream.on_read = None
            assert next(download) == b"b"
            with pytest.raises(StopIteration):
                next(download)
            assert download.is_closed
            assert stream.close_calls == 1


def test_direct_sync_read_claims_download() -> None:
    stream = _FaultingStream()
    with session(
        httpx_client_factory=lambda: httpx.Client(
            transport=httpx.MockTransport(lambda _: _response(stream))
        )
    ):
        with blob.sync.stream(_URL, access="public") as download:
            assert next(download) == _CHUNK
            with pytest.raises(BlobStreamError, match="consumed once"):
                iter(download)


@pytest.mark.anyio
async def test_direct_async_read_claims_download() -> None:
    stream = _FaultingStream()
    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: _response(stream))
        )
    ):
        async with blob.stream(_URL, access="public") as download:
            assert await anext(download) == _CHUNK
            with pytest.raises(BlobStreamError, match="consumed once"):
                aiter(download)


@pytest.mark.anyio
async def test_async_overlap_rejection_preserves_first_read() -> None:
    entered = anyio.Event()
    release = anyio.Event()

    class BlockingStream(_FaultingStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            entered.set()
            await release.wait()
            async for chunk in super().__aiter__():
                yield chunk

    stream = BlockingStream()
    received: list[bytes] = []
    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: _response(stream))
        )
    ):
        async with blob.stream(_URL, access="public") as download:

            async def first_read() -> None:
                received.append(await anext(download))

            async with anyio.create_task_group() as tasks:
                tasks.start_soon(first_read)
                await entered.wait()
                try:
                    with pytest.raises(BlobStreamError, match="Concurrent reads"):
                        await anext(download)
                    assert not download.is_closed
                    assert stream.close_calls == 0
                finally:
                    release.set()

            assert received == [_CHUNK]
            assert await anext(download) == b"b"
            with pytest.raises(StopAsyncIteration):
                await anext(download)
            assert download.is_closed
            assert stream.close_calls == 1
