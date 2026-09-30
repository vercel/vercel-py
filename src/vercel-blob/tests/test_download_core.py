"""Lifecycle parity through public download handles and faulting streams."""

from collections.abc import AsyncIterator, Callable, Iterator
from typing import Literal

import anyio
import httpx2 as httpx
import pytest

from vercel import blob
from vercel._internal.core.errors import VercelSessionClosedError
from vercel._internal.core.http.transport import StreamingResponse
from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel.api import session
from vercel.blob._internal.download import AsyncBlobDownload, SyncBlobDownload
from vercel.blob.errors import BlobStreamError
from vercel.blob.models import DownloadMetadata

_URL = "https://localstore.public.blob.vercel-storage.com/core.bin"
_METADATA = DownloadMetadata(url=_URL, status_code=200)
_Scenario = Literal["eof", "explicit", "eof-close-error", "explicit-close-error", "read", "session"]
_SCENARIOS: tuple[_Scenario, ...] = (
    "eof",
    "explicit",
    "eof-close-error",
    "explicit-close-error",
    "read",
    "session",
)


class _ReadFailure(BaseException):
    pass


class _FaultingResponse(StreamingResponse):
    def __init__(self, *, read_error: BaseException | None = None, close_error: bool = False):
        self.response = httpx.Response(200)
        self.read_error = read_error
        self.close_error = close_error
        self.close_calls = 0
        self.read_calls = 0
        self.on_close: Callable[[], None] | None = None
        self.on_read: Callable[[], None] | None = None

    async def __anext__(self) -> bytes:
        self.read_calls += 1
        if self.on_read is not None:
            self.on_read()
        if self.read_error is not None:
            raise self.read_error
        if self.read_calls == 1:
            return b"chunk"
        raise StopAsyncIteration

    async def aclose(self) -> None:
        self.close_calls += 1
        if self.on_close is not None:
            self.on_close()
        if self.close_error:
            raise RuntimeError("close failure")

    def aiter_lines(self) -> AsyncIterator[str]:
        raise NotImplementedError


async def _next(download: AsyncBlobDownload | SyncBlobDownload) -> bytes:
    if isinstance(download, SyncBlobDownload):
        return next(download)
    return await anext(download)


async def _close(download: AsyncBlobDownload | SyncBlobDownload) -> None:
    if isinstance(download, SyncBlobDownload):
        download.close()
    else:
        await download.aclose()


async def _check_lifecycle(
    handle_type: type[AsyncBlobDownload] | type[SyncBlobDownload], scenario: _Scenario
) -> None:
    primary = _ReadFailure("read failure")
    session_error = VercelSessionClosedError("session failure")
    session_closed = False

    def check_session() -> None:
        if session_closed:
            raise session_error

    stream = _FaultingResponse(
        read_error=primary if scenario == "read" else None,
        close_error=scenario not in ("eof", "explicit"),
    )
    download = handle_type(stream, _METADATA, check_session=check_session)

    def check_closed_before_cleanup() -> None:
        assert download.is_closed

    stream.on_close = check_closed_before_cleanup
    assert download.metadata is _METADATA
    assert not download.is_closed
    iterator = iter(download) if isinstance(download, SyncBlobDownload) else aiter(download)
    assert iterator is not download
    with pytest.raises(BlobStreamError, match="consumed once"):
        if isinstance(download, SyncBlobDownload):
            iter(download)
        else:
            aiter(download)

    if scenario.startswith("explicit"):
        if scenario == "explicit-close-error":
            with pytest.raises(RuntimeError, match="close failure"):
                await _close(download)
        else:
            await _close(download)
    elif scenario in ("read", "session"):
        session_closed = scenario == "session"
        error = session_error if session_closed else primary
        with pytest.raises(type(error)) as caught:
            await _next(download)
        assert caught.value is error
        assert stream.read_calls == (0 if session_closed else 1)
    else:
        assert await _next(download) == b"chunk"
        if scenario == "eof-close-error":
            with pytest.raises(RuntimeError, match="close failure"):
                await _next(download)
        elif isinstance(download, SyncBlobDownload):
            with pytest.raises(StopIteration):
                next(download)
        else:
            with pytest.raises(StopAsyncIteration):
                await anext(download)

    assert download.is_closed
    assert not download._reading
    await _close(download)
    await _close(download)
    assert stream.close_calls == 1
    with pytest.raises(BlobStreamError, match="closed download"):
        await _next(download)
    with pytest.raises(BlobStreamError, match="closed download"):
        if isinstance(download, SyncBlobDownload):
            iter(download)
        else:
            aiter(download)


@pytest.mark.parametrize("scenario", _SCENARIOS)
def test_sync_lifecycle_outside_event_loop(scenario: _Scenario) -> None:
    iter_coroutine(_check_lifecycle(SyncBlobDownload, scenario))


@pytest.mark.anyio
@pytest.mark.parametrize("scenario", _SCENARIOS)
async def test_async_lifecycle(scenario: _Scenario) -> None:
    await _check_lifecycle(AsyncBlobDownload, scenario)


class _HTTPStream(httpx.SyncByteStream, httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.close_calls = 0

    def __iter__(self) -> Iterator[bytes]:
        yield b"a" * (64 * 1024)
        yield b"b"

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self:
            yield chunk

    def close(self) -> None:
        self.close_calls += 1

    async def aclose(self) -> None:
        await anyio.lowlevel.checkpoint()
        self.close()


def test_public_sync_handle_eof_and_idempotency() -> None:
    stream = _HTTPStream()
    with session(
        httpx_client_factory=lambda: httpx.Client(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=stream))
        )
    ):
        with blob.sync.get(_URL, access="public") as download:
            iterator = iter(download)
            assert iter(iterator) is iterator
            assert list(iterator) == [b"a" * (64 * 1024), b"b"]
            assert download.is_closed
            download.close()
    assert stream.close_calls == 1


@pytest.mark.anyio
async def test_public_async_handle_eof_and_idempotency() -> None:
    stream = _HTTPStream()
    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=stream))
        )
    ):
        async with blob.get(_URL, access="public") as download:
            iterator = aiter(download)
            assert aiter(iterator) is iterator
            assert [chunk async for chunk in iterator] == [b"a" * (64 * 1024), b"b"]
            assert download.is_closed
            await download.aclose()
    assert stream.close_calls == 1


class _CheckpointResponse(_FaultingResponse):
    def __init__(self, *, close_error: bool = False) -> None:
        super().__init__(close_error=close_error)
        self.cleanup_completed = False

    async def __anext__(self) -> bytes:
        await anyio.lowlevel.checkpoint()
        return await super().__anext__()

    async def aclose(self) -> None:
        assert self.on_close is not None
        self.on_close()
        await anyio.lowlevel.checkpoint()
        self.cleanup_completed = True
        await super().aclose()


@pytest.mark.anyio
@pytest.mark.parametrize("close_error", [False, True])
@pytest.mark.parametrize("explicit", [False, True])
async def test_async_cleanup_checkpoints_under_cancellation(
    close_error: bool, explicit: bool
) -> None:
    stream = _CheckpointResponse(close_error=close_error)
    download = AsyncBlobDownload(stream, _METADATA)

    def assert_closed() -> None:
        assert download.is_closed

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
    assert not download._reading
    await download.aclose()
    assert stream.close_calls == 1


def test_sync_overlap_rejection_preserves_first_read() -> None:
    stream = _FaultingResponse()
    download = SyncBlobDownload(stream, _METADATA)

    def overlap() -> None:
        with pytest.raises(BlobStreamError, match="Concurrent reads"):
            next(download)
        assert not download.is_closed
        assert stream.close_calls == 0

    stream.on_read = overlap
    assert next(download) == b"chunk"
    assert not download._reading
    stream.on_read = None
    with pytest.raises(StopIteration):
        next(download)
    assert stream.close_calls == 1


@pytest.mark.anyio
async def test_async_overlap_rejection_preserves_first_read() -> None:
    entered = anyio.Event()
    release = anyio.Event()

    class BlockingResponse(_FaultingResponse):
        async def __anext__(self) -> bytes:
            entered.set()
            await release.wait()
            return await super().__anext__()

    stream = BlockingResponse()
    download = AsyncBlobDownload(stream, _METADATA)
    received: list[bytes] = []

    async def first_read() -> None:
        received.append(await anext(download))

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(first_read)
        await entered.wait()
        with pytest.raises(BlobStreamError, match="Concurrent reads"):
            await anext(download)
        assert not download.is_closed
        assert stream.close_calls == 0
        release.set()

    assert received == [b"chunk"]
    assert not download._reading
    with pytest.raises(StopAsyncIteration):
        await anext(download)
    assert stream.close_calls == 1
