"""Runtime-specific streaming download wrappers for Vercel Blob."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Coroutine, Iterator
from types import TracebackType

import anyio

from vercel._internal.core.errors import VercelSessionClosedError
from vercel._internal.core.http.transport import StreamingResponse
from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel.blob.errors import BlobStreamError
from vercel.blob.models import DownloadMetadata


class AsyncBlobDownload:
    """Asynchronous streaming download entered via an async context manager."""

    __slots__ = (
        "_stream",
        "_metadata",
        "_closed",
        "_consumed",
        "_reading",
        "_check_session",
    )

    def __init__(
        self,
        stream: StreamingResponse,
        metadata: DownloadMetadata,
        *,
        check_session: Callable[[], None] | None = None,
    ) -> None:
        self._stream = stream
        self._metadata = metadata
        self._closed = False
        self._consumed = False
        self._reading = False
        self._check_session = check_session

    @property
    def metadata(self) -> DownloadMetadata:
        return self._metadata

    @property
    def is_closed(self) -> bool:
        return self._closed

    def __aiter__(self) -> AsyncIterator[bytes]:
        if self._closed:
            raise BlobStreamError("Cannot read from closed download")
        if self._consumed:
            raise BlobStreamError("Download stream can only be consumed once")
        if self._check_session is not None:
            self._check_session()
        self._consumed = True
        return self._chunks()

    async def _chunks(self) -> AsyncIterator[bytes]:
        while True:
            try:
                chunk = await self.__anext__()
            except StopAsyncIteration:
                return
            yield chunk

    async def __anext__(self) -> bytes:
        if self._closed:
            raise BlobStreamError("Cannot read from closed download")
        if self._check_session is not None:
            try:
                self._check_session()
            except VercelSessionClosedError:
                with anyio.CancelScope(shield=True):
                    try:
                        await self._stream.aclose()
                    except BaseException:
                        pass
                self._closed = True
                raise

        if self._reading:
            raise BlobStreamError("Concurrent reads on the same download stream are not permitted")
        self._reading = True
        try:
            return await self._stream.__anext__()
        except StopAsyncIteration:
            self._closed = True
            with anyio.CancelScope(shield=True):
                await self._stream.aclose()
            raise
        except BaseException:
            self._closed = True
            with anyio.CancelScope(shield=True):
                try:
                    await self._stream.aclose()
                except BaseException:
                    pass
            raise
        finally:
            self._reading = False

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        with anyio.CancelScope(shield=True):
            await self._stream.aclose()


class SyncBlobDownload:
    """Synchronous streaming download entered via a standard context manager."""

    __slots__ = (
        "_stream",
        "_metadata",
        "_closed",
        "_consumed",
        "_reading",
        "_check_session",
    )

    def __init__(
        self,
        stream: StreamingResponse,
        metadata: DownloadMetadata,
        *,
        check_session: Callable[[], None] | None = None,
    ) -> None:
        self._stream = stream
        self._metadata = metadata
        self._closed = False
        self._consumed = False
        self._reading = False
        self._check_session = check_session

    @property
    def metadata(self) -> DownloadMetadata:
        return self._metadata

    @property
    def is_closed(self) -> bool:
        return self._closed

    def __iter__(self) -> Iterator[bytes]:
        if self._closed:
            raise BlobStreamError("Cannot read from closed download")
        if self._consumed:
            raise BlobStreamError("Download stream can only be consumed once")
        if self._check_session is not None:
            self._check_session()
        self._consumed = True
        return self._chunks()

    def _chunks(self) -> Iterator[bytes]:
        while True:
            try:
                chunk = self.__next__()
            except StopIteration:
                return
            yield chunk

    def __next__(self) -> bytes:
        if self._closed:
            raise BlobStreamError("Cannot read from closed download")
        if self._check_session is not None:
            try:
                self._check_session()
            except VercelSessionClosedError:
                self._closed = True
                try:
                    iter_coroutine(self._stream.aclose())
                except BaseException:
                    pass
                raise

        if self._reading:
            raise BlobStreamError("Concurrent reads on the same download stream are not permitted")
        self._reading = True
        try:
            return iter_coroutine(self._stream.__anext__())
        except StopAsyncIteration:
            self._closed = True
            iter_coroutine(self._stream.aclose())
            raise StopIteration from None
        except BaseException:
            self._closed = True
            try:
                iter_coroutine(self._stream.aclose())
            except BaseException:
                pass
            raise
        finally:
            self._reading = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        iter_coroutine(self._stream.aclose())


class AsyncDownloadContext:
    """Async context manager returned by `vercel.blob.get(...)`."""

    def __init__(
        self,
        opener: Callable[[], Coroutine[None, None, AsyncBlobDownload]],
    ) -> None:
        self._opener = opener
        self._download: AsyncBlobDownload | None = None
        self._entered = False

    async def __aenter__(self) -> AsyncBlobDownload:
        if self._entered:
            raise RuntimeError("Context manager cannot be re-entered")
        self._entered = True
        self._download = await self._opener()
        return self._download

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._download is not None:
            await self._download.aclose()


class SyncDownloadContext:
    """Synchronous context manager returned by `vercel.blob.sync.get(...)`."""

    def __init__(
        self,
        opener: Callable[[], SyncBlobDownload],
    ) -> None:
        self._opener = opener
        self._download: SyncBlobDownload | None = None
        self._entered = False

    def __enter__(self) -> SyncBlobDownload:
        if self._entered:
            raise RuntimeError("Context manager cannot be re-entered")
        self._entered = True
        self._download = self._opener()
        return self._download

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._download is not None:
            self._download.close()
