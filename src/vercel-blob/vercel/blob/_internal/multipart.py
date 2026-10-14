"""Scoped, sequential multipart uploads shared by both runtimes."""

from __future__ import annotations

from threading import Lock
from types import TracebackType
from typing import overload

import anyio

from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel.blob.errors import BlobError
from vercel.blob.models import PutBody, PutResult, SyncPutBody

from .api_client import _MultipartCreated, _PutRequest
from .service import BlobService
from .upload import AnyPutBody, _BufferedUpload

_MIN_PART_SIZE = 5 * 1024 * 1024
_MAX_PART_SIZE = 5 * 1024 * 1024 * 1024
_MAX_PARTS = 10_000


class _MultipartUpload:
    def __init__(self, service: BlobService, request: _PutRequest) -> None:
        self._service = service
        self._request = request
        self._created: _MultipartCreated | None = None
        self._entered = False
        self._closed = False
        self._failed = False
        self._state_lock = Lock()
        self._busy = False
        self._parts: list[dict[str, str | int]] = []
        self._last_size = 0
        self._result: PutResult | None = None

    @property
    def result(self) -> PutResult:
        """The published object, available only after successful context exit."""
        if self._result is None:
            raise BlobError("multipart result is available only after successful context exit")
        return self._result

    async def _start(self) -> None:
        with self._state_lock:
            if self._entered:
                self._failed = True
                raise BlobError("multipart context cannot be entered more than once")
            self._entered = True
        try:
            self._service.check_open()
            credentials = await self._service._get_credentials()
            self._created = await self._service._api_client.create_multipart(
                self._request, credentials=credentials
            )
        except BaseException:
            with self._state_lock:
                self._closed = True
                self._failed = True
            raise

    async def _upload_part(self, body: AnyPutBody, content_length: int | None) -> None:
        # Record overlap under the same lock used by the completion decision.
        # A rejected thread must not leave a window in which exit can publish.
        with self._state_lock:
            if self._busy:
                self._failed = True
                raise BlobError("overlapping multipart part uploads are not supported")
            self._busy = True
        try:
            self._service.check_open()
            with self._state_lock:
                if self._closed or self._created is None or self._failed:
                    raise BlobError("multipart upload is not active or has failed")
            if len(self._parts) >= _MAX_PARTS:
                raise BlobError("multipart uploads support at most 10,000 parts")
            if self._parts and self._last_size < _MIN_PART_SIZE:
                raise BlobError("only the final multipart part may be smaller than 5 MiB")
            upload = self._service._upload_runtime.classify(body, content_length=content_length)
            if not 0 < upload.content_length <= _MAX_PART_SIZE:
                raise BlobError("multipart part length must be between 1 byte and 5 GiB")
            source = (
                upload.body
                if isinstance(upload, _BufferedUpload)
                else self._service._upload_runtime.chunk_source(upload)
            )
            credentials = await self._service._get_credentials()
            number = len(self._parts) + 1
            etag = await self._service._api_client.upload_part(
                self._request,
                self._created,
                number,
                source,
                upload.content_length,
                credentials=credentials,
            )
            with self._state_lock:
                if self._closed or self._failed:
                    raise BlobError("multipart upload failed while a part was in flight")
                self._parts.append({"partNumber": number, "etag": etag})
                self._last_size = upload.content_length
        except BaseException:
            with self._state_lock:
                self._failed = True
            raise
        finally:
            with self._state_lock:
                self._busy = False

    async def _finish(self, exc_type: type[BaseException] | None) -> None:
        with self._state_lock:
            if self._closed:
                raise BlobError("multipart context is already closed")
            self._closed = True
            if exc_type is not None:
                self._failed = True
                return
            if self._busy or self._failed:
                self._failed = True
                raise BlobError("multipart upload failed; object was not completed")
            if not self._parts or self._created is None:
                self._failed = True
                raise BlobError("multipart upload requires at least one nonempty part")
        try:
            self._service.check_open()
            credentials = await self._service._get_credentials()
            self._result = await self._service._api_client.complete_multipart(
                self._request, self._created, self._parts, credentials=credentials
            )
        except BaseException:
            with self._state_lock:
                self._failed = True
            raise


class AsyncMultipartUpload(_MultipartUpload):
    """Async multipart context and its sequential part uploader."""

    async def __aenter__(self) -> AsyncMultipartUpload:
        await self._start()
        return self

    @overload
    async def upload_part(
        self,
        body: bytes | bytearray | memoryview,
        *,
        content_length: int | None = None,
    ) -> None: ...

    @overload
    async def upload_part(self, body: PutBody, *, content_length: int) -> None: ...

    async def upload_part(
        self,
        body: bytes | bytearray | memoryview | PutBody,
        *,
        content_length: int | None = None,
    ) -> None:
        """Upload one complete part; streaming bodies require content_length."""
        await self._upload_part(body, content_length)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            # Observe cancellation even if user code caught it inside the scope.
            if exc_type is None:
                await anyio.lowlevel.checkpoint()
            await self._finish(exc_type)
        finally:
            with self._state_lock:
                self._closed = True


class SyncMultipartUpload(_MultipartUpload):
    """Sync multipart context; runs without an event loop."""

    def __enter__(self) -> SyncMultipartUpload:
        iter_coroutine(self._start())
        return self

    @overload
    def upload_part(
        self,
        body: bytes | bytearray | memoryview,
        *,
        content_length: int | None = None,
    ) -> None: ...

    @overload
    def upload_part(self, body: SyncPutBody, *, content_length: int) -> None: ...

    def upload_part(
        self,
        body: bytes | bytearray | memoryview | SyncPutBody,
        *,
        content_length: int | None = None,
    ) -> None:
        """Upload one complete part; streaming bodies require content_length."""
        iter_coroutine(self._upload_part(body, content_length))

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        iter_coroutine(self._finish(exc_type))
