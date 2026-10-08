"""Streaming upload runtime, source adapters, and exact-length enforcement for Vercel Blob."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable, Iterator
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import NoReturn, Protocol, TypeAlias, cast

import anyio
import httpx2 as httpx

from vercel._internal.core.byte_stream import (
    AsyncByteReader,
    AsyncByteStreamRuntime,
    BytesLike,
    ReadableByteStream,
    SyncByteStreamRuntime,
)
from vercel._internal.core.http._compat import http_errors
from vercel._internal.core.http.transport import StreamingRequest
from vercel.blob._internal.validation import (
    validate_content_length,
    validate_optional_content_length,
)
from vercel.blob.errors import BlobContentLengthError, BlobUnknownError
from vercel.blob.models import PutBody, SyncPutBody

# Every body accepted by the sync or async public ``put``. Each runtime rejects
# the half of this union that belongs to the other surface.
AnyPutBody: TypeAlias = BytesLike | PutBody | SyncPutBody

_CHUNK_SIZE = 64 * 1024
_PUT_BODY_TYPE_ERROR = "put body must be bytes, a byte reader, or an iterable of bytes, got {name}"
_ASYNC_PUT_SYNC_READER_ERROR = (
    "async put requires a reader whose read is a coroutine function (async def read) "
    "or an async iterable, got reader {name}; for sync files use anyio.open_file() or "
    "anyio.wrap_file(), otherwise adapt the source into an async iterable"
)
_ASYNC_PUT_SYNC_ITERABLE_ERROR = (
    "async put requires an async byte reader or async iterable, got sync iterable {name}; "
    "adapt it into an async iterable, such as an async generator"
)
_STREAMING_TRANSPORT_ERRORS: tuple[type[Exception], ...] = (
    *http_errors(),
    anyio.BrokenResourceError,
    anyio.ClosedResourceError,
)


def _normalize_chunk(chunk: object) -> bytes:
    if not isinstance(chunk, (bytes, bytearray, memoryview)):
        raise TypeError(f"iterable must yield bytes-like objects, got {type(chunk).__name__}")
    return bytes(chunk)


def _reject_invalid_body_type(body: object) -> NoReturn:
    raise TypeError(_PUT_BODY_TYPE_ERROR.format(name=type(body).__name__))


class _ChunkSource(Protocol):
    async def next_chunk(self, max_size: int) -> bytes: ...


class _ReaderChunkSource:
    __slots__ = ("_reader",)

    def __init__(self, reader: ReadableByteStream) -> None:
        self._reader = reader

    async def next_chunk(self, max_size: int) -> bytes:
        return await self._reader.read(max_size)


class _SyncIterableChunkSource:
    __slots__ = ("_iterator",)

    def __init__(self, iterator: Iterator[bytes]) -> None:
        self._iterator = iterator

    async def next_chunk(self, max_size: int) -> bytes:
        while True:
            try:
                chunk = next(self._iterator)
            except StopIteration:
                return b""
            normalized = _normalize_chunk(chunk)
            if normalized:
                return normalized


class _AsyncIterableChunkSource:
    __slots__ = ("_iterator",)

    def __init__(self, iterator: AsyncIterator[bytes]) -> None:
        self._iterator = iterator

    async def next_chunk(self, max_size: int) -> bytes:
        while True:
            try:
                chunk = await anext(self._iterator)
            except StopAsyncIteration:
                return b""
            normalized = _normalize_chunk(chunk)
            if normalized:
                return normalized


@dataclass(frozen=True, slots=True)
class _BufferedUpload:
    body: bytes
    content_length: int


@dataclass(frozen=True, slots=True)
class _ReaderUpload:
    reader: ReadableByteStream
    content_length: int


@dataclass(frozen=True, slots=True)
class _IterableUpload:
    source: object
    content_length: int


_StreamingUpload = _ReaderUpload | _IterableUpload
_UploadBody = _BufferedUpload | _StreamingUpload


def _classify_buffered_body(body: AnyPutBody, content_length: int | None) -> _BufferedUpload | None:
    if not isinstance(body, (bytes, bytearray, memoryview)):
        return None
    snapshot = bytes(body)
    valid_length = validate_optional_content_length(content_length)
    if valid_length is not None and valid_length != len(snapshot):
        if len(snapshot) < valid_length:
            msg = (
                f"body ended after {len(snapshot)} of {valid_length} "
                "bytes declared by content_length"
            )
            raise BlobContentLengthError(msg)
        raise BlobContentLengthError(f"body exceeded content_length of {valid_length} bytes")
    return _BufferedUpload(snapshot, len(snapshot))


class UploadRuntime(Protocol):
    def classify(self, body: AnyPutBody, *, content_length: int | None) -> _UploadBody: ...
    def chunk_source(self, upload: _StreamingUpload) -> _ChunkSource: ...


class SyncUploadRuntime:
    """Upload runtime for the synchronous SDK surface."""

    __slots__ = ("_runtime",)

    def __init__(self, runtime: SyncByteStreamRuntime) -> None:
        self._runtime = runtime

    def classify(self, body: AnyPutBody, *, content_length: int | None) -> _UploadBody:
        buffered = _classify_buffered_body(body, content_length)
        if buffered is not None:
            return buffered

        if isinstance(body, str):
            _reject_invalid_body_type(body)

        read = getattr(body, "read", None)
        if callable(read):
            length = validate_content_length(content_length)
            reader = self._runtime.reader(body)  # type: ignore[arg-type]
            return _ReaderUpload(reader, length)

        if hasattr(body, "__iter__"):
            length = validate_content_length(content_length)
            return _IterableUpload(body, length)

        if hasattr(body, "__aiter__"):
            raise TypeError("sync put does not support async iterables")

        _reject_invalid_body_type(body)

    def chunk_source(self, upload: _StreamingUpload) -> _ChunkSource:
        if isinstance(upload, _ReaderUpload):
            return _ReaderChunkSource(upload.reader)
        return _SyncIterableChunkSource(iter(cast(Iterable[bytes], upload.source)))


class AsyncUploadRuntime:
    """Upload runtime for the asynchronous SDK surface."""

    __slots__ = ("_runtime",)

    def __init__(self, runtime: AsyncByteStreamRuntime) -> None:
        self._runtime = runtime

    def classify(self, body: AnyPutBody, *, content_length: int | None) -> _UploadBody:
        buffered = _classify_buffered_body(body, content_length)
        if buffered is not None:
            return buffered

        if isinstance(body, str):
            _reject_invalid_body_type(body)

        read = getattr(body, "read", None)
        if callable(read) and inspect.iscoroutinefunction(read):
            length = validate_content_length(content_length)
            reader = self._runtime.reader(cast(AsyncByteReader, body))
            return _ReaderUpload(reader, length)

        if hasattr(body, "__aiter__"):
            length = validate_content_length(content_length)
            return _IterableUpload(body, length)

        name = type(body).__name__
        if callable(read):
            raise TypeError(_ASYNC_PUT_SYNC_READER_ERROR.format(name=name))
        if hasattr(body, "__iter__"):
            raise TypeError(_ASYNC_PUT_SYNC_ITERABLE_ERROR.format(name=name))

        _reject_invalid_body_type(body)

    def chunk_source(self, upload: _StreamingUpload) -> _ChunkSource:
        if isinstance(upload, _ReaderUpload):
            return _ReaderChunkSource(upload.reader)
        return _AsyncIterableChunkSource(aiter(cast(AsyncIterable[bytes], upload.source)))


async def _transport_write(
    streaming_request: StreamingRequest,
    chunk: bytes,
    map_http_error: Callable[[httpx.Response], Exception],
) -> None:
    try:
        await streaming_request.write(chunk)
    except _STREAMING_TRANSPORT_ERRORS as exc:
        try:
            stream_resp = await streaming_request.finish()
        except _STREAMING_TRANSPORT_ERRORS:
            raise BlobUnknownError() from exc
        await stream_resp.aclose()
        if not stream_resp.response.is_success:
            raise map_http_error(stream_resp.response) from exc
        raise BlobUnknownError() from exc


async def _write_exact_length(
    streaming_request: StreamingRequest,
    source: _ChunkSource,
    content_length: int,
    map_http_error: Callable[[httpx.Response], Exception],
) -> None:
    remaining = content_length
    while remaining > 0:
        chunk = await source.next_chunk(min(_CHUNK_SIZE, remaining))
        if not chunk:
            sent = content_length - remaining
            msg = f"body ended after {sent} of {content_length} bytes declared by content_length"
            raise BlobContentLengthError(msg)
        if len(chunk) > remaining:
            raise BlobContentLengthError(f"body exceeded content_length of {content_length} bytes")
        if len(chunk) == remaining:
            probe = await source.next_chunk(1)
            if probe:
                raise BlobContentLengthError(
                    f"body exceeded content_length of {content_length} bytes"
                )
            await _transport_write(streaming_request, chunk, map_http_error)
            break
        await _transport_write(streaming_request, chunk, map_http_error)
        remaining -= len(chunk)


async def send_streaming_upload(
    request_stream_cm: AbstractAsyncContextManager[StreamingRequest],
    source: _ChunkSource,
    content_length: int,
    map_http_error: Callable[[httpx.Response], Exception],
) -> httpx.Response:
    async with request_stream_cm as streaming_request:
        await _write_exact_length(streaming_request, source, content_length, map_http_error)
        try:
            streaming_response = await streaming_request.finish()
        except _STREAMING_TRANSPORT_ERRORS as exc:
            raise BlobUnknownError() from exc
        await streaming_response.aclose()
        response = streaming_response.response
        if not response.is_success:
            raise map_http_error(response)
        return response
