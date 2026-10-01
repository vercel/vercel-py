"""HTTP transport implementations for sync and async clients."""

from __future__ import annotations

import abc
import json
import queue
import sys
import threading
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Generator,
    Iterator,
    Mapping,
    Sequence,
)
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from types import TracebackType
from typing import Any, Final, TypeAlias, cast, final

import anyio
import httpx2 as httpx
from anyio.abc import ObjectReceiveStream, ObjectSendStream
from httpx2._client import BoundAsyncStream, BoundSyncStream

from vercel._internal.core.http._compat import is_async_http_client
from vercel._internal.core.polyfills import StrEnum
from vercel._internal.core.time import to_seconds_float

PrimitiveData: TypeAlias = str | int | float | bool | None
HeaderTypes: TypeAlias = (
    httpx.Headers
    | Mapping[str, str]
    | Mapping[bytes, bytes]
    | Sequence[tuple[str, str]]
    | Sequence[tuple[bytes, bytes]]
)
QueryParamTypes: TypeAlias = (
    httpx.QueryParams
    | Mapping[str, PrimitiveData | Sequence[PrimitiveData]]
    | list[tuple[str, PrimitiveData]]
    | tuple[tuple[str, PrimitiveData], ...]
    | str
    | bytes
)


def _normalize_path(path: str) -> str:
    return path.lstrip("/")


@dataclass(frozen=True, slots=True)
class JSONBody:
    data: Any


@dataclass(frozen=True, slots=True)
class BytesBody:
    data: bytes
    content_type: str = "application/octet-stream"


@dataclass(frozen=True, slots=True)
class RawBody:
    """Unmodified request content (bytes, iterables, async iterables, file-like, etc.)."""

    data: Any


RequestBody = JSONBody | BytesBody | RawBody | None


@final
class _NoTimeout:
    """Select no HTTPX timeout instead of inheriting the client default."""

    __slots__ = ()


NO_TIMEOUT: Final = _NoTimeout()
RequestTimeout: TypeAlias = timedelta | _NoTimeout | None


class ReadResponsePolicy(StrEnum):
    ALWAYS = "always"
    NON_SUCCESS_ONLY = "non_success_only"
    NEVER = "never"


@dataclass(frozen=True, slots=True)
class TransportOptions:
    timeout: timedelta
    base_url: str | None
    max_connections: int
    enable_http2: bool


class BaseTransport(abc.ABC):
    @abc.abstractmethod
    async def send(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: QueryParamTypes | None = None,
        body: RequestBody = None,
        headers: HeaderTypes | None = None,
        timeout: RequestTimeout = None,
        follow_redirects: bool | None = None,
        stream: bool = False,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NEVER,
    ) -> httpx.Response:
        raise NotImplementedError()

    def request_stream(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: QueryParamTypes | None = None,
        headers: HeaderTypes | None = None,
        timeout: RequestTimeout = None,
        follow_redirects: bool | None = None,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NON_SUCCESS_ONLY,
        response_chunk_size: int | None = None,
    ) -> AbstractAsyncContextManager[StreamingRequest]:
        """Open a lexical scope for an incrementally supplied request body."""
        raise NotImplementedError()

    async def open_response_stream(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: QueryParamTypes | None = None,
        body: RequestBody = None,
        headers: HeaderTypes | None = None,
        timeout: RequestTimeout = None,
        follow_redirects: bool | None = None,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NON_SUCCESS_ONLY,
        chunk_size: int | None = None,
    ) -> StreamingResponse:
        """Open a response whose body is consumed incrementally."""
        raise NotImplementedError()

    @staticmethod
    def _build_request(
        client: httpx.Client | httpx.AsyncClient,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: QueryParamTypes | None = None,
        body: RequestBody = None,
        headers: HeaderTypes | None = None,
        timeout: RequestTimeout = None,
    ) -> httpx.Request:
        headers = httpx.Headers(headers)
        if token is not None:
            headers.setdefault("authorization", f"Bearer {token}")

        json = None
        content = None
        match body:
            case JSONBody():
                json = body.data
            case BytesBody():
                content = body.data
                headers.setdefault("content-type", body.content_type)
            case RawBody():
                content = body.data

        if timeout is NO_TIMEOUT:
            return client.build_request(
                method,
                _normalize_path(path),
                params=params,
                timeout=None,
                headers=headers,
                json=json,
                content=content,
            )

        if isinstance(timeout, timedelta):
            return client.build_request(
                method,
                _normalize_path(path),
                params=params,
                timeout=to_seconds_float(timeout),
                headers=headers,
                json=json,
                content=content,
            )

        return client.build_request(
            method,
            _normalize_path(path),
            params=params,
            headers=headers,
            json=json,
            content=content,
        )


class StreamingRequest(abc.ABC):
    """An in-flight request with an incrementally supplied request body."""

    @abc.abstractmethod
    async def write(self, data: bytes) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def finish(self) -> StreamingResponse:
        raise NotImplementedError()

    @abc.abstractmethod
    async def abort(self) -> None:
        raise NotImplementedError()


class StreamingResponse(abc.ABC):
    """An owned streaming response with async-shaped iteration."""

    response: httpx.Response

    def __aiter__(self) -> StreamingResponse:
        return self

    async def read(self) -> bytes:
        """Consume and close the remaining response body."""
        body = bytearray()
        try:
            async for chunk in self:
                body.extend(chunk)
        finally:
            await self.aclose()
        return bytes(body)

    @abc.abstractmethod
    async def __anext__(self) -> bytes:
        raise NotImplementedError()

    @abc.abstractmethod
    def aiter_lines(self) -> AsyncIterator[str]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def aclose(self) -> None:
        raise NotImplementedError()


def _read_sync_response(response: httpx.Response, policy: ReadResponsePolicy) -> None:
    if policy is ReadResponsePolicy.ALWAYS or (
        policy is ReadResponsePolicy.NON_SUCCESS_ONLY and not response.is_success
    ):
        response.read()


async def _read_async_response(response: httpx.Response, policy: ReadResponsePolicy) -> None:
    if policy is ReadResponsePolicy.ALWAYS or (
        policy is ReadResponsePolicy.NON_SUCCESS_ONLY and not response.is_success
    ):
        await response.aread()


_STREAM_EOF = object()
_STREAM_ABORT = object()


class _RequestStreamAborted(Exception):
    pass


class _SyncRequestBody:
    def __init__(self, chunks: queue.Queue[bytes | object]) -> None:
        self._chunks = chunks

    def __iter__(self) -> Iterator[bytes]:
        while True:
            item = self._chunks.get()
            if item is _STREAM_EOF:
                return
            if item is _STREAM_ABORT:
                raise _RequestStreamAborted
            yield item  # type: ignore[misc]


class _SyncStreamingRequest(StreamingRequest):
    def __init__(
        self,
        *,
        client: httpx.Client,
        request: httpx.Request,
        chunks: queue.Queue[bytes | object],
        follow_redirects: bool | None,
        read_response: ReadResponsePolicy,
        chunk_size: int | None,
    ) -> None:
        self._client = client
        self._request = request
        self._chunks = chunks
        self._follow_redirects = follow_redirects
        self._read_response = read_response
        self._chunk_size = chunk_size
        self._response: httpx.Response | None = None
        self._error: BaseException | None = None
        self._closed = False
        self._aborted = False
        self._completed = False
        self._finished = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        response: httpx.Response | None = None
        try:
            if self._follow_redirects is None:
                response = self._client.send(self._request, stream=True)
            else:
                response = self._client.send(
                    self._request,
                    stream=True,
                    follow_redirects=self._follow_redirects,
                )
            _read_sync_response(response, self._read_response)
            self._response = response
        except _RequestStreamAborted:
            if not self._aborted:
                self._error = anyio.BrokenResourceError()
        except BaseException as exc:
            self._error = exc
            if response is not None:
                try:
                    response.close()
                except BaseException:
                    pass
        finally:
            self._finished.set()

    def _raise_worker_error(self) -> None:
        if self._error is not None:
            raise self._error
        if self._finished.is_set() and self._response is None and not self._aborted:
            raise anyio.BrokenResourceError

    def _put(self, item: bytes | object) -> None:
        while True:
            self._raise_worker_error()
            if self._finished.is_set():
                self._raise_worker_error()
                raise anyio.BrokenResourceError
            try:
                self._chunks.put(item, timeout=0.05)
                return
            except queue.Full:
                continue

    async def write(self, data: bytes) -> None:
        if self._closed:
            raise anyio.ClosedResourceError
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"a bytes-like object is required, not {type(data).__name__}")
        chunk = bytes(data)
        if chunk:
            self._put(chunk)
        else:
            self._raise_worker_error()

    async def finish(self) -> StreamingResponse:
        if self._closed:
            raise anyio.ClosedResourceError
        self._closed = True
        try:
            self._put(_STREAM_EOF)
        except BaseException:
            self._thread.join()
            raise
        self._thread.join()
        self._raise_worker_error()
        if self._response is None:
            raise anyio.BrokenResourceError
        self._completed = True
        return _SyncStreamingResponse(self._response, self._chunk_size)

    async def abort(self) -> None:
        if self._aborted:
            return
        self._closed = True
        self._aborted = True
        while not self._finished.is_set():
            try:
                self._chunks.put(_STREAM_ABORT, timeout=0.05)
                break
            except queue.Full:
                continue
        self._thread.join()
        if not self._completed and self._response is not None:
            try:
                self._response.close()
            except BaseException:
                pass


class _ResponseCleanup:
    """Own cleanup once, deferring its failure until primary reads have finished.

    HTTPX closes in iterator finally blocks. Raising there would replace a read
    error and (on async streams) cancellation could leave a closed response with
    incomplete cleanup. The byte-stream boundary records failures instead.
    """

    def __init__(self) -> None:
        self.closed = False
        self.error: BaseException | None = None

    def record(self, error: BaseException) -> None:
        if self.error is None:
            self.error = error

    def raise_error(self) -> None:
        if self.error is not None:
            raise self.error


class _OwnedSyncStream(httpx.SyncByteStream, _ResponseCleanup):
    def __init__(self, stream: httpx.SyncByteStream) -> None:
        _ResponseCleanup.__init__(self)
        self._stream = stream
        self._iterator: Iterator[bytes] | None = None

    def __iter__(self) -> Iterator[bytes]:
        self._iterator = iter(self._stream)
        return self

    def __next__(self) -> bytes:
        assert self._iterator is not None
        return next(self._iterator)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            close = getattr(self._iterator, "close", None)
            if close is not None:
                close()
        except BaseException as exc:
            self.record(exc)
        try:
            self._stream.close()
        except BaseException as exc:
            self.record(exc)


class _OwnedAsyncStream(httpx.AsyncByteStream, _ResponseCleanup):
    def __init__(self, stream: httpx.AsyncByteStream) -> None:
        _ResponseCleanup.__init__(self)
        self._stream = stream
        self._iterator: AsyncIterator[bytes] | None = None

    def __aiter__(self) -> AsyncIterator[bytes]:
        # Return an ordinary iterator so HTTPX cannot finalize the underlying
        # generator outside our shield before calling response.aclose().
        self._iterator = self._stream.__aiter__()
        return self

    async def __anext__(self) -> bytes:
        assert self._iterator is not None
        return await anext(self._iterator)

    async def aclose(self) -> None:
        if self.closed:
            return
        self.closed = True
        with anyio.CancelScope(shield=True):
            try:
                close = getattr(self._iterator, "aclose", None)
                if close is not None:
                    await close()
            except BaseException as exc:
                self.record(exc)
            try:
                await self._stream.aclose()
            except BaseException as exc:
                self.record(exc)


def _owned_stream(owner: type[Any], stream: Any) -> Any:
    # Explicitly supplied legacy clients require their own ByteStream base in
    # Response's isinstance checks. Keep that optional dependency lazy.
    legacy = sys.modules.get("httpx")
    if legacy is not None:
        base = legacy.SyncByteStream if owner is _OwnedSyncStream else legacy.AsyncByteStream
        if isinstance(stream, base):
            owner = type(owner.__name__, (owner, base), {})
    return owner(stream)


class _SyncStreamingResponse(StreamingResponse):
    def __init__(self, response: httpx.Response, chunk_size: int | None) -> None:
        self.response = response
        stream = response.stream
        if isinstance(stream, BoundSyncStream):
            # Own the actual body inside HTTPX's elapsed-time wrapper. Its
            # iterator can otherwise finalize the body before response.close.
            self._cleanup = _OwnedSyncStream(stream._stream)
            stream._stream = self._cleanup
        else:
            self._cleanup = _owned_stream(_OwnedSyncStream, stream)
            response.stream = self._cleanup
        self._cleanup.closed = response.is_closed
        self._iterator = cast(Generator[bytes, None, None], response.iter_bytes(chunk_size))
        self._closed = False

    async def __anext__(self) -> bytes:
        if self._closed:
            raise StopAsyncIteration
        try:
            return next(self._iterator)
        except StopIteration:
            await self.aclose()
            raise StopAsyncIteration from None
        except BaseException:
            await self._close(primary=True)
            raise

    async def aiter_lines(self) -> AsyncIterator[str]:
        lines = cast(Generator[str, None, None], self.response.iter_lines())
        primary = False
        try:
            while not self._closed:
                try:
                    yield next(lines)
                except StopIteration:
                    return
        except GeneratorExit:
            raise
        except BaseException:
            primary = True
            raise
        finally:
            try:
                lines.close()
            except BaseException as exc:
                self._cleanup.record(exc)
            await self._close(primary=primary)

    async def _close(self, *, primary: bool = False) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._iterator.close()
        except BaseException as exc:
            self._cleanup.record(exc)
        try:
            self.response.close()
        except BaseException as exc:
            self._cleanup.record(exc)
        self._cleanup.close()
        if not primary:
            self._cleanup.raise_error()

    async def aclose(self) -> None:
        await self._close()


class _AsyncRequestBody:
    def __init__(self, receive: ObjectReceiveStream[bytes]) -> None:
        self._receive = receive

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async with self._receive:
            async for chunk in self._receive:
                yield chunk


class _AsyncStreamingRequest(StreamingRequest):
    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        request: httpx.Request,
        send: ObjectSendStream[bytes],
        receive: ObjectReceiveStream[bytes],
        follow_redirects: bool | None,
        read_response: ReadResponsePolicy,
        chunk_size: int | None,
    ) -> None:
        self._client = client
        self._request = request
        self._send = send
        self._receive = receive
        self._follow_redirects = follow_redirects
        self._read_response = read_response
        self._chunk_size = chunk_size
        self._response: httpx.Response | None = None
        self._error: BaseException | None = None
        self._closed = False
        self._aborted = False
        self._completed = False
        self._cancel_scope: anyio.CancelScope | None = None
        self._done = anyio.Event()

    async def _run(self) -> None:
        response: httpx.Response | None = None
        try:
            with anyio.CancelScope() as cancel_scope:
                self._cancel_scope = cancel_scope
                if self._aborted:
                    cancel_scope.cancel()
                else:
                    if self._follow_redirects is None:
                        response = await self._client.send(self._request, stream=True)
                    else:
                        response = await self._client.send(
                            self._request,
                            stream=True,
                            follow_redirects=self._follow_redirects,
                        )
                    await _read_async_response(response, self._read_response)
                    self._response = response
            if self._aborted and response is not None and self._response is None:
                with anyio.CancelScope(shield=True):
                    await response.aclose()
        except BaseException as exc:
            if not self._aborted:
                self._error = exc
            if response is not None:
                try:
                    with anyio.CancelScope(shield=True):
                        await response.aclose()
                except BaseException:
                    pass
        finally:
            try:
                with anyio.CancelScope(shield=True):
                    await self._receive.aclose()
            except BaseException as exc:
                if not self._aborted and self._error is None:
                    self._error = exc
            self._done.set()

    def _raise_worker_error(self) -> None:
        if self._error is not None:
            raise self._error

    async def write(self, data: bytes) -> None:
        if self._closed:
            raise anyio.ClosedResourceError
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"a bytes-like object is required, not {type(data).__name__}")
        chunk = bytes(data)
        self._raise_worker_error()
        if not chunk:
            return
        try:
            await self._send.send(chunk)
        except anyio.get_cancelled_exc_class():
            with anyio.CancelScope(shield=True):
                await self.abort()
            raise
        except (anyio.BrokenResourceError, anyio.ClosedResourceError):
            self._raise_worker_error()
            raise
        self._raise_worker_error()

    async def finish(self) -> StreamingResponse:
        if self._closed:
            raise anyio.ClosedResourceError
        self._closed = True
        try:
            await self._send.aclose()
            await self._done.wait()
        except BaseException:
            with anyio.CancelScope(shield=True):
                await self.abort()
            raise
        self._raise_worker_error()
        if self._response is None:
            raise anyio.BrokenResourceError
        self._completed = True
        return _AsyncStreamingResponse(self._response, self._chunk_size)

    async def abort(self) -> None:
        if self._aborted:
            return
        self._closed = True
        self._aborted = True
        if self._cancel_scope is not None:
            self._cancel_scope.cancel()
        with anyio.CancelScope(shield=True):
            await self._send.aclose()
            await self._done.wait()
            if not self._completed and self._response is not None:
                try:
                    await self._response.aclose()
                except BaseException:
                    pass


class _AsyncStreamingResponse(StreamingResponse):
    def __init__(self, response: httpx.Response, chunk_size: int | None) -> None:
        self.response = response
        stream = response.stream
        if isinstance(stream, BoundAsyncStream):
            self._cleanup = _OwnedAsyncStream(stream._stream)
            stream._stream = self._cleanup
        else:
            self._cleanup = _owned_stream(_OwnedAsyncStream, stream)
            response.stream = self._cleanup
        self._cleanup.closed = response.is_closed
        self._iterator = response.aiter_bytes(chunk_size)
        self._closed = False

    async def __anext__(self) -> bytes:
        if self._closed:
            raise StopAsyncIteration
        try:
            return await anext(self._iterator)
        except StopAsyncIteration:
            await self.aclose()
            raise
        except BaseException:
            await self._close(primary=True)
            raise

    async def aiter_lines(self) -> AsyncIterator[str]:
        lines = cast(AsyncGenerator[str, None], self.response.aiter_lines())
        primary = False
        try:
            while not self._closed:
                try:
                    yield await anext(lines)
                except StopAsyncIteration:
                    return
        except GeneratorExit:
            raise
        except BaseException:
            primary = True
            raise
        finally:
            with anyio.CancelScope(shield=True):
                try:
                    await lines.aclose()
                except BaseException as exc:
                    self._cleanup.record(exc)
                await self._close(primary=primary)

    async def _close(self, *, primary: bool = False) -> None:
        if self._closed:
            return
        self._closed = True
        with anyio.CancelScope(shield=True):
            try:
                await self._iterator.aclose()
            except BaseException as exc:
                self._cleanup.record(exc)
            try:
                await self.response.aclose()
            except BaseException as exc:
                self._cleanup.record(exc)
            await self._cleanup.aclose()
        if not primary:
            self._cleanup.raise_error()

    async def aclose(self) -> None:
        await self._close()


class SyncTransport(BaseTransport):
    """Sync transport with a non-suspending async-shaped interface."""

    _client: httpx.Client

    def __init__(self, client: httpx.Client) -> None:
        self._client = client

    async def send(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: QueryParamTypes | None = None,
        body: RequestBody = None,
        headers: HeaderTypes | None = None,
        timeout: RequestTimeout = None,
        follow_redirects: bool | None = None,
        stream: bool = False,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NEVER,
    ) -> httpx.Response:
        request = self._build_request(
            self._client,
            method,
            path,
            token=token,
            params=params,
            body=body,
            headers=headers,
            timeout=timeout,
        )
        if follow_redirects is None:
            response = self._client.send(request, stream=stream)
        else:
            response = self._client.send(
                request,
                stream=stream,
                follow_redirects=follow_redirects,
            )
        _read_sync_response(response, read_response)
        return response

    @asynccontextmanager
    async def request_stream(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: QueryParamTypes | None = None,
        headers: HeaderTypes | None = None,
        timeout: RequestTimeout = None,
        follow_redirects: bool | None = None,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NON_SUCCESS_ONLY,
        response_chunk_size: int | None = None,
    ) -> AsyncIterator[StreamingRequest]:
        chunks: queue.Queue[bytes | object] = queue.Queue(maxsize=1)
        request = self._build_request(
            self._client,
            method,
            path,
            token=token,
            params=params,
            body=RawBody(_SyncRequestBody(chunks)),
            headers=headers,
            timeout=timeout,
        )
        streaming_request = _SyncStreamingRequest(
            client=self._client,
            request=request,
            chunks=chunks,
            follow_redirects=follow_redirects,
            read_response=read_response,
            chunk_size=response_chunk_size,
        )
        try:
            yield streaming_request
        except BaseException:
            try:
                await streaming_request.abort()
            except BaseException:
                pass
            raise
        else:
            if not streaming_request._completed:
                await streaming_request.abort()

    async def open_response_stream(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: QueryParamTypes | None = None,
        body: RequestBody = None,
        headers: HeaderTypes | None = None,
        timeout: RequestTimeout = None,
        follow_redirects: bool | None = None,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NON_SUCCESS_ONLY,
        chunk_size: int | None = None,
    ) -> StreamingResponse:
        response = await self.send(
            method,
            path,
            token=token,
            params=params,
            body=body,
            headers=headers,
            timeout=timeout,
            follow_redirects=follow_redirects,
            stream=True,
            read_response=read_response,
        )
        return _SyncStreamingResponse(response, chunk_size)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> SyncTransport:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


class AsyncTransport(BaseTransport):
    def __init__(self, client: httpx.AsyncClient | Callable[[], httpx.AsyncClient]) -> None:
        """Take a client, or a callable consulted before every request.

        Keep-alive sockets belong to the event loop that opened them, so an owner
        that outlives a loop passes a callable and gets a client per loop. The
        transport object itself is memoized by callers and cannot be swapped out.
        """
        if is_async_http_client(client):
            self._resolve_client = lambda: cast(httpx.AsyncClient, client)
        else:
            self._resolve_client = cast(Callable[[], httpx.AsyncClient], client)
        # Resolved once here so a factory that yields the wrong kind of client
        # fails when the transport is built, as it did before.
        self._resolve_client()

    async def send(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: QueryParamTypes | None = None,
        body: RequestBody = None,
        headers: HeaderTypes | None = None,
        timeout: RequestTimeout = None,
        follow_redirects: bool | None = None,
        stream: bool = False,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NEVER,
    ) -> httpx.Response:
        client = self._resolve_client()
        request = self._build_request(
            client,
            method,
            path,
            token=token,
            params=params,
            body=body,
            headers=headers,
            timeout=timeout,
        )
        if follow_redirects is None:
            response = await client.send(request, stream=stream)
        else:
            response = await client.send(
                request,
                stream=stream,
                follow_redirects=follow_redirects,
            )
        await _read_async_response(response, read_response)
        return response

    @asynccontextmanager
    async def request_stream(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: QueryParamTypes | None = None,
        headers: HeaderTypes | None = None,
        timeout: RequestTimeout = None,
        follow_redirects: bool | None = None,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NON_SUCCESS_ONLY,
        response_chunk_size: int | None = None,
    ) -> AsyncIterator[StreamingRequest]:
        client = self._resolve_client()
        send, receive = anyio.create_memory_object_stream[bytes](1)
        request = self._build_request(
            client,
            method,
            path,
            token=token,
            params=params,
            body=RawBody(_AsyncRequestBody(receive)),
            headers=headers,
            timeout=timeout,
        )
        streaming_request = _AsyncStreamingRequest(
            client=client,
            request=request,
            send=send,
            receive=receive,
            follow_redirects=follow_redirects,
            read_response=read_response,
            chunk_size=response_chunk_size,
        )
        scope_error: BaseException | None = None
        scope_traceback: TracebackType | None = None
        try:
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(streaming_request._run)
                try:
                    await anyio.lowlevel.checkpoint()
                    yield streaming_request
                except BaseException as exc:
                    scope_error = exc
                    scope_traceback = exc.__traceback__
                    with anyio.CancelScope(shield=True):
                        try:
                            await streaming_request.abort()
                        except BaseException:
                            pass
                else:
                    if not streaming_request._completed:
                        with anyio.CancelScope(shield=True):
                            await streaming_request.abort()
        finally:
            with anyio.CancelScope(shield=True):
                await send.aclose()
                await receive.aclose()
        if scope_error is not None:
            raise scope_error.with_traceback(scope_traceback)

    async def open_response_stream(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: QueryParamTypes | None = None,
        body: RequestBody = None,
        headers: HeaderTypes | None = None,
        timeout: RequestTimeout = None,
        follow_redirects: bool | None = None,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NON_SUCCESS_ONLY,
        chunk_size: int | None = None,
    ) -> StreamingResponse:
        response = await self.send(
            method,
            path,
            token=token,
            params=params,
            body=body,
            headers=headers,
            timeout=timeout,
            follow_redirects=follow_redirects,
            stream=True,
            read_response=read_response,
        )
        return _AsyncStreamingResponse(response, chunk_size)

    async def aclose(self) -> None:
        await self._resolve_client().aclose()

    async def __aenter__(self) -> AsyncTransport:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()


def extract_structured_error(response: httpx.Response) -> tuple[str, object | None]:
    error_body = response.text

    # Parse a helpful error message
    parsed: object | None = None
    message = f"HTTP {response.status_code}"
    try:
        parsed = json.loads(error_body)
        if isinstance(parsed, dict):
            if "message" in parsed and isinstance(parsed["message"], str):
                message = f"{message}: {parsed['message']}"
            elif "error" in parsed:
                err = parsed["error"]
                if isinstance(err, dict):
                    code = err.get("code")
                    msg = err.get("message") or err.get("msg")
                    if msg:
                        message = f"{message}: {msg}"
                    if code:
                        message = f"{message} (code={code})"
    except Exception:
        parsed = None

    if parsed is None:
        try:
            text = response.text
            if text:
                snippet = text if len(text) <= 500 else text[:500] + "\u2026"
                message = f"{message}: {snippet}"
        except Exception:
            pass

    return (message, parsed)


__all__ = [
    "BaseTransport",
    "SyncTransport",
    "AsyncTransport",
    "JSONBody",
    "BytesBody",
    "RawBody",
    "ReadResponsePolicy",
    "RequestBody",
    "StreamingRequest",
    "StreamingResponse",
    "extract_structured_error",
]
