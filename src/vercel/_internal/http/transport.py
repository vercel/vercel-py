"""HTTP transport implementations for sync and async clients."""

from __future__ import annotations

import abc
import json
import queue
import threading
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from types import TracebackType
from typing import Any, TypeAlias

import anyio
import anyio.abc
import httpx
from httpx import USE_CLIENT_DEFAULT

from vercel._internal.polyfills import StrEnum
from vercel._internal.time import to_seconds_float

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
    _client: httpx.Client | httpx.AsyncClient

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
        timeout: timedelta | None = None,
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
        timeout: timedelta | None = None,
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
        timeout: timedelta | None = None,
        follow_redirects: bool | None = None,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NON_SUCCESS_ONLY,
        chunk_size: int | None = None,
    ) -> StreamingResponse:
        """Open a response whose body is consumed incrementally."""
        raise NotImplementedError()

    def _build_request(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: QueryParamTypes | None = None,
        body: RequestBody = None,
        headers: HeaderTypes | None = None,
        timeout: timedelta | None = None,
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

        if timeout is not None:
            return self._client.build_request(
                method,
                _normalize_path(path),
                params=params,
                timeout=httpx.Timeout(to_seconds_float(timeout)),
                headers=headers,
                json=json,
                content=content,
            )

        return self._client.build_request(
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
            response = self._client.send(
                self._request,
                stream=True,
                follow_redirects=self._follow_redirects
                if self._follow_redirects is not None
                else USE_CLIENT_DEFAULT,
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


class _SyncStreamingResponse(StreamingResponse):
    def __init__(self, response: httpx.Response, chunk_size: int | None) -> None:
        self.response = response
        self._iterator = response.iter_bytes(chunk_size)
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
            await self.aclose()
            raise

    async def aiter_lines(self) -> AsyncIterator[str]:
        try:
            lines = self.response.iter_lines()
            while not self._closed:
                try:
                    yield next(lines)
                except StopIteration:
                    return
        finally:
            await self.aclose()

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                self.response.close()
            except BaseException:
                pass


class _AsyncRequestBody:
    def __init__(self, receive: anyio.abc.ObjectReceiveStream[bytes]) -> None:
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
        send: anyio.abc.ObjectSendStream[bytes],
        receive: anyio.abc.ObjectReceiveStream[bytes],
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
                    response = await self._client.send(
                        self._request,
                        stream=True,
                        follow_redirects=self._follow_redirects
                        if self._follow_redirects is not None
                        else USE_CLIENT_DEFAULT,
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
            with anyio.CancelScope(shield=True):
                await self.aclose()
            raise

    async def aiter_lines(self) -> AsyncIterator[str]:
        try:
            lines = self.response.aiter_lines()
            while not self._closed:
                try:
                    yield await anext(lines)
                except StopAsyncIteration:
                    return
        finally:
            with anyio.CancelScope(shield=True):
                await self.aclose()

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                with anyio.CancelScope(shield=True):
                    await self.response.aclose()
            except BaseException:
                pass


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
        timeout: timedelta | None = None,
        follow_redirects: bool | None = None,
        stream: bool = False,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NEVER,
    ) -> httpx.Response:
        request = self._build_request(
            method, path, token=token, params=params, body=body, headers=headers, timeout=timeout
        )
        response = self._client.send(
            request,
            stream=stream,
            follow_redirects=follow_redirects
            if follow_redirects is not None
            else USE_CLIENT_DEFAULT,
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
        timeout: timedelta | None = None,
        follow_redirects: bool | None = None,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NON_SUCCESS_ONLY,
        response_chunk_size: int | None = None,
    ) -> AsyncIterator[StreamingRequest]:
        chunks: queue.Queue[bytes | object] = queue.Queue(maxsize=1)
        request = self._build_request(
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
        timeout: timedelta | None = None,
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
    _client: httpx.AsyncClient

    def __init__(self, client: httpx.AsyncClient) -> None:
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
        timeout: timedelta | None = None,
        follow_redirects: bool | None = None,
        stream: bool = False,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NEVER,
    ) -> httpx.Response:
        request = self._build_request(
            method, path, token=token, params=params, body=body, headers=headers, timeout=timeout
        )
        response = await self._client.send(
            request,
            stream=stream,
            follow_redirects=follow_redirects
            if follow_redirects is not None
            else USE_CLIENT_DEFAULT,
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
        timeout: timedelta | None = None,
        follow_redirects: bool | None = None,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NON_SUCCESS_ONLY,
        response_chunk_size: int | None = None,
    ) -> AsyncIterator[StreamingRequest]:
        send, receive = anyio.create_memory_object_stream[bytes](1)
        request = self._build_request(
            method,
            path,
            token=token,
            params=params,
            body=RawBody(_AsyncRequestBody(receive)),
            headers=headers,
            timeout=timeout,
        )
        streaming_request = _AsyncStreamingRequest(
            client=self._client,
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
        timeout: timedelta | None = None,
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
        await self._client.aclose()

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
