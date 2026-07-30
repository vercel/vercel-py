from __future__ import annotations

from typing import Any, Generic, Protocol, TypeAlias, TypeVar, runtime_checkable

import contextlib
import logging
import warnings
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Iterable,
    Iterator,
    Mapping,
)
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from threading import Lock
from urllib.parse import urljoin, urlsplit

import httpx
from anyio.lowlevel import get_async_backend

from .asynctools import iter_bytes_async
from .config import resolve_token, resolve_token_async
from .constants import HEADER_RETRY_AFTER
from .errors import CommunicationError
from .log import content_type, debug_enabled, debug_log, redact_text, safe_header_names, safe_url
from .types import Duration, Headers, RawHeaders, RequestContent, duration_to_float_seconds

_httpx_logger = logging.getLogger("httpx")


@dataclass
class _HttpxFilterState:
    lock: Lock
    installed: bool = False


_httpx_filter_state = _HttpxFilterState(lock=Lock())


class BaseHttpResponse(Protocol):
    @property
    def headers(self) -> Mapping[str, str]: ...


@runtime_checkable
class AsyncHttpMessage(BaseHttpResponse, Protocol):
    """Minimal async HTTP message surface for queue push deliveries."""

    @property
    def headers(self) -> Mapping[str, str]: ...

    def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]: ...


@runtime_checkable
class AsyncHttpResponse(AsyncHttpMessage, Protocol):
    """Minimal async HTTP response surface used by the queue client."""

    @property
    def status_code(self) -> int: ...

    @property
    def text(self) -> str: ...

    def json(self) -> Any: ...


@runtime_checkable
class HttpResponse(BaseHttpResponse, Protocol):
    """Minimal sync HTTP response interface."""

    def iter_bytes(self, chunk_size: int | None = None) -> Iterator[bytes]: ...


class SyncHttpResponse(HttpResponse, Protocol):
    """Minimal sync HTTP response surface used by the queue client."""

    @property
    def status_code(self) -> int: ...

    @property
    def text(self) -> str: ...

    def json(self) -> Any: ...


PushDeliveryBody: TypeAlias = bytes | Iterable[bytes] | AsyncIterable[bytes]
AsyncPushDeliveryBody: TypeAlias = bytes | AsyncIterable[bytes]
PushDeliveryInput: TypeAlias = PushDeliveryBody | HttpResponse
AsyncPushDeliveryInput: TypeAlias = AsyncPushDeliveryBody | AsyncHttpMessage


class SyncHttpClient(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        content: RequestContent | None = None,
        json: object | None = None,
        timeout: float | None = None,
    ) -> SyncHttpResponse: ...

    def stream(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout: float | None = None,
    ) -> AbstractContextManager[SyncHttpResponse]: ...

    def close(self) -> None: ...


class AsyncHttpClient(Protocol):
    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        content: RequestContent | None = None,
        json: object | None = None,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> AsyncHttpResponse: ...

    def stream(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout: float | None = None,
    ) -> AbstractAsyncContextManager[AsyncHttpResponse]: ...

    async def aclose(self) -> None: ...


SyncHttpClientFactory: TypeAlias = Callable[..., SyncHttpClient]
AsyncHttpClientFactory: TypeAlias = Callable[..., AsyncHttpClient]
_ClientT = TypeVar("_ClientT", bound=SyncHttpClient | AsyncHttpClient)


@dataclass(kw_only=True)
class _HttpClientPoolEntry(Generic[_ClientT]):
    client: _ClientT
    in_use: int = 0


class _HttpClientPool(Generic[_ClientT]):
    def __init__(self, *, max_size: int | None = 32) -> None:
        if max_size is not None and max_size < 1:
            raise ValueError("max_size must be positive or None")
        self._max_size = max_size
        self._clients: dict[object, _HttpClientPoolEntry[_ClientT]] = {}
        self._lru: list[object] = []
        self._lock = Lock()

    def acquire(self, key: object, factory: Callable[[], _ClientT]) -> _ClientT:
        with self._lock:
            entry = self._clients.get(key)
            if entry is None:
                entry = _HttpClientPoolEntry(client=factory())
                self._clients[key] = entry
                self._lru.append(key)
            else:
                self._mark_recent(key)
            entry.in_use += 1
            return entry.client

    def release(self, key: object) -> list[_ClientT]:
        with self._lock:
            entry = self._clients.get(key)
            if entry is None:
                raise RuntimeError("HTTP client pool entry was released after eviction")
            if entry.in_use <= 0:
                raise RuntimeError("HTTP client pool entry was released too many times")
            entry.in_use -= 1
            if entry.in_use == 0:
                self._mark_recent(key)
            return self._evict_if_needed_locked()

    def reset(self) -> list[_ClientT]:
        with self._lock:
            clients = [entry.client for entry in self._clients.values()]
            self._clients.clear()
            self._lru.clear()
            return clients

    def _mark_recent(self, key: object) -> None:
        try:
            self._lru.remove(key)
        except ValueError:
            pass
        self._lru.append(key)

    def _evict_if_needed_locked(self) -> list[_ClientT]:
        evicted: list[_ClientT] = []
        if self._max_size is None:
            return evicted
        while len(self._clients) > self._max_size:
            for key in list(self._lru):
                entry = self._clients[key]
                if entry.in_use > 0:
                    continue
                self._lru.remove(key)
                self._clients.pop(key, None)
                evicted.append(entry.client)
                break
            else:
                warnings.warn(
                    "queue HTTP client pool is over capacity because all clients are active",
                    ResourceWarning,
                    stacklevel=3,
                )
                break
        return evicted


_sync_http_client_pool: _HttpClientPool[SyncHttpClient] = _HttpClientPool()
_async_http_client_pool: _HttpClientPool[AsyncHttpClient] = _HttpClientPool()


def _sync_factory_key(factory: SyncHttpClientFactory | None) -> object:
    return factory or httpx.Client


def _async_factory_key(factory: AsyncHttpClientFactory | None) -> object:
    return factory or httpx.AsyncClient


def _async_backend_key() -> object:
    return get_async_backend().current_token()


def reset_http_client_pools_for_tests() -> None:
    for client in _sync_http_client_pool.reset():
        client.close()


async def reset_async_http_client_pool_for_tests() -> None:
    for client in _async_http_client_pool.reset():
        try:
            await client.aclose()
        except RuntimeError as exc:
            if not _is_async_client_close_context_error(exc):
                raise


def _is_async_client_close_context_error(exc: RuntimeError) -> bool:
    message = str(exc)
    return (
        "Event loop is closed" in message
        or "must be called from async context" in message
        or "no running event loop" in message
        or "handler is closed" in message
    )


def headers_from_raw(headers: RawHeaders) -> Headers:
    if isinstance(headers, httpx.Headers):
        return headers
    return httpx.Headers(headers)


class _SyncResponse:
    def __init__(self, response: SyncHttpResponse) -> None:
        self._response = response

    @property
    def status_code(self) -> int:
        return self._response.status_code

    @property
    def headers(self) -> Mapping[str, str]:
        return self._response.headers

    @property
    def text(self) -> str:
        return self._response.text

    def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        return iter_bytes_async(self._response.iter_bytes(chunk_size=chunk_size))

    def json(self) -> Any:
        return self._response.json()


async def response_text(response: AsyncHttpResponse) -> str:
    try:
        return response.text
    except httpx.ResponseNotRead:
        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
        return body.decode("utf-8", errors="replace")


def parse_retry_after(response: AsyncHttpResponse) -> int | None:
    value = response.headers.get(HEADER_RETRY_AFTER)
    if not value:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        delay = (retry_at.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds()
        return max(1, int(delay))
    return max(1, parsed) if parsed >= 0 else None


def response_headers(response: BaseHttpResponse) -> Mapping[str, str]:
    headers = response.headers
    if isinstance(headers, httpx.Headers):
        return headers
    return headers_from_raw(headers)


class BaseQueueRuntime:
    def __init__(self, *, timeout: Duration | None) -> None:
        _install_httpx_request_log_filter()
        self.timeout = None if timeout is None else duration_to_float_seconds(timeout)
        if self.timeout is not None and self.timeout < 0:
            raise ValueError("timeout must be non-negative")
        self._base_url: str | None = None

    def configure_base_url(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")

    def _resolve_runtime_url(self, url: str) -> str:
        parsed = urlsplit(url)
        if parsed.scheme and parsed.netloc:
            return url
        if url.startswith("/"):
            if self._base_url is None:
                raise RuntimeError("queue runtime base URL is not configured")
            return urljoin(self._base_url + "/", url.lstrip("/"))
        raise ValueError("queue runtime HTTP URLs must be absolute or root-relative")

    async def token(self, token: str | None) -> str:
        raise NotImplementedError

    async def _request(self, method: str, url: str, **kwargs: Any) -> AsyncHttpResponse:
        raise NotImplementedError

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        content: RequestContent | None = None,
        json: object | None = None,
    ) -> AsyncHttpResponse:
        return await self._request(
            method=method,
            url=self._resolve_runtime_url(url),
            content=content,
            json=json,
            headers=headers,
        )

    async def get(self, url: str, *, headers: Mapping[str, str]) -> AsyncHttpResponse:
        return await self.request("GET", url, headers=headers)

    async def post(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        content: RequestContent | None = None,
        json: object | None = None,
    ) -> AsyncHttpResponse:
        return await self.request(
            method="POST",
            url=url,
            content=content,
            json=json,
            headers=headers,
        )

    def stream_post(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
    ) -> AbstractAsyncContextManager[AsyncHttpResponse]:
        return self.stream("POST", url, headers=headers)

    def stream(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
    ) -> AbstractAsyncContextManager[AsyncHttpResponse]:
        raise NotImplementedError

    async def delete(self, url: str, *, headers: Mapping[str, str]) -> AsyncHttpResponse:
        return await self.request(method="DELETE", url=url, headers=headers)

    async def patch(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        json: object,
    ) -> AsyncHttpResponse:
        return await self.request(method="PATCH", url=url, json=json, headers=headers)


class SyncQueueRuntime(BaseQueueRuntime):
    def __init__(
        self,
        *,
        timeout: Duration | None,
        client_factory: SyncHttpClientFactory | None = None,
    ) -> None:
        super().__init__(timeout=timeout)
        self._client_factory = client_factory
        self._client_key = _sync_factory_key(client_factory)

    def _acquire_client(self) -> SyncHttpClient:
        factory = self._client_factory or httpx.Client
        return _sync_http_client_pool.acquire(
            self._client_key,
            factory,
        )

    def _release_client(self) -> None:
        for client in _sync_http_client_pool.release(self._client_key):
            client.close()

    async def token(self, token: str | None) -> str:
        return resolve_token(token)

    async def _request(self, method: str, url: str, **kwargs: Any) -> AsyncHttpResponse:
        _log_http_request(method, url, self.timeout, "request", kwargs.get("headers", {}))
        kwargs["timeout"] = self.timeout
        client = self._acquire_client()
        try:
            response = client.request(method, url, **kwargs)
        except httpx.TransportError as exc:
            _log_http_error(method, url, exc)
            raise CommunicationError(str(exc)) from exc
        finally:
            self._release_client()
        _log_http_response(method, url, response.status_code, response.headers)
        return _SyncResponse(response)

    @contextlib.asynccontextmanager
    async def stream(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
    ) -> AsyncIterator[AsyncHttpResponse]:
        resolved_url = self._resolve_runtime_url(url)
        _log_http_request(method, resolved_url, self.timeout, "stream", headers)
        client = self._acquire_client()
        try:
            with client.stream(
                method,
                resolved_url,
                headers=headers,
                timeout=self.timeout,
            ) as response:
                _log_http_response(method, resolved_url, response.status_code, response.headers)
                yield _SyncResponse(response)
        except httpx.TransportError as exc:
            _log_http_error(method, resolved_url, exc)
            raise CommunicationError(str(exc)) from exc
        finally:
            self._release_client()


class AsyncQueueRuntime(BaseQueueRuntime):
    def __init__(
        self,
        *,
        timeout: Duration | None,
        client_factory: AsyncHttpClientFactory | None = None,
    ) -> None:
        super().__init__(timeout=timeout)
        self._client_factory = client_factory

    def _client_key(self) -> tuple[object, object]:
        return (_async_backend_key(), _async_factory_key(self._client_factory))

    def _acquire_client(self) -> tuple[object, AsyncHttpClient]:
        key = self._client_key()
        factory = self._client_factory or httpx.AsyncClient
        return key, _async_http_client_pool.acquire(
            key,
            factory,
        )

    async def _release_client(self, key: object) -> None:
        for client in _async_http_client_pool.release(key):
            try:
                await client.aclose()
            except RuntimeError as exc:
                if not _is_async_client_close_context_error(exc):
                    raise

    async def token(self, token: str | None) -> str:
        return await resolve_token_async(token)

    async def _request(self, method: str, url: str, **kwargs: Any) -> AsyncHttpResponse:
        _log_http_request(method, url, self.timeout, "request", kwargs.get("headers", {}))
        kwargs["timeout"] = self.timeout
        key, client = self._acquire_client()
        try:
            response = await client.request(method, url, **kwargs)
        except httpx.TransportError as exc:
            _log_http_error(method, url, exc)
            raise CommunicationError(str(exc)) from exc
        finally:
            await self._release_client(key)
        _log_http_response(method, url, response.status_code, response.headers)
        return response

    @contextlib.asynccontextmanager
    async def stream(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
    ) -> AsyncIterator[AsyncHttpResponse]:
        resolved_url = self._resolve_runtime_url(url)
        _log_http_request(method, resolved_url, self.timeout, "stream", headers)
        key, client = self._acquire_client()
        try:
            async with client.stream(
                method,
                resolved_url,
                headers=headers,
                timeout=self.timeout,
            ) as response:
                _log_http_response(method, resolved_url, response.status_code, response.headers)
                yield response
        except httpx.TransportError as exc:
            _log_http_error(method, resolved_url, exc)
            raise CommunicationError(str(exc)) from exc
        finally:
            await self._release_client(key)


def _log_http_request(
    method: str,
    url: str,
    timeout: float | None,
    kind: str,
    headers: object,
) -> None:
    if not debug_enabled():
        return
    debug_log(
        "http.request",
        method=method,
        url=safe_url(url),
        timeout=timeout,
        kind=kind,
        headers=safe_header_names(headers) if isinstance(headers, Mapping) else [],
    )


def _log_http_response(
    method: str,
    url: str,
    status_code: int,
    headers: Mapping[str, str],
) -> None:
    if not debug_enabled():
        return
    debug_log(
        "http.response",
        method=method,
        url=safe_url(url),
        status_code=status_code,
        content_type=content_type(headers),
    )


def _log_http_error(method: str, url: str, exc: BaseException) -> None:
    if not debug_enabled():
        return
    debug_log(
        "http.error",
        method=method,
        url=safe_url(url),
        exception_class=exc.__class__.__name__,
        exception_message=redact_text(str(exc)),
    )


class _QueueHttpxRequestLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if not isinstance(args, tuple):
            return True
        return not any(_is_queue_url(str(arg)) for arg in args)


def _is_queue_url(value: str) -> bool:
    path_parts = [part for part in urlsplit(value).path.split("/") if part]
    for index in range(len(path_parts) - 3):
        if path_parts[index : index + 3] == ["api", "v3", "topic"]:
            return True
    return False


def _install_httpx_request_log_filter() -> None:
    if _httpx_filter_state.installed:
        return
    with _httpx_filter_state.lock:
        if _httpx_filter_state.installed:
            return
        _httpx_logger.addFilter(_QueueHttpxRequestLogFilter())
        _httpx_filter_state.installed = True
