"""HTTP transport implementations for sync and async clients."""

from __future__ import annotations

import abc
import json
from dataclasses import dataclass
from datetime import timedelta
from types import TracebackType
from typing import Any

import httpx
from httpx import USE_CLIENT_DEFAULT
from httpx._types import HeaderTypes, QueryParamTypes

from vercel._internal.time import to_seconds_float


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


@dataclass(frozen=True, slots=True)
class TransportOptions:
    timeout: timedelta
    base_url: str
    max_connections: int
    enable_http2: bool


def _build_request(
    *,
    body: RequestBody,
    headers: HeaderTypes | None,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    request_headers = httpx.Headers(headers)

    if isinstance(body, JSONBody):
        kwargs["json"] = body.data
    elif isinstance(body, BytesBody):
        kwargs["content"] = body.data
        request_headers.setdefault("content-type", body.content_type)
    elif isinstance(body, RawBody):
        kwargs["content"] = body.data

    if request_headers:
        kwargs["headers"] = request_headers

    return kwargs


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
    ) -> httpx.Response:
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


class SyncTransport(BaseTransport):
    """Sync transport with async interface for use with iter_coroutine()."""

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
    ) -> httpx.Response:
        request = self._build_request(
            method, path, token=token, params=params, body=body, headers=headers, timeout=timeout
        )
        return self._client.send(
            request,
            stream=stream,
            follow_redirects=follow_redirects
            if follow_redirects is not None
            else USE_CLIENT_DEFAULT,
        )

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
    ) -> httpx.Response:
        request = self._build_request(
            method, path, token=token, params=params, body=body, headers=headers, timeout=timeout
        )
        return await self._client.send(
            request,
            stream=stream,
            follow_redirects=follow_redirects
            if follow_redirects is not None
            else USE_CLIENT_DEFAULT,
        )

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


def extract_structured_error(response: httpx.Response) -> tuple[str, object]:
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
    "RequestBody",
    "extract_structured_error",
]
