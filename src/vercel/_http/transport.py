"""HTTP transport implementations for sync and async clients."""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Any

import httpx


def _normalize_path(path: str) -> str:
    """Strip leading slash from path for consistent URL joining with base_url."""
    return path.lstrip("/")


@dataclass(frozen=True, slots=True)
class JSONBody:
    """JSON request body - automatically sets Content-Type to application/json."""

    data: Any


@dataclass(frozen=True, slots=True)
class BytesBody:
    """Raw bytes request body with explicit content type."""

    data: bytes
    content_type: str = "application/octet-stream"


RequestBody = JSONBody | BytesBody | None


class BaseTransport(abc.ABC):
    """Abstract base class for HTTP transports."""

    @abc.abstractmethod
    async def send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: RequestBody = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        """Send an HTTP request and return the response."""
        ...

    @abc.abstractmethod
    def close(self) -> None:
        """Close any underlying resources."""
        ...


class BlockingTransport(BaseTransport):
    """
    Synchronous HTTP transport using httpx.Client.

    Methods are declared async but don't actually await anything,
    allowing them to be executed via iter_coroutine().
    """

    def __init__(self, client: httpx.Client) -> None:
        self._client = client

    async def send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: RequestBody = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        """Send a synchronous HTTP request (wrapped as async for iter_coroutine)."""
        request_headers: dict[str, str] = {}
        if headers:
            request_headers.update(headers)

        # Unpack content based on type
        json_data: Any | None = None
        raw_content: bytes | None = None
        if isinstance(body, JSONBody):
            json_data = body.data
        elif isinstance(body, BytesBody):
            raw_content = body.data
            request_headers["Content-Type"] = body.content_type

        # Build request kwargs, only including timeout if explicitly provided
        kwargs: dict[str, Any] = {
            "params": params or None,
            "json": json_data,
            "content": raw_content,
            "headers": request_headers if request_headers else None,
        }
        if timeout is not None:
            kwargs["timeout"] = httpx.Timeout(timeout)

        return self._client.request(method, _normalize_path(path), **kwargs)

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()


class AsyncTransport(BaseTransport):
    """Asynchronous HTTP transport using httpx.AsyncClient."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: RequestBody = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        """Send an asynchronous HTTP request."""
        request_headers: dict[str, str] = {}
        if headers:
            request_headers.update(headers)

        # Unpack content based on type
        json_data: Any | None = None
        raw_content: bytes | None = None
        if isinstance(body, JSONBody):
            json_data = body.data
        elif isinstance(body, BytesBody):
            raw_content = body.data
            request_headers["Content-Type"] = body.content_type

        # Build request kwargs, only including timeout if explicitly provided
        kwargs: dict[str, Any] = {
            "params": params or None,
            "json": json_data,
            "content": raw_content,
            "headers": request_headers if request_headers else None,
        }
        if timeout is not None:
            kwargs["timeout"] = httpx.Timeout(timeout)

        return await self._client.request(method, _normalize_path(path), **kwargs)

    def close(self) -> None:
        """Close the underlying HTTP client (sync no-op, use aclose)."""
        pass

    async def aclose(self) -> None:
        """Asynchronously close the underlying HTTP client."""
        await self._client.aclose()


__all__ = [
    "BaseTransport",
    "BlockingTransport",
    "AsyncTransport",
    "JSONBody",
    "BytesBody",
    "RequestBody",
]
