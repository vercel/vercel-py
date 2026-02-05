"""HTTP transport implementations for sync and async clients."""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Any

import httpx


def _normalize_path(path: str) -> str:
    return path.lstrip("/")


@dataclass(frozen=True, slots=True)
class JSONBody:
    data: Any


@dataclass(frozen=True, slots=True)
class BytesBody:
    data: bytes
    content_type: str = "application/octet-stream"


RequestBody = JSONBody | BytesBody | None


class BaseTransport(abc.ABC):
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
        raise NotImplementedError


class BlockingTransport(BaseTransport):
    """Sync transport with async interface for use with iter_coroutine()."""

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
        request_headers: dict[str, str] = {}
        if headers:
            request_headers.update(headers)

        json_data: Any | None = None
        raw_content: bytes | None = None
        if isinstance(body, JSONBody):
            json_data = body.data
        elif isinstance(body, BytesBody):
            raw_content = body.data
            request_headers["Content-Type"] = body.content_type

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
        self._client.close()


class AsyncTransport(BaseTransport):
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
        request_headers: dict[str, str] = {}
        if headers:
            request_headers.update(headers)

        json_data: Any | None = None
        raw_content: bytes | None = None
        if isinstance(body, JSONBody):
            json_data = body.data
        elif isinstance(body, BytesBody):
            raw_content = body.data
            request_headers["Content-Type"] = body.content_type

        kwargs: dict[str, Any] = {
            "params": params or None,
            "json": json_data,
            "content": raw_content,
            "headers": request_headers if request_headers else None,
        }
        if timeout is not None:
            kwargs["timeout"] = httpx.Timeout(timeout)

        return await self._client.request(method, _normalize_path(path), **kwargs)

    async def aclose(self) -> None:
        await self._client.aclose()


__all__ = [
    "BaseTransport",
    "BlockingTransport",
    "AsyncTransport",
    "JSONBody",
    "BytesBody",
    "RequestBody",
]
