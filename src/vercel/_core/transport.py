"""Transport layer for HTTP operations."""

from __future__ import annotations

import abc
from typing import Any

import httpx

from .config import ClientConfig


class BaseTransport(abc.ABC):
    """Abstract transport with async interface."""

    def __init__(self, config: ClientConfig):
        self.config = config

    @abc.abstractmethod
    async def send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response: ...

    @abc.abstractmethod
    async def close(self) -> None: ...


class BlockingTransport(BaseTransport):
    """Sync I/O transport. Methods are async def but don't suspend."""

    def __init__(self, config: ClientConfig):
        super().__init__(config)
        self._client: httpx.Client | None = None

    def _get_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                timeout=httpx.Timeout(self.config.timeout),
            )
        return self._client

    async def send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        url = self.config.build_url(path)
        request_headers = self.config.get_auth_headers()
        if headers:
            request_headers.update(headers)

        return self._get_client().request(
            method,
            url,
            params=params,
            json=json,
            content=content,
            headers=request_headers,
        )

    async def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None


class AsyncTransport(BaseTransport):
    """Async I/O transport using httpx.AsyncClient."""

    def __init__(self, config: ClientConfig):
        super().__init__(config)
        self._client: httpx.AsyncClient | None = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.config.timeout),
            )
        return self._client

    async def send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        url = self.config.build_url(path)
        request_headers = self.config.get_auth_headers()
        if headers:
            request_headers.update(headers)

        return await self._get_client().request(
            method,
            url,
            params=params,
            json=json,
            content=content,
            headers=request_headers,
        )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
