"""HTTP transport implementations for sync and async clients."""

from __future__ import annotations

import abc
from typing import Any

import httpx

from .config import HTTPConfig, require_token


class BaseTransport(abc.ABC):
    """Abstract base class for HTTP transports."""

    def __init__(self, config: HTTPConfig) -> None:
        self._config = config

    def _require_token(self) -> str:
        """Resolve and validate the API token."""
        return require_token(self._config.token)

    @abc.abstractmethod
    async def send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
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

    def __init__(self, config: HTTPConfig) -> None:
        super().__init__(config)
        self._client: httpx.Client | None = None

    def _get_client(self, timeout: float) -> httpx.Client:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.Client(timeout=httpx.Timeout(timeout))
        return self._client

    async def send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        """Send a synchronous HTTP request (wrapped as async for iter_coroutine)."""
        bearer = self._require_token()
        url = self._config.base_url.rstrip("/") + path
        effective_timeout = timeout if timeout is not None else self._config.timeout
        headers = self._config.get_headers(bearer)

        # Use a fresh client for each request (ephemeral pattern)
        with httpx.Client(timeout=httpx.Timeout(effective_timeout)) as client:
            resp = client.request(
                method,
                url,
                params=params or None,
                json=json,
                headers=headers,
            )
        return resp

    def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._client is not None:
            self._client.close()
            self._client = None


class AsyncTransport(BaseTransport):
    """Asynchronous HTTP transport using httpx.AsyncClient."""

    def __init__(self, config: HTTPConfig) -> None:
        super().__init__(config)
        self._client: httpx.AsyncClient | None = None

    async def send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        """Send an asynchronous HTTP request."""
        bearer = self._require_token()
        url = self._config.base_url.rstrip("/") + path
        effective_timeout = timeout if timeout is not None else self._config.timeout
        headers = self._config.get_headers(bearer)

        # Use a fresh client for each request (ephemeral pattern)
        async with httpx.AsyncClient(timeout=httpx.Timeout(effective_timeout)) as client:
            resp = await client.request(
                method,
                url,
                params=params or None,
                json=json,
                headers=headers,
            )
        return resp

    def close(self) -> None:
        """Close the underlying HTTP client (no-op for ephemeral pattern)."""
        pass


__all__ = ["BaseTransport", "BlockingTransport", "AsyncTransport"]
