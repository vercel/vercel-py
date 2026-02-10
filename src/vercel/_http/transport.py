"""HTTP transport implementations for sync and async clients."""

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


def _build_request_kwargs(
    *,
    params: dict[str, Any] | None,
    body: RequestBody,
    headers: dict[str, str] | None,
    timeout: float | None,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}

    if params:
        kwargs["params"] = params

    request_headers: dict[str, str] = {}
    if headers:
        request_headers.update(headers)

    if isinstance(body, JSONBody):
        kwargs["json"] = body.data
    elif isinstance(body, BytesBody):
        kwargs["content"] = body.data
        request_headers["Content-Type"] = body.content_type

    if request_headers:
        kwargs["headers"] = request_headers

    if timeout is not None:
        kwargs["timeout"] = httpx.Timeout(timeout)

    return kwargs


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
        kwargs = _build_request_kwargs(
            params=params,
            body=body,
            headers=headers,
            timeout=timeout,
        )

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
        kwargs = _build_request_kwargs(
            params=params,
            body=body,
            headers=headers,
            timeout=timeout,
        )

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
