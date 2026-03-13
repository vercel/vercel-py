"""Shared request-level HTTP client with token resolution, header/param merging, and retry."""

from __future__ import annotations

import inspect
import os
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, cast

import httpx

from vercel._internal.http.transport import BaseTransport, RequestBody

SleepFn = Callable[[float], Awaitable[None] | None]
RequestHeadersInput = dict[str, str] | Callable[[int], dict[str, str] | None] | None
RequestBodyInput = RequestBody | Callable[[int], RequestBody]


def sync_sleep(seconds: float) -> None:
    """Synchronous sleep wrapper for use with :class:`RequestClient`."""
    time.sleep(seconds)


@dataclass(frozen=True)
class RetryPolicy:
    """Configuration for automatic request retries."""

    retries: int = 0
    retry_on_network_error: bool = True
    retry_on_response: Callable[[httpx.Response], bool] | None = None
    backoff_base: float = 0.1
    backoff_max: float = 2.0


def _resolve_token(
    token: str | None,
    token_env_vars: str | Sequence[str],
) -> str:
    if token:
        return token

    env_names: Sequence[str] = (
        [token_env_vars] if isinstance(token_env_vars, str) else token_env_vars
    )
    for name in env_names:
        value = os.getenv(name)
        if value:
            return value

    names_str = ", ".join(env_names)
    raise RuntimeError(f"Missing API token. Pass token=... or set one of: {names_str}.")


class RequestClient:
    """Shared request client with token resolution, base headers/params, and retry.

    Token is resolved at construction time and stored.  Every ``send()`` call
    merges base headers/params with per-request overrides (per-request wins)
    and optionally retries with exponential backoff.
    """

    def __init__(
        self,
        *,
        transport: BaseTransport,
        token: str | None = None,
        token_env_vars: str | Sequence[str] = "VERCEL_TOKEN",
        base_headers: dict[str, str] | None = None,
        base_params: dict[str, Any] | None = None,
        retry: RetryPolicy | None = None,
        sleep_fn: SleepFn,
    ) -> None:
        self._transport = transport
        self._token = _resolve_token(token, token_env_vars)
        self._retry = retry or RetryPolicy()
        self._sleep_fn = sleep_fn

        headers = dict(base_headers) if base_headers else {}
        headers.setdefault("authorization", f"Bearer {self._token}")
        self._base_headers = headers

        self._base_params: dict[str, Any] = dict(base_params) if base_params else {}

    @property
    def token(self) -> str:
        """The resolved API token."""
        return self._token

    @property
    def transport(self) -> BaseTransport:
        """The underlying transport (for direct access when needed)."""
        return self._transport

    def _merge_headers(self, headers: dict[str, str] | None) -> dict[str, str]:
        merged = dict(self._base_headers)
        if headers:
            merged.update(headers)
        return merged

    def _merge_params(self, params: dict[str, Any] | None) -> dict[str, Any] | None:
        if not self._base_params and not params:
            return None
        merged = dict(self._base_params)
        if params:
            merged.update(params)
        return merged

    async def send(
        self,
        method: str,
        path: str,
        *,
        headers: RequestHeadersInput = None,
        params: dict[str, Any] | None = None,
        body: RequestBodyInput = None,
        stream: bool = False,
        timeout: float | None = None,
        follow_redirects: bool | None = None,
    ) -> httpx.Response:
        """Send a request through the transport with header/param merging and retry.

        ``headers`` and ``body`` may be static values or per-attempt factories.
        """
        merged_params = self._merge_params(params)
        return await self._send_with_retry(
            method,
            path,
            headers=headers,
            params=merged_params,
            body=body,
            stream=stream,
            timeout=timeout,
            follow_redirects=follow_redirects,
        )

    async def _send_with_retry(
        self,
        method: str,
        path: str,
        *,
        headers: RequestHeadersInput,
        params: dict[str, Any] | None,
        body: RequestBodyInput,
        stream: bool,
        timeout: float | None,
        follow_redirects: bool | None,
    ) -> httpx.Response:
        retry = self._retry
        last_response: httpx.Response | None = None
        for attempt in range(retry.retries + 1):
            try:
                request_headers = headers(attempt) if callable(headers) else headers
                request_body = body(attempt) if callable(body) else body
                response = await self._transport.send(
                    method,
                    path,
                    headers=self._merge_headers(request_headers),
                    params=params,
                    body=request_body,
                    stream=stream,
                    timeout=timeout,
                    follow_redirects=follow_redirects,
                )
                last_response = response

                if (
                    retry.retry_on_response is not None
                    and retry.retry_on_response(response)
                    and attempt < retry.retries
                ):
                    await self._backoff(attempt)
                    continue

                return response
            except httpx.TransportError:
                if retry.retry_on_network_error and attempt < retry.retries:
                    await self._backoff(attempt)
                    continue
                raise

        assert last_response is not None
        return last_response

    async def _backoff(self, attempt: int) -> None:
        delay = min(self._retry.backoff_base * (2**attempt), self._retry.backoff_max)
        result = self._sleep_fn(delay)
        if inspect.isawaitable(result):
            await cast(Awaitable[None], result)

    def close(self) -> None:
        from vercel._internal.http.transport import SyncTransport

        if isinstance(self._transport, SyncTransport):
            self._transport.close()

    async def aclose(self) -> None:
        from vercel._internal.http.transport import AsyncTransport

        if isinstance(self._transport, AsyncTransport):
            await self._transport.aclose()


__all__ = [
    "RequestClient",
    "RetryPolicy",
    "SleepFn",
    "sync_sleep",
]
