"""Request-time authenticated Sandbox client for unstable APIs."""

from __future__ import annotations

import json
import platform
import sys
from abc import ABC, abstractmethod
from datetime import timedelta
from importlib.metadata import version as _pkg_version
from typing import Any

import httpx

from vercel._internal.http import JSONBody, RequestClient, RetryPolicy, SleepFn, sync_sleep
from vercel._internal.http.clients import create_base_async_client, create_base_client
from vercel._internal.http.transport import AsyncTransport, BaseTransport, SyncTransport
from vercel._internal.sandbox.errors import (
    APIError as StableSandboxAPIError,
    SandboxRateLimitError as StableSandboxRateLimitError,
)
from vercel._internal.unstable.sandbox.types import SandboxAPIError, SandboxOptions

try:
    _VERSION = _pkg_version("vercel")
except Exception:
    _VERSION = "development"

_PLATFORM = platform.uname()
_USER_AGENT = (
    f"vercel/unstable-sandbox/{_VERSION} "
    f"(Python/{sys.version}; {_PLATFORM.system}/{_PLATFORM.machine})"
)

DEFAULT_SANDBOX_API_URL = "https://api.vercel.com"


class BaseUnstableSandboxRequestClient(ABC):
    """Shared Sandbox request client that resolves credentials per request."""

    def __init__(
        self,
        *,
        options: SandboxOptions | None = None,
        transport: BaseTransport | None = None,
        sleep_fn: SleepFn,
    ) -> None:
        self._options = options or SandboxOptions()
        self._owns_transport = transport is None
        self._client = RequestClient(
            transport=transport or self._create_transport(self._options),
            token="unstable-sandbox-request-time-auth",
            base_headers={},
            retry=_retry_policy(self._options),
            sleep_fn=sleep_fn,
        )

    @abstractmethod
    def _create_transport(self, options: SandboxOptions) -> BaseTransport:
        raise NotImplementedError

    async def request_json(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str],
        query: dict[str, Any],
        body: JSONBody,
    ) -> object:
        response = await self._client.send(
            method,
            path,
            headers=headers,
            params={key: value for key, value in query.items() if value is not None},
            body=body,
        )
        if 200 <= response.status_code < 300:
            return response.json()
        raise _build_api_error(response)


class UnstableSandboxRequestClient(BaseUnstableSandboxRequestClient):
    """Async request client that resolves Sandbox credentials per request."""

    def __init__(
        self,
        *,
        options: SandboxOptions | None = None,
        transport: BaseTransport | None = None,
    ) -> None:
        import asyncio

        super().__init__(options=options, transport=transport, sleep_fn=asyncio.sleep)

    def _create_transport(self, options: SandboxOptions) -> AsyncTransport:
        return _create_async_transport(options)

    async def aclose(self) -> None:
        if self._owns_transport:
            await self._client.aclose()


class SyncUnstableSandboxRequestClient(BaseUnstableSandboxRequestClient):
    """Sync request client with async-shaped methods for iter_coroutine()."""

    def __init__(
        self,
        *,
        options: SandboxOptions | None = None,
        transport: BaseTransport | None = None,
    ) -> None:
        super().__init__(options=options, transport=transport, sleep_fn=sync_sleep)

    def _create_transport(self, options: SandboxOptions) -> SyncTransport:
        return _create_sync_transport(options)

    def close(self) -> None:
        if self._owns_transport:
            self._client.close()


def _create_async_transport(options: SandboxOptions) -> AsyncTransport:
    return AsyncTransport(
        create_base_async_client(
            timeout=_seconds(options.request_timeout),
            base_url=options.api_url or DEFAULT_SANDBOX_API_URL,
        )
    )


def _create_sync_transport(options: SandboxOptions) -> SyncTransport:
    return SyncTransport(
        create_base_client(
            timeout=_seconds(options.request_timeout),
            base_url=options.api_url or DEFAULT_SANDBOX_API_URL,
        )
    )


def _retry_policy(options: SandboxOptions) -> RetryPolicy | None:
    if options.retry_attempts is None:
        return None
    return RetryPolicy(retries=options.retry_attempts)


def _seconds(duration: timedelta | None) -> float | None:
    if duration is None:
        return None
    return duration.total_seconds()


def _build_api_error(response: httpx.Response) -> SandboxAPIError:
    data: object | None = None
    message = f"HTTP {response.status_code}"
    try:
        data = response.json()
    except json.JSONDecodeError:
        text = response.text
        if text:
            message = f"{message}: {text}"
    else:
        if isinstance(data, dict):
            raw_message = data.get("message")
            if isinstance(raw_message, str):
                message = f"{message}: {raw_message}"
            else:
                raw_error = data.get("error")
                if isinstance(raw_error, dict):
                    nested_message = raw_error.get("message") or raw_error.get("msg")
                    if isinstance(nested_message, str):
                        message = f"{message}: {nested_message}"

    stable_error: StableSandboxAPIError
    if response.status_code == 429:
        stable_error = StableSandboxRateLimitError(
            response,
            message,
            data=data,
            retry_after=response.headers.get("retry-after"),
        )
    else:
        stable_error = StableSandboxAPIError(response, message, data=data)
    return SandboxAPIError.from_stable_error(stable_error)


__all__ = [
    "DEFAULT_SANDBOX_API_URL",
    "SyncUnstableSandboxRequestClient",
    "UnstableSandboxRequestClient",
]
