"""Client factory functions for creating pre-configured httpx clients."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Sequence
from typing import Any

import httpx

from vercel._internal.http.config import DEFAULT_TIMEOUT
from vercel._internal.http.request_client import (
    RequestClient,
    RetryPolicy,
    sync_sleep,
)
from vercel._internal.http.transport import AsyncTransport, SyncTransport


def _normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/") + "/"


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
        value = os.environ.get(name)
        if value:
            return value

    names_str = ", ".join(env_names)
    raise RuntimeError(f"Missing API token. Pass token=... or set one of: {names_str}.")


def _merge_bearer_auth(
    base_headers: dict[str, str] | None,
    token: str,
) -> dict[str, str]:
    headers = dict(base_headers) if base_headers else {}

    # Respect any user-specified Authorization header, regardless of case.
    for header_name in headers:
        if header_name.lower() == "authorization":
            return headers

    headers["authorization"] = f"Bearer {token}"
    return headers


def create_base_client(
    timeout: float | None = None,
    base_url: str | None = None,
) -> httpx.Client:
    """Create a sync httpx client without auth hooks."""
    effective_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
    kwargs: dict = {"timeout": httpx.Timeout(effective_timeout)}
    if base_url is not None:
        kwargs["base_url"] = _normalize_base_url(base_url)
    return httpx.Client(**kwargs)


def create_base_async_client(
    timeout: float | None = None,
    base_url: str | None = None,
) -> httpx.AsyncClient:
    """Create an async httpx client without auth hooks."""
    effective_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
    kwargs: dict = {"timeout": httpx.Timeout(effective_timeout)}
    if base_url is not None:
        kwargs["base_url"] = _normalize_base_url(base_url)
    return httpx.AsyncClient(**kwargs)


def create_request_client(
    *,
    token: str | None = None,
    token_env_vars: str | Sequence[str] = "VERCEL_TOKEN",
    base_headers: dict[str, str] | None = None,
    base_params: dict[str, Any] | None = None,
    retry: RetryPolicy | None = None,
    timeout: float | None = None,
    base_url: str | None = None,
) -> RequestClient:
    """Create a sync :class:`RequestClient` backed by an httpx.Client."""
    http_client = create_base_client(timeout=timeout, base_url=base_url)
    transport = SyncTransport(http_client)
    resolved_token = _resolve_token(token, token_env_vars)
    return RequestClient(
        transport=transport,
        base_headers=_merge_bearer_auth(base_headers, resolved_token),
        base_params=base_params,
        retry=retry,
        sleep_fn=sync_sleep,
    )


def create_async_request_client(
    *,
    token: str | None = None,
    token_env_vars: str | Sequence[str] = "VERCEL_TOKEN",
    base_headers: dict[str, str] | None = None,
    base_params: dict[str, Any] | None = None,
    retry: RetryPolicy | None = None,
    timeout: float | None = None,
    base_url: str | None = None,
) -> RequestClient:
    """Create an async :class:`RequestClient` backed by an httpx.AsyncClient."""
    http_client = create_base_async_client(timeout=timeout, base_url=base_url)
    transport = AsyncTransport(http_client)
    resolved_token = _resolve_token(token, token_env_vars)
    return RequestClient(
        transport=transport,
        base_headers=_merge_bearer_auth(base_headers, resolved_token),
        base_params=base_params,
        retry=retry,
        sleep_fn=asyncio.sleep,
    )


__all__ = [
    "create_base_client",
    "create_base_async_client",
    "create_request_client",
    "create_async_request_client",
]
