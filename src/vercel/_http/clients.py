"""Client factory functions for creating pre-configured httpx clients."""

import os
from collections.abc import Callable, Coroutine, Mapping, Sequence
from typing import Any

import httpx

from .config import DEFAULT_TIMEOUT


def _normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/") + "/"


def _require_token(token: str | None) -> str:
    env_token = os.getenv("VERCEL_TOKEN")
    resolved = token or env_token
    if not resolved:
        raise RuntimeError("Missing Vercel API token. Pass token=... or set VERCEL_TOKEN.")
    return resolved


def _create_vercel_auth_hook(
    token: str,
) -> Callable[[httpx.Request], httpx.Request]:
    def hook(request: httpx.Request) -> httpx.Request:
        request.headers.setdefault("authorization", f"Bearer {token}")
        request.headers.setdefault("accept", "application/json")
        request.headers.setdefault("content-type", "application/json")
        return request

    return hook


def _create_vercel_auth_hook_async(
    token: str,
) -> Callable[[httpx.Request], Coroutine[Any, Any, None]]:
    async def hook(request: httpx.Request) -> None:
        request.headers.setdefault("authorization", f"Bearer {token}")
        request.headers.setdefault("accept", "application/json")
        request.headers.setdefault("content-type", "application/json")

    return hook


def _create_static_headers_hook(
    headers: Mapping[str, str],
) -> Callable[[httpx.Request], httpx.Request]:
    def hook(request: httpx.Request) -> httpx.Request:
        for key, value in headers.items():
            request.headers.setdefault(key, value)
        return request

    return hook


def _create_static_headers_hook_async(
    headers: Mapping[str, str],
) -> Callable[[httpx.Request], Coroutine[Any, Any, None]]:
    async def hook(request: httpx.Request) -> None:
        for key, value in headers.items():
            request.headers.setdefault(key, value)

    return hook


SyncRequestHook = Callable[[httpx.Request], httpx.Request]
AsyncRequestHook = Callable[[httpx.Request], Coroutine[Any, Any, None]]


def _prepend_request_hooks(
    client: httpx.Client | httpx.AsyncClient,
    hooks: Sequence[SyncRequestHook | AsyncRequestHook],
) -> None:
    existing_hooks = list(client.event_hooks.get("request", []))
    client.event_hooks["request"] = list(hooks) + existing_hooks


def create_vercel_client(
    token: str | None = None,
    timeout: float | None = None,
    base_url: str | None = None,
    *,
    client: httpx.Client | None = None,
) -> httpx.Client:
    """Create or configure a sync httpx client for Vercel API."""
    resolved_token = _require_token(token)
    auth_hook = _create_vercel_auth_hook(resolved_token)

    if client is not None:
        _prepend_request_hooks(client, [auth_hook])
        return client

    effective_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
    kwargs: dict = {
        "timeout": httpx.Timeout(effective_timeout),
        "event_hooks": {"request": [auth_hook]},
    }
    if base_url is not None:
        kwargs["base_url"] = _normalize_base_url(base_url)
    return httpx.Client(**kwargs)


def create_vercel_async_client(
    token: str | None = None,
    timeout: float | None = None,
    base_url: str | None = None,
    *,
    client: httpx.AsyncClient | None = None,
) -> httpx.AsyncClient:
    """Create or configure an async httpx client for Vercel API."""
    resolved_token = _require_token(token)
    auth_hook = _create_vercel_auth_hook_async(resolved_token)

    if client is not None:
        _prepend_request_hooks(client, [auth_hook])
        return client

    effective_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
    kwargs: dict = {
        "timeout": httpx.Timeout(effective_timeout),
        "event_hooks": {"request": [auth_hook]},
    }
    if base_url is not None:
        kwargs["base_url"] = _normalize_base_url(base_url)
    return httpx.AsyncClient(**kwargs)


def create_headers_client(
    headers: Mapping[str, str],
    timeout: float | None = None,
    base_url: str | None = None,
    *,
    client: httpx.Client | None = None,
) -> httpx.Client:
    """Create or configure a sync httpx client with static headers."""
    headers_hook = _create_static_headers_hook(headers)

    if client is not None:
        _prepend_request_hooks(client, [headers_hook])
        return client

    effective_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
    kwargs: dict = {
        "timeout": httpx.Timeout(effective_timeout),
        "event_hooks": {"request": [headers_hook]},
    }
    if base_url is not None:
        kwargs["base_url"] = _normalize_base_url(base_url)
    return httpx.Client(**kwargs)


def create_headers_async_client(
    headers: Mapping[str, str],
    timeout: float | None = None,
    base_url: str | None = None,
    *,
    client: httpx.AsyncClient | None = None,
) -> httpx.AsyncClient:
    """Create or configure an async httpx client with static headers."""
    headers_hook = _create_static_headers_hook_async(headers)

    if client is not None:
        _prepend_request_hooks(client, [headers_hook])
        return client

    effective_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
    kwargs: dict = {
        "timeout": httpx.Timeout(effective_timeout),
        "event_hooks": {"request": [headers_hook]},
    }
    if base_url is not None:
        kwargs["base_url"] = _normalize_base_url(base_url)
    return httpx.AsyncClient(**kwargs)


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


__all__ = [
    "create_vercel_client",
    "create_vercel_async_client",
    "create_headers_client",
    "create_headers_async_client",
    "create_base_client",
    "create_base_async_client",
]
