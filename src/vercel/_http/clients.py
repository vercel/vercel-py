"""Client factory functions for creating pre-configured httpx clients."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping, Sequence

import httpx

from .config import DEFAULT_TIMEOUT


def _normalize_base_url(base_url: str) -> str:
    """Ensure base_url ends with a trailing slash for consistent URL joining."""
    return base_url.rstrip("/") + "/"


def _require_token(token: str | None) -> str:
    """Resolve token from argument or environment, raising if not found."""
    env_token = os.getenv("VERCEL_TOKEN")
    resolved = token or env_token
    if not resolved:
        raise RuntimeError("Missing Vercel API token. Pass token=... or set VERCEL_TOKEN.")
    return resolved


def _create_vercel_auth_hook(
    token: str,
) -> Callable[[httpx.Request], httpx.Request]:
    """Create a request hook that adds Vercel API auth headers.

    The hook adds:
    - Authorization: Bearer <token>
    - Accept: application/json
    - Content-Type: application/json

    Uses setdefault so user-provided headers take precedence.
    """

    def hook(request: httpx.Request) -> httpx.Request:
        request.headers.setdefault("authorization", f"Bearer {token}")
        request.headers.setdefault("accept", "application/json")
        request.headers.setdefault("content-type", "application/json")
        return request

    return hook


def _create_static_headers_hook(
    headers: Mapping[str, str],
) -> Callable[[httpx.Request], httpx.Request]:
    """Create a request hook that adds static headers to every request.

    Uses setdefault so user-provided headers take precedence.
    """

    def hook(request: httpx.Request) -> httpx.Request:
        for key, value in headers.items():
            request.headers.setdefault(key, value)
        return request

    return hook


def _prepend_request_hooks(
    client: httpx.Client | httpx.AsyncClient,
    hooks: Sequence[Callable[[httpx.Request], httpx.Request]],
) -> None:
    """Prepend request hooks to an existing client's event hooks.

    Prepending ensures our default hooks run first, allowing user-configured
    hooks to override or intercept the defaults.

    Args:
        client: The httpx client to modify.
        hooks: Request hooks to prepend.
    """
    existing_hooks = list(client.event_hooks.get("request", []))
    client.event_hooks["request"] = list(hooks) + existing_hooks


def create_vercel_client(
    token: str | None = None,
    timeout: float | None = None,
    base_url: str | None = None,
    *,
    client: httpx.Client | None = None,
) -> httpx.Client:
    """Create or configure a sync httpx client for Vercel API.

    Args:
        token: API token. Falls back to VERCEL_TOKEN env var if not provided.
        timeout: Request timeout in seconds. Defaults to DEFAULT_TIMEOUT.
            Ignored if client is provided.
        base_url: Base URL for API requests. Ignored if client is provided.
        client: Optional existing client to configure. If provided, auth hooks
            are prepended to existing hooks, allowing user hooks to override.

    Returns:
        An httpx.Client with auth event hook configured.

    Raises:
        RuntimeError: If no token is provided and VERCEL_TOKEN is not set.
    """
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
    """Create or configure an async httpx client for Vercel API.

    Args:
        token: API token. Falls back to VERCEL_TOKEN env var if not provided.
        timeout: Request timeout in seconds. Defaults to DEFAULT_TIMEOUT.
            Ignored if client is provided.
        base_url: Base URL for API requests. Ignored if client is provided.
        client: Optional existing client to configure. If provided, auth hooks
            are prepended to existing hooks, allowing user hooks to override.

    Returns:
        An httpx.AsyncClient with auth event hook configured.

    Raises:
        RuntimeError: If no token is provided and VERCEL_TOKEN is not set.
    """
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
    return httpx.AsyncClient(**kwargs)


def create_headers_client(
    headers: Mapping[str, str],
    timeout: float | None = None,
    base_url: str | None = None,
    *,
    client: httpx.Client | None = None,
) -> httpx.Client:
    """Create or configure a sync httpx client with static headers.

    Useful for the cache module where auth is passed via custom headers.

    Args:
        headers: Static headers to add to every request.
        timeout: Request timeout in seconds. Defaults to DEFAULT_TIMEOUT.
            Ignored if client is provided.
        base_url: Base URL for API requests. Ignored if client is provided.
        client: Optional existing client to configure. If provided, header hooks
            are prepended to existing hooks, allowing user hooks to override.

    Returns:
        An httpx.Client with static headers event hook configured.
    """
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
    """Create or configure an async httpx client with static headers.

    Useful for the cache module where auth is passed via custom headers.

    Args:
        headers: Static headers to add to every request.
        timeout: Request timeout in seconds. Defaults to DEFAULT_TIMEOUT.
            Ignored if client is provided.
        base_url: Base URL for API requests. Ignored if client is provided.
        client: Optional existing client to configure. If provided, header hooks
            are prepended to existing hooks, allowing user hooks to override.

    Returns:
        An httpx.AsyncClient with static headers event hook configured.
    """
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
    return httpx.AsyncClient(**kwargs)


def create_base_client(
    timeout: float | None = None,
    base_url: str | None = None,
) -> httpx.Client:
    """Create a sync httpx client with basic configuration (no auth).

    Useful for clients that handle auth per-request rather than via hooks.

    Args:
        timeout: Request timeout in seconds. Defaults to DEFAULT_TIMEOUT.
        base_url: Base URL for API requests.

    Returns:
        An httpx.Client with basic configuration.
    """
    effective_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
    kwargs: dict = {"timeout": httpx.Timeout(effective_timeout)}
    if base_url is not None:
        kwargs["base_url"] = _normalize_base_url(base_url)
    return httpx.Client(**kwargs)


def create_base_async_client(
    timeout: float | None = None,
    base_url: str | None = None,
) -> httpx.AsyncClient:
    """Create an async httpx client with basic configuration (no auth).

    Useful for clients that handle auth per-request rather than via hooks.

    Args:
        timeout: Request timeout in seconds. Defaults to DEFAULT_TIMEOUT.
        base_url: Base URL for API requests.

    Returns:
        An httpx.AsyncClient with basic configuration.
    """
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
