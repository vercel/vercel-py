"""Client factory functions for creating pre-configured httpx clients."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping

import httpx

from .config import DEFAULT_TIMEOUT


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
    """Create a request hook that adds static headers to every request."""

    def hook(request: httpx.Request) -> httpx.Request:
        for key, value in headers.items():
            request.headers.setdefault(key, value)
        return request

    return hook


def create_vercel_client(
    token: str | None = None,
    timeout: float | None = None,
) -> httpx.Client:
    """Create a sync httpx client pre-configured for Vercel API.

    Args:
        token: API token. Falls back to VERCEL_TOKEN env var if not provided.
        timeout: Request timeout in seconds. Defaults to DEFAULT_TIMEOUT.

    Returns:
        An httpx.Client with auth event hook configured.

    Raises:
        RuntimeError: If no token is provided and VERCEL_TOKEN is not set.
    """
    resolved_token = _require_token(token)
    effective_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
    return httpx.Client(
        timeout=httpx.Timeout(effective_timeout),
        event_hooks={"request": [_create_vercel_auth_hook(resolved_token)]},
    )


def create_vercel_async_client(
    token: str | None = None,
    timeout: float | None = None,
) -> httpx.AsyncClient:
    """Create an async httpx client pre-configured for Vercel API.

    Args:
        token: API token. Falls back to VERCEL_TOKEN env var if not provided.
        timeout: Request timeout in seconds. Defaults to DEFAULT_TIMEOUT.

    Returns:
        An httpx.AsyncClient with auth event hook configured.

    Raises:
        RuntimeError: If no token is provided and VERCEL_TOKEN is not set.
    """
    resolved_token = _require_token(token)
    effective_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
    return httpx.AsyncClient(
        timeout=httpx.Timeout(effective_timeout),
        event_hooks={"request": [_create_vercel_auth_hook(resolved_token)]},
    )


def create_headers_client(
    headers: Mapping[str, str],
    timeout: float | None = None,
) -> httpx.Client:
    """Create a sync httpx client with static headers.

    Useful for the cache module where auth is passed via custom headers.

    Args:
        headers: Static headers to add to every request.
        timeout: Request timeout in seconds. Defaults to DEFAULT_TIMEOUT.

    Returns:
        An httpx.Client with static headers event hook configured.
    """
    effective_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
    return httpx.Client(
        timeout=httpx.Timeout(effective_timeout),
        event_hooks={"request": [_create_static_headers_hook(headers)]},
    )


def create_headers_async_client(
    headers: Mapping[str, str],
    timeout: float | None = None,
) -> httpx.AsyncClient:
    """Create an async httpx client with static headers.

    Useful for the cache module where auth is passed via custom headers.

    Args:
        headers: Static headers to add to every request.
        timeout: Request timeout in seconds. Defaults to DEFAULT_TIMEOUT.

    Returns:
        An httpx.AsyncClient with static headers event hook configured.
    """
    effective_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
    return httpx.AsyncClient(
        timeout=httpx.Timeout(effective_timeout),
        event_hooks={"request": [_create_static_headers_hook(headers)]},
    )


__all__ = [
    "create_vercel_client",
    "create_vercel_async_client",
    "create_headers_client",
    "create_headers_async_client",
]
