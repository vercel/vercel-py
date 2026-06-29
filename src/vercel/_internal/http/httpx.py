"""Client factory functions for creating pre-configured httpx clients."""

from __future__ import annotations

from typing import TypedDict

import httpx

from vercel._internal.http.transport import TransportOptions
from vercel._internal.time import to_seconds_float


def _normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/") + "/"


class _HttpxClientKwargs(TypedDict, total=False):
    timeout: httpx.Timeout
    base_url: str
    limits: httpx.Limits
    http2: bool


def _options_to_httpx_kwargs(options: TransportOptions) -> _HttpxClientKwargs:
    kwargs: _HttpxClientKwargs = {"timeout": httpx.Timeout(to_seconds_float(options.timeout))}
    if options.base_url is not None:
        kwargs["base_url"] = _normalize_base_url(options.base_url)
    if options.max_connections is not None:
        kwargs["limits"] = httpx.Limits(max_connections=options.max_connections)
    if options.enable_http2 is not None:
        kwargs["http2"] = options.enable_http2
    return kwargs


def create_base_client(options: TransportOptions) -> httpx.Client:
    """Create a sync httpx client without auth hooks."""
    return httpx.Client(**_options_to_httpx_kwargs(options))


def create_base_async_client(options: TransportOptions) -> httpx.AsyncClient:
    """Create an async httpx client without auth hooks."""
    return httpx.AsyncClient(**_options_to_httpx_kwargs(options))


__all__ = [
    "create_base_client",
    "create_base_async_client",
]
