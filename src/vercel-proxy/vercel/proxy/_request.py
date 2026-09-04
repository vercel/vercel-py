from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ._headers import Headers

__all__ = ["Request"]


@dataclass(frozen=True, slots=True)
class Request:
    """Immutable snapshot of an incoming proxy request.

    Constructed by :class:`~vercel.proxy.Proxy` before dispatching to a
    route handler. The request body is intentionally not exposed — proxy
    handlers should not buffer the request body.
    """

    method: str
    """HTTP method, normalised to uppercase (e.g. ``"GET"``)."""
    path: str
    """Request path (e.g. ``"/api/users/42"``)."""
    url: str
    """Full URL including query string."""
    headers: Headers
    """Request headers with case-insensitive access."""
    path_params: Mapping[str, str]
    """Named captures extracted from the matched route pattern."""
    query_params: Mapping[str, str]
    """Query parameters parsed from the URL."""

    @classmethod
    def _from_asgi_scope(cls, scope: dict[str, Any], path_params: Mapping[str, str]) -> Request:
        """Internal: construct from an ASGI http scope dict."""
        raise NotImplementedError
