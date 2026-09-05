"""Proxy request dataclass."""

from __future__ import annotations

import urllib.parse
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from ._headers import Headers
from ._params import Params

__all__ = ["Request"]


@dataclass(frozen=True, slots=True)
class Request:
    """Immutable snapshot of an incoming proxy request."""

    method: str
    """HTTP method, normalised to uppercase (e.g. ``"GET"``)."""
    path: str
    """Request path (e.g. ``"/api/users/42"``)."""
    url: str
    """Full URL including query string."""
    headers: Headers
    """Request headers with case-insensitive access."""
    path_params: Params = field(default_factory=lambda: Params(()))
    """Named captures extracted from the matched route pattern."""
    query_params: Params = field(default_factory=lambda: Params(()))
    """Query parameters parsed from the URL."""

    @classmethod
    def _from_asgi_scope(
        cls,
        scope: dict[str, Any],
        path_params: Mapping[str, str] | None = None,
    ) -> Request:
        """Internal: construct from an ASGI http scope dict."""
        method = scope["method"].upper()
        path = scope.get("path", "/")
        query_bytes: bytes = scope.get("query_string", b"")
        scheme = scope.get("scheme", "https")

        headers = Headers.from_asgi(scope.get("headers", []))

        host = headers.get("host") or _host_from_scope(scope)
        query_str = query_bytes.decode("latin-1")
        url = f"{scheme}://{host}{path}"
        if query_str:
            url = f"{url}?{query_str}"

        parsed_qs: dict[str, str] = {}
        for k, v in urllib.parse.parse_qsl(query_str, keep_blank_values=True):
            parsed_qs.setdefault(k, v)

        return cls(
            method=method,
            path=path,
            url=url,
            headers=headers,
            path_params=Params(tuple(path_params.items()) if path_params else ()),
            query_params=Params(tuple(parsed_qs.items())),
        )


def _host_from_scope(scope: dict[str, Any]) -> str:
    server = scope.get("server")
    if server:
        host, port = server
        return f"{host}:{port}" if port not in (80, 443) else str(host)
    return "localhost"
