"""Proxy response factories."""

from __future__ import annotations

import enum
import types
from collections.abc import Mapping
from typing import cast, final

__all__ = ["Response"]


class Kind(enum.Enum):
    CONTINUING = "continuing"
    TERMINATING = "terminating"


@final
class Response:
    """Proxy response produced by a route handler.

    Use the static factory methods rather than constructing directly:
    :meth:`next`, :meth:`rewrite`, :meth:`redirect`, :meth:`json`,
    or the low-level :meth:`respond`.
    """

    __slots__ = ("_body", "_destination", "_headers", "_kind", "_status")
    _kind: Kind
    _destination: str | None
    _status: int
    _body: bytes
    _headers: dict[str, str | None] | None

    # ------------------------------------------------------------------
    # Public factories
    # ------------------------------------------------------------------

    @staticmethod
    def next(*, headers: dict[str, str | None] | None = None) -> Response:
        """Pass the request to the next handler.

        *headers* replace the named headers in the upstream request.
        ``None`` leaves the original request headers untouched.
        """
        return Response._make(Kind.CONTINUING, None, 200, b"", headers)

    @staticmethod
    def rewrite(destination: str, *, headers: dict[str, str | None] | None = None) -> Response:
        """Serve *destination* transparently (URL seen by client is unchanged).

        *headers* replace the named headers in the upstream request.
        ``None`` leaves the original request headers untouched.
        """
        return Response._make(Kind.CONTINUING, destination, 200, b"", headers)

    @staticmethod
    def redirect(
        destination: str,
        *,
        status: int = 307,
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Redirect the client to *destination*.

        *status* should be a 3xx code; defaults to 307 (Temporary Redirect).
        *headers* are sent to the client as response headers (e.g. ``Set-Cookie``).
        """
        h = cast(dict[str, str | None], headers or {})
        return Response._make(Kind.TERMINATING, destination, status, b"", h)

    @staticmethod
    def json(
        data: object,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Terminate the request with a JSON body.

        *status* is the HTTP response code.
        *headers* are response headers sent to the client.
        Sets ``Content-Type: application/json`` automatically.
        """
        import json as _json

        body = _json.dumps(data).encode()
        out: dict[str, str] = dict(headers) if headers else {}
        out["content-type"] = "application/json"
        h = cast(dict[str, str | None], out)
        return Response._make(Kind.TERMINATING, None, status, body, h)

    @staticmethod
    def respond(
        *,
        status: int,
        body: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Terminate the request with a raw response.

        *status* is the HTTP response code.
        *body* is the response body.
        *headers* are response headers sent to the client.
        """
        h = cast(dict[str, str | None], headers or {})
        return Response._make(Kind.TERMINATING, None, status, body, h)

    # ------------------------------------------------------------------
    # Read-only properties (for Proxy and testing)
    # ------------------------------------------------------------------

    @property
    def kind(self) -> Kind:
        """Response category: ``"continuing"`` passes to the next handler;
        ``"terminating"`` ends the request.
        """
        return self._kind

    @property
    def destination(self) -> str | None:
        """Destination URL for ``rewrite`` and ``redirect`` responses."""
        return self._destination

    @property
    def status(self) -> int:
        """HTTP status code."""
        return self._status

    @property
    def body(self) -> bytes:
        """Response body bytes."""
        return self._body

    @property
    def headers(self) -> Mapping[str, str | None] | None:
        """User-specified headers, or ``None`` if no header modification is requested.

        For continuing responses (``next``, ``rewrite``), a ``None`` value
        means drop that header from the upstream request.
        """
        h = self._headers
        return types.MappingProxyType(h) if h is not None else None

    # ------------------------------------------------------------------
    # Immutability
    # ------------------------------------------------------------------

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(f"{type(self).__name__!r} object is immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f"{type(self).__name__!r} object is immutable")

    # ------------------------------------------------------------------
    # Internal constructor
    # ------------------------------------------------------------------

    @staticmethod
    def _make(
        kind: Kind,
        destination: str | None,
        status: int,
        body: bytes,
        headers: dict[str, str | None] | None,
    ) -> Response:
        obj = object.__new__(Response)
        object.__setattr__(obj, "_kind", kind)
        object.__setattr__(obj, "_destination", destination)
        object.__setattr__(obj, "_status", status)
        object.__setattr__(obj, "_body", body)
        object.__setattr__(obj, "_headers", headers)
        return obj

    def __repr__(self) -> str:
        parts = [f"kind={self._kind!r}"]
        if self._destination is not None:
            parts.append(f"destination={self._destination!r}")
        if self._status != 200:
            parts.append(f"status={self._status!r}")
        if self._body:
            parts.append(f"body={self._body!r}")
        if self._headers:
            parts.append(f"headers={self._headers!r}")
        return f"{type(self).__name__}({', '.join(parts)})"
