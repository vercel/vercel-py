"""Proxy response factories."""

import enum
import types
from collections.abc import Mapping
from typing import final

from starlette.responses import JSONResponse

__all__ = ["Kind", "Response"]

# Response headers with this prefix are proxy control headers.
_RESERVED_PREFIX = "x-middleware-"


class Kind(enum.Enum):
    """Whether a response lets the request continue or ends it."""

    CONTINUING = "continuing"
    TERMINATING = "terminating"


@final
class Response:
    """The result of a proxy handler.

    Create one with ``next``, ``rewrite``, ``redirect``, ``json`` or
    ``respond``.
    """

    __slots__ = ("_body", "_destination", "_headers", "_kind", "_request_headers", "_status")

    _kind: Kind
    _destination: str | None
    _status: int
    _body: bytes
    _headers: dict[str, str]
    _request_headers: dict[str, str | None]

    # ------------------------------------------------------------------
    # Public factories
    # ------------------------------------------------------------------

    @staticmethod
    def next(
        *,
        headers: Mapping[str, str] | None = None,
        request_headers: Mapping[str, str | None] | None = None,
    ) -> "Response":
        """Let the request continue to its destination.

        *headers* are added to the response.

        *request_headers* sets headers on the forwarded request. A ``None``
        value removes that header. Omit to forward the headers unchanged.
        """
        return Response._make(Kind.CONTINUING, None, 200, b"", headers or {}, request_headers or {})

    @staticmethod
    def rewrite(
        destination: str,
        *,
        headers: Mapping[str, str] | None = None,
        request_headers: Mapping[str, str | None] | None = None,
    ) -> "Response":
        """Serve the request from *destination* without changing the URL the client sees.

        *destination* is the URL to serve the request from.

        *headers* are added to the response.

        *request_headers* sets headers on the forwarded request. A ``None``
        value removes that header. Omit to forward the headers unchanged.
        """
        return Response._make(
            Kind.CONTINUING, destination, 200, b"", headers or {}, request_headers or {}
        )

    @staticmethod
    def redirect(
        destination: str,
        *,
        status: int = 307,
        headers: Mapping[str, str] | None = None,
    ) -> "Response":
        """Redirect the client to *destination*.

        *destination* is the URL to redirect to.

        *status* is the redirect status code and must be 3xx. Defaults to 307.

        *headers* are added to the response.
        """
        if not (300 <= status <= 399):
            raise ValueError(f"invalid redirect status {status}: must be a 3xx code")
        return Response._make(Kind.TERMINATING, destination, status, b"", headers or {}, {})

    @staticmethod
    def json(
        data: object,
        *,
        status: int = 200,
        headers: Mapping[str, str] | None = None,
    ) -> "Response":
        """Answer the request with a JSON body.

        *data* is any value that can be serialized to JSON.

        *status* is the HTTP status code. Defaults to 200.

        *headers* are added to the response. ``Content-Type`` is always
        ``application/json``.
        """
        body = bytes(JSONResponse(data).body)
        out = {k: v for k, v in (headers or {}).items() if k.lower() != "content-type"}
        out["content-type"] = "application/json"
        return Response._make(Kind.TERMINATING, None, status, body, out, {})

    @staticmethod
    def respond(
        *,
        status: int,
        body: bytes = b"",
        headers: Mapping[str, str] | None = None,
    ) -> "Response":
        """Answer the request with a raw HTTP response.

        *status* is the HTTP status code.

        *body* is the response body. Defaults to empty.

        *headers* are added to the response.
        """
        return Response._make(Kind.TERMINATING, None, status, body, headers or {}, {})

    # ------------------------------------------------------------------
    # Read-only properties
    # ------------------------------------------------------------------

    @property
    def kind(self) -> Kind:
        """Whether the request continues or ends here."""
        return self._kind

    @property
    def destination(self) -> str | None:
        """The URL for ``rewrite`` and ``redirect`` responses."""
        return self._destination

    @property
    def status(self) -> int:
        """The HTTP status code."""
        return self._status

    @property
    def body(self) -> bytes:
        """The response body."""
        return self._body

    @property
    def headers(self) -> Mapping[str, str]:
        """The headers added to the response."""
        return types.MappingProxyType(self._headers)

    @property
    def request_headers(self) -> Mapping[str, str | None]:
        """The headers set on the forwarded request.

        A ``None`` value means the header is removed. Always empty for
        ``redirect``, ``json`` and ``respond``.
        """
        return types.MappingProxyType(self._request_headers)

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
        headers: Mapping[str, str],
        request_headers: Mapping[str, str | None],
    ) -> "Response":
        names = [*headers, *request_headers]
        if any(name.lower().startswith(_RESERVED_PREFIX) for name in names):
            raise ValueError("invalid headers: x-middleware-* headers are reserved")
        obj = object.__new__(Response)
        object.__setattr__(obj, "_kind", kind)
        object.__setattr__(obj, "_destination", destination)
        object.__setattr__(obj, "_status", status)
        object.__setattr__(obj, "_body", body)
        # Copy so later mutation of the caller's dicts cannot leak in.
        object.__setattr__(obj, "_headers", dict(headers))
        object.__setattr__(obj, "_request_headers", dict(request_headers))
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
        if self._request_headers:
            parts.append(f"request_headers={self._request_headers!r}")
        return f"{type(self).__name__}({', '.join(parts)})"
