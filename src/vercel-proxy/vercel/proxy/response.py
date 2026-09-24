"""Proxy response factories."""

import enum
import json as _json
import types
from collections.abc import Mapping
from typing import final

__all__ = ["Kind", "Response"]


class Kind(enum.Enum):
    CONTINUING = "continuing"
    TERMINATING = "terminating"


@final
class Response:
    """Proxy response produced by a route handler.

    Use the static factory methods rather than constructing directly:
    :meth:`next`, :meth:`rewrite`, :meth:`redirect`, :meth:`json`,
    or :meth:`respond`.
    """

    __slots__ = ("_body", "_destination", "_headers", "_kind", "_status")

    _kind: Kind
    _destination: str | None
    _status: int
    _body: bytes
    _headers: dict[str, str | None]

    # ------------------------------------------------------------------
    # Public factories
    # ------------------------------------------------------------------

    @staticmethod
    def next(
        *,
        headers: dict[str, str | None] | None = None,
    ) -> "Response":
        """Pass the request to the next handler.

        *headers* overrides named headers in the upstream request; a ``None``
        value for a key drops that header. Omit to leave upstream headers
        untouched.
        """
        return Response._make(Kind.CONTINUING, None, 200, b"", headers or {})

    @staticmethod
    def rewrite(
        destination: str,
        *,
        headers: dict[str, str | None] | None = None,
    ) -> "Response":
        """Serve the response from *destination* with the client URL unchanged.

        *destination* is the URL to fetch the response from.

        *headers* overrides named headers in the upstream request; a ``None``
        value for a key drops that header. Omit to leave upstream headers
        untouched.
        """
        return Response._make(Kind.CONTINUING, destination, 200, b"", headers or {})

    @staticmethod
    def redirect(
        destination: str,
        *,
        status: int = 307,
        headers: dict[str, str] | None = None,
    ) -> "Response":
        """Redirect the client to *destination*.

        *destination* is the URL to redirect to.

        *status* is the HTTP redirect code; must be 3xx. Defaults to 307
        (Temporary Redirect).

        *headers* are sent to the client as response headers.
        """
        if not (300 <= status <= 399):
            raise ValueError(f"invalid redirect status {status}: must be a 3xx code")
        return Response._make(Kind.TERMINATING, destination, status, b"", headers or {})

    @staticmethod
    def json(
        data: object,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> "Response":
        """Terminate the request with a JSON body.

        *data* is any JSON-serialisable value.

        *status* is the HTTP response code. Defaults to 200.

        *headers* are sent to the client as response headers. ``Content-Type``
        is always set to ``application/json``; any value in *headers* is
        overridden.
        """
        body = _json.dumps(data).encode()
        out: dict[str, str] = dict(headers) if headers else {}
        out["content-type"] = "application/json"
        return Response._make(Kind.TERMINATING, None, status, body, out)

    @staticmethod
    def respond(
        *,
        status: int,
        body: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> "Response":
        """Terminate the request with a raw HTTP response.

        *status* is the HTTP response code.

        *body* is the raw response body. Defaults to an empty byte string.

        *headers* are sent to the client as response headers.
        """
        return Response._make(Kind.TERMINATING, None, status, body, headers or {})

    # ------------------------------------------------------------------
    # Read-only properties
    # ------------------------------------------------------------------

    @property
    def kind(self) -> Kind:
        """Response category."""
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
    def headers(self) -> Mapping[str, str | None]:
        """Read-only view of the headers passed to the factory.

        For continuing responses (``next``, ``rewrite``), a ``None`` value
        for a key means drop that header from the upstream request.
        """
        return types.MappingProxyType(self._headers)

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
        headers: Mapping[str, str | None],
    ) -> "Response":
        obj = object.__new__(Response)
        object.__setattr__(obj, "_kind", kind)
        object.__setattr__(obj, "_destination", destination)
        object.__setattr__(obj, "_status", status)
        object.__setattr__(obj, "_body", body)
        # Copy so later mutation of the caller's dict cannot leak in.
        object.__setattr__(obj, "_headers", dict(headers))
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
