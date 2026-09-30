"""Proxy responses."""

import urllib.parse
from collections.abc import Mapping

from starlette import responses
from starlette.datastructures import MutableHeaders
from starlette.types import Receive, Scope, Send

__all__ = [
    "ContinueResponse",
    "HTMLResponse",
    "JSONResponse",
    "PlainTextResponse",
    "RedirectResponse",
    "Response",
    "RewriteResponse",
]

# Response headers with this prefix are proxy control headers.
_RESERVED_PREFIX = "x-middleware-"

# Characters left unescaped when a destination URL is written into a
# x-middleware-* header. Covers every valid URI character, plus "%" so
# already-encoded destinations are not double-encoded.
_DESTINATION_SAFE = "/:@!$&'()*+,;=?#%[]"


def _check_name(name: str) -> None:
    if name.lower().startswith(_RESERVED_PREFIX):
        raise ValueError(f'invalid header "{name}": x-middleware-* headers are reserved')


class _CheckedHeaders(MutableHeaders):
    """Response headers that reject reserved names as they are added."""

    def __setitem__(self, key: str, value: str) -> None:
        _check_name(key)
        super().__setitem__(key, value)

    def setdefault(self, key: str, value: str) -> str:
        _check_name(key)
        return super().setdefault(key, value)

    def append(self, key: str, value: str) -> None:
        _check_name(key)
        super().append(key, value)


class Response(responses.Response):
    """A response that answers the request directly.

    Works like Starlette's ``Response``. Header names starting with
    ``x-middleware-`` are reserved.
    """

    def init_headers(self, headers: Mapping[str, str] | None = None) -> None:
        for name in headers or {}:
            _check_name(name)
        super().init_headers(headers)

    @property
    def headers(self) -> MutableHeaders:
        """The response headers."""
        return _CheckedHeaders(raw=self.raw_headers)


class HTMLResponse(Response, responses.HTMLResponse):
    """A response with an HTML body. Works like Starlette's ``HTMLResponse``."""


class PlainTextResponse(Response, responses.PlainTextResponse):
    """A response with a plain text body. Works like Starlette's ``PlainTextResponse``."""


class JSONResponse(Response, responses.JSONResponse):
    """A response with a JSON body. Works like Starlette's ``JSONResponse``."""


class RedirectResponse(Response, responses.RedirectResponse):
    """A response that redirects the client. Works like Starlette's ``RedirectResponse``."""


class ContinueResponse(Response):
    """A response that lets the request continue to its destination."""

    def __init__(
        self,
        *,
        headers: Mapping[str, str] | None = None,
        request_headers: Mapping[str, str | None] | None = None,
    ) -> None:
        """Let the request continue to its destination.

        *headers* are added to the response.

        *request_headers* sets headers on the forwarded request. A ``None``
        value removes that header. Omit to forward the headers unchanged.
        """
        super().__init__(headers=headers)
        self.request_headers: dict[str, str | None] = dict(request_headers or {})
        for name in self.request_headers:
            _check_name(name)

    @property
    def status_code(self) -> int:
        return 200

    @status_code.setter
    def status_code(self, value: int) -> None:
        if value != 200:
            raise RuntimeError("continue responses always have status 200")

    @property
    def body(self) -> bytes:
        return b""

    @body.setter
    def body(self, value: bytes | memoryview) -> None:
        if value:
            raise RuntimeError("continue responses cannot have a body")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Send the response.

        *scope*, *receive* and *send* are the standard ASGI arguments.
        """
        control = MutableHeaders()
        header, destination = self._destination_header()
        control[header] = destination
        if self.request_headers:
            for name in self.request_headers:
                _check_name(name)
            names = ",".join(name.lower() for name in self.request_headers)
            control["x-middleware-override-headers-diff"] = names
            for name, value in self.request_headers.items():
                if value is not None:
                    control[f"x-middleware-request-{name.lower()}"] = value
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [*control.raw, *self.raw_headers],
            }
        )
        await send({"type": "http.response.body", "body": b""})

    def _destination_header(self) -> tuple[str, str]:
        return "x-middleware-next", "1"


class RewriteResponse(ContinueResponse):
    """A response that serves the request from another URL.

    The URL the client sees does not change.
    """

    def __init__(
        self,
        destination: str,
        *,
        headers: Mapping[str, str] | None = None,
        request_headers: Mapping[str, str | None] | None = None,
    ) -> None:
        """Serve the request from *destination*.

        *destination* is the URL to serve the request from.

        *headers* are added to the response.

        *request_headers* sets headers on the forwarded request. A ``None``
        value removes that header. Omit to forward the headers unchanged.
        """
        super().__init__(headers=headers, request_headers=request_headers)
        self.destination = destination

    def _destination_header(self) -> tuple[str, str]:
        return "x-middleware-rewrite", urllib.parse.quote(self.destination, safe=_DESTINATION_SAFE)
