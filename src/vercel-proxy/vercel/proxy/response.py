"""Proxy responses."""

import re
import urllib.parse
from collections.abc import Iterator, Mapping, MutableMapping
from typing import ClassVar

from starlette import responses
from starlette.datastructures import MutableHeaders
from starlette.types import Message, Receive, Scope, Send

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

# Statuses Vercel follows as redirects.
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})


# Characters allowed in an HTTP header name.
_RESPONSE_HEADER_NAME = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")

# Characters Vercel accepts in the name of a forwarded request header.
_REQUEST_HEADER_NAME = re.compile(r"[A-Za-z0-9_-]+")


def _check_reserved(name: str) -> None:
    if name.lower().startswith(_RESERVED_PREFIX):
        raise ValueError(f'invalid header "{name}": x-middleware-* headers are reserved')


def _check_response_name(name: str) -> None:
    if not _RESPONSE_HEADER_NAME.fullmatch(name):
        raise ValueError(f'invalid header "{name}": not a valid header name')
    _check_reserved(name)


def _check_request_name(name: str) -> None:
    if not _REQUEST_HEADER_NAME.fullmatch(name):
        raise ValueError(
            f'invalid request header "{name}": may only contain letters, digits, "-" and "_"'
        )
    _check_reserved(name)


class _ResponseHeaders(MutableHeaders):
    """Response headers that reject reserved names."""

    def __setitem__(self, key: str, value: str) -> None:
        _check_response_name(key)
        super().__setitem__(key, value)

    def setdefault(self, key: str, value: str) -> str:
        _check_response_name(key)
        return super().setdefault(key, value)

    def append(self, key: str, value: str) -> None:
        _check_response_name(key)
        super().append(key, value)


class _RequestHeaders(MutableMapping[str, str | None]):
    """Request header changes that reject reserved names."""

    def __init__(self, headers: Mapping[str, str | None]) -> None:
        self._headers: dict[str, str | None] = {}
        self.update(headers)

    def __getitem__(self, key: str) -> str | None:
        return self._headers[key.lower()]

    def __setitem__(self, key: str, value: str | None) -> None:
        _check_request_name(key)
        if value is not None:
            value.encode("latin-1")
        self._headers[key.lower()] = value

    def __delitem__(self, key: str) -> None:
        del self._headers[key.lower()]

    def __iter__(self) -> Iterator[str]:
        return iter(self._headers)

    def __len__(self) -> int:
        return len(self._headers)

    def __repr__(self) -> str:
        return repr(self._headers)


class Response(responses.Response):
    """A response that answers the request directly.

    Works like Starlette's ``Response``. Header names starting with
    ``x-middleware-`` are reserved.
    """

    # Whether Vercel should send this response to the client as is.
    is_terminating: ClassVar[bool] = True

    def init_headers(self, headers: Mapping[str, str] | None = None) -> None:
        for name in headers or {}:
            _check_response_name(name)
        super().init_headers(headers)

    @property
    def headers(self) -> MutableHeaders:
        """The headers added to the response. Names are case-insensitive."""
        return _ResponseHeaders(raw=self.raw_headers)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Send the response.

        *scope*, *receive* and *send* are the standard ASGI arguments.
        """
        if not self.is_terminating:
            await super().__call__(scope, receive, send)
            return

        # Vercel only treats a response as final on its own when it has no
        # Location header, so mark it explicitly to keep its body.
        async def send_final(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = [*message["headers"], (b"x-middleware-refresh", b"1")]
                message = {**message, "headers": headers}
            await send(message)

        await super().__call__(scope, receive, send_final)


class HTMLResponse(Response, responses.HTMLResponse):
    """A response with an HTML body. Works like Starlette's ``HTMLResponse``."""


class PlainTextResponse(Response, responses.PlainTextResponse):
    """A response with a plain text body. Works like Starlette's ``PlainTextResponse``."""


class JSONResponse(Response, responses.JSONResponse):
    """A response with a JSON body. Works like Starlette's ``JSONResponse``."""


class RedirectResponse(Response, responses.RedirectResponse):
    """A response that redirects the client. Works like Starlette's ``RedirectResponse``."""

    # Vercel follows redirects through their Location header.
    is_terminating = False

    @property
    def status_code(self) -> int:
        """The redirect status code. It must be 301, 302, 303, 307 or 308."""
        return self._status_code

    @status_code.setter
    def status_code(self, value: int) -> None:
        if value not in _REDIRECT_STATUSES:
            raise ValueError(f"invalid redirect status {value}: must be 301, 302, 303, 307 or 308")
        self._status_code = value


class ContinueResponse(Response):
    """A response that lets the request continue to its destination."""

    is_terminating = False

    def __init__(
        self,
        *,
        headers: Mapping[str, str] | None = None,
        request_headers: Mapping[str, str | None] | None = None,
    ) -> None:
        """Let the request continue to its destination.

        *headers* are added to the response.

        *request_headers* are set on the forwarded request. A ``None`` value
        removes that header. Omit to forward the headers unchanged.
        """
        super().__init__(headers=headers)
        self.request_headers = request_headers or {}

    @property
    def request_headers(self) -> MutableMapping[str, str | None]:
        """The headers set on the forwarded request.

        Names are case-insensitive. A ``None`` value removes that header.
        """
        return self._request_headers

    @request_headers.setter
    def request_headers(self, value: Mapping[str, str | None]) -> None:
        self._request_headers = _RequestHeaders(value)

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
        if self._request_headers:
            control["x-middleware-override-headers-diff"] = ",".join(self._request_headers)
            for name, value in self._request_headers.items():
                if value is not None:
                    control[f"x-middleware-request-{name}"] = value
        if cookies := [value for name, value in self.raw_headers if name == b"set-cookie"]:
            control.raw.append((b"x-middleware-set-cookie", b",".join(cookies)))
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

        *request_headers* are set on the forwarded request. A ``None`` value
        removes that header. Omit to forward the headers unchanged.
        """
        super().__init__(headers=headers, request_headers=request_headers)
        self.destination = destination

    def _destination_header(self) -> tuple[str, str]:
        return "x-middleware-rewrite", urllib.parse.quote(self.destination, safe=_DESTINATION_SAFE)
