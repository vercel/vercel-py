from __future__ import annotations

from collections.abc import Mapping
from typing import Literal
from urllib.parse import quote

from starlette.datastructures import URL, MutableHeaders
from starlette.responses import RedirectResponse, Response
from starlette.types import Receive, Scope, Send

_CONTINUE_HEADER = b"x-middleware-next"
_REWRITE_HEADER = b"x-middleware-rewrite"
_OVERRIDE_HEADERS = b"x-middleware-override-headers"
_REQUEST_HEADER_PREFIX = b"x-middleware-request-"
_INTERNAL_HEADER_PREFIX = b"x-middleware-"

RoutingAction = Literal["continue", "rewrite"]


class RoutingResponse(Response):
    """A synthetic response that tells Vercel how to continue routing.

    Routing responses never contain the eventual CDN or origin response. Their
    ordinary headers are merged into that eventual response by Vercel.
    """

    media_type = None

    def __init__(
        self,
        *,
        headers: Mapping[str, str] | None = None,
        request_headers: Mapping[str, str] | None = None,
        _destination: str | URL | None = None,
    ) -> None:
        explicit_content_length = headers is not None and any(
            name.lower() == "content-length" for name in headers
        )
        super().__init__(content=b"", status_code=200, headers=headers)
        if not explicit_content_length:
            self.raw_headers[:] = [
                (name, value) for name, value in self.raw_headers if name != b"content-length"
            ]

        self._destination = (
            quote(str(_destination), safe=":/%#?=@[]!$&'()*+,;")
            if _destination is not None
            else None
        )
        self._request_headers: MutableHeaders | None = None
        self.request_headers = request_headers

    @property
    def action(self) -> RoutingAction:
        return "rewrite" if self._destination is not None else "continue"

    @property
    def destination(self) -> str | None:
        return self._destination

    @property
    def request_headers(self) -> MutableHeaders | None:
        """The complete request header set to forward after this proxy."""
        return self._request_headers

    @request_headers.setter
    def request_headers(self, value: Mapping[str, str] | None) -> None:
        self._request_headers = MutableHeaders(headers=value) if value is not None else None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        headers = list(self.raw_headers)
        if any(name.startswith(_INTERNAL_HEADER_PREFIX) for name, _ in headers):
            raise ValueError("x-middleware-* response headers are reserved by vercel.proxy")

        if self._destination is None:
            headers.append((_CONTINUE_HEADER, b"1"))
        else:
            headers.append((_REWRITE_HEADER, self._destination.encode("latin-1")))

        if self._request_headers is not None:
            request_header_names = list(dict.fromkeys(self._request_headers.keys()))
            headers.append((_OVERRIDE_HEADERS, ",".join(request_header_names).encode("latin-1")))
            for name in request_header_names:
                headers.append(
                    (
                        _REQUEST_HEADER_PREFIX + name.encode("latin-1"),
                        self._request_headers[name].encode("latin-1"),
                    )
                )

        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": headers,
            }
        )
        await send({"type": "http.response.body", "body": b""})
        if self.background is not None:
            await self.background()


def continue_routing(
    *,
    headers: Mapping[str, str] | None = None,
    request_headers: Mapping[str, str] | None = None,
) -> RoutingResponse:
    """Continue through Vercel routing without changing the destination."""
    return RoutingResponse(headers=headers, request_headers=request_headers)


def rewrite(
    destination: str | URL,
    *,
    headers: Mapping[str, str] | None = None,
    request_headers: Mapping[str, str] | None = None,
) -> RoutingResponse:
    """Continue Vercel routing using a different URL."""
    return RoutingResponse(
        headers=headers,
        request_headers=request_headers,
        _destination=destination,
    )


def redirect(
    destination: str | URL,
    *,
    status_code: int = 307,
    headers: Mapping[str, str] | None = None,
) -> RedirectResponse:
    """End routing with an HTTP redirect response."""
    if not 300 <= status_code < 400:
        raise ValueError("redirect status_code must be between 300 and 399")
    return RedirectResponse(str(destination), status_code=status_code, headers=headers)
