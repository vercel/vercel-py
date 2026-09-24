"""Proxy request with body access blocked."""

from typing import NoReturn

from starlette.requests import Request as StarletteRequest
from starlette.types import Scope

__all__ = ["Request"]

_BODY_UNAVAILABLE = (
    "request body is not available in proxy handlers: Vercel forwards it to the destination unread"
)


class Request(StarletteRequest):
    """The incoming request passed to a proxy route handler.

    Use it to decide how to route the request: inspect ``method``, ``url``,
    ``headers``, ``query_params``, ``cookies``, ``path_params`` and
    ``client``. These work exactly as in Starlette.

    The request body is not available. A proxy handler runs before the request
    reaches its destination, and the body is forwarded there unread, so
    ``body()``, ``json()``, ``form()``, ``stream()``, ``receive`` and
    ``is_disconnected()`` raise :exc:`RuntimeError`. Route on headers, the
    path or the query string instead, and read the body in the destination.
    """

    def __init__(self, scope: Scope) -> None:
        """Create a request from an ASGI connection scope.

        You do not normally construct this yourself; the proxy creates one for
        each incoming request and passes it to the matching handler.

        *scope* is the ASGI ``http`` scope of the incoming request.
        """
        super().__init__(scope)

    @property
    def receive(self) -> NoReturn:
        """Not available in proxy handlers; always raises :exc:`RuntimeError`."""
        raise RuntimeError(_BODY_UNAVAILABLE)

    def stream(self) -> NoReturn:
        """Not available in proxy handlers; always raises :exc:`RuntimeError`."""
        raise RuntimeError(_BODY_UNAVAILABLE)

    async def body(self) -> NoReturn:
        """Not available in proxy handlers; always raises :exc:`RuntimeError`."""
        raise RuntimeError(_BODY_UNAVAILABLE)

    async def json(self) -> NoReturn:
        """Not available in proxy handlers; always raises :exc:`RuntimeError`."""
        raise RuntimeError(_BODY_UNAVAILABLE)

    def form(
        self,
        *,
        max_files: int | float = 1000,
        max_fields: int | float = 1000,
        max_part_size: int = 1024 * 1024,
    ) -> NoReturn:
        """Not available in proxy handlers; always raises :exc:`RuntimeError`.

        *max_files*, *max_fields* and *max_part_size* are accepted for
        compatibility with Starlette and have no effect.
        """
        raise RuntimeError(_BODY_UNAVAILABLE)

    async def is_disconnected(self) -> NoReturn:
        """Not available in proxy handlers; always raises :exc:`RuntimeError`.

        Checking for a disconnect requires reading the request body stream.
        """
        raise RuntimeError(_BODY_UNAVAILABLE)
