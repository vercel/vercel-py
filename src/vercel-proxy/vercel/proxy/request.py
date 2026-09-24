"""Proxy request with body access blocked."""

from starlette.requests import Request as StarletteRequest
from starlette.types import Message, Scope

__all__ = ["Request"]

_BODY_UNAVAILABLE = (
    "request body is not available in proxy handlers: Vercel forwards it to the destination unread"
)


async def _receive() -> Message:
    raise RuntimeError(_BODY_UNAVAILABLE)


class Request(StarletteRequest):
    """The incoming request passed to a proxy handler.

    Use ``method``, ``url``, ``headers``, ``query_params``, ``cookies``,
    ``path_params`` and ``client`` to decide how to route it. They work as in
    Starlette.

    The request body is not available because it is forwarded to the
    destination unread. Methods that read it raise ``RuntimeError``. Route on
    the path, headers or query string instead.
    """

    def __init__(self, scope: Scope) -> None:
        """Create a request from an ASGI scope.

        The proxy creates requests for you, so you rarely need this.

        *scope* is the ASGI HTTP scope of the request.
        """
        super().__init__(scope, _receive)
