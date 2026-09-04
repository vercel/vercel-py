"""Response factories for proxy route handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, final

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

__all__ = ["Response"]


@final
class Response:
    """Proxy response produced by a route handler.

    Use the static factory methods rather than constructing directly:
    :meth:`next`, :meth:`rewrite`, :meth:`redirect`, :meth:`block`,
    or the low-level :meth:`respond`.
    """

    @staticmethod
    def next(*, headers: dict[str, str] | None = None) -> Response:
        """Pass the request to the next handler in the chain.

        *headers* are injected into the downstream request via
        ``x-middleware-override-headers``.
        """
        raise NotImplementedError

    @staticmethod
    def rewrite(destination: str) -> Response:
        """Serve *destination* transparently (URL seen by client is unchanged)."""
        raise NotImplementedError

    @staticmethod
    def redirect(destination: str, *, status: int = 307) -> Response:
        """Redirect the client to *destination*.

        *status* should be a 3xx code; defaults to 307 (Temporary Redirect).
        """
        raise NotImplementedError

    @staticmethod
    def block(*, status: int = 403, body: bytes = b"") -> Response:
        """Reject the request with *status* (default 403) and optional *body*."""
        raise NotImplementedError

    @staticmethod
    def respond(
        *,
        status: int,
        body: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Low-level factory — full control over status, body, and headers."""
        raise NotImplementedError

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        """Serialise this response as ASGI send events."""
        raise NotImplementedError
