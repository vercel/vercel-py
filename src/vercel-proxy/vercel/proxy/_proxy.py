"""Proxy ASGI router."""

from __future__ import annotations

import asyncio
import dataclasses
import functools
import urllib.parse
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeAlias

from ._params import Params
from ._request import Request
from ._response import Kind, Response
from ._routing import compile_path

__all__ = ["Proxy"]

_Handler: TypeAlias = Callable[[Request], Response] | Callable[[Request], Awaitable[Response]]
_Fallback: TypeAlias = (  # noqa: E501
    Response | Callable[[Request], Response] | Callable[[Request], Awaitable[Response]]
)


@dataclass(frozen=True, slots=True)
class _Route:
    pattern: Any  # CompiledPattern
    methods: frozenset[str] | None  # None = all methods
    handler: _Handler


class Proxy:
    """ASGI-callable proxy router.

    Register route handlers with :meth:`route` and an optional fallback
    with :meth:`fallback` (or the *fallback* constructor argument).
    Unmatched requests fall through to ``Response.next()`` by default.
    """

    def __init__(
        self,
        *,
        strict: bool = False,
        fallback: _Fallback | None = None,
    ) -> None:
        self._strict = strict
        self._fallback: _Fallback = fallback if fallback is not None else Response.next()
        self._routes: list[_Route] = []

    def route(
        self,
        path: str,
        *,
        methods: list[str] | None = None,
    ) -> Callable[[_Handler], _Handler]:
        """Register a route handler.

        *path* supports ``{param}``, ``{param:path}``, and ``{param:str}`` patterns.
        *methods* is a list of HTTP methods to match; omit to match all methods.
        """

        def decorator(func: _Handler) -> _Handler:
            pattern = compile_path(path, strict=self._strict)
            method_set = frozenset(m.upper() for m in methods) if methods else None
            self._routes.append(_Route(pattern=pattern, methods=method_set, handler=func))
            return func

        return decorator

    def fallback(self, func: _Handler) -> _Handler:
        """Decorator to set a dynamic fallback handler for unmatched requests.

        Overrides any *fallback* passed to the constructor.
        """
        self._fallback = func
        return func

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        scope_type = scope.get("type")
        if scope_type == "lifespan":
            await _handle_lifespan(receive, send)
            return
        if scope_type != "http":
            raise RuntimeError(
                f"vercel.proxy received an unexpected ASGI scope type {scope_type!r}. "
                "Only 'http' and 'lifespan' scopes are handled."
            )

        request = Request._from_asgi_scope(scope)

        for route in self._routes:
            params = route.pattern.match(request.path)
            if params is None:
                continue
            if route.methods is not None and request.method not in route.methods:
                continue

            final_request = dataclasses.replace(
                request,
                path_params=Params(tuple(params.items())),
            )
            response = await _call(route.handler, final_request)
            await _emit(response, send)
            return

        fallback = self._fallback
        response = fallback if isinstance(fallback, Response) else await _call(fallback, request)
        await _emit(response, send)


def _encode_destination(destination: str) -> bytes:
    """Percent-encode a destination URL for use in x-middleware-* headers.

    Reserves all valid URI characters unencoded; ``%`` is safe to prevent
    double-encoding already-encoded destinations.
    """
    return urllib.parse.quote(destination, safe="/:@!$&'()*+,;=?#%[]").encode("ascii")


async def _handle_lifespan(
    receive: Callable[[], Awaitable[dict[str, Any]]],
    send: Callable[[dict[str, Any]], Awaitable[None]],
) -> None:
    """Consume ASGI lifespan messages and reply with completion events.

    The proxy has no startup or shutdown work; this keeps the ASGI contract
    intact without leaving the lifespan messages unread.
    """
    while True:
        message = await receive()
        if message["type"] == "lifespan.startup":
            await send({"type": "lifespan.startup.complete"})
        elif message["type"] == "lifespan.shutdown":
            await send({"type": "lifespan.shutdown.complete"})
            return


def _is_async_callable(obj: Any) -> bool:
    """Return True if *obj* is (or wraps) a coroutine function.

    Unwraps :class:`functools.partial` chains and checks the ``__call__``
    method of callable objects, covering cases that
    :func:`inspect.iscoroutinefunction` misses on Python < 3.12.
    """
    while isinstance(obj, functools.partial):
        obj = obj.func
    if asyncio.iscoroutinefunction(obj):
        return True
    call = getattr(obj, "__call__", None)  # noqa: B004
    return call is not None and asyncio.iscoroutinefunction(call)


async def _call(handler: Any, request: Request) -> Response:
    if _is_async_callable(handler):
        return await handler(request)
    # Run sync handlers in a thread so they don't block the event loop.
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, handler, request)


async def _emit(
    response: Response,
    send: Callable[[dict[str, Any]], Awaitable[None]],
) -> None:
    if response.kind == Kind.CONTINUING:
        await _emit_continuing(response, send)
    else:
        await _emit_terminating(response, send)


async def _emit_continuing(
    response: Response,
    send: Callable[[dict[str, Any]], Awaitable[None]],
) -> None:
    headers: list[tuple[bytes, bytes]] = []

    if response.destination is None:
        headers.append((b"x-middleware-next", b"1"))
    else:
        headers.append((b"x-middleware-rewrite", _encode_destination(response.destination)))

    if response.headers is not None:
        diff = ",".join(k.lower() for k in response.headers)
        headers.append((b"x-middleware-override-headers-diff", diff.encode("latin-1")))
        for k, v in response.headers.items():
            if v is not None:
                key = f"x-middleware-request-{k.lower()}".encode("latin-1")
                headers.append((key, v.encode("latin-1")))

    await send({"type": "http.response.start", "status": 200, "headers": headers})
    await send({"type": "http.response.body", "body": b""})


async def _emit_terminating(
    response: Response,
    send: Callable[[dict[str, Any]], Awaitable[None]],
) -> None:
    headers: list[tuple[bytes, bytes]] = []

    if response.destination is not None:
        headers.append((b"x-middleware-redirect", _encode_destination(response.destination)))

    if response.headers:
        for k, v in response.headers.items():
            if v is not None:
                headers.append((k.encode("latin-1"), v.encode("latin-1")))

    await send({"type": "http.response.start", "status": response.status, "headers": headers})
    await send({"type": "http.response.body", "body": response.body})
