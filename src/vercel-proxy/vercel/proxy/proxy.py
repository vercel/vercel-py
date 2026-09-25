"""Proxy router."""

import functools
import inspect
import urllib.parse
from collections.abc import Awaitable, Callable, Collection, Mapping
from dataclasses import dataclass
from typing import TypeAlias, TypeVar, cast

from starlette.concurrency import run_in_threadpool
from starlette.responses import Response as StarletteResponse
from starlette.routing import Host, Match, Route
from starlette.types import Receive, Scope, Send

from vercel.proxy.request import Request
from vercel.proxy.response import Kind, Response

__all__ = ["Handler", "Proxy"]

Handler: TypeAlias = Callable[[Request], Response | Awaitable[Response]]
"""A sync or async function that takes a request and returns a response."""

_H = TypeVar("_H", bound=Handler)

# Characters left unescaped when a destination URL is written into a
# x-middleware-* header. Covers every valid URI character, plus "%" so
# already-encoded destinations are not double-encoded.
_DESTINATION_SAFE = "/:@!$&'()*+,;=?#%[]~"


@dataclass(frozen=True)
class _Route:
    path: Route
    host: Host | None
    handler: Handler


class Proxy:
    """Routes incoming requests to handlers.

    Register handlers with ``route``. Each request goes to the first matching
    route in the order they were registered. Requests that match no route go
    to the fallback.

    A handler returns a ``Response`` that lets the request continue,
    rewrites it, redirects it or answers it directly. If a handler raises an
    exception, the request fails with a 500 error.

    Export a ``Proxy`` as your proxy entrypoint.
    """

    def __init__(self, *, fallback: Response | Handler | None = None) -> None:
        """Create a proxy with no routes.

        *fallback* handles requests that match no route. Pass a ``Response``
        to always return it, or a handler to decide per request. Defaults to
        ``Response.next()``, which lets the request continue unchanged.
        """
        if fallback is None:
            fallback = Response.next()
        self._routes: list[_Route] = []
        self._fallback = _respond_with(fallback) if isinstance(fallback, Response) else fallback

    def route(
        self,
        path: str,
        *,
        methods: Collection[str] | None = None,
        host: str | None = None,
    ) -> Callable[[_H], _H]:
        """Register the decorated function as the handler for matching requests.

        *path* is the URL path to match and must start with ``/``. Use
        ``{name}`` to capture a path segment, or ``{name:type}`` to also
        convert it. The type can be ``str``, ``int``, ``float``, ``uuid`` or
        ``path``, which also matches ``/``. Captured values are in
        ``request.path_params``.

        *methods* limits the route to these HTTP methods, such as
        ``["GET", "POST"]``. ``GET`` routes also match ``HEAD``. Omit to match
        every method.

        *host* limits the route to a hostname, such as
        ``{tenant}.example.com``. Captures work as in *path* and the port is
        ignored. ``{name}`` can match dots, so this example also matches
        ``a.b.example.com``. Omit to match every host.

        The function can be sync or async and is returned unchanged. Sync
        functions run in a worker thread.
        """
        if not path.startswith("/"):
            raise ValueError(f'invalid route path "{path}": must start with "/"')
        if host is not None and host.startswith("/"):
            raise ValueError(f'invalid route host "{host}": must be a hostname, not a path')
        if isinstance(methods, str):
            raise TypeError(f'invalid route methods "{methods}": must be a collection of strings')
        if methods is not None and not methods:
            raise ValueError("invalid route methods: must not be empty; omit to match every method")

        # Starlette's Route and Host are used only for matching, so their
        # endpoint is never called. Passing the proxy (an ASGI app, not a
        # function) keeps methods=None meaning "every method"; Starlette
        # restricts function endpoints to GET by default.
        path_route = Route(path, endpoint=self, methods=methods)
        host_route = Host(host, app=self) if host is not None else None
        if host_route is not None:
            overlap = path_route.param_convertors.keys() & host_route.param_convertors.keys()
            if overlap:
                names = ", ".join(f'"{name}"' for name in sorted(overlap))
                raise ValueError(f"invalid route: {names} captured in both host and path")

        def decorator(handler: _H) -> _H:
            self._routes.append(_Route(path_route, host_route, handler))
            return handler

        return decorator

    def fallback(self, handler: _H) -> _H:
        """Use the decorated function to handle requests that match no route.

        *handler* can be sync or async and replaces any fallback given to the
        constructor. It is returned unchanged.
        """
        self._fallback = handler
        return handler

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handle an ASGI connection.

        *scope*, *receive* and *send* are the standard ASGI arguments.
        """
        if scope["type"] == "lifespan":
            await _handle_lifespan(receive, send)
            return
        if scope["type"] != "http":
            raise RuntimeError(f'unsupported ASGI scope type "{scope["type"]}": expected "http"')

        response = await self._dispatch(scope)
        await _send_response(response, scope, receive, send)

    async def _dispatch(self, scope: Scope) -> Response:
        for route in self._routes:
            matched_scope = scope
            if route.host is not None:
                match, child_scope = route.host.matches(matched_scope)
                if match != Match.FULL:
                    continue
                matched_scope = {**matched_scope, "path_params": child_scope["path_params"]}
            match, child_scope = route.path.matches(matched_scope)
            if match != Match.FULL:
                continue
            request = Request({**scope, "path_params": child_scope["path_params"]})
            return await _call(route.handler, request)

        return await _call(self._fallback, Request({**scope, "path_params": {}}))


def _respond_with(response: Response) -> Handler:
    async def handler(request: Request) -> Response:
        return response

    return handler


def _is_async_callable(obj: object) -> bool:
    while isinstance(obj, functools.partial):
        obj = obj.func
    if inspect.iscoroutinefunction(obj):
        return True
    return callable(obj) and inspect.iscoroutinefunction(obj.__call__)


async def _call(handler: Handler, request: Request) -> Response:
    if _is_async_callable(handler):
        result = handler(request)
    else:
        result = await run_in_threadpool(handler, request)
    if inspect.isawaitable(result):
        result = await result
    if not isinstance(result, Response):
        raise TypeError(
            f"invalid return value from proxy handler: expected Response, got "
            f"{type(result).__name__}"
        )
    return result


async def _handle_lifespan(receive: Receive, send: Send) -> None:
    # The proxy has no startup or shutdown work, so acknowledge both events.
    while True:
        message = await receive()
        if message["type"] == "lifespan.startup":
            await send({"type": "lifespan.startup.complete"})
        elif message["type"] == "lifespan.shutdown":
            await send({"type": "lifespan.shutdown.complete"})
            return


def _encode_destination(destination: str) -> str:
    return urllib.parse.quote(destination, safe=_DESTINATION_SAFE)


async def _send_response(response: Response, scope: Scope, receive: Receive, send: Send) -> None:
    headers: dict[str, str] = {}
    if response.kind == Kind.CONTINUING:
        if response.destination is None:
            headers["x-middleware-next"] = "1"
        else:
            headers["x-middleware-rewrite"] = _encode_destination(response.destination)
        if response.headers:
            names = ",".join(name.lower() for name in response.headers)
            headers["x-middleware-override-headers-diff"] = names
            for name, value in response.headers.items():
                if value is not None:
                    headers[f"x-middleware-request-{name.lower()}"] = value
    else:
        if response.destination is not None:
            headers["x-middleware-redirect"] = _encode_destination(response.destination)
        # Terminating factories only accept str header values.
        headers.update(cast("Mapping[str, str]", response.headers))

    asgi_response = StarletteResponse(response.body, status_code=response.status, headers=headers)
    await asgi_response(scope, receive, send)
