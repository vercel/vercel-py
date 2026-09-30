"""Proxy router."""

import functools
import inspect
from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass
from typing import Any, TypeAlias, TypeVar

from starlette.concurrency import run_in_threadpool
from starlette.routing import Host, Match, Route, Router
from starlette.types import Receive, Scope, Send

from vercel.proxy.request import Request
from vercel.proxy.response import ContinueResponse, Response

__all__ = ["Handler", "Proxy"]

Handler: TypeAlias = Callable[[Request], Response | Awaitable[Response]]
"""A sync or async function that takes a request and returns a response."""

_H = TypeVar("_H", bound=Handler)


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

    A handler returns a ``ContinueResponse`` or ``RewriteResponse`` to let
    the request continue, or another ``Response`` to answer it directly. If a handler raises an
    exception, the request fails with a 500 error.
    """

    def __init__(self, *, fallback: Response | Handler | None = None, strict: bool = False) -> None:
        """Create a proxy with no routes.

        *fallback* handles requests that match no route. Pass a ``Response``
        to always return it, or a handler to decide per request. Defaults to
        ``ContinueResponse()``, which lets the request continue unchanged.

        *strict* requires paths to match routes exactly. Defaults to
        ``False``, which makes a trailing slash optional.
        """
        if fallback is None:
            fallback = ContinueResponse()
        self._routes: list[_Route] = []
        self._strict = strict
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
        ``request.path_params``. A trailing slash is optional unless the
        proxy is strict.

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
            await Router().lifespan(scope, receive, send)
            return
        if scope["type"] != "http":
            raise RuntimeError(f'unsupported ASGI scope type "{scope["type"]}": expected "http"')

        response = await self._dispatch(scope)
        await response(scope, receive, send)

    async def _dispatch(self, scope: Scope) -> Response:
        matched = self._match(scope)
        # An exact match wins, so the other trailing slash form is tried
        # only when no route matches the path as given.
        if matched is None and not self._strict:
            path = scope["path"]
            toggled = path[:-1] if path.endswith("/") else f"{path}/"
            matched = self._match({**scope, "path": toggled})
        handler, path_params = matched or (self._fallback, {})
        # The handler sees the path as the client sent it.
        request = Request({**scope, "app": self, "path_params": path_params})
        return await _call(handler, request)

    def _match(self, scope: Scope) -> tuple[Handler, dict[str, Any]] | None:
        for route in self._routes:
            matched_scope = scope
            if route.host is not None:
                match, child_scope = route.host.matches(matched_scope)
                if match != Match.FULL:
                    continue
                matched_scope = {**matched_scope, "path_params": child_scope["path_params"]}
            match, child_scope = route.path.matches(matched_scope)
            if match == Match.FULL:
                return route.handler, child_scope["path_params"]
        return None


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
            "invalid return value from proxy handler: expected vercel.proxy.Response, "
            f"got {type(result).__module__}.{type(result).__qualname__}"
        )
    return result
