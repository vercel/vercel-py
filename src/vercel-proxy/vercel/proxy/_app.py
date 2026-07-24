from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeAlias, TypeVar

from starlette.requests import Request
from starlette.responses import Response
from starlette.types import Receive, Scope, Send

from ._responses import continue_routing
from ._routing import ProxyResult, Route

CallNext: TypeAlias = Callable[[Request], Awaitable[Response]]
HTTPMiddleware: TypeAlias = Callable[
    [Request, CallNext],
    ProxyResult | Awaitable[ProxyResult],
]
MiddlewareFunction = TypeVar("MiddlewareFunction", bound=HTTPMiddleware)


def _normalize_result(result: Any, source: str) -> Response:
    if result is None:
        return continue_routing()
    if not isinstance(result, Response):
        raise TypeError(f"{source} must return a Starlette Response or None")
    return result


class Proxy:
    """An HTTP routing middleware application for Vercel."""

    __vercel_proxy__ = True

    def __init__(self, *, routes: Sequence[Route] = ()) -> None:
        if not all(isinstance(route, Route) for route in routes):
            raise TypeError("Proxy routes must be vercel.proxy.Route instances")
        self.routes = tuple(routes)
        self._middleware: list[HTTPMiddleware] = []
        self._frozen = False

    def middleware(
        self, middleware_type: str
    ) -> Callable[[MiddlewareFunction], MiddlewareFunction]:
        """Register HTTP middleware. The last registered function runs first."""
        if middleware_type != "http":
            raise ValueError("vercel.proxy only supports HTTP middleware")

        def decorator(function: MiddlewareFunction) -> MiddlewareFunction:
            if self._frozen:
                raise RuntimeError("proxy middleware cannot be registered after the proxy starts")
            self._middleware.append(function)
            return function

        return decorator

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        self._frozen = True
        scope_type = scope["type"]
        if scope_type == "lifespan":
            await self._handle_lifespan(receive, send)
            return
        if scope_type == "websocket":
            await send({"type": "websocket.close", "code": 1008})
            return
        if scope_type != "http":
            raise RuntimeError(f"unsupported ASGI scope type: {scope_type!r}")

        request = Request(scope, receive=receive)
        middleware = tuple(reversed(self._middleware))

        async def dispatch(index: int, current_request: Request) -> Response:
            if index == len(middleware):
                return await self._dispatch_route(current_request)

            async def call_next(next_request: Request) -> Response:
                if not isinstance(next_request, Request):
                    raise TypeError("call_next expects a Starlette Request")
                return await dispatch(index + 1, next_request)

            result = middleware[index](current_request, call_next)
            if inspect.isawaitable(result):
                result = await result
            return _normalize_result(result, "proxy middleware")

        response = await dispatch(0, request)
        await response(scope, receive, send)

    async def _dispatch_route(self, request: Request) -> Response:
        for route in self.routes:
            child_scope = route.match(request)
            if child_scope is None:
                continue
            request.scope.update(child_scope)
            result = await route.handle(request)
            return _normalize_result(result, "proxy route")
        return continue_routing()

    async def _handle_lifespan(self, receive: Receive, send: Send) -> None:
        while True:
            message = await receive()
            message_type = message["type"]
            if message_type == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message_type == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return
            else:
                raise RuntimeError(f"unsupported lifespan message: {message_type!r}")
