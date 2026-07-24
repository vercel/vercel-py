from __future__ import annotations

import inspect
import re
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from functools import partial
from typing import Any, TypeAlias, cast

from starlette.concurrency import run_in_threadpool
from starlette.datastructures import URL
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Match, Route as StarletteRoute
from starlette.types import Receive, Scope, Send

from ._responses import redirect, rewrite
from .matchers import Condition

ProxyResult: TypeAlias = Response | None
Endpoint: TypeAlias = Callable[[Request], ProxyResult | Awaitable[ProxyResult]]

_PARAMETER_PATTERN = re.compile(r"{([a-zA-Z_][a-zA-Z0-9_]*)(?::[^}]+)?}")
_DESTINATION_PARAMETER_PATTERN = re.compile(r"{([a-zA-Z_][a-zA-Z0-9_]*)}")


class _MatcherEndpoint:
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        raise RuntimeError("route matcher was invoked as an ASGI application")


def _is_async_callable(function: Callable[..., Any]) -> bool:
    candidate: Any = function
    while isinstance(candidate, partial):
        candidate = candidate.func
    if inspect.iscoroutinefunction(candidate):
        return True
    return callable(candidate) and inspect.iscoroutinefunction(candidate.__call__)


def _render_destination(destination: str, path_params: Mapping[str, Any]) -> str:
    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in path_params:
            raise ValueError(f"rewrite destination references unknown route parameter {name!r}")
        return str(path_params[name])

    return _DESTINATION_PARAMETER_PATTERN.sub(replace, destination)


class Route:
    """A path route that can select a Python routing middleware handler."""

    def __init__(
        self,
        path: str,
        endpoint: Endpoint,
        *,
        methods: Collection[str] | None = None,
        has: Sequence[Condition] = (),
        missing: Sequence[Condition] = (),
    ) -> None:
        if not callable(endpoint):
            raise TypeError("route endpoint must be callable")
        self.path = path
        self.endpoint = endpoint
        self.has = tuple(has)
        self.missing = tuple(missing)
        if not all(isinstance(condition, Condition) for condition in (*self.has, *self.missing)):
            raise TypeError("route conditions must be vercel.proxy.matchers.Condition instances")

        path_parameters = frozenset(_PARAMETER_PATTERN.findall(path))
        condition_parameters: set[str] = set()
        for condition in self.has:
            duplicate = condition_parameters.intersection(condition.parameter_names)
            if duplicate:
                names = ", ".join(sorted(duplicate))
                raise ValueError(f"duplicate condition route parameter: {names}")
            condition_parameters.update(condition.parameter_names)

        path_duplicate = path_parameters.intersection(condition_parameters)
        if path_duplicate:
            names = ", ".join(sorted(path_duplicate))
            raise ValueError(f"host and path route parameters overlap: {names}")
        self.parameter_names = path_parameters.union(condition_parameters)

        self._matcher = StarletteRoute(
            path,
            _MatcherEndpoint(),
            methods=methods,
        )

    @classmethod
    def rewrite(
        cls,
        path: str,
        destination: str | URL,
        *,
        methods: Collection[str] | None = None,
        has: Sequence[Condition] = (),
        missing: Sequence[Condition] = (),
        headers: Mapping[str, str] | None = None,
        request_headers: Mapping[str, str] | None = None,
    ) -> Route:
        """Create a route that rewrites to a destination template."""
        destination_template = str(destination)

        async def endpoint(request: Request) -> Response:
            return rewrite(
                _render_destination(destination_template, request.path_params),
                headers=headers,
                request_headers=request_headers,
            )

        route = cls(path, endpoint, methods=methods, has=has, missing=missing)
        unknown = set(_DESTINATION_PARAMETER_PATTERN.findall(destination_template)).difference(
            route.parameter_names
        )
        if unknown:
            names = ", ".join(sorted(unknown))
            raise ValueError(f"rewrite destination references unknown route parameter: {names}")
        return route

    @classmethod
    def redirect(
        cls,
        path: str,
        destination: str | URL,
        *,
        status_code: int = 307,
        methods: Collection[str] | None = None,
        has: Sequence[Condition] = (),
        missing: Sequence[Condition] = (),
        headers: Mapping[str, str] | None = None,
    ) -> Route:
        """Create a route that redirects to a destination template."""
        destination_template = str(destination)

        async def endpoint(request: Request) -> Response:
            return redirect(
                _render_destination(destination_template, request.path_params),
                status_code=status_code,
                headers=headers,
            )

        route = cls(path, endpoint, methods=methods, has=has, missing=missing)
        unknown = set(_DESTINATION_PARAMETER_PATTERN.findall(destination_template)).difference(
            route.parameter_names
        )
        if unknown:
            names = ", ".join(sorted(unknown))
            raise ValueError(f"redirect destination references unknown route parameter: {names}")
        return route

    def match(self, request: Request) -> Mapping[str, Any] | None:
        result, child_scope = self._matcher.matches(request.scope)
        if result is not Match.FULL:
            return None

        path_params = dict(child_scope.get("path_params", {}))
        for condition in self.has:
            matches, captured = condition.match(request)
            if not matches:
                return None
            for name, value in captured.items():
                if name in path_params:
                    raise ValueError(f"duplicate route parameter {name!r}")
                path_params[name] = value

        for condition in self.missing:
            matches, _ = condition.match(request)
            if matches:
                return None

        return {**child_scope, "path_params": path_params}

    async def handle(self, request: Request) -> ProxyResult:
        if _is_async_callable(self.endpoint):
            result = self.endpoint(request)
        else:
            result = await run_in_threadpool(self.endpoint, request)
        if inspect.isawaitable(result):
            return cast(ProxyResult, await result)
        return result
