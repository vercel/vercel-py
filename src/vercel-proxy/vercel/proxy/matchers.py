from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from starlette.datastructures import MutableHeaders
from starlette.requests import Request
from starlette.routing import Host, Match
from starlette.types import Receive, Scope, Send

__all__ = ["Condition", "cookie", "header", "host", "query"]

_PARAMETER_PATTERN = re.compile(r"{([a-zA-Z_][a-zA-Z0-9_]*)(?::[^}]+)?}")


async def _unreachable_app(scope: Scope, receive: Receive, send: Send) -> None:
    raise RuntimeError("route matcher was invoked as an ASGI application")


class Condition(ABC):
    """A request condition used by :class:`vercel.proxy.Route`."""

    parameter_names: frozenset[str] = frozenset()

    @abstractmethod
    def match(self, request: Request) -> tuple[bool, Mapping[str, Any]]:
        """Return whether the request matches and any captured parameters."""


@dataclass(frozen=True)
class _ValueCondition(Condition):
    source: str
    name: str
    value: str | None

    def match(self, request: Request) -> tuple[bool, Mapping[str, Any]]:
        if self.source == "header":
            values = request.headers.getlist(self.name)
        elif self.source == "query":
            values = request.query_params.getlist(self.name)
        else:
            cookies = request.cookies
            values = [cookies[self.name]] if self.name in cookies else []

        matches = bool(values) if self.value is None else self.value in values
        return matches, {}


@dataclass(frozen=True)
class _HostCondition(Condition):
    pattern: str
    parameter_names: frozenset[str] = field(init=False)
    _matcher: Host = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        normalized_pattern = self.pattern.lower()
        if not normalized_pattern:
            raise ValueError("host pattern must not be empty")
        object.__setattr__(self, "pattern", normalized_pattern)
        object.__setattr__(
            self,
            "parameter_names",
            frozenset(_PARAMETER_PATTERN.findall(normalized_pattern)),
        )
        object.__setattr__(self, "_matcher", Host(normalized_pattern, _unreachable_app))

    def match(self, request: Request) -> tuple[bool, Mapping[str, Any]]:
        scope = dict(request.scope)
        scope["headers"] = list(request.scope.get("headers", []))
        normalized_headers = MutableHeaders(scope=scope)
        normalized_headers["host"] = request.url.hostname or ""
        result, child_scope = self._matcher.matches(scope)
        return result is Match.FULL, child_scope.get("path_params", {})


def _value_condition(source: str, name: str, value: str | None) -> Condition:
    if not name:
        raise ValueError(f"{source} name must not be empty")
    return _ValueCondition(source, name, value)


def header(name: str, value: str | None = None) -> Condition:
    """Match a request header by presence or exact value."""
    return _value_condition("header", name, value)


def cookie(name: str, value: str | None = None) -> Condition:
    """Match a cookie by presence or exact value."""
    return _value_condition("cookie", name, value)


def query(name: str, value: str | None = None) -> Condition:
    """Match a query parameter by presence or exact value."""
    return _value_condition("query", name, value)


def host(pattern: str) -> Condition:
    """Match a hostname, optionally capturing ``{parameters}``."""
    return _HostCondition(pattern)
