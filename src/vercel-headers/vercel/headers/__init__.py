from __future__ import annotations

import sys
import threading
import urllib.parse
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import Context, ContextVar
from dataclasses import dataclass
from functools import wraps
from types import MappingProxyType
from typing import Any, ParamSpec, Protocol, TypedDict, TypeVar

__all__ = [
    "ip_address",
    "geolocation",
    "Geo",
    "HeadersContext",
    "set_headers",
    "get_headers",
    "get_headers_context",
    "context_aware_thread",
    "headers_from_asgi_scope",
    "headers_from_wsgi_environ",
]


_cv_headers: ContextVar[Mapping[str, str] | None] = ContextVar("vercel_headers", default=None)
P = ParamSpec("P")
R = TypeVar("R")

# Header constants (same as TS names)
CITY_HEADER_NAME = "x-vercel-ip-city"
COUNTRY_HEADER_NAME = "x-vercel-ip-country"
IP_HEADER_NAME = "x-real-ip"
LATITUDE_HEADER_NAME = "x-vercel-ip-latitude"
LONGITUDE_HEADER_NAME = "x-vercel-ip-longitude"
REGION_HEADER_NAME = "x-vercel-ip-country-region"
POSTAL_CODE_HEADER_NAME = "x-vercel-ip-postal-code"
REQUEST_ID_HEADER_NAME = "x-vercel-id"

EMOJI_FLAG_UNICODE_STARTING_POSITION = 127397


def set_headers(headers: Mapping[str, str] | None) -> None:
    _cv_headers.set(headers)


def get_headers() -> Mapping[str, str] | None:
    return _cv_headers.get()


@dataclass(frozen=True)
class HeadersContext:
    """Immutable snapshot of the current Vercel request headers."""

    headers: Mapping[str, str] | None

    @contextmanager
    def use(self) -> Iterator[None]:
        token = _cv_headers.set(self.headers)
        try:
            yield
        finally:
            _cv_headers.reset(token)

    def run(self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        with self.use():
            return func(*args, **kwargs)


def get_headers_context() -> HeadersContext:
    headers = _cv_headers.get()
    if headers is None:
        return HeadersContext(None)
    return HeadersContext(MappingProxyType(dict(headers)))


if sys.version_info >= (3, 14):

    def context_aware_thread(
        group: None = None,
        target: Callable[..., object] | None = None,
        name: str | None = None,
        args: Iterable[Any] = (),
        kwargs: Mapping[str, Any] | None = None,
        *,
        daemon: bool | None = None,
        context: Context | None = None,
    ) -> threading.Thread:
        headers_context = get_headers_context()
        if target is not None:
            target = _wrap_thread_target(headers_context, target)
        return threading.Thread(
            group=group,
            target=target,
            name=name,
            args=tuple(args),
            kwargs=None if kwargs is None else dict(kwargs),
            daemon=daemon,
            context=context,
        )

else:

    def context_aware_thread(
        group: None = None,
        target: Callable[..., object] | None = None,
        name: str | None = None,
        args: Iterable[Any] = (),
        kwargs: Mapping[str, Any] | None = None,
        *,
        daemon: bool | None = None,
    ) -> threading.Thread:
        headers_context = get_headers_context()
        if target is not None:
            target = _wrap_thread_target(headers_context, target)
        return threading.Thread(
            group=group,
            target=target,
            name=name,
            args=tuple(args),
            kwargs=None if kwargs is None else dict(kwargs),
            daemon=daemon,
        )


def _wrap_thread_target(context: HeadersContext, target: Callable[P, R]) -> Callable[P, R]:
    @wraps(target)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        return context.run(target, *args, **kwargs)

    return wrapped


class _HeadersLike(Protocol):
    def get(self, name: str) -> str | None: ...


class _RequestLike(Protocol):
    headers: _HeadersLike


class Geo(TypedDict, total=False):
    city: str | None
    country: str | None
    flag: str | None
    region: str | None
    countryRegion: str | None
    latitude: str | None
    longitude: str | None
    postalCode: str | None


def _get_header(headers: _HeadersLike, key: str) -> str | None:
    return headers.get(key)


def _get_header_decode(req: _RequestLike, key: str) -> str | None:
    raw = _get_header(req.headers, key)
    return urllib.parse.unquote(raw) if raw is not None else None


def _get_flag(country_code: str | None) -> str | None:
    if not country_code or len(country_code) != 2 or not country_code.isalpha():
        return None
    return "".join(chr(EMOJI_FLAG_UNICODE_STARTING_POSITION + ord(c)) for c in country_code.upper())


def headers_from_asgi_scope(scope: Mapping[str, Any]) -> dict[str, str]:
    """Return request headers decoded from an ASGI scope."""
    return {
        name.decode("latin-1"): value.decode("latin-1") for name, value in scope.get("headers", [])
    }


def headers_from_wsgi_environ(environ: Mapping[str, Any]) -> dict[str, str]:
    """Return request headers decoded from a WSGI environ mapping."""
    headers: dict[str, str] = {}
    if "CONTENT_TYPE" in environ:
        headers["Content-Type"] = str(environ["CONTENT_TYPE"])
    if "CONTENT_LENGTH" in environ:
        headers["Content-Length"] = str(environ["CONTENT_LENGTH"])
    for name, value in environ.items():
        if not name.startswith("HTTP_"):
            continue
        header_name = name[5:].replace("_", "-").title()
        headers[header_name] = str(value)
    return headers


def ip_address(input: _RequestLike | _HeadersLike) -> str | None:
    headers = input.headers if hasattr(input, "headers") else input
    return _get_header(headers, IP_HEADER_NAME)


def _region_from_request_id(request_id: str | None) -> str | None:
    if request_id is None:
        return "dev1"
    return request_id.split(":")[0]


def geolocation(request: _RequestLike) -> Geo:
    headers = request.headers
    return {
        "city": _get_header_decode(request, CITY_HEADER_NAME),
        "country": _get_header(headers, COUNTRY_HEADER_NAME),
        "flag": _get_flag(_get_header(headers, COUNTRY_HEADER_NAME)),
        "countryRegion": _get_header(headers, REGION_HEADER_NAME),
        "region": _region_from_request_id(_get_header(headers, REQUEST_ID_HEADER_NAME)),
        "latitude": _get_header(headers, LATITUDE_HEADER_NAME),
        "longitude": _get_header(headers, LONGITUDE_HEADER_NAME),
        "postalCode": _get_header(headers, POSTAL_CODE_HEADER_NAME),
    }
