"""Runtime compatibility helpers for HTTPX client families."""

from __future__ import annotations

import sys
from typing import Any, Protocol, TypeVar, cast

import httpx2

_ExceptionT = TypeVar("_ExceptionT", bound=BaseException)


class _LegacyHttpxModule(Protocol):
    Client: type[Any]
    AsyncClient: type[Any]


def _loaded_legacy_httpx() -> _LegacyHttpxModule | None:
    """Return legacy HTTPX only when the application already imported it."""
    module = sys.modules.get("httpx")
    return cast(_LegacyHttpxModule, module) if module is not None else None


def is_sync_http_client(value: object) -> bool:
    if isinstance(value, httpx2.Client):
        return True
    module = _loaded_legacy_httpx()
    return module is not None and isinstance(value, module.Client)


def is_async_http_client(value: object) -> bool:
    if isinstance(value, httpx2.AsyncClient):
        return True
    module = _loaded_legacy_httpx()
    return module is not None and isinstance(value, module.AsyncClient)


def _exception_types(primary: type[_ExceptionT], legacy_name: str) -> tuple[type[_ExceptionT], ...]:
    module = _loaded_legacy_httpx()
    if module is None:
        return (primary,)
    legacy = getattr(module, legacy_name, None)
    if not isinstance(legacy, type) or not issubclass(legacy, BaseException):
        return (primary,)
    return (primary, cast(type[_ExceptionT], legacy))


def transport_errors() -> tuple[type[httpx2.TransportError], ...]:
    return _exception_types(httpx2.TransportError, "TransportError")


def http_errors() -> tuple[type[httpx2.HTTPError], ...]:
    return _exception_types(httpx2.HTTPError, "HTTPError")


def http_status_errors() -> tuple[type[httpx2.HTTPStatusError], ...]:
    return _exception_types(httpx2.HTTPStatusError, "HTTPStatusError")


def response_not_read_errors() -> tuple[type[httpx2.ResponseNotRead], ...]:
    return _exception_types(httpx2.ResponseNotRead, "ResponseNotRead")
