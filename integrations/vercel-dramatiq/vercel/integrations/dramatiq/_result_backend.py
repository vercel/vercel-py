from __future__ import annotations

from typing import Any, cast

import base64
from collections.abc import Callable

from dramatiq.results.backend import Missing, ResultBackend
from vercel.cache import RuntimeCache
from vercel.queue import sanitize_name

DEFAULT_RESULT_NAMESPACE = "dramatiq-results"
_WRAPPER_MARKER = "__vercel_dramatiq_result__"
_WRAPPER_VERSION = 1


class VercelRuntimeCacheBackend(ResultBackend):
    """Dramatiq result backend backed by Vercel Runtime Cache."""

    def __init__(
        self,
        *,
        namespace: str = DEFAULT_RESULT_NAMESPACE,
        runtime_cache_namespace: str | None = None,
        namespace_separator: str | None = None,
        key_hash_function: Callable[[str], str] | None = None,
        name: str | None = None,
        tags: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(namespace=str(sanitize_name(namespace)), **kwargs)
        self.runtime_cache_namespace = runtime_cache_namespace or self.namespace
        self.name = name
        self.tags = tags
        self._runtime_cache = RuntimeCache(
            key_hash_function=key_hash_function,
            namespace=self.runtime_cache_namespace,
            namespace_separator=namespace_separator,
            strict=True,
        )

    def _get(self, message_key: str) -> object:
        value = self._runtime_cache.get(message_key)
        if value is None:
            return Missing
        return self.encoder.decode(_unwrap_value(value))

    def _store(self, message_key: str, result: object, ttl: int) -> None:
        self._runtime_cache.set(
            message_key,
            _wrap_value(self.encoder.encode(cast("dict[str, Any]", result))),
            self._set_options(message_key, ttl),
        )

    def _set_options(self, message_key: str, ttl: int) -> dict[str, object]:
        options: dict[str, object] = {"name": self.name or message_key}
        ttl_seconds = _ttl_milliseconds_to_seconds(ttl)
        if ttl_seconds is not None:
            options["ttl"] = ttl_seconds
        if self.tags:
            options["tags"] = self.tags
        return options


def _ttl_milliseconds_to_seconds(ttl: int) -> int | None:
    if ttl <= 0:
        return None
    return max(1, int(ttl / 1000))


def _wrap_value(value: bytes) -> dict[str, object]:
    return {
        _WRAPPER_MARKER: _WRAPPER_VERSION,
        "encoding": "base64",
        "payload": base64.b64encode(value).decode("ascii"),
    }


def _unwrap_value(value: object) -> bytes:
    if not isinstance(value, dict):
        raise TypeError("Runtime Cache result payload is not a wrapper object")
    if value.get(_WRAPPER_MARKER) != _WRAPPER_VERSION:
        raise ValueError("Runtime Cache result payload has an unknown wrapper version")
    if value.get("encoding") != "base64":
        raise ValueError("Runtime Cache result payload has an unknown encoding")
    payload = value.get("payload")
    if not isinstance(payload, str):
        raise TypeError("Runtime Cache result payload is not text")
    return base64.b64decode(payload.encode("ascii"), validate=True)


__all__ = [
    "DEFAULT_RESULT_NAMESPACE",
    "VercelRuntimeCacheBackend",
]
