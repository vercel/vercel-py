from __future__ import annotations

from typing import Any

import base64
from collections.abc import Callable, Iterable
from datetime import timedelta

import httpx

from celery.backends.base import BackendGetMetaError, BackendStoreError, KeyValueStoreBackend
from celery.utils.time import maybe_timedelta
from vercel.cache import RuntimeCache, RuntimeCacheError
from vercel.queue import sanitize_name

DEFAULT_RESULT_BACKEND_ALIAS = "vercel-runtime-cache"
DEFAULT_RESULT_NAMESPACE = "celery-results"
_WRAPPER_MARKER = "__vercel_celery_result__"
_WRAPPER_VERSION = 1


class VercelRuntimeCacheBackend(KeyValueStoreBackend):
    """Celery result backend backed by Vercel Runtime Cache.

    ``install_vercel_celery_integration`` configures Celery to use
    ``vercel-runtime-cache://`` by default when no result backend is already
    configured. Backend-specific options are read from
    ``result_backend_transport_options``.

    Celery result backends are not required to retain results indefinitely;
    results may expire according to Celery's result expiration settings or the
    backend's own retention behavior. This backend is therefore an expiring,
    cache-backed result backend, not durable result storage. Missing cache
    entries are treated by Celery as pending results. Stored results use
    Celery's ``result_expires`` value as their Runtime Cache TTL by default;
    set ``result_backend_transport_options["ttl"]`` to override that retention
    period.
    """

    supports_autoexpire = True
    supports_native_join = False
    implements_incr = False

    def __init__(self, *args: Any, url: str | None = None, **kwargs: Any) -> None:
        super().__init__(*args, expires_type=int, **kwargs)
        self.url = url
        self.options = dict(self.app.conf.result_backend_transport_options or {})
        if (ttl := self._option_ttl()) is not None:
            self.expires = ttl
        self._runtime_cache = RuntimeCache(
            key_hash_function=self._option_key_hash_function(),
            namespace=self._option_namespace(),
            namespace_separator=self._option_namespace_separator(),
            strict=True,
        )

    def get(self, key: bytes | str) -> bytes | str | None:
        normalized_key = self._normalize_key(key)
        try:
            value = self._runtime_cache.get(normalized_key)
        except Exception as exc:
            raise BackendGetMetaError("failed to get result from Runtime Cache") from exc
        if value is None:
            return None
        try:
            return self._unwrap_value(value)
        except Exception as exc:
            raise BackendGetMetaError("malformed Runtime Cache result payload") from exc

    def mget(self, keys: Iterable[bytes | str]) -> list[bytes | str | None]:
        return [self.get(key) for key in keys]

    def set(self, key: bytes | str, value: object) -> None:
        original_key = self._normalize_key(key)
        try:
            wrapped_value = self._wrap_value(value)
            self._runtime_cache.set(original_key, wrapped_value, self._set_options(original_key))
        except BackendStoreError:
            raise
        except Exception as exc:
            raise BackendStoreError("failed to store result in Runtime Cache") from exc

    def delete(self, key: bytes | str) -> None:
        try:
            self._runtime_cache.delete(self._normalize_key(key))
        except Exception as exc:
            raise BackendStoreError("failed to delete result from Runtime Cache") from exc

    def exception_safe_to_retry(self, exc: BaseException) -> bool:
        current: BaseException | None = exc
        while current is not None:
            if isinstance(
                current,
                (RuntimeCacheError, httpx.TimeoutException, httpx.TransportError),
            ):
                return True
            current = current.__cause__
        return False

    def as_uri(self, *args: Any, **kwargs: Any) -> str:
        del args, kwargs
        return f"{DEFAULT_RESULT_BACKEND_ALIAS}://"

    def _option_ttl(self) -> int | None:
        ttl = self.options.get("ttl")
        if isinstance(ttl, bool):
            return None
        if isinstance(ttl, (int, float, timedelta)):
            seconds = int(maybe_timedelta(ttl).total_seconds())
            if seconds > 0:
                return seconds
        return None

    def _option_namespace(self) -> str | None:
        namespace = self.options.get("namespace")
        if isinstance(namespace, str):
            return namespace
        main = getattr(self.app, "main", None)
        if main:
            return str(
                sanitize_name(
                    f"{DEFAULT_RESULT_NAMESPACE}-{main}",
                )
            )
        return DEFAULT_RESULT_NAMESPACE

    def _option_namespace_separator(self) -> str | None:
        separator = self.options.get("namespace_separator")
        return separator if isinstance(separator, str) else None

    def _option_key_hash_function(self) -> Callable[[str], str] | None:
        key_hash_function = self.options.get("key_hash_function")
        return key_hash_function if callable(key_hash_function) else None

    def _option_tags(self) -> list[str] | None:
        tags = self.options.get("tags")
        if not isinstance(tags, (list, tuple, set)):
            return None
        return [tag for tag in tags if isinstance(tag, str)]

    def _option_name(self, key: str) -> str:
        name = self.options.get("name", key)
        return name if isinstance(name, str) else key

    def _set_options(self, key: str) -> dict[str, object]:
        options: dict[str, object] = {"name": self._option_name(key)}
        if self.expires:
            options["ttl"] = self.expires
        if tags := self._option_tags():
            options["tags"] = tags
        return options

    @staticmethod
    def _normalize_key(key: bytes | str) -> str:
        if isinstance(key, bytes):
            return key.decode("utf-8")
        return key

    @staticmethod
    def _wrap_value(value: object) -> dict[str, object]:
        if isinstance(value, str):
            return {
                _WRAPPER_MARKER: _WRAPPER_VERSION,
                "encoding": "utf-8",
                "payload": value,
            }
        if isinstance(value, bytes):
            # Runtime Cache stores JSON-compatible values, while binary Celery
            # serializers like pickle produce bytes.
            return {
                _WRAPPER_MARKER: _WRAPPER_VERSION,
                "encoding": "base64",
                "payload": base64.b64encode(value).decode("ascii"),
            }
        raise BackendStoreError(f"unsupported Celery result payload type: {type(value).__name__}")

    @staticmethod
    def _unwrap_value(value: object) -> bytes | str:
        if not isinstance(value, dict):
            raise TypeError("Runtime Cache result payload is not a wrapper object")
        if value.get(_WRAPPER_MARKER) != _WRAPPER_VERSION:
            raise ValueError("Runtime Cache result payload has an unknown wrapper version")
        encoding = value.get("encoding")
        payload = value.get("payload")
        if not isinstance(payload, str):
            raise TypeError("Runtime Cache result payload is not text")
        if encoding == "utf-8":
            return payload
        if encoding == "base64":
            return base64.b64decode(payload.encode("ascii"), validate=True)
        raise ValueError("Runtime Cache result payload has an unknown encoding")


__all__ = [
    "DEFAULT_RESULT_BACKEND_ALIAS",
    "DEFAULT_RESULT_NAMESPACE",
    "VercelRuntimeCacheBackend",
]
