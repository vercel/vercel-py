from __future__ import annotations

import json
import os
from typing import Callable

from .._context import get_context
from .in_memory_cache import InMemoryCache
from .build_client import BuildCache
from .types import RuntimeCache


def _default_key_hash_function(key: str) -> str:
    # Mirror TS defaultKeyHashFunction: djb2 xor variant, 32-bit unsigned hex
    h = 5381
    for ch in key:
        h = ((h * 33) ^ ord(ch)) & 0xFFFFFFFF
    return format(h, "x")


_DEFAULT_NAMESPACE_SEPARATOR = "$"

_in_memory_cache_instance: InMemoryCache | None = None
_build_cache_instance: BuildCache | None = None
_warned_cache_unavailable = False


def get_cache(
    *,
    key_hash_function: Callable[[str], str] | None = None,
    namespace: str | None = None,
    namespace_separator: str | None = None,
) -> RuntimeCache:
    def resolve_cache() -> RuntimeCache:
        ctx = get_context()
        cache = getattr(ctx, "cache", None)
        if cache is not None:
            return cache  # type: ignore[return-value]
        return _get_cache_implementation(os.getenv("SUSPENSE_CACHE_DEBUG") == "true")

    def create_key_transformer(
        key_fn: Callable[[str], str] | None,
        ns: str | None,
        sep: str | None,
    ) -> Callable[[str], str]:
        key_fn = key_fn or _default_key_hash_function
        sep = sep or _DEFAULT_NAMESPACE_SEPARATOR

        def make(key: str) -> str:
            if not ns:
                return key_fn(key)
            return f"{ns}{sep}{key_fn(key)}"

        return make

    def wrap_with_key_transformation(
        resolver: Callable[[], RuntimeCache], make_key: Callable[[str], str]
    ) -> RuntimeCache:
        class _Wrapper:
            async def get(self, key: str):
                return await resolver().get(make_key(key))

            async def set(self, key: str, value: object, options: dict | None = None):
                return await resolver().set(make_key(key), value, options)

            async def delete(self, key: str):
                return await resolver().delete(make_key(key))

            async def expire_tag(self, tag):
                return await resolver().expire_tag(tag)

        return _Wrapper()  # type: ignore[return-value]

    return wrap_with_key_transformation(
        resolve_cache,
        create_key_transformer(key_hash_function, namespace, namespace_separator),
    )


def _get_cache_implementation(debug: bool = False) -> RuntimeCache:
    global _in_memory_cache_instance, _build_cache_instance, _warned_cache_unavailable

    if _in_memory_cache_instance is None:
        _in_memory_cache_instance = InMemoryCache()

    if os.getenv("RUNTIME_CACHE_DISABLE_BUILD_CACHE") == "true":
        if debug:
            print("Using InMemoryCache as build cache is disabled")
        return _in_memory_cache_instance

    endpoint = os.getenv("RUNTIME_CACHE_ENDPOINT")
    headers = os.getenv("RUNTIME_CACHE_HEADERS")

    if debug:
        print(
            "Runtime cache environment variables:",
            {"RUNTIME_CACHE_ENDPOINT": endpoint, "RUNTIME_CACHE_HEADERS": headers},
        )

    if not endpoint or not headers:
        if not _warned_cache_unavailable:
            print(
                "Runtime Cache unavailable in this environment. Falling back to in-memory cache."
            )
            _warned_cache_unavailable = True
        return _in_memory_cache_instance

    if _build_cache_instance is None:
        try:
            parsed_headers = json.loads(headers)
            if not isinstance(parsed_headers, dict):
                raise ValueError("RUNTIME_CACHE_HEADERS must be a JSON object")
        except Exception as e:
            print("Failed to parse RUNTIME_CACHE_HEADERS:", e)
            return _in_memory_cache_instance
        _build_cache_instance = BuildCache(
            endpoint=endpoint,
            headers=parsed_headers,
            on_error=lambda e: print(e),
        )

    return _build_cache_instance
