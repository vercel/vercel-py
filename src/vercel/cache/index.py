from __future__ import annotations

import os
from typing import Callable

from ._context import get_context
from .in_memory_cache import InMemoryCache
from .types import RuntimeCache


def _default_key_hash_function(key: str) -> str:
    # Mirror TS defaultKeyHashFunction: djb2 xor variant, 32-bit unsigned hex
    h = 5381
    for ch in key:
        h = ((h * 33) ^ ord(ch)) & 0xFFFFFFFF
    return format(h, "x")


_DEFAULT_NAMESPACE_SEPARATOR = "$"

_in_memory_cache_instance: InMemoryCache | None = None


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

        return _Wrapper()

    return wrap_with_key_transformation(
        resolve_cache,
        create_key_transformer(key_hash_function, namespace, namespace_separator),
    )


def _get_cache_implementation(debug: bool = False) -> RuntimeCache:
    global _in_memory_cache_instance

    if _in_memory_cache_instance is None:
        _in_memory_cache_instance = InMemoryCache()

    if debug:
        print("Using InMemoryCache for runtime cache (Vercel uses HTTP headers and Data Cache)")

    # Always use in-memory cache since Vercel doesn't provide a runtime cache endpoint
    # Vercel uses HTTP caching headers and Data Cache instead
    return _in_memory_cache_instance
