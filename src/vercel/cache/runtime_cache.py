import json
import os
from typing import Callable, Sequence

from .context import get_context
from .cache_in_memory import InMemoryCache, AsyncInMemoryCache
from .cache_build import BuildCache, AsyncBuildCache
from .types import Cache, AsyncCache
from .utils import create_key_transformer


_in_memory_cache_instance: InMemoryCache | AsyncInMemoryCache | None = None
_build_cache_instance: BuildCache | AsyncBuildCache | None = None
_warned_cache_unavailable = False


class RuntimeCache(Cache):
    def __init__(self):
        self.cache = {}

    def get(self, key: str):
        return self.cache.get(key)

    def set(self, key: str, value: object):
        self.cache[key] = value

    def delete(self, key: str):
        self.cache.pop(key, None)

    def expire_tag(self, tag: str | Sequence[str]):
        self.cache.pop(tag, None)


class AsyncRuntimeCache(AsyncCache):
    def __init__(self):
        self.cache = {}

    async def get(self, key: str):
        return self.cache.get(key)

    async def set(self, key: str, value: object):
        self.cache[key] = value

    async def delete(self, key: str):
        self.cache.pop(key, None)

    async def expire_tag(self, tag: str | Sequence[str]):
        self.cache.pop(tag, None)


def get_cache(
    *,
    key_hash_function: Callable[[str], str] | None = None,
    namespace: str | None = None,
    namespace_separator: str | None = None,
) -> RuntimeCache:
    def wrap_with_key_transformation(
        resolver: Callable[[], RuntimeCache], make_key: Callable[[str], str]
    ) -> RuntimeCache:
        class _Wrapper:
            def get(self, key: str):
                return resolver().get(make_key(key))

            def set(self, key: str, value: object, options: dict | None = None):
                return resolver().set(make_key(key), value, options)

            def delete(self, key: str):
                return resolver().delete(make_key(key))

            def expire_tag(self, tag):
                return resolver().expire_tag(tag)

        return _Wrapper()

    return wrap_with_key_transformation(
        resolve_cache(sync=True),
        create_key_transformer(key_hash_function, namespace, namespace_separator),
    )


def get_async_cache(
    *,
    key_hash_function: Callable[[str], str] | None = None,
    namespace: str | None = None,
    namespace_separator: str | None = None,
) -> AsyncRuntimeCache:
    def wrap_with_key_transformation(
        resolver: Callable[[], AsyncRuntimeCache], make_key: Callable[[str], str]
    ) -> AsyncRuntimeCache:
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
        resolve_cache(sync=False),
        create_key_transformer(key_hash_function, namespace, namespace_separator),
    )


def _get_cache_implementation(
    debug: bool = False, sync: bool = True
) -> RuntimeCache | AsyncRuntimeCache:
    global _in_memory_cache_instance, _build_cache_instance, _warned_cache_unavailable

    if _in_memory_cache_instance is None:
        _in_memory_cache_instance = InMemoryCache() if sync else AsyncInMemoryCache()

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
            print("Runtime Cache unavailable in this environment. Falling back to in-memory cache.")
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


def resolve_cache(sync: bool = True) -> RuntimeCache | AsyncRuntimeCache:
    ctx = get_context()
    cache = getattr(ctx, "cache", None)
    if cache is not None:
        return cache
    return _get_cache_implementation(os.getenv("SUSPENSE_CACHE_DEBUG") == "true", sync)
