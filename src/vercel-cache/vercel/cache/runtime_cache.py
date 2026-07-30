import json
import os
from collections.abc import Callable, Sequence
from typing import Literal, cast, overload

from .cache_build import AsyncBuildCache, BuildCache, RuntimeCacheError
from .cache_in_memory import AsyncInMemoryCache, InMemoryCache
from .context import get_context
from .types import AsyncCache, Cache
from .utils import create_key_transformer

_in_memory_cache_instance: InMemoryCache | None = None
_async_in_memory_cache_instance: AsyncInMemoryCache | None = None
_build_cache_instance: BuildCache | None = None
_async_build_cache_instance: AsyncBuildCache | None = None
_cached_cache_instance: Cache | None = None
_cached_async_cache_instance: AsyncCache | None = None
_warned_cache_unavailable = False


class RuntimeCache(Cache):
    def __init__(
        self,
        *,
        key_hash_function: Callable[[str], str] | None = None,
        namespace: str | None = None,
        namespace_separator: str | None = None,
        strict: bool = False,
    ) -> None:
        # Transform keys to match get_cache behavior
        self._make_key = create_key_transformer(key_hash_function, namespace, namespace_separator)
        self._strict = strict

    def get(self, key: str):
        return resolve_cache(sync=True, strict=self._strict).get(self._make_key(key))

    def set(self, key: str, value: object, options: dict | None = None):
        cache = resolve_cache(sync=True, strict=self._strict)
        return cache.set(self._make_key(key), value, options)

    def delete(self, key: str):
        return resolve_cache(sync=True, strict=self._strict).delete(self._make_key(key))

    def expire_tag(self, tag: str | Sequence[str]):
        # Tag invalidation is not namespaced/hashed by design
        return resolve_cache(sync=True, strict=self._strict).expire_tag(tag)

    def __contains__(self, key: str) -> bool:
        # Delegate membership to the underlying cache implementation with transformed key
        return self._make_key(key) in resolve_cache(sync=True, strict=self._strict)

    def __getitem__(self, key: str):
        if key in self:
            return self.get(key)
        raise KeyError(key)


class AsyncRuntimeCache(AsyncCache):
    def __init__(
        self,
        *,
        key_hash_function: Callable[[str], str] | None = None,
        namespace: str | None = None,
        namespace_separator: str | None = None,
    ) -> None:
        self._make_key = create_key_transformer(key_hash_function, namespace, namespace_separator)

    async def get(self, key: str):
        return await resolve_cache(sync=False).get(self._make_key(key))

    async def set(self, key: str, value: object, options: dict | None = None):
        return await resolve_cache(sync=False).set(self._make_key(key), value, options)

    async def delete(self, key: str):
        return await resolve_cache(sync=False).delete(self._make_key(key))

    async def expire_tag(self, tag: str | Sequence[str]):
        return await resolve_cache(sync=False).expire_tag(tag)

    async def contains(self, key: str) -> bool:
        return await resolve_cache(sync=False).contains(self._make_key(key))


@overload
def get_cache(
    *,
    key_hash_function: Callable[[str], str] | None = ...,
    namespace: str | None = ...,
    namespace_separator: str | None = ...,
    sync: Literal[True] = ...,
) -> RuntimeCache: ...


@overload
def get_cache(
    *,
    key_hash_function: Callable[[str], str] | None = ...,
    namespace: str | None = ...,
    namespace_separator: str | None = ...,
    sync: Literal[False],
) -> AsyncRuntimeCache: ...


def get_cache(
    *,
    key_hash_function: Callable[[str], str] | None = None,
    namespace: str | None = None,
    namespace_separator: str | None = None,
    sync: bool = True,
) -> RuntimeCache | AsyncRuntimeCache:
    if sync:
        return RuntimeCache(
            key_hash_function=key_hash_function,
            namespace=namespace,
            namespace_separator=namespace_separator,
        )
    return AsyncRuntimeCache(
        key_hash_function=key_hash_function,
        namespace=namespace,
        namespace_separator=namespace_separator,
    )


def _get_cache_implementation(
    debug: bool = False,
    sync: bool = True,
    strict: bool = False,
) -> Cache | AsyncCache:
    global _in_memory_cache_instance, _async_in_memory_cache_instance
    global _build_cache_instance, _async_build_cache_instance, _warned_cache_unavailable

    # Prepare a single shared InMemoryCache backing store and an async wrapper over it
    if _in_memory_cache_instance is None:
        _in_memory_cache_instance = InMemoryCache()
    if _async_in_memory_cache_instance is None:
        _async_in_memory_cache_instance = AsyncInMemoryCache(delegate=_in_memory_cache_instance)

    # Disable build cache via env
    if os.getenv("RUNTIME_CACHE_DISABLE_BUILD_CACHE") == "true":
        if debug:
            print("Using InMemoryCache as build cache is disabled")
        if strict:
            raise RuntimeCacheError("Runtime Cache unavailable: build cache is disabled")
        return _in_memory_cache_instance if sync else _async_in_memory_cache_instance

    endpoint = os.getenv("RUNTIME_CACHE_ENDPOINT")
    headers = os.getenv("RUNTIME_CACHE_HEADERS")

    if debug:
        print(
            "Runtime cache environment variables:",
            {"RUNTIME_CACHE_ENDPOINT": endpoint, "RUNTIME_CACHE_HEADERS": headers},
        )

    if not endpoint or not headers:
        cached = _cached_cache_instance if sync else _cached_async_cache_instance
        if cached is not None:
            return cached
        if strict:
            raise RuntimeCacheError(
                "Runtime Cache unavailable: no request cache context or runtime cache environment"
            )
        if not _warned_cache_unavailable:
            print("Runtime Cache unavailable in this environment. Falling back to in-memory cache.")
            _warned_cache_unavailable = True
        return _in_memory_cache_instance if sync else _async_in_memory_cache_instance  # type: ignore[return-value]

    # Build cache clients
    try:
        parsed_headers = json.loads(headers)
        if not isinstance(parsed_headers, dict):
            raise ValueError("RUNTIME_CACHE_HEADERS must be a JSON object")
    except Exception as e:
        print("Failed to parse RUNTIME_CACHE_HEADERS:", e)
        if strict:
            raise RuntimeCacheError(
                "Runtime Cache unavailable: invalid RUNTIME_CACHE_HEADERS"
            ) from e
        return _in_memory_cache_instance if sync else _async_in_memory_cache_instance  # type: ignore[return-value]

    if sync:
        if _build_cache_instance is None:
            _build_cache_instance = BuildCache(
                endpoint=endpoint,
                headers=parsed_headers,
                on_error=lambda e: print(e),
            )
        if not strict:
            remember_cache(_build_cache_instance, sync=True)
            return _build_cache_instance
        cache = BuildCache(endpoint=endpoint, headers=parsed_headers, strict=True)
        remember_cache(cache, sync=True)
        return cache
    else:
        if _async_build_cache_instance is None:
            _async_build_cache_instance = AsyncBuildCache(
                endpoint=endpoint,
                headers=parsed_headers,
                on_error=lambda e: print(e),
            )
        remember_cache(_async_build_cache_instance, sync=False)
        return _async_build_cache_instance


def remember_cache(cache: Cache | AsyncCache, *, sync: bool) -> None:
    """Remember a live Runtime Cache client for background worker threads."""
    global _cached_cache_instance, _cached_async_cache_instance
    if sync:
        _cached_cache_instance = cast(Cache, cache)
    else:
        _cached_async_cache_instance = cast(AsyncCache, cache)


def prime_runtime_cache() -> None:
    """Prime process-wide Runtime Cache fallback from the current request context."""
    try:
        resolve_cache(sync=True, strict=True)
    except RuntimeCacheError:
        return


@overload
def resolve_cache(sync: Literal[True] = ..., strict: bool = ...) -> Cache: ...


@overload
def resolve_cache(sync: Literal[False], strict: bool = ...) -> AsyncCache: ...


def resolve_cache(sync: bool = True, strict: bool = False) -> Cache | AsyncCache:
    ctx = get_context()
    if sync:
        cache = getattr(ctx, "cache", None)
        if cache is not None:
            resolved = cast(Cache, cache)
            remember_cache(resolved, sync=True)
            return resolved
        return _get_cache_implementation(os.getenv("SUSPENSE_CACHE_DEBUG") == "true", True, strict)

    async_cache = getattr(ctx, "async_cache", None)
    if async_cache is not None:
        resolved_async = cast(AsyncCache, async_cache)
        remember_cache(resolved_async, sync=False)
        return resolved_async
    return _get_cache_implementation(os.getenv("SUSPENSE_CACHE_DEBUG") == "true", False, strict)
