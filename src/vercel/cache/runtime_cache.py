import json
import os
from typing import Callable, Sequence, overload, Literal, cast

from .context import get_context
from .cache_in_memory import InMemoryCache, AsyncInMemoryCache
from .cache_build import BuildCache, AsyncBuildCache
from .types import Cache, AsyncCache
from .utils import create_key_transformer


_in_memory_cache_instance: InMemoryCache | None = None
_async_in_memory_cache_instance: AsyncInMemoryCache | None = None
_build_cache_instance: BuildCache | None = None
_async_build_cache_instance: AsyncBuildCache | None = None
_warned_cache_unavailable = False


class RuntimeCache(Cache):
    def __init__(
        self,
        *,
        key_hash_function: Callable[[str], str] | None = None,
        namespace: str | None = None,
        namespace_separator: str | None = None,
    ) -> None:
        # Transform keys to match get_cache behavior
        self._make_key = create_key_transformer(key_hash_function, namespace, namespace_separator)

    def get(self, key: str):
        return resolve_cache(sync=True).get(self._make_key(key))

    def set(self, key: str, value: object, options: dict | None = None):
        return resolve_cache(sync=True).set(self._make_key(key), value, options)

    def delete(self, key: str):
        return resolve_cache(sync=True).delete(self._make_key(key))

    def expire_tag(self, tag: str | Sequence[str]):
        # Tag invalidation is not namespaced/hashed by design
        return resolve_cache(sync=True).expire_tag(tag)

    def __contains__(self, key: str) -> bool:
        # Delegate membership to the underlying cache implementation with transformed key
        return self._make_key(key) in resolve_cache(sync=True)

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


def get_cache(
    *,
    key_hash_function: Callable[[str], str] | None = None,
    namespace: str | None = None,
    namespace_separator: str | None = None,
) -> RuntimeCache:
    return RuntimeCache(
        key_hash_function=key_hash_function,
        namespace=namespace,
        namespace_separator=namespace_separator,
    )


def _get_cache_implementation(debug: bool = False, sync: bool = True) -> Cache | AsyncCache:
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
        return _in_memory_cache_instance if sync else _async_in_memory_cache_instance

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
        return _in_memory_cache_instance if sync else _async_in_memory_cache_instance  # type: ignore[return-value]

    # Build cache clients
    try:
        parsed_headers = json.loads(headers)
        if not isinstance(parsed_headers, dict):
            raise ValueError("RUNTIME_CACHE_HEADERS must be a JSON object")
    except Exception as e:
        print("Failed to parse RUNTIME_CACHE_HEADERS:", e)
        return _in_memory_cache_instance if sync else _async_in_memory_cache_instance  # type: ignore[return-value]

    if sync:
        if _build_cache_instance is None:
            _build_cache_instance = BuildCache(
                endpoint=endpoint,
                headers=parsed_headers,
                on_error=lambda e: print(e),
            )
        return _build_cache_instance
    else:
        if _async_build_cache_instance is None:
            _async_build_cache_instance = AsyncBuildCache(
                endpoint=endpoint,
                headers=parsed_headers,
                on_error=lambda e: print(e),
            )
        return _async_build_cache_instance


@overload
def resolve_cache(sync: Literal[True] = ...) -> Cache: ...


@overload
def resolve_cache(sync: Literal[False]) -> AsyncCache: ...


def resolve_cache(sync: bool = True) -> Cache | AsyncCache:
    ctx = get_context()
    cache = getattr(ctx, "cache", None)
    if cache is not None:
        return cast(Cache, cache) if sync else cast(AsyncCache, cache)
    return _get_cache_implementation(os.getenv("SUSPENSE_CACHE_DEBUG") == "true", sync)
