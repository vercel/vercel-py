from typing import Callable
from .runtime_cache import AsyncRuntimeCache


def get_cache(
    *,
    key_hash_function: Callable[[str], str] | None = None,
    namespace: str | None = None,
    namespace_separator: str | None = None,
) -> AsyncRuntimeCache:
    return AsyncRuntimeCache(
        key_hash_function=key_hash_function,
        namespace=namespace,
        namespace_separator=namespace_separator,
    )


__all__ = ["get_cache", "AsyncRuntimeCache"]
