from __future__ import annotations

from typing import Sequence
from .types import Cache, AsyncCache


class InMemoryCache(Cache):
    def __init__(self) -> None:
        self._cache: dict[str, dict] = {}

    def get(self, key: str):
        entry = self._cache.get(key)
        if not entry:
            return None
        ttl = entry.get("ttl")
        if (
            ttl is not None
            and entry["last_modified"] + ttl * 1000 < __import__("time").time() * 1000
        ):
            self.delete(key)
            return None
        return entry["value"]

    def set(self, key: str, value: object, options: dict | None = None) -> None:
        from time import time

        opts = options or {}
        ttl = opts.get("ttl")
        tags = set(opts.get("tags", []))
        self._cache[key] = {
            "value": value,
            "tags": tags,
            "last_modified": int(time() * 1000),
            "ttl": ttl,
        }

    def delete(self, key: str) -> None:
        self._cache.pop(key, None)

    def expire_tag(self, tag: str | Sequence[str]) -> None:
        tags = {tag} if isinstance(tag, str) else set(tag)
        to_delete = []
        for k, entry in self._cache.items():
            entry_tags = entry.get("tags", set())
            if any(t in entry_tags for t in tags):
                to_delete.append(k)
        for k in to_delete:
            self._cache.pop(k, None)


class AsyncInMemoryCache(AsyncCache):
    def __init__(self, delegate: InMemoryCache | None = None) -> None:
        # Reuse the synchronous implementation under the hood and expose async API
        self.cache = delegate or InMemoryCache()

    async def get(self, key: str):
        return self.cache.get(key)

    async def set(self, key: str, value: object, options: dict | None = None) -> None:
        self.cache.set(key, value, options)

    async def delete(self, key: str) -> None:
        self.cache.delete(key)

    async def expire_tag(self, tag: str | Sequence[str]) -> None:
        self.cache.expire_tag(tag)
