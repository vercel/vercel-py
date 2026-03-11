"""Public cache clients for the stable root client."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from vercel._internal.stable.cache import (
    sync_contains,
    sync_delete,
    sync_expire_tag,
    sync_get,
    sync_set,
)
from vercel._internal.stable.cache.client import CacheClientLineage, StableCacheBackend
from vercel._internal.stable.options import merge_dataclass_options, merge_mapping
from vercel._internal.stable.runtime import AsyncRuntime
from vercel.stable.options import CacheOptions, CacheSetOptions


@dataclass(frozen=True, slots=True)
class SyncCacheClient:
    _lineage: CacheClientLineage
    _options: CacheOptions

    def with_options(
        self,
        *,
        endpoint: str | None = None,
        headers: dict[str, str] | None = None,
        namespace: str | None = None,
        namespace_separator: str | None = None,
        key_hash_function: Callable[[str], str] | None = None,
    ) -> SyncCacheClient:
        return SyncCacheClient(
            _lineage=self._lineage,
            _options=merge_dataclass_options(
                self._options,
                endpoint=endpoint,
                headers=merge_mapping(self._options.headers, headers),
                namespace=namespace,
                namespace_separator=namespace_separator,
                key_hash_function=key_hash_function,
            ),
        )

    def ensure_connected(self) -> SyncCacheClient:
        self._lineage.runtime.ensure_connected(timeout=self._lineage.root_timeout)
        return self

    def get(self, key: str) -> object | None:
        return sync_get(backend=self._backend(), key=key)

    def set(
        self,
        key: str,
        value: object,
        options: CacheSetOptions | Mapping[str, Any] | None = None,
    ) -> None:
        sync_set(backend=self._backend(), key=key, value=value, options=options)

    def delete(self, key: str) -> None:
        sync_delete(backend=self._backend(), key=key)

    def expire_tag(self, tag: str | Sequence[str]) -> None:
        sync_expire_tag(backend=self._backend(), tag=tag)

    def __contains__(self, key: str) -> bool:
        return sync_contains(backend=self._backend(), key=key)

    def __getitem__(self, key: str) -> object:
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def _backend(self) -> StableCacheBackend:
        return StableCacheBackend(
            _lineage=self._lineage,
            _options=self._options,
        )


@dataclass(frozen=True, slots=True)
class AsyncCacheClient:
    _lineage: CacheClientLineage
    _options: CacheOptions

    def with_options(
        self,
        *,
        endpoint: str | None = None,
        headers: dict[str, str] | None = None,
        namespace: str | None = None,
        namespace_separator: str | None = None,
        key_hash_function: Callable[[str], str] | None = None,
    ) -> AsyncCacheClient:
        return AsyncCacheClient(
            _lineage=self._lineage,
            _options=merge_dataclass_options(
                self._options,
                endpoint=endpoint,
                headers=merge_mapping(self._options.headers, headers),
                namespace=namespace,
                namespace_separator=namespace_separator,
                key_hash_function=key_hash_function,
            ),
        )

    async def ensure_connected(self) -> AsyncCacheClient:
        await cast(AsyncRuntime, self._lineage.runtime).ensure_connected(
            timeout=self._lineage.root_timeout
        )
        return self

    async def get(self, key: str) -> object | None:
        return await self._backend().get(key)

    async def set(
        self,
        key: str,
        value: object,
        options: CacheSetOptions | Mapping[str, Any] | None = None,
    ) -> None:
        await self._backend().set(key, value, options)

    async def delete(self, key: str) -> None:
        await self._backend().delete(key)

    async def expire_tag(self, tag: str | Sequence[str]) -> None:
        await self._backend().expire_tag(tag)

    async def contains(self, key: str) -> bool:
        return await self._backend().contains(key)

    def _backend(self) -> StableCacheBackend:
        return StableCacheBackend(
            _lineage=self._lineage,
            _options=self._options,
        )


__all__ = ["SyncCacheClient", "AsyncCacheClient"]
