"""Public cache client placeholders for the stable root client."""

from __future__ import annotations

from dataclasses import dataclass

from vercel._internal.stable.options import merge_dataclass_options, merge_mapping
from vercel._internal.stable.runtime import AsyncRuntime, SyncRuntime
from vercel.stable.options import CacheOptions


@dataclass(frozen=True, slots=True)
class SyncCacheClient:
    _runtime: SyncRuntime
    _options: CacheOptions

    def with_options(
        self,
        *,
        endpoint: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> "SyncCacheClient":
        return SyncCacheClient(
            _runtime=self._runtime,
            _options=merge_dataclass_options(
                self._options,
                endpoint=endpoint,
                headers=merge_mapping(self._options.headers, headers),
            ),
        )


@dataclass(frozen=True, slots=True)
class AsyncCacheClient:
    _runtime: AsyncRuntime
    _options: CacheOptions

    def with_options(
        self,
        *,
        endpoint: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> "AsyncCacheClient":
        return AsyncCacheClient(
            _runtime=self._runtime,
            _options=merge_dataclass_options(
                self._options,
                endpoint=endpoint,
                headers=merge_mapping(self._options.headers, headers),
            ),
        )


__all__ = ["SyncCacheClient", "AsyncCacheClient"]
