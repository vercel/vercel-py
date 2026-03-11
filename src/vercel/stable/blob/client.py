"""Public blob client placeholders for the stable root client."""

from __future__ import annotations

from dataclasses import dataclass

from vercel._internal.stable.options import merge_dataclass_options
from vercel._internal.stable.runtime import AsyncRuntime, SyncRuntime
from vercel.stable.options import BlobOptions


@dataclass(frozen=True, slots=True)
class SyncBlobClient:
    _runtime: SyncRuntime
    _options: BlobOptions

    def with_options(
        self,
        *,
        token: str | None = None,
        base_url: str | None = None,
    ) -> "SyncBlobClient":
        return SyncBlobClient(
            _runtime=self._runtime,
            _options=merge_dataclass_options(self._options, token=token, base_url=base_url),
        )


@dataclass(frozen=True, slots=True)
class AsyncBlobClient:
    _runtime: AsyncRuntime
    _options: BlobOptions

    def with_options(
        self,
        *,
        token: str | None = None,
        base_url: str | None = None,
    ) -> "AsyncBlobClient":
        return AsyncBlobClient(
            _runtime=self._runtime,
            _options=merge_dataclass_options(self._options, token=token, base_url=base_url),
        )


__all__ = ["SyncBlobClient", "AsyncBlobClient"]
