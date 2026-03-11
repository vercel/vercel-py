"""Public root client wrappers for the clean-room stable surface."""

from __future__ import annotations

from dataclasses import dataclass

from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.stable.options import merge_root_options
from vercel._internal.stable.runtime import AsyncRuntime, SyncRuntime
from vercel.stable.blob.client import AsyncBlobClient, SyncBlobClient
from vercel.stable.cache.client import AsyncCacheClient, SyncCacheClient
from vercel.stable.options import BlobOptions, CacheOptions, RootOptions, SandboxOptions, SdkOptions
from vercel.stable.sandbox.client import AsyncSandboxClient, SyncSandboxClient
from vercel.stable.sdk.client import AsyncSdk, SyncSdk


@dataclass(frozen=True, slots=True)
class SyncVercel:
    _runtime: SyncRuntime
    _options: RootOptions

    def with_options(self, *, timeout: float | None = None) -> "SyncVercel":
        return SyncVercel(
            _runtime=self._runtime,
            _options=merge_root_options(self._options, timeout=timeout),
        )

    def ensure_connected(self) -> "SyncVercel":
        iter_coroutine(self._runtime.ensure_connected(timeout=self._options.timeout))
        return self

    def get_sdk(
        self,
        *,
        token: str | None = None,
        base_url: str | None = None,
        team_id: str | None = None,
        team_slug: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> SyncSdk:
        return SyncSdk(
            _runtime=self._runtime,
            _options=SdkOptions(
                token=token,
                base_url=base_url,
                team_id=team_id,
                team_slug=team_slug,
                headers=dict(headers or {}),
            ),
        )

    def get_blob(
        self,
        *,
        token: str | None = None,
        base_url: str | None = None,
    ) -> SyncBlobClient:
        return SyncBlobClient(
            _runtime=self._runtime,
            _options=BlobOptions(token=token, base_url=base_url),
        )

    def get_cache(
        self,
        *,
        endpoint: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> SyncCacheClient:
        return SyncCacheClient(
            _runtime=self._runtime,
            _options=CacheOptions(endpoint=endpoint, headers=dict(headers or {})),
        )

    def get_sandbox(
        self,
        *,
        token: str | None = None,
        base_url: str | None = None,
        team_id: str | None = None,
        team_slug: str | None = None,
    ) -> SyncSandboxClient:
        return SyncSandboxClient(
            _runtime=self._runtime,
            _options=SandboxOptions(
                token=token,
                base_url=base_url,
                team_id=team_id,
                team_slug=team_slug,
            ),
        )

    def close(self) -> None:
        self._runtime.close()


@dataclass(frozen=True, slots=True)
class AsyncVercel:
    _runtime: AsyncRuntime
    _options: RootOptions

    def with_options(self, *, timeout: float | None = None) -> "AsyncVercel":
        return AsyncVercel(
            _runtime=self._runtime,
            _options=merge_root_options(self._options, timeout=timeout),
        )

    async def ensure_connected(self) -> "AsyncVercel":
        await self._runtime.ensure_connected(timeout=self._options.timeout)
        return self

    def get_sdk(
        self,
        *,
        token: str | None = None,
        base_url: str | None = None,
        team_id: str | None = None,
        team_slug: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> AsyncSdk:
        return AsyncSdk(
            _runtime=self._runtime,
            _options=SdkOptions(
                token=token,
                base_url=base_url,
                team_id=team_id,
                team_slug=team_slug,
                headers=dict(headers or {}),
            ),
        )

    def get_blob(
        self,
        *,
        token: str | None = None,
        base_url: str | None = None,
    ) -> AsyncBlobClient:
        return AsyncBlobClient(
            _runtime=self._runtime,
            _options=BlobOptions(token=token, base_url=base_url),
        )

    def get_cache(
        self,
        *,
        endpoint: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> AsyncCacheClient:
        return AsyncCacheClient(
            _runtime=self._runtime,
            _options=CacheOptions(endpoint=endpoint, headers=dict(headers or {})),
        )

    def get_sandbox(
        self,
        *,
        token: str | None = None,
        base_url: str | None = None,
        team_id: str | None = None,
        team_slug: str | None = None,
    ) -> AsyncSandboxClient:
        return AsyncSandboxClient(
            _runtime=self._runtime,
            _options=SandboxOptions(
                token=token,
                base_url=base_url,
                team_id=team_id,
                team_slug=team_slug,
            ),
        )

    async def aclose(self) -> None:
        await self._runtime.aclose()


def create_sync_client(*, timeout: float | None = None) -> SyncVercel:
    return SyncVercel(_runtime=SyncRuntime(), _options=RootOptions(timeout=timeout))


def create_async_client(*, timeout: float | None = None) -> AsyncVercel:
    return AsyncVercel(_runtime=AsyncRuntime(), _options=RootOptions(timeout=timeout))


__all__ = ["create_sync_client", "create_async_client", "SyncVercel", "AsyncVercel"]
