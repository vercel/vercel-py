"""Public root client wrappers for the clean-room stable surface."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from vercel._internal.stable.options import merge_root_options
from vercel._internal.stable.runtime import AsyncRuntime, SyncRuntime
from vercel._internal.stable.sdk.request_client import SdkClientLineage, SdkRequestState
from vercel.stable.blob.client import AsyncBlobClient, SyncBlobClient
from vercel.stable.options import BlobOptions, RootOptions, SandboxOptions, SdkOptions
from vercel.stable.sandbox.client import AsyncSandboxClient, SyncSandboxClient
from vercel.stable.sdk.client import AsyncSdk, SyncSdk


@dataclass(frozen=True, slots=True)
class SyncVercel:
    _runtime: SyncRuntime
    _options: RootOptions

    def __enter__(self) -> SyncVercel:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def with_options(self, *, timeout: float | None = None) -> SyncVercel:
        return SyncVercel(
            _runtime=self._runtime,
            _options=merge_root_options(self._options, timeout=timeout),
        )

    def ensure_connected(self) -> SyncVercel:
        self._runtime.ensure_connected(timeout=self._options.timeout)
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
            _lineage=SdkClientLineage(
                runtime=self._runtime,
                root_timeout=self._options.timeout,
                env=self._options.env,
                request_state=SdkRequestState(),
            ),
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

    async def __aenter__(self) -> AsyncVercel:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    def with_options(self, *, timeout: float | None = None) -> AsyncVercel:
        return AsyncVercel(
            _runtime=self._runtime,
            _options=merge_root_options(self._options, timeout=timeout),
        )

    async def ensure_connected(self) -> AsyncVercel:
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
            _lineage=SdkClientLineage(
                runtime=self._runtime,
                root_timeout=self._options.timeout,
                env=self._options.env,
                request_state=SdkRequestState(),
            ),
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


def create_sync_client(
    *,
    timeout: float | None = None,
    env: Mapping[str, str] | None = None,
) -> SyncVercel:
    return SyncVercel(
        _runtime=SyncRuntime(),
        _options=RootOptions(timeout=timeout, env=env if env is not None else os.environ),
    )


def create_async_client(
    *,
    timeout: float | None = None,
    env: Mapping[str, str] | None = None,
) -> AsyncVercel:
    return AsyncVercel(
        _runtime=AsyncRuntime(),
        _options=RootOptions(timeout=timeout, env=env if env is not None else os.environ),
    )


__all__ = ["create_sync_client", "create_async_client", "SyncVercel", "AsyncVercel"]
