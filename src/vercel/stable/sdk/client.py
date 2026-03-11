"""Public SDK family wrappers for the stable client surface."""

from __future__ import annotations

from dataclasses import dataclass, field

from vercel._internal.stable.options import merge_dataclass_options, merge_mapping
from vercel._internal.stable.runtime import AsyncRuntime, SyncRuntime
from vercel._internal.stable.sdk.deployments import DeploymentsBackend
from vercel._internal.stable.sdk.projects import ProjectsBackend
from vercel._internal.stable.sdk.request_client import (
    SdkClientLineage,
    SdkRequestState,
    create_async_request_client,
    create_sync_request_client,
)
from vercel.stable.options import SdkOptions
from vercel.stable.sdk.deployments import AsyncDeploymentsClient, SyncDeploymentsClient
from vercel.stable.sdk.projects import AsyncProjectsClient, SyncProjectsClient


@dataclass(frozen=True, slots=True)
class SyncSdk:
    _lineage: SdkClientLineage
    _options: SdkOptions
    _runtime: SyncRuntime = field(init=False, repr=False)
    _root_timeout: float | None = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_runtime", self._lineage.runtime)
        object.__setattr__(self, "_root_timeout", self._lineage.root_timeout)

    def with_options(
        self,
        *,
        token: str | None = None,
        base_url: str | None = None,
        team_id: str | None = None,
        team_slug: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> SyncSdk:
        merged_headers = merge_mapping(self._options.headers, headers)
        return SyncSdk(
            _lineage=SdkClientLineage(
                runtime=self._lineage.runtime,
                root_timeout=self._lineage.root_timeout,
                env=self._lineage.env,
                request_state=SdkRequestState(),
            ),
            _options=merge_dataclass_options(
                self._options,
                token=token,
                base_url=base_url,
                team_id=team_id,
                team_slug=team_slug,
                headers=merged_headers,
            ),
        )

    def ensure_connected(self) -> SyncSdk:
        self._runtime.ensure_connected(timeout=self._root_timeout)
        return self

    def get_projects(self) -> SyncProjectsClient:
        return SyncProjectsClient(
            _lineage=self._lineage,
            _backend=ProjectsBackend(
                create_sync_request_client(
                    lineage=self._lineage,
                    options=self._options,
                )
            ),
        )

    def get_deployments(self) -> SyncDeploymentsClient:
        return SyncDeploymentsClient(
            _lineage=self._lineage,
            _backend=DeploymentsBackend(
                create_sync_request_client(
                    lineage=self._lineage,
                    options=self._options,
                )
            ),
        )


@dataclass(frozen=True, slots=True)
class AsyncSdk:
    _lineage: SdkClientLineage
    _options: SdkOptions
    _runtime: AsyncRuntime = field(init=False, repr=False)
    _root_timeout: float | None = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_runtime", self._lineage.runtime)
        object.__setattr__(self, "_root_timeout", self._lineage.root_timeout)

    def with_options(
        self,
        *,
        token: str | None = None,
        base_url: str | None = None,
        team_id: str | None = None,
        team_slug: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> AsyncSdk:
        merged_headers = merge_mapping(self._options.headers, headers)
        return AsyncSdk(
            _lineage=SdkClientLineage(
                runtime=self._lineage.runtime,
                root_timeout=self._lineage.root_timeout,
                env=self._lineage.env,
                request_state=SdkRequestState(),
            ),
            _options=merge_dataclass_options(
                self._options,
                token=token,
                base_url=base_url,
                team_id=team_id,
                team_slug=team_slug,
                headers=merged_headers,
            ),
        )

    async def ensure_connected(self) -> AsyncSdk:
        await self._runtime.ensure_connected(timeout=self._root_timeout)
        return self

    def get_projects(self) -> AsyncProjectsClient:
        return AsyncProjectsClient(
            _lineage=self._lineage,
            _backend=ProjectsBackend(
                create_async_request_client(
                    lineage=self._lineage,
                    options=self._options,
                )
            ),
        )

    def get_deployments(self) -> AsyncDeploymentsClient:
        return AsyncDeploymentsClient(
            _lineage=self._lineage,
            _backend=DeploymentsBackend(
                create_async_request_client(
                    lineage=self._lineage,
                    options=self._options,
                )
            ),
        )


__all__ = ["SyncSdk", "AsyncSdk"]
