"""Public stable SDK deployments surface."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.stable.runtime import AsyncRuntime, SyncRuntime
from vercel._internal.stable.sdk.request_client import SdkClientLineage
from vercel.stable.options import DeploymentCreateRequest

if TYPE_CHECKING:
    from vercel._internal.stable.sdk.deployments import DeploymentsBackend


@dataclass(frozen=True, slots=True)
class Deployment:
    id: str
    name: str | None = None
    url: str | None = None
    inspector_url: str | None = None
    ready_state: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.raw)


@dataclass(frozen=True, slots=True)
class UploadedDeploymentFile:
    file_hash: str | None = None
    size: int | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.raw)


@dataclass(frozen=True, slots=True)
class SyncDeploymentsClient:
    _lineage: SdkClientLineage
    _backend: DeploymentsBackend
    _runtime: SyncRuntime = field(init=False, repr=False)
    _root_timeout: float | None = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_runtime", self._lineage.runtime)
        object.__setattr__(self, "_root_timeout", self._lineage.root_timeout)

    def ensure_connected(self) -> SyncDeploymentsClient:
        self._runtime.ensure_connected(timeout=self._root_timeout)
        return self

    def create(
        self,
        *,
        request: DeploymentCreateRequest | None = None,
        body: dict[str, object] | None = None,
        name: str | None = None,
        project: str | None = None,
        target: str | None = None,
        files: tuple[dict[str, object], ...] | None = None,
        team_id: str | None = None,
        team_slug: str | None = None,
        force_new: bool | None = None,
        skip_auto_detection_confirmation: bool | None = None,
    ) -> Deployment:
        return iter_coroutine(
            self._backend.create(
                request=request,
                body=body,
                name=name,
                project=project,
                target=target,
                files=files,
                team_id=team_id,
                team_slug=team_slug,
                force_new=force_new,
                skip_auto_detection_confirmation=skip_auto_detection_confirmation,
            )
        )

    def upload_file(
        self,
        *,
        content: bytes | bytearray | memoryview,
        content_length: int,
        x_vercel_digest: str | None = None,
        x_now_digest: str | None = None,
        x_now_size: int | None = None,
        team_id: str | None = None,
        team_slug: str | None = None,
    ) -> UploadedDeploymentFile:
        return iter_coroutine(
            self._backend.upload_file(
                content=content,
                content_length=content_length,
                x_vercel_digest=x_vercel_digest,
                x_now_digest=x_now_digest,
                x_now_size=x_now_size,
                team_id=team_id,
                team_slug=team_slug,
            )
        )


@dataclass(frozen=True, slots=True)
class AsyncDeploymentsClient:
    _lineage: SdkClientLineage
    _backend: DeploymentsBackend
    _runtime: AsyncRuntime = field(init=False, repr=False)
    _root_timeout: float | None = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_runtime", self._lineage.runtime)
        object.__setattr__(self, "_root_timeout", self._lineage.root_timeout)

    async def ensure_connected(self) -> AsyncDeploymentsClient:
        await self._runtime.ensure_connected(timeout=self._root_timeout)
        return self

    async def create(
        self,
        *,
        request: DeploymentCreateRequest | None = None,
        body: dict[str, object] | None = None,
        name: str | None = None,
        project: str | None = None,
        target: str | None = None,
        files: tuple[dict[str, object], ...] | None = None,
        team_id: str | None = None,
        team_slug: str | None = None,
        force_new: bool | None = None,
        skip_auto_detection_confirmation: bool | None = None,
    ) -> Deployment:
        return await self._backend.create(
            request=request,
            body=body,
            name=name,
            project=project,
            target=target,
            files=files,
            team_id=team_id,
            team_slug=team_slug,
            force_new=force_new,
            skip_auto_detection_confirmation=skip_auto_detection_confirmation,
        )

    async def upload_file(
        self,
        *,
        content: bytes | bytearray | memoryview,
        content_length: int,
        x_vercel_digest: str | None = None,
        x_now_digest: str | None = None,
        x_now_size: int | None = None,
        team_id: str | None = None,
        team_slug: str | None = None,
    ) -> UploadedDeploymentFile:
        return await self._backend.upload_file(
            content=content,
            content_length=content_length,
            x_vercel_digest=x_vercel_digest,
            x_now_digest=x_now_digest,
            x_now_size=x_now_size,
            team_id=team_id,
            team_slug=team_slug,
        )


__all__ = [
    "Deployment",
    "UploadedDeploymentFile",
    "SyncDeploymentsClient",
    "AsyncDeploymentsClient",
]
