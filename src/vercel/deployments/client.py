from __future__ import annotations

from typing import Any

from .deployments import create_deployment, upload_file
from .aio import create_deployment as acreate_deployment, upload_file as aupload_file


class DeploymentsClient:
    def __init__(
        self,
        access_token: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
    ):
        self._access_token = access_token
        self._base_url = base_url or "https://api.vercel.com"
        self._timeout = timeout or 30.0

    def create_deployment(
        self,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
        force_new: bool | None = None,
        skip_auto_detection_confirmation: bool | None = None,
    ) -> dict[str, Any]:
        return create_deployment(
            body=body,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            force_new=force_new,
            skip_auto_detection_confirmation=skip_auto_detection_confirmation,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    def upload_file(
        self,
        *,
        content: bytes,
        content_length: int,
        x_vercel_digest: str | None = None,
        x_now_digest: str | None = None,
        x_now_size: int | None = None,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        return upload_file(
            content=content,
            content_length=content_length,
            x_vercel_digest=x_vercel_digest,
            x_now_digest=x_now_digest,
            x_now_size=x_now_size,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )


class AsyncDeploymentsClient:
    def __init__(
        self,
        access_token: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
    ):
        self._access_token = access_token
        self._base_url = base_url or "https://api.vercel.com"
        self._timeout = timeout or 30.0

    async def create_deployment(
        self,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
        force_new: bool | None = None,
        skip_auto_detection_confirmation: bool | None = None,
    ) -> dict[str, Any]:
        return await acreate_deployment(
            body=body,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            force_new=force_new,
            skip_auto_detection_confirmation=skip_auto_detection_confirmation,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    async def upload_file(
        self,
        *,
        content: bytes,
        content_length: int,
        x_vercel_digest: str | None = None,
        x_now_digest: str | None = None,
        x_now_size: int | None = None,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        return await aupload_file(
            content=content,
            content_length=content_length,
            x_vercel_digest=x_vercel_digest,
            x_now_digest=x_now_digest,
            x_now_size=x_now_size,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )


__all__ = [
    "DeploymentsClient",
    "AsyncDeploymentsClient",
]
