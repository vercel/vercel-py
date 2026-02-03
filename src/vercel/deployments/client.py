"""Deployments API client classes."""

from __future__ import annotations

from typing import Any

from .._http import (
    DEFAULT_API_BASE_URL,
    DEFAULT_TIMEOUT,
    AsyncTransport,
    BlockingTransport,
    HTTPConfig,
    iter_coroutine,
)
from ._core import _BaseDeploymentsClient


class DeploymentsClient(_BaseDeploymentsClient):
    """Synchronous client for Vercel Deployments API."""

    def __init__(
        self,
        access_token: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
    ):
        config = HTTPConfig(
            base_url=base_url or DEFAULT_API_BASE_URL,
            timeout=timeout or DEFAULT_TIMEOUT,
            token=access_token,
        )
        self._transport = BlockingTransport(config)

    def create_deployment(
        self,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
        force_new: bool | None = None,
        skip_auto_detection_confirmation: bool | None = None,
    ) -> dict[str, Any]:
        """Create a new deployment."""
        return iter_coroutine(
            self._create_deployment(
                body=body,
                team_id=team_id,
                slug=slug,
                force_new=force_new,
                skip_auto_detection_confirmation=skip_auto_detection_confirmation,
            )
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
        """Upload a single deployment file to Vercel."""
        return iter_coroutine(
            self._upload_file(
                content=content,
                content_length=content_length,
                x_vercel_digest=x_vercel_digest,
                x_now_digest=x_now_digest,
                x_now_size=x_now_size,
                team_id=team_id,
                slug=slug,
            )
        )


class AsyncDeploymentsClient(_BaseDeploymentsClient):
    """Asynchronous client for Vercel Deployments API."""

    def __init__(
        self,
        access_token: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
    ):
        config = HTTPConfig(
            base_url=base_url or DEFAULT_API_BASE_URL,
            timeout=timeout or DEFAULT_TIMEOUT,
            token=access_token,
        )
        self._transport = AsyncTransport(config)

    async def create_deployment(
        self,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
        force_new: bool | None = None,
        skip_auto_detection_confirmation: bool | None = None,
    ) -> dict[str, Any]:
        """Create a new deployment."""
        return await self._create_deployment(
            body=body,
            team_id=team_id,
            slug=slug,
            force_new=force_new,
            skip_auto_detection_confirmation=skip_auto_detection_confirmation,
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
        """Upload a single deployment file to Vercel."""
        return await self._upload_file(
            content=content,
            content_length=content_length,
            x_vercel_digest=x_vercel_digest,
            x_now_digest=x_now_digest,
            x_now_size=x_now_size,
            team_id=team_id,
            slug=slug,
        )


__all__ = [
    "DeploymentsClient",
    "AsyncDeploymentsClient",
]
