"""Vercel Deployments API - asynchronous functions."""

from __future__ import annotations

from typing import Any

from .._http import (
    DEFAULT_API_BASE_URL,
    DEFAULT_TIMEOUT,
)
from ._core import AsyncDeploymentsClient


async def create_deployment(
    *,
    body: dict[str, Any],
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    force_new: bool | None = None,
    skip_auto_detection_confirmation: bool | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Create a new deployment.

    body: matches the Deployments Create request body (name, project,
    files|gitSource, target, projectSettings, etc.)
    Optional query params: team_id -> teamId, slug -> slug, force_new ->
    forceNew, skip_auto_detection_confirmation ->
    skipAutoDetectionConfirmation
    """
    client = AsyncDeploymentsClient(token, base_url, timeout)
    return await client._create_deployment(
        body=body,
        team_id=team_id,
        slug=slug,
        force_new=force_new,
        skip_auto_detection_confirmation=skip_auto_detection_confirmation,
    )


async def upload_file(
    *,
    content: bytes | bytearray | memoryview,
    content_length: int,
    x_vercel_digest: str | None = None,
    x_now_digest: str | None = None,
    x_now_size: int | None = None,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Upload a single deployment file to Vercel (async)."""
    client = AsyncDeploymentsClient(token, base_url, timeout)
    return await client._upload_file(
        content=content,
        content_length=content_length,
        x_vercel_digest=x_vercel_digest,
        x_now_digest=x_now_digest,
        x_now_size=x_now_size,
        team_id=team_id,
        slug=slug,
    )


__all__ = ["create_deployment", "upload_file"]
