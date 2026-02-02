"""Deployments API client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import httpx

from .._telemetry.tracker import track
from .iter_coroutine import iter_coroutine

if TYPE_CHECKING:
    from .config import ClientConfig
    from .transport import BaseTransport


class BaseDeploymentsClient:
    """Base deployments client with shared async business logic."""

    def __init__(self, transport: BaseTransport, config: ClientConfig):
        self._transport = transport
        self._config = config

    def _build_params(
        self,
        team_id: str | None = None,
        slug: str | None = None,
        force_new: bool | None = None,
        skip_auto_detection_confirmation: bool | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        resolved_team_id = team_id or self._config.default_team_id
        resolved_slug = slug or self._config.default_slug
        if resolved_team_id:
            params["teamId"] = resolved_team_id
        if resolved_slug:
            params["slug"] = resolved_slug
        if force_new is not None:
            params["forceNew"] = "1" if force_new else "0"
        if skip_auto_detection_confirmation is not None:
            params["skipAutoDetectionConfirmation"] = (
                "1" if skip_auto_detection_confirmation else "0"
            )
        return params

    def _handle_error(self, response: httpx.Response, operation: str) -> None:
        try:
            data = response.json()
        except Exception:
            data = {"error": response.text}
        raise RuntimeError(
            f"Failed to {operation}: {response.status_code} "
            f"{response.reason_phrase} - {data}"
        )

    async def _create(
        self,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
        force_new: bool | None = None,
        skip_auto_detection_confirmation: bool | None = None,
    ) -> dict[str, Any]:
        if not isinstance(body, dict):
            raise ValueError("body must be a dict")

        params = self._build_params(
            team_id, slug, force_new, skip_auto_detection_confirmation
        )

        response = await self._transport.send(
            "POST", "/v13/deployments", params=params, json=body
        )

        if not (200 <= response.status_code < 300):
            self._handle_error(response, "create deployment")

        track(
            "deployment_create",
            token=self._config.access_token,
            target=body.get("target"),
            force_new=bool(force_new) if force_new is not None else None,
        )
        return response.json()

    async def _upload_file(
        self,
        *,
        content: bytes | bytearray | memoryview,
        content_length: int,
        x_vercel_digest: str | None = None,
        x_now_digest: str | None = None,
        x_now_size: int | None = None,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        params = self._build_params(team_id, slug)

        headers: dict[str, str] = {
            "content-type": "application/octet-stream",
            "Content-Length": str(content_length),
        }
        if x_vercel_digest:
            headers["x-vercel-digest"] = x_vercel_digest
        if x_now_digest:
            headers["x-now-digest"] = x_now_digest
        if x_now_size is not None:
            headers["x-now-size"] = str(x_now_size)

        response = await self._transport.send(
            "POST", "/v2/files", params=params, content=bytes(content), headers=headers
        )

        if not (200 <= response.status_code < 300):
            self._handle_error(response, "upload file")

        return response.json()


class DeploymentsClient(BaseDeploymentsClient):
    def create(
        self,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
        force_new: bool | None = None,
        skip_auto_detection_confirmation: bool | None = None,
    ) -> dict[str, Any]:
        return iter_coroutine(
            self._create(
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
        content: bytes | bytearray | memoryview,
        content_length: int,
        x_vercel_digest: str | None = None,
        x_now_digest: str | None = None,
        x_now_size: int | None = None,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
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


class AsyncDeploymentsClient(BaseDeploymentsClient):
    async def create(
        self,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
        force_new: bool | None = None,
        skip_auto_detection_confirmation: bool | None = None,
    ) -> dict[str, Any]:
        return await self._create(
            body=body,
            team_id=team_id,
            slug=slug,
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
        slug: str | None = None,
    ) -> dict[str, Any]:
        return await self._upload_file(
            content=content,
            content_length=content_length,
            x_vercel_digest=x_vercel_digest,
            x_now_digest=x_now_digest,
            x_now_size=x_now_size,
            team_id=team_id,
            slug=slug,
        )
