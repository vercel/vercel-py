"""Core business logic for Vercel Deployments API."""

from __future__ import annotations

from typing import Any

import httpx

from .._http import (
    DEFAULT_API_BASE_URL,
    DEFAULT_TIMEOUT,
    AsyncTransport,
    BaseTransport,
    BlockingTransport,
    BytesBody,
    HTTPConfig,
    JSONBody,
)
from .._telemetry.tracker import track


def _build_team_params(
    team_id: str | None = None,
    slug: str | None = None,
) -> dict[str, Any]:
    """Build query params for team scoping."""
    params: dict[str, Any] = {}
    if team_id:
        params["teamId"] = team_id
    if slug:
        params["slug"] = slug
    return params


def _handle_error_response(
    resp: httpx.Response,
    operation: str,
) -> None:
    """Raise RuntimeError with formatted error message if response indicates failure."""
    try:
        data = resp.json()
    except Exception:
        data = {"error": resp.text}
    raise RuntimeError(f"Failed to {operation}: {resp.status_code} {resp.reason_phrase} - {data}")


class _BaseDeploymentsClient:
    """
    Base class containing shared business logic for Deployments API operations.

    All methods are async and use the abstract _transport property for HTTP requests.
    Subclasses must provide a concrete transport implementation.
    """

    _transport: BaseTransport

    async def _create_deployment(
        self,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
        force_new: bool | None = None,
        skip_auto_detection_confirmation: bool | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Create a new deployment.

        body: matches the Deployments Create request body (name, project,
        files|gitSource, target, projectSettings, etc.)
        Optional query params: team_id -> teamId, slug -> slug, force_new ->
        forceNew, skip_auto_detection_confirmation ->
        skipAutoDetectionConfirmation
        """
        if not isinstance(body, dict):
            raise ValueError("body must be a dict")

        params = _build_team_params(team_id, slug)
        if force_new is not None:
            params["forceNew"] = "1" if force_new else "0"
        if skip_auto_detection_confirmation is not None:
            params["skipAutoDetectionConfirmation"] = (
                "1" if skip_auto_detection_confirmation else "0"
            )

        resp = await self._transport.send(
            "POST",
            "/v13/deployments",
            params=params,
            body=JSONBody(body),
            timeout=timeout,
        )

        if not (200 <= resp.status_code < 300):
            _handle_error_response(resp, "create deployment")

        # Track telemetry
        track(
            "deployment_create",
            token=self._transport._config.token,
            target=body.get("target"),
            force_new=bool(force_new) if force_new is not None else None,
        )

        return resp.json()

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
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Upload a single deployment file to Vercel.

        Headers required:
        - Content-Length: size in bytes
        - x-vercel-digest or x-now-digest: sha1 digest (one of them supported)
        - x-now-size: alternative file size
        """
        params = _build_team_params(team_id, slug)

        headers: dict[str, str] = {
            "Content-Length": str(content_length),
        }
        if x_vercel_digest:
            headers["x-vercel-digest"] = x_vercel_digest
        if x_now_digest:
            headers["x-now-digest"] = x_now_digest
        if x_now_size is not None:
            headers["x-now-size"] = str(x_now_size)

        resp = await self._transport.send(
            "POST",
            "/v2/files",
            params=params,
            body=BytesBody(bytes(content)),
            headers=headers,
            timeout=timeout,
        )

        if not (200 <= resp.status_code < 300):
            _handle_error_response(resp, "upload file")

        return resp.json()


class SyncDeploymentsClient(_BaseDeploymentsClient):
    """Sync client for Deployments API operations."""

    def __init__(
        self,
        token: str | None,
        base_url: str = DEFAULT_API_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        config = HTTPConfig(
            base_url=base_url,
            timeout=timeout,
            token=token,
        )
        self._transport = BlockingTransport(config)


class AsyncDeploymentsClient(_BaseDeploymentsClient):
    """Async client for Deployments API operations."""

    def __init__(
        self,
        token: str | None,
        base_url: str = DEFAULT_API_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        config = HTTPConfig(
            base_url=base_url,
            timeout=timeout,
            token=token,
        )
        self._transport = AsyncTransport(config)


__all__ = [
    "SyncDeploymentsClient",
    "AsyncDeploymentsClient",
    "_build_team_params",
    "_handle_error_response",
]
