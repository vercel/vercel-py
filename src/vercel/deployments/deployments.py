from __future__ import annotations

import os
import httpx
from typing import Any, Iterable


DEFAULT_API_BASE_URL = "https://api.vercel.com"
DEFAULT_TIMEOUT = 60.0


def _require_token(token: str | None) -> str:
    resolved = token or os.getenv("VERCEL_TOKEN")
    if not resolved:
        raise RuntimeError("Missing Vercel API token. Pass token=... or set VERCEL_TOKEN.")
    return resolved


def _request(
    method: str,
    path: str,
    *,
    token: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    params: dict[str, Any] | None = None,
    json: Any | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> httpx.Response:
    bearer = _require_token(token)
    url = base_url.rstrip("/") + path
    with httpx.Client(timeout=httpx.Timeout(timeout)) as client:
        resp = client.request(
            method,
            url,
            params=params or None,
            json=json,
            headers={
                "authorization": f"Bearer {bearer}",
                "accept": "application/json",
                "content-type": "application/json",
            },
        )
        return resp


async def _request_async(
    method: str,
    path: str,
    *,
    token: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    params: dict[str, Any] | None = None,
    json: Any | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> httpx.Response:
    bearer = _require_token(token)
    url = base_url.rstrip("/") + path
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
        resp = await client.request(
            method,
            url,
            params=params or None,
            json=json,
            headers={
                "authorization": f"Bearer {bearer}",
                "accept": "application/json",
                "content-type": "application/json",
            },
        )
        return resp


def create_deployment(
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

    body: matches the Deployments Create request body (name, project, files|gitSource, target, projectSettings, etc.)
    Optional query params: team_id -> teamId, slug -> slug, force_new -> forceNew, skip_auto_detection_confirmation -> skipAutoDetectionConfirmation
    """
    params: dict[str, Any] = {}
    if team_id:
        params["teamId"] = team_id
    if slug:
        params["slug"] = slug
    if force_new is not None:
        params["forceNew"] = "1" if force_new else "0"
    if skip_auto_detection_confirmation is not None:
        params["skipAutoDetectionConfirmation"] = "1" if skip_auto_detection_confirmation else "0"

    resp = _request(
        "POST",
        "/v13/deployments",
        token=token,
        base_url=base_url,
        params=params,
        json=body,
        timeout=timeout,
    )
    if not (200 <= resp.status_code < 300):
        try:
            data = resp.json()
        except Exception:
            data = {"error": resp.text}
        raise RuntimeError(
            f"Failed to create deployment: {resp.status_code} {resp.reason_phrase} - {data}"
        )
    return resp.json()


def upload_file(
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
    """Upload a single deployment file to Vercel.

    Headers required:
    - Content-Length: size in bytes
    - x-vercel-digest or x-now-digest: sha1 digest (one of them supported)
    - x-now-size: alternative file size
    """
    params: dict[str, Any] = {}
    if team_id:
        params["teamId"] = team_id
    if slug:
        params["slug"] = slug

    bearer = _require_token(token)
    url = base_url.rstrip("/") + "/v2/files"
    headers: dict[str, str] = {
        "authorization": f"Bearer {bearer}",
        "accept": "application/json",
        "content-type": "application/octet-stream",
        "Content-Length": str(content_length),
    }
    if x_vercel_digest:
        headers["x-vercel-digest"] = x_vercel_digest
    if x_now_digest:
        headers["x-now-digest"] = x_now_digest
    if x_now_size is not None:
        headers["x-now-size"] = str(x_now_size)

    with httpx.Client(timeout=httpx.Timeout(timeout)) as client:
        resp = client.post(url, params=params, content=content, headers=headers)
    if not (200 <= resp.status_code < 300):
        try:
            data = resp.json()
        except Exception:
            data = {"error": resp.text}
        raise RuntimeError(
            f"Failed to upload file: {resp.status_code} {resp.reason_phrase} - {data}"
        )
    return resp.json()


async def upload_file_async(
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
    params: dict[str, Any] = {}
    if team_id:
        params["teamId"] = team_id
    if slug:
        params["slug"] = slug

    bearer = _require_token(token)
    url = base_url.rstrip("/") + "/v2/files"
    headers: dict[str, str] = {
        "authorization": f"Bearer {bearer}",
        "accept": "application/json",
        "content-type": "application/octet-stream",
        "Content-Length": str(content_length),
    }
    if x_vercel_digest:
        headers["x-vercel-digest"] = x_vercel_digest
    if x_now_digest:
        headers["x-now-digest"] = x_now_digest
    if x_now_size is not None:
        headers["x-now-size"] = str(x_now_size)

    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
        resp = await client.post(url, params=params, content=content, headers=headers)
    if not (200 <= resp.status_code < 300):
        try:
            data = resp.json()
        except Exception:
            data = {"error": resp.text}
        raise RuntimeError(
            f"Failed to upload file: {resp.status_code} {resp.reason_phrase} - {data}"
        )
    return resp.json()


async def create_deployment_async(
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

    body: matches the Deployments Create request body (name, project, files|gitSource, target, projectSettings, etc.)
    Optional query params: team_id -> teamId, slug -> slug, force_new -> forceNew, skip_auto_detection_confirmation -> skipAutoDetectionConfirmation
    """
    params: dict[str, Any] = {}
    if team_id:
        params["teamId"] = team_id
    if slug:
        params["slug"] = slug
    if force_new is not None:
        params["forceNew"] = "1" if force_new else "0"
    if skip_auto_detection_confirmation is not None:
        params["skipAutoDetectionConfirmation"] = "1" if skip_auto_detection_confirmation else "0"

    resp = await _request_async(
        "POST",
        "/v13/deployments",
        token=token,
        base_url=base_url,
        params=params,
        json=body,
        timeout=timeout,
    )
    if not (200 <= resp.status_code < 300):
        try:
            data = resp.json()
        except Exception:
            data = {"error": resp.text}
        raise RuntimeError(
            f"Failed to create deployment: {resp.status_code} {resp.reason_phrase} - {data}"
        )
    return resp.json()
