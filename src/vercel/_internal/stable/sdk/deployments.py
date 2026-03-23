"""Shared async-shaped backend for stable SDK deployments operations."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from vercel._internal.http import RawBody
from vercel._internal.stable.sdk.request_client import (
    VercelRequestClient,
    decode_json_object_response,
)
from vercel.stable.options import DeploymentCreateRequest

if TYPE_CHECKING:
    from vercel.stable.sdk.deployments import Deployment, UploadedDeploymentFile


@dataclass(slots=True)
class DeploymentsBackend:
    _request_client: VercelRequestClient

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
        payload = decode_json_object_response(
            await self._request_client.request(
                "POST",
                "/v13/deployments",
                params=_deployment_params(
                    team_id=team_id,
                    team_slug=team_slug,
                    force_new=force_new,
                    skip_auto_detection_confirmation=skip_auto_detection_confirmation,
                ),
                body=_deployment_body(
                    request=request,
                    body=body,
                    name=name,
                    project=project,
                    target=target,
                    files=files,
                ),
            ),
        )
        return _parse_deployment(payload)

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
        headers = {
            "accept": "application/json",
            "content-type": "application/octet-stream",
            "Content-Length": str(content_length),
        }
        if x_vercel_digest is not None:
            headers["x-vercel-digest"] = x_vercel_digest
        if x_now_digest is not None:
            headers["x-now-digest"] = x_now_digest
        if x_now_size is not None:
            headers["x-now-size"] = str(x_now_size)

        payload = decode_json_object_response(
            await self._request_client.request(
                "POST",
                "/v2/files",
                params=_deployment_params(team_id=team_id, team_slug=team_slug),
                body=RawBody(bytes(content)),
                headers=headers,
            )
        )
        return _parse_uploaded_file(payload)


def _deployment_params(
    *,
    team_id: str | None = None,
    team_slug: str | None = None,
    force_new: bool | None = None,
    skip_auto_detection_confirmation: bool | None = None,
) -> dict[str, Any] | None:
    params: dict[str, Any] = {}
    if team_id is not None:
        params["teamId"] = team_id
    if team_slug is not None:
        params["slug"] = team_slug
    if force_new is not None:
        params["forceNew"] = "1" if force_new else "0"
    if skip_auto_detection_confirmation is not None:
        params["skipAutoDetectionConfirmation"] = "1" if skip_auto_detection_confirmation else "0"
    return params or None


def _deployment_body(
    *,
    request: DeploymentCreateRequest | None,
    body: dict[str, object] | None,
    name: str | None = None,
    project: str | None = None,
    target: str | None = None,
    files: tuple[dict[str, object], ...] | None = None,
) -> dict[str, object]:
    if body is not None:
        return dict(body)
    if request is None:
        request = DeploymentCreateRequest(
            name=name,
            project=project,
            target=target,
            files=files or (),
        )

    payload: dict[str, object] = {}
    for key, value in asdict(request).items():
        if value is not None and value != ():
            payload[key] = list(value) if key == "files" else value
    return payload


def _parse_deployment(payload: dict[str, Any]) -> Deployment:
    from vercel.stable.sdk.deployments import Deployment

    return Deployment(
        id=_required_str(payload, "id"),
        name=_optional_str(payload.get("name")),
        url=_optional_str(payload.get("url")),
        inspector_url=_optional_str(payload.get("inspectorUrl")),
        ready_state=_optional_str(payload.get("readyState")),
        raw=dict(payload),
    )


def _parse_uploaded_file(payload: dict[str, Any]) -> UploadedDeploymentFile:
    from vercel.stable.sdk.deployments import UploadedDeploymentFile

    return UploadedDeploymentFile(
        file_hash=_optional_str(payload.get("fileHash")),
        size=_optional_int(payload.get("size")),
        raw=dict(payload),
    )


def _required_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Expected deployments payload field {key!r} to be a non-empty string.")
    return value


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) else None


__all__ = ["DeploymentsBackend"]
