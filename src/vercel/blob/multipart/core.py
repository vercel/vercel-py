from __future__ import annotations

from typing import Any, Callable, Awaitable
from urllib.parse import quote

from ..api import request_api, request_api_async
from ..utils import UploadProgressEvent


def _create_multipart_upload(
    path: str, headers: dict[str, str], *, token: str | None = None
) -> dict[str, str]:
    params = {"pathname": path}
    return request_api(
        "/mpu",
        "POST",
        options={"token": token} if token else {},
        headers={**headers, "x-mpu-action": "create"},
        params=params,
    )


async def _create_multipart_upload_async(
    path: str, headers: dict[str, str], *, token: str | None = None
) -> dict[str, str]:
    params = {"pathname": path}
    return await request_api_async(
        "/mpu",
        "POST",
        options={"token": token} if token else {},
        headers={**headers, "x-mpu-action": "create"},
        params=params,
    )


def _upload_part(
    *,
    upload_id: str,
    key: str,
    path: str,
    headers: dict[str, str],
    part_number: int,
    body: Any,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
    token: str | None = None,
):
    params = {"pathname": path}
    return request_api(
        "/mpu",
        "POST",
        options={"token": token} if token else {},
        headers={
            **headers,
            "x-mpu-action": "upload",
            "x-mpu-key": quote(key, safe=""),
            "x-mpu-upload-id": upload_id,
            "x-mpu-part-number": str(part_number),
        },
        params=params,
        body=body,
        on_upload_progress=on_upload_progress,
    )


async def _upload_part_async(
    *,
    upload_id: str,
    key: str,
    path: str,
    headers: dict[str, str],
    part_number: int,
    body: Any,
    on_upload_progress: Callable[[UploadProgressEvent], None]
    | Callable[[UploadProgressEvent], Awaitable[None]]
    | None = None,
    token: str | None = None,
):
    params = {"pathname": path}
    return await request_api_async(
        "/mpu",
        "POST",
        options={"token": token} if token else {},
        headers={
            **headers,
            "x-mpu-action": "upload",
            "x-mpu-key": quote(key, safe=""),
            "x-mpu-upload-id": upload_id,
            "x-mpu-part-number": str(part_number),
        },
        params=params,
        body=body,
        on_upload_progress=on_upload_progress,
    )


def _complete_multipart_upload(
    *,
    upload_id: str,
    key: str,
    path: str,
    headers: dict[str, str],
    parts: list[dict[str, Any]],
    token: str | None = None,
) -> dict[str, Any]:
    params = {"pathname": path}
    return request_api(
        "/mpu",
        "POST",
        options={"token": token} if token else {},
        headers={
            **headers,
            "content-type": "application/json",
            "x-mpu-action": "complete",
            "x-mpu-upload-id": upload_id,
            "x-mpu-key": quote(key, safe=""),
        },
        params=params,
        body=parts,
    )


async def _complete_multipart_upload_async(
    *,
    upload_id: str,
    key: str,
    path: str,
    headers: dict[str, str],
    parts: list[dict[str, Any]],
    token: str | None = None,
) -> dict[str, Any]:
    params = {"pathname": path}
    return await request_api_async(
        "/mpu",
        "POST",
        options={"token": token} if token else {},
        headers={
            **headers,
            "content-type": "application/json",
            "x-mpu-action": "complete",
            "x-mpu-upload-id": upload_id,
            "x-mpu-key": quote(key, safe=""),
        },
        params=params,
        body=parts,
    )
