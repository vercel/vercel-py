from __future__ import annotations

from typing import Any
from urllib.parse import quote

from .._request import request_api
from .._helpers import UploadProgressEvent


async def _create_multipart_upload(
    pathname: str, headers: dict[str, str], *, token: str | None = None
) -> dict[str, str]:
    params = {"pathname": pathname}
    return await request_api(
        "/mpu",
        "POST",
        options={"token": token} if token else {},
        headers={**headers, "x-mpu-action": "create"},
        params=params,
    )


async def _upload_part(
    *,
    upload_id: str,
    key: str,
    pathname: str,
    headers: dict[str, str],
    part_number: int,
    body: Any,
    on_upload_progress: callable[[UploadProgressEvent], None] | None = None,
    token: str | None = None,
):
    params = {"pathname": pathname}
    return await request_api(
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


async def _complete_multipart_upload(
    *,
    upload_id: str,
    key: str,
    pathname: str,
    headers: dict[str, str],
    parts: list[dict[str, Any]],
    token: str | None = None,
) -> dict[str, Any]:
    params = {"pathname": pathname}
    return await request_api(
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
