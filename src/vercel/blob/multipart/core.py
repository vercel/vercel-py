from __future__ import annotations

from typing import Any, Callable, Awaitable, cast
from urllib.parse import quote

from ..api import request_api, request_api_async
from ..utils import UploadProgressEvent, PutHeaders


def call_create_multipart_upload(
    path: str, headers: PutHeaders | dict[str, str], *, token: str | None = None
) -> dict[str, str]:
    params = {"pathname": path}
    request_headers = cast(dict[str, str], headers).copy()
    request_headers["x-mpu-action"] = "create"
    return request_api(
        "/mpu",
        "POST",
        token=token,
        headers=request_headers,
        params=params,
    )


async def call_create_multipart_upload_async(
    path: str, headers: PutHeaders | dict[str, str], *, token: str | None = None
) -> dict[str, str]:
    params = {"pathname": path}
    request_headers = cast(dict[str, str], headers).copy()
    request_headers["x-mpu-action"] = "create"
    return await request_api_async(
        "/mpu",
        "POST",
        token=token,
        headers=request_headers,
        params=params,
    )


def call_upload_part(
    *,
    upload_id: str,
    key: str,
    path: str,
    headers: PutHeaders | dict[str, str],
    part_number: int,
    body: Any,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
    token: str | None = None,
):
    params = {"pathname": path}
    request_headers = cast(dict[str, str], headers).copy()
    request_headers["x-mpu-action"] = "upload"
    request_headers["x-mpu-key"] = quote(key, safe="")
    request_headers["x-mpu-upload-id"] = upload_id
    request_headers["x-mpu-part-number"] = str(part_number)
    return request_api(
        "/mpu",
        "POST",
        token=token,
        headers=request_headers,
        params=params,
        body=body,
        on_upload_progress=on_upload_progress,
    )


async def call_upload_part_async(
    *,
    upload_id: str,
    key: str,
    path: str,
    headers: PutHeaders | dict[str, str],
    part_number: int,
    body: Any,
    on_upload_progress: (
        Callable[[UploadProgressEvent], None]
        | Callable[[UploadProgressEvent], Awaitable[None]]
        | None
    ) = None,
    token: str | None = None,
):
    params = {"pathname": path}
    request_headers = cast(dict[str, str], headers).copy()
    request_headers["x-mpu-action"] = "upload"
    request_headers["x-mpu-key"] = quote(key, safe="")
    request_headers["x-mpu-upload-id"] = upload_id
    request_headers["x-mpu-part-number"] = str(part_number)
    return await request_api_async(
        "/mpu",
        "POST",
        token=token,
        headers=request_headers,
        params=params,
        body=body,
        on_upload_progress=on_upload_progress,
    )


def call_complete_multipart_upload(
    *,
    upload_id: str,
    key: str,
    path: str,
    headers: PutHeaders | dict[str, str],
    parts: list[dict[str, Any]],
    token: str | None = None,
) -> dict[str, Any]:
    params = {"pathname": path}
    request_headers = cast(dict[str, str], headers).copy()
    request_headers["content-type"] = "application/json"
    request_headers["x-mpu-action"] = "complete"
    request_headers["x-mpu-upload-id"] = upload_id
    request_headers["x-mpu-key"] = quote(key, safe="")
    return request_api(
        "/mpu",
        "POST",
        token=token,
        headers=request_headers,
        params=params,
        body=parts,
    )


async def call_complete_multipart_upload_async(
    *,
    upload_id: str,
    key: str,
    path: str,
    headers: PutHeaders | dict[str, str],
    parts: list[dict[str, Any]],
    token: str | None = None,
) -> dict[str, Any]:
    params = {"pathname": path}
    request_headers = cast(dict[str, str], headers).copy()
    request_headers["content-type"] = "application/json"
    request_headers["x-mpu-action"] = "complete"
    request_headers["x-mpu-upload-id"] = upload_id
    request_headers["x-mpu-key"] = quote(key, safe="")
    return await request_api_async(
        "/mpu",
        "POST",
        token=token,
        headers=request_headers,
        params=params,
        body=parts,
    )
