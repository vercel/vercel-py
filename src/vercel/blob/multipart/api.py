from __future__ import annotations

from typing import Any, Callable

from .._put_helpers import create_put_headers, create_put_options
from .._helpers import UploadProgressEvent
from .core import (
    _create_multipart_upload as _create_mpu,
    _upload_part as _upload_part,
    _complete_multipart_upload as _complete_mpu,
)


async def create_multipart_upload(
    pathname: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    allow_overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> dict[str, str]:
    options: dict[str, Any] = {
        "access": access,
        "contentType": content_type,
        "addRandomSuffix": add_random_suffix,
        "allowOverwrite": allow_overwrite,
        "cacheControlMaxAge": cache_control_max_age,
        "token": token,
    }
    opts = await create_put_options(pathname=pathname, options=options)
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )
    return await _create_mpu(pathname, headers, token=opts.get("token"))


async def upload_part(
    pathname: str,
    body: Any,
    *,
    access: str = "public",
    token: str | None = None,
    upload_id: str,
    key: str,
    part_number: int,
    content_type: str | None = None,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
) -> dict[str, Any]:
    options: dict[str, Any] = {
        "access": access,
        "contentType": content_type,
        "token": token,
        "uploadId": upload_id,
        "key": key,
        "partNumber": part_number,
    }
    opts = await create_put_options(pathname=pathname, options=options)
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )
    return await _upload_part(
        upload_id=opts["uploadId"],
        key=opts["key"],
        pathname=pathname,
        headers=headers,
        token=opts.get("token"),
        part_number=opts["partNumber"],
        body=body,
        on_upload_progress=on_upload_progress,
    )


async def complete_multipart_upload(
    pathname: str,
    parts: list[dict[str, Any]],
    *,
    access: str = "public",
    content_type: str | None = None,
    token: str | None = None,
    upload_id: str,
    key: str,
) -> dict[str, Any]:
    options: dict[str, Any] = {
        "access": access,
        "contentType": content_type,
        "token": token,
        "uploadId": upload_id,
        "key": key,
    }
    opts = await create_put_options(pathname=pathname, options=options)
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )
    return await _complete_mpu(
        upload_id=opts["uploadId"],
        key=opts["key"],
        pathname=pathname,
        headers=headers,
        token=opts.get("token"),
        parts=parts,
    )
