from __future__ import annotations

from typing import Any, Callable, Awaitable

from ..utils import UploadProgressEvent, create_put_headers, create_put_options
from .core import (
    _create_multipart_upload,
    _create_multipart_upload_async,
    _upload_part,
    _complete_multipart_upload,
    _upload_part_async,
    _complete_multipart_upload_async,
)


def create_multipart_upload(
    path: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> dict[str, str]:
    options: dict[str, Any] = {
        "access": access,
        "contentType": content_type,
        "addRandomSuffix": add_random_suffix,
        "allowOverwrite": overwrite,
        "cacheControlMaxAge": cache_control_max_age,
        "token": token,
    }
    opts = create_put_options(path=path, options=options)
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )
    return _create_multipart_upload(path, headers, token=opts.get("token"))


async def create_multipart_upload_async(
    path: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> dict[str, str]:
    options: dict[str, Any] = {
        "access": access,
        "contentType": content_type,
        "addRandomSuffix": add_random_suffix,
        "allowOverwrite": overwrite,
        "cacheControlMaxAge": cache_control_max_age,
        "token": token,
    }
    opts = create_put_options(path=path, options=options)
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )
    return await _create_multipart_upload_async(path, headers, token=opts.get("token"))


def upload_part(
    path: str,
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
    opts = create_put_options(path=path, options=options)
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )
    return _upload_part(
        upload_id=opts["uploadId"],
        key=opts["key"],
        path=path,
        headers=headers,
        token=opts.get("token"),
        part_number=opts["partNumber"],
        body=body,
        on_upload_progress=on_upload_progress,
    )


async def upload_part_async(
    path: str,
    body: Any,
    *,
    access: str = "public",
    token: str | None = None,
    upload_id: str,
    key: str,
    part_number: int,
    content_type: str | None = None,
    on_upload_progress: Callable[[UploadProgressEvent], None]
    | Callable[[UploadProgressEvent], Awaitable[None]]
    | None = None,
) -> dict[str, Any]:
    options: dict[str, Any] = {
        "access": access,
        "contentType": content_type,
        "token": token,
        "uploadId": upload_id,
        "key": key,
        "partNumber": part_number,
    }
    opts = create_put_options(path=path, options=options)
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )
    return await _upload_part_async(
        upload_id=opts["uploadId"],
        key=opts["key"],
        path=path,
        headers=headers,
        token=opts.get("token"),
        part_number=opts["partNumber"],
        body=body,
        on_upload_progress=on_upload_progress,
    )


def complete_multipart_upload(
    path: str,
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
    opts = create_put_options(path=path, options=options)
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )
    return _complete_multipart_upload(
        upload_id=opts["uploadId"],
        key=opts["key"],
        path=path,
        headers=headers,
        token=opts.get("token"),
        parts=parts,
    )


async def complete_multipart_upload_async(
    path: str,
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
    opts = create_put_options(path=path, options=options)
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )
    return await _complete_multipart_upload_async(
        upload_id=opts["uploadId"],
        key=opts["key"],
        path=path,
        headers=headers,
        token=opts.get("token"),
        parts=parts,
    )
