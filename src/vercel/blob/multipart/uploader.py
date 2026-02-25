from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from vercel._internal.blob import compute_body_length
from vercel._internal.blob.multipart import (
    DEFAULT_PART_SIZE,
    MIN_PART_SIZE,
    _AsyncMultipartClient,
    _MultipartUploadSession,
    _SyncMultipartClient,
    _order_uploaded_parts,
    _prepare_upload_headers,
    _shape_complete_upload_result,
    _validate_part_size,
    create_async_multipart_upload_runtime,
    create_sync_multipart_upload_runtime,
)
from vercel._internal.iter_coroutine import iter_coroutine
from vercel.blob.types import Access, UploadProgressEvent


def auto_multipart_upload(
    path: str,
    body: Any,
    *,
    access: Access = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
    part_size: int = DEFAULT_PART_SIZE,
) -> dict[str, Any]:
    client = _SyncMultipartClient()
    headers = _prepare_upload_headers(
        access=access,
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        overwrite=overwrite,
        cache_control_max_age=cache_control_max_age,
    )
    part_size = _validate_part_size(part_size)

    create_response = iter_coroutine(client.create_multipart_upload(path, headers, token=token))
    session = _MultipartUploadSession(
        upload_id=create_response["uploadId"],
        key=create_response["key"],
        path=path,
        headers=headers,
        token=token,
    )

    runtime = create_sync_multipart_upload_runtime()
    total = compute_body_length(body)
    parts = runtime.upload(
        session=session,
        body=body,
        part_size=part_size,
        total=total,
        on_upload_progress=on_upload_progress,
        upload_part_fn=lambda **kwargs: iter_coroutine(client.upload_part(**kwargs)),
    )
    ordered_parts = _order_uploaded_parts(parts)

    complete_response = iter_coroutine(
        client.complete_multipart_upload(
            upload_id=session.upload_id,
            key=session.key,
            path=session.path,
            headers=session.headers,
            token=session.token,
            parts=ordered_parts,
        )
    )
    return _shape_complete_upload_result(complete_response)


async def auto_multipart_upload_async(
    path: str,
    body: Any,
    *,
    access: Access = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    on_upload_progress: (
        Callable[[UploadProgressEvent], None]
        | Callable[[UploadProgressEvent], Awaitable[None]]
        | None
    ) = None,
    part_size: int = DEFAULT_PART_SIZE,
) -> dict[str, Any]:
    client = _AsyncMultipartClient()
    headers = _prepare_upload_headers(
        access=access,
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        overwrite=overwrite,
        cache_control_max_age=cache_control_max_age,
    )
    part_size = _validate_part_size(part_size)

    create_response = await client.create_multipart_upload(path, headers, token=token)
    session = _MultipartUploadSession(
        upload_id=create_response["uploadId"],
        key=create_response["key"],
        path=path,
        headers=headers,
        token=token,
    )

    runtime = create_async_multipart_upload_runtime()
    total = compute_body_length(body)
    parts = await runtime.upload(
        session=session,
        body=body,
        part_size=part_size,
        total=total,
        on_upload_progress=on_upload_progress,
        upload_part_fn=client.upload_part,
    )
    ordered_parts = _order_uploaded_parts(parts)

    complete_response = await client.complete_multipart_upload(
        upload_id=session.upload_id,
        key=session.key,
        path=session.path,
        headers=session.headers,
        token=session.token,
        parts=ordered_parts,
    )
    return _shape_complete_upload_result(complete_response)
