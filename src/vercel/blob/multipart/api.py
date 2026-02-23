from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any, cast

from ..._iter_coroutine import iter_coroutine
from ..errors import BlobError
from ..types import MultipartCreateResult, MultipartPart, PutBlobResult
from ..utils import (
    Access,
    PutHeaders,
    UploadProgressEvent,
    create_put_headers,
    ensure_token,
    validate_access,
    validate_path,
)
from .core import _AsyncMultipartClient, _BaseMultipartClient, _SyncMultipartClient


def _validate_multipart_context(path: str, access: Access, token: str | None) -> str:
    resolved_token = ensure_token(token)
    validate_path(path)
    validate_access(access)
    return resolved_token


def _build_put_headers(
    *,
    access: Access,
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
) -> dict[str, str]:
    return cast(
        dict[str, str],
        create_put_headers(
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            allow_overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            access=access,
        ),
    )


def _build_multipart_create_result(response: dict[str, Any]) -> MultipartCreateResult:
    return MultipartCreateResult(upload_id=response["uploadId"], key=response["key"])


def _build_multipart_part_result(part_number: int, response: dict[str, Any]) -> MultipartPart:
    return MultipartPart(part_number=part_number, etag=response["etag"])


def _build_put_blob_result(response: dict[str, Any]) -> PutBlobResult:
    return PutBlobResult(
        url=response["url"],
        download_url=response["downloadUrl"],
        pathname=response["pathname"],
        content_type=response["contentType"],
        content_disposition=response["contentDisposition"],
    )


def _normalize_complete_parts(parts: list[MultipartPart]) -> list[dict[str, Any]]:
    return [{"partNumber": part.part_number, "etag": part.etag} for part in parts]


def _validate_part_upload_inputs(part_number: int, body: Any) -> None:
    if part_number < 1 or part_number > 10000:
        raise BlobError("part_number must be between 1 and 10,000")

    if isinstance(body, dict) and not hasattr(body, "read"):
        raise BlobError(
            "Body must be a string, bytes, or file-like object. "
            "You sent a plain dictionary, double check what you're trying to upload."
        )


def create_multipart_upload(
    path: str,
    *,
    access: Access = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> MultipartCreateResult:
    resolved_token = _validate_multipart_context(path, access, token)
    headers = _build_put_headers(
        access=access,
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        overwrite=overwrite,
        cache_control_max_age=cache_control_max_age,
    )
    response = iter_coroutine(
        _SyncMultipartClient().create_multipart_upload(path, headers, token=resolved_token)
    )
    return _build_multipart_create_result(response)


async def create_multipart_upload_async(
    path: str,
    *,
    access: Access = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> MultipartCreateResult:
    resolved_token = _validate_multipart_context(path, access, token)
    headers = _build_put_headers(
        access=access,
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        overwrite=overwrite,
        cache_control_max_age=cache_control_max_age,
    )
    response = await _AsyncMultipartClient().create_multipart_upload(
        path, headers, token=resolved_token
    )
    return _build_multipart_create_result(response)


def upload_part(
    path: str,
    body: Any,
    *,
    access: Access = "public",
    token: str | None = None,
    upload_id: str,
    key: str,
    part_number: int,
    content_type: str | None = None,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
) -> MultipartPart:
    resolved_token = _validate_multipart_context(path, access, token)
    _validate_part_upload_inputs(part_number, body)
    headers = _build_put_headers(access=access, content_type=content_type)
    response = iter_coroutine(
        _SyncMultipartClient().upload_part(
            upload_id=upload_id,
            key=key,
            path=path,
            headers=headers,
            part_number=part_number,
            body=body,
            on_upload_progress=on_upload_progress,
            token=resolved_token,
        ),
    )
    return _build_multipart_part_result(part_number, response)


async def upload_part_async(
    path: str,
    body: Any,
    *,
    access: Access = "public",
    token: str | None = None,
    upload_id: str,
    key: str,
    part_number: int,
    content_type: str | None = None,
    on_upload_progress: (
        Callable[[UploadProgressEvent], None]
        | Callable[[UploadProgressEvent], Awaitable[None]]
        | None
    ) = None,
) -> MultipartPart:
    resolved_token = _validate_multipart_context(path, access, token)
    _validate_part_upload_inputs(part_number, body)
    headers = _build_put_headers(access=access, content_type=content_type)
    response = await _AsyncMultipartClient().upload_part(
        upload_id=upload_id,
        key=key,
        path=path,
        headers=headers,
        part_number=part_number,
        body=body,
        on_upload_progress=on_upload_progress,
        token=resolved_token,
    )
    return _build_multipart_part_result(part_number, response)


def complete_multipart_upload(
    path: str,
    parts: list[MultipartPart],
    *,
    access: Access = "public",
    content_type: str | None = None,
    token: str | None = None,
    upload_id: str,
    key: str,
) -> PutBlobResult:
    resolved_token = _validate_multipart_context(path, access, token)
    headers = _build_put_headers(access=access, content_type=content_type)
    response = iter_coroutine(
        _SyncMultipartClient().complete_multipart_upload(
            upload_id=upload_id,
            key=key,
            path=path,
            headers=headers,
            parts=_normalize_complete_parts(parts),
            token=resolved_token,
        ),
    )
    return _build_put_blob_result(response)


async def complete_multipart_upload_async(
    path: str,
    parts: list[MultipartPart],
    *,
    access: Access = "public",
    content_type: str | None = None,
    token: str | None = None,
    upload_id: str,
    key: str,
) -> PutBlobResult:
    resolved_token = _validate_multipart_context(path, access, token)
    headers = _build_put_headers(access=access, content_type=content_type)
    response = await _AsyncMultipartClient().complete_multipart_upload(
        upload_id=upload_id,
        key=key,
        path=path,
        headers=headers,
        parts=_normalize_complete_parts(parts),
        token=resolved_token,
    )
    return _build_put_blob_result(response)


class _BaseMultipartUploader:
    def __init__(
        self,
        path: str,
        upload_id: str,
        key: str,
        headers: PutHeaders | dict[str, str],
        token: str | None,
        multipart_client: _BaseMultipartClient,
    ):
        self._path = path
        self._upload_id = upload_id
        self._key = key
        self._headers: dict[str, str] = cast(dict[str, str], headers)
        self._token = token
        self._multipart_client = multipart_client

    @property
    def upload_id(self) -> str:
        """The upload ID for this multipart upload."""
        return self._upload_id

    @property
    def key(self) -> str:
        """The key (blob identifier) for this multipart upload."""
        return self._key


class MultipartUploader(_BaseMultipartUploader):
    def upload_part(
        self,
        part_number: int,
        body: Any,
        *,
        on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
        per_part_progress: Callable[[int, UploadProgressEvent], None] | None = None,
    ) -> MultipartPart:
        _validate_part_upload_inputs(part_number, body)

        effective = on_upload_progress
        if per_part_progress is not None and on_upload_progress is None:

            def effective(evt: UploadProgressEvent) -> None:
                per_part_progress(part_number, evt)

        result = iter_coroutine(
            self._multipart_client.upload_part(
                upload_id=self._upload_id,
                key=self._key,
                path=self._path,
                headers=self._headers,
                part_number=part_number,
                body=body,
                on_upload_progress=effective,
                token=self._token,
            )
        )
        return _build_multipart_part_result(part_number, result)

    def complete(self, parts: list[MultipartPart]) -> PutBlobResult:
        response = iter_coroutine(
            self._multipart_client.complete_multipart_upload(
                upload_id=self._upload_id,
                key=self._key,
                path=self._path,
                headers=self._headers,
                parts=_normalize_complete_parts(parts),
                token=self._token,
            )
        )
        return _build_put_blob_result(response)


class AsyncMultipartUploader(_BaseMultipartUploader):
    async def upload_part(
        self,
        part_number: int,
        body: Any,
        *,
        on_upload_progress: (
            Callable[[UploadProgressEvent], None]
            | Callable[[UploadProgressEvent], Awaitable[None]]
            | None
        ) = None,
        per_part_progress: (
            Callable[[int, UploadProgressEvent], None]
            | Callable[[int, UploadProgressEvent], Awaitable[None]]
            | None
        ) = None,
    ) -> MultipartPart:
        _validate_part_upload_inputs(part_number, body)

        effective_progress = on_upload_progress
        if per_part_progress is not None and on_upload_progress is None:

            async def effective_progress(evt: UploadProgressEvent):
                result = per_part_progress(part_number, evt)
                if inspect.isawaitable(result):
                    await result

        response = await self._multipart_client.upload_part(
            upload_id=self._upload_id,
            key=self._key,
            path=self._path,
            headers=self._headers,
            part_number=part_number,
            body=body,
            on_upload_progress=effective_progress,
            token=self._token,
        )
        return _build_multipart_part_result(part_number, response)

    async def complete(self, parts: list[MultipartPart]) -> PutBlobResult:
        response = await self._multipart_client.complete_multipart_upload(
            upload_id=self._upload_id,
            key=self._key,
            path=self._path,
            headers=self._headers,
            parts=_normalize_complete_parts(parts),
            token=self._token,
        )
        return _build_put_blob_result(response)


def create_multipart_uploader(
    path: str,
    *,
    access: Access = "public",
    content_type: str | None = None,
    add_random_suffix: bool = True,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    multipart_client: _BaseMultipartClient | None = None,
) -> MultipartUploader:
    resolved_token = _validate_multipart_context(path, access, token)
    headers = _build_put_headers(
        access=access,
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        overwrite=overwrite,
        cache_control_max_age=cache_control_max_age,
    )
    effective_multipart_client = multipart_client or _SyncMultipartClient()
    create_response = iter_coroutine(
        effective_multipart_client.create_multipart_upload(path, headers, token=resolved_token)
    )

    return MultipartUploader(
        path=path,
        upload_id=create_response["uploadId"],
        key=create_response["key"],
        headers=headers,
        token=resolved_token,
        multipart_client=effective_multipart_client,
    )


async def create_multipart_uploader_async(
    path: str,
    *,
    access: Access = "public",
    content_type: str | None = None,
    add_random_suffix: bool = True,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    multipart_client: _BaseMultipartClient | None = None,
) -> AsyncMultipartUploader:
    resolved_token = _validate_multipart_context(path, access, token)
    headers = _build_put_headers(
        access=access,
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        overwrite=overwrite,
        cache_control_max_age=cache_control_max_age,
    )
    effective_multipart_client = multipart_client or _AsyncMultipartClient()
    create_response = await effective_multipart_client.create_multipart_upload(
        path, headers, token=resolved_token
    )

    return AsyncMultipartUploader(
        path=path,
        upload_id=create_response["uploadId"],
        key=create_response["key"],
        headers=headers,
        token=resolved_token,
        multipart_client=effective_multipart_client,
    )
