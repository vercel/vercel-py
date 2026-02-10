from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any, cast

from ..._iter_coroutine import iter_coroutine
from ..errors import BlobError
from ..types import MultipartCreateResult, MultipartPart, PutBlobResult
from ..utils import (
    PutHeaders,
    UploadProgressEvent,
    create_put_headers,
    ensure_token,
    require_public_access,
    validate_path,
)
from .core import (
    call_complete_multipart_upload,
    call_complete_multipart_upload_async,
    call_create_multipart_upload,
    call_create_multipart_upload_async,
    call_upload_part,
    call_upload_part_async,
)

SyncProgressCallback = Callable[[UploadProgressEvent], None]
AsyncProgressCallback = (
    Callable[[UploadProgressEvent], None] | Callable[[UploadProgressEvent], Awaitable[None]]
)
SyncPerPartProgressCallback = Callable[[int, UploadProgressEvent], None]
AsyncPerPartProgressCallback = (
    Callable[[int, UploadProgressEvent], None]
    | Callable[[int, UploadProgressEvent], Awaitable[None]]
)


def _validate_multipart_context(path: str, access: str, token: str | None) -> str:
    resolved_token = ensure_token(token)
    validate_path(path)
    require_public_access(access)
    return resolved_token


def _build_put_headers(
    *,
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


class _BaseMultipartApiClient:
    async def _call_create(
        self,
        path: str,
        headers: PutHeaders | dict[str, str],
        *,
        token: str | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    async def _call_upload(
        self,
        *,
        upload_id: str,
        key: str,
        path: str,
        headers: PutHeaders | dict[str, str],
        part_number: int,
        body: Any,
        on_upload_progress: AsyncProgressCallback | None = None,
        token: str | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    async def _call_complete(
        self,
        *,
        upload_id: str,
        key: str,
        path: str,
        headers: PutHeaders | dict[str, str],
        parts: list[dict[str, Any]],
        token: str | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    async def create_multipart_upload(
        self,
        path: str,
        *,
        access: str = "public",
        content_type: str | None = None,
        add_random_suffix: bool = False,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
        token: str | None = None,
    ) -> MultipartCreateResult:
        resolved_token = _validate_multipart_context(path, access, token)
        headers = _build_put_headers(
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
        )
        response = await self._call_create(path, headers, token=resolved_token)
        return _build_multipart_create_result(response)

    async def upload_part(
        self,
        path: str,
        body: Any,
        *,
        access: str = "public",
        token: str | None = None,
        upload_id: str,
        key: str,
        part_number: int,
        content_type: str | None = None,
        on_upload_progress: AsyncProgressCallback | None = None,
    ) -> MultipartPart:
        resolved_token = _validate_multipart_context(path, access, token)
        headers = _build_put_headers(content_type=content_type)
        response = await self._call_upload(
            upload_id=upload_id,
            key=key,
            path=path,
            headers=headers,
            token=resolved_token,
            part_number=part_number,
            body=body,
            on_upload_progress=on_upload_progress,
        )
        return _build_multipart_part_result(part_number, response)

    async def complete_multipart_upload(
        self,
        path: str,
        parts: list[MultipartPart],
        *,
        access: str = "public",
        content_type: str | None = None,
        token: str | None = None,
        upload_id: str,
        key: str,
    ) -> PutBlobResult:
        resolved_token = _validate_multipart_context(path, access, token)
        headers = _build_put_headers(content_type=content_type)
        response = await self._call_complete(
            upload_id=upload_id,
            key=key,
            path=path,
            headers=headers,
            token=resolved_token,
            parts=_normalize_complete_parts(parts),
        )
        return _build_put_blob_result(response)


class _SyncMultipartApiClient(_BaseMultipartApiClient):
    async def _call_create(
        self,
        path: str,
        headers: PutHeaders | dict[str, str],
        *,
        token: str | None = None,
    ) -> dict[str, Any]:
        return call_create_multipart_upload(path, headers, token=token)

    async def _call_upload(
        self,
        *,
        upload_id: str,
        key: str,
        path: str,
        headers: PutHeaders | dict[str, str],
        part_number: int,
        body: Any,
        on_upload_progress: AsyncProgressCallback | None = None,
        token: str | None = None,
    ) -> dict[str, Any]:
        return call_upload_part(
            upload_id=upload_id,
            key=key,
            path=path,
            headers=headers,
            token=token,
            part_number=part_number,
            body=body,
            on_upload_progress=cast(SyncProgressCallback | None, on_upload_progress),
        )

    async def _call_complete(
        self,
        *,
        upload_id: str,
        key: str,
        path: str,
        headers: PutHeaders | dict[str, str],
        parts: list[dict[str, Any]],
        token: str | None = None,
    ) -> dict[str, Any]:
        return call_complete_multipart_upload(
            upload_id=upload_id,
            key=key,
            path=path,
            headers=headers,
            token=token,
            parts=parts,
        )


class _AsyncMultipartApiClient(_BaseMultipartApiClient):
    async def _call_create(
        self,
        path: str,
        headers: PutHeaders | dict[str, str],
        *,
        token: str | None = None,
    ) -> dict[str, Any]:
        return await call_create_multipart_upload_async(path, headers, token=token)

    async def _call_upload(
        self,
        *,
        upload_id: str,
        key: str,
        path: str,
        headers: PutHeaders | dict[str, str],
        part_number: int,
        body: Any,
        on_upload_progress: AsyncProgressCallback | None = None,
        token: str | None = None,
    ) -> dict[str, Any]:
        return await call_upload_part_async(
            upload_id=upload_id,
            key=key,
            path=path,
            headers=headers,
            token=token,
            part_number=part_number,
            body=body,
            on_upload_progress=on_upload_progress,
        )

    async def _call_complete(
        self,
        *,
        upload_id: str,
        key: str,
        path: str,
        headers: PutHeaders | dict[str, str],
        parts: list[dict[str, Any]],
        token: str | None = None,
    ) -> dict[str, Any]:
        return await call_complete_multipart_upload_async(
            upload_id=upload_id,
            key=key,
            path=path,
            headers=headers,
            token=token,
            parts=parts,
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
) -> MultipartCreateResult:
    return iter_coroutine(
        _SyncMultipartApiClient().create_multipart_upload(
            path,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
        )
    )


async def create_multipart_upload_async(
    path: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> MultipartCreateResult:
    return await _AsyncMultipartApiClient().create_multipart_upload(
        path,
        access=access,
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        overwrite=overwrite,
        cache_control_max_age=cache_control_max_age,
        token=token,
    )


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
) -> MultipartPart:
    return iter_coroutine(
        _SyncMultipartApiClient().upload_part(
            path,
            body,
            access=access,
            token=token,
            upload_id=upload_id,
            key=key,
            part_number=part_number,
            content_type=content_type,
            on_upload_progress=on_upload_progress,
        )
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
    on_upload_progress: (
        Callable[[UploadProgressEvent], None]
        | Callable[[UploadProgressEvent], Awaitable[None]]
        | None
    ) = None,
) -> MultipartPart:
    return await _AsyncMultipartApiClient().upload_part(
        path,
        body,
        access=access,
        token=token,
        upload_id=upload_id,
        key=key,
        part_number=part_number,
        content_type=content_type,
        on_upload_progress=on_upload_progress,
    )


def complete_multipart_upload(
    path: str,
    parts: list[MultipartPart],
    *,
    access: str = "public",
    content_type: str | None = None,
    token: str | None = None,
    upload_id: str,
    key: str,
) -> PutBlobResult:
    return iter_coroutine(
        _SyncMultipartApiClient().complete_multipart_upload(
            path,
            parts,
            access=access,
            content_type=content_type,
            token=token,
            upload_id=upload_id,
            key=key,
        )
    )


async def complete_multipart_upload_async(
    path: str,
    parts: list[MultipartPart],
    *,
    access: str = "public",
    content_type: str | None = None,
    token: str | None = None,
    upload_id: str,
    key: str,
) -> PutBlobResult:
    return await _AsyncMultipartApiClient().complete_multipart_upload(
        path,
        parts,
        access=access,
        content_type=content_type,
        token=token,
        upload_id=upload_id,
        key=key,
    )


class _BaseMultipartUploader:
    def __init__(
        self,
        path: str,
        upload_id: str,
        key: str,
        headers: PutHeaders | dict[str, str],
        token: str | None,
    ):
        self._path = path
        self._upload_id = upload_id
        self._key = key
        self._headers: dict[str, str] = cast(dict[str, str], headers)
        self._token = token

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

        result = call_upload_part(
            upload_id=self._upload_id,
            key=self._key,
            path=self._path,
            headers=self._headers,
            part_number=part_number,
            body=body,
            on_upload_progress=effective,
            token=self._token,
        )
        return _build_multipart_part_result(part_number, result)

    def complete(self, parts: list[MultipartPart]) -> PutBlobResult:
        response = call_complete_multipart_upload(
            upload_id=self._upload_id,
            key=self._key,
            path=self._path,
            headers=self._headers,
            parts=_normalize_complete_parts(parts),
            token=self._token,
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

        response = await call_upload_part_async(
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
        response = await call_complete_multipart_upload_async(
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
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = True,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> MultipartUploader:
    resolved_token = _validate_multipart_context(path, access, token)
    headers = _build_put_headers(
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        overwrite=overwrite,
        cache_control_max_age=cache_control_max_age,
    )
    create_response = call_create_multipart_upload(path, headers, token=resolved_token)

    return MultipartUploader(
        path=path,
        upload_id=create_response["uploadId"],
        key=create_response["key"],
        headers=headers,
        token=resolved_token,
    )


async def create_multipart_uploader_async(
    path: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = True,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> AsyncMultipartUploader:
    resolved_token = _validate_multipart_context(path, access, token)
    headers = _build_put_headers(
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        overwrite=overwrite,
        cache_control_max_age=cache_control_max_age,
    )
    create_response = await call_create_multipart_upload_async(path, headers, token=resolved_token)

    return AsyncMultipartUploader(
        path=path,
        upload_id=create_response["uploadId"],
        key=create_response["key"],
        headers=headers,
        token=resolved_token,
    )
