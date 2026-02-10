from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, cast
from urllib.parse import quote

from ..._iter_coroutine import iter_coroutine
from ..api import request_api, request_api_async
from ..utils import PutHeaders, UploadProgressEvent

SyncProgressCallback = Callable[[UploadProgressEvent], None]
AsyncProgressCallback = (
    Callable[[UploadProgressEvent], None] | Callable[[UploadProgressEvent], Awaitable[None]]
)


def _build_headers(
    headers: PutHeaders | dict[str, str],
    *,
    action: str,
    key: str | None = None,
    upload_id: str | None = None,
    part_number: int | None = None,
    set_json_content_type: bool = False,
) -> dict[str, str]:
    request_headers = cast(dict[str, str], headers).copy()
    if set_json_content_type:
        request_headers["content-type"] = "application/json"

    request_headers["x-mpu-action"] = action
    if key is not None:
        request_headers["x-mpu-key"] = quote(key, safe="")
    if upload_id is not None:
        request_headers["x-mpu-upload-id"] = upload_id
    if part_number is not None:
        request_headers["x-mpu-part-number"] = str(part_number)

    return request_headers


class _BaseMultipartClient:
    async def _request_api(self, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def create_multipart_upload(
        self,
        path: str,
        headers: PutHeaders | dict[str, str],
        *,
        token: str | None = None,
    ) -> dict[str, str]:
        response = await self._request_api(
            pathname="/mpu",
            method="POST",
            token=token,
            headers=_build_headers(headers, action="create"),
            params={"pathname": path},
        )
        return cast(dict[str, str], response)

    async def upload_part(
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
        response = await self._request_api(
            pathname="/mpu",
            method="POST",
            token=token,
            headers=_build_headers(
                headers,
                action="upload",
                key=key,
                upload_id=upload_id,
                part_number=part_number,
            ),
            params={"pathname": path},
            body=body,
            on_upload_progress=on_upload_progress,
        )
        return cast(dict[str, Any], response)

    async def complete_multipart_upload(
        self,
        *,
        upload_id: str,
        key: str,
        path: str,
        headers: PutHeaders | dict[str, str],
        parts: list[dict[str, Any]],
        token: str | None = None,
    ) -> dict[str, Any]:
        response = await self._request_api(
            pathname="/mpu",
            method="POST",
            token=token,
            headers=_build_headers(
                headers,
                action="complete",
                key=key,
                upload_id=upload_id,
                set_json_content_type=True,
            ),
            params={"pathname": path},
            body=parts,
        )
        return cast(dict[str, Any], response)


class _SyncMultipartClient(_BaseMultipartClient):
    async def _request_api(self, **kwargs: Any) -> Any:
        return request_api(**kwargs)


class _AsyncMultipartClient(_BaseMultipartClient):
    async def _request_api(self, **kwargs: Any) -> Any:
        return await request_api_async(**kwargs)


def call_create_multipart_upload(
    path: str, headers: PutHeaders | dict[str, str], *, token: str | None = None
) -> dict[str, str]:
    return iter_coroutine(
        _SyncMultipartClient().create_multipart_upload(path, headers, token=token)
    )


async def call_create_multipart_upload_async(
    path: str, headers: PutHeaders | dict[str, str], *, token: str | None = None
) -> dict[str, str]:
    return await _AsyncMultipartClient().create_multipart_upload(path, headers, token=token)


def call_upload_part(
    *,
    upload_id: str,
    key: str,
    path: str,
    headers: PutHeaders | dict[str, str],
    part_number: int,
    body: Any,
    on_upload_progress: SyncProgressCallback | None = None,
    token: str | None = None,
) -> dict[str, Any]:
    return iter_coroutine(
        _SyncMultipartClient().upload_part(
            upload_id=upload_id,
            key=key,
            path=path,
            headers=headers,
            part_number=part_number,
            body=body,
            on_upload_progress=on_upload_progress,
            token=token,
        )
    )


async def call_upload_part_async(
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
    return await _AsyncMultipartClient().upload_part(
        upload_id=upload_id,
        key=key,
        path=path,
        headers=headers,
        part_number=part_number,
        body=body,
        on_upload_progress=on_upload_progress,
        token=token,
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
    return iter_coroutine(
        _SyncMultipartClient().complete_multipart_upload(
            upload_id=upload_id,
            key=key,
            path=path,
            headers=headers,
            parts=parts,
            token=token,
        )
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
    return await _AsyncMultipartClient().complete_multipart_upload(
        upload_id=upload_id,
        key=key,
        path=path,
        headers=headers,
        parts=parts,
        token=token,
    )
