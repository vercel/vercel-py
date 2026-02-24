from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any, cast
from urllib.parse import quote

from vercel._internal.blob import PutHeaders
from vercel.blob.types import UploadProgressEvent

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
    def __init__(
        self,
        request_api_fn: Callable[..., Any] | Callable[..., Awaitable[Any]],
    ) -> None:
        self._request_api_fn = request_api_fn

    async def _request_api(self, **kwargs: Any) -> Any:
        result = self._request_api_fn(**kwargs)
        if inspect.isawaitable(result):
            return await cast(Awaitable[Any], result)
        return result

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
    def __init__(
        self,
        request_api_fn: Callable[..., Any] | Callable[..., Awaitable[Any]] | None = None,
    ) -> None:
        if request_api_fn is not None:
            super().__init__(request_api_fn)
        else:
            from vercel.blob.api import request_api

            super().__init__(request_api)


class _AsyncMultipartClient(_BaseMultipartClient):
    def __init__(
        self,
        request_api_fn: Callable[..., Any] | Callable[..., Awaitable[Any]] | None = None,
    ) -> None:
        if request_api_fn is not None:
            super().__init__(request_api_fn)
        else:
            from vercel.blob.api import request_api_async

            super().__init__(request_api_async)
