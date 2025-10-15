from __future__ import annotations

import os
from os import PathLike
from typing import Any, Callable, Awaitable, Iterable, Iterator

from .errors import BlobError
from .utils import UploadProgressEvent
from .types import (
    PutBlobResult as PutBlobResultType,
    HeadBlobResult as HeadBlobResultType,
    ListBlobResult as ListBlobResultType,
    ListBlobItem,
    CreateFolderResult as CreateFolderResultType,
)
from .ops import (
    put,
    put_async,
    delete,
    delete_async,
    head,
    head_async,
    get,
    get_async,
    list_objects,
    list_objects_async,
    copy,
    copy_async,
    create_folder,
    create_folder_async,
    download_file,
    download_file_async,
    upload_file,
    upload_file_async,
    iter_objects,
    iter_objects_async,
)


class BlobClient:
    def __init__(self, token: str | None = None):
        self.token = token or os.getenv("BLOB_READ_WRITE_TOKEN")
        if not self.token:
            raise BlobError(
                "No token found. Either configure the `BLOB_READ_WRITE_TOKEN` environment variable, or pass a `token` option to your calls."
            )

    def put(
        self,
        path: str,
        body: Any,
        *,
        access: str = "public",
        content_type: str | None = None,
        add_random_suffix: bool = False,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
        multipart: bool = False,
        on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
    ) -> PutBlobResultType:
        return put(
            path=path,
            body=body,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=self.token,
            multipart=multipart,
            on_upload_progress=on_upload_progress,
        )

    def get(self, url_or_path: str, *, timeout: float | None = None) -> bytes:
        return get(url_or_path, token=self.token, timeout=timeout)

    def head(self, url_or_path: str) -> HeadBlobResultType:
        return head(url_or_path, token=self.token)

    def delete(self, url_or_path: str | Iterable[str]) -> None:
        return delete(url_or_path, token=self.token)

    def list_objects(
        self,
        *,
        limit: int | None = None,
        prefix: str | None = None,
        cursor: str | None = None,
        mode: str | None = None,
    ) -> ListBlobResultType:
        return list_objects(limit=limit, prefix=prefix, cursor=cursor, mode=mode, token=self.token)

    def iter_objects(
        self,
        *,
        prefix: str | None = None,
        mode: str | None = None,
        batch_size: int | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Iterator[ListBlobItem]:
        return iter_objects(
            prefix=prefix,
            mode=mode,
            token=self.token,
            batch_size=batch_size,
            limit=limit,
            cursor=cursor,
        )

    def copy(
        self,
        src_path: str,
        dst_path: str,
        *,
        access: str = "public",
        content_type: str | None = None,
        add_random_suffix: bool = False,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
    ) -> PutBlobResultType:
        return copy(
            src_path,
            dst_path,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=self.token,
        )

    def create_folder(self, path: str, *, overwrite: bool = False) -> CreateFolderResultType:
        return create_folder(path, token=self.token, overwrite=overwrite)

    def download_file(
        self,
        url_or_path: str,
        local_path: str | PathLike,
        *,
        timeout: float | None = None,
        overwrite: bool = True,
        create_parents: bool = True,
        progress: Callable[[int, int | None], None] | None = None,
    ) -> str:
        return download_file(
            url_or_path,
            local_path,
            token=self.token,
            timeout=timeout,
            overwrite=overwrite,
            create_parents=create_parents,
            progress=progress,
        )

    def upload_file(
        self,
        local_path: str | PathLike,
        path: str,
        *,
        access: str = "public",
        content_type: str | None = None,
        add_random_suffix: bool = False,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
        multipart: bool = False,
        on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
    ) -> PutBlobResultType:
        return upload_file(
            local_path,
            path,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=self.token,
            multipart=multipart,
            on_upload_progress=on_upload_progress,
        )


class AsyncBlobClient:
    def __init__(self, token: str | None = None):
        self.token = token or os.getenv("BLOB_READ_WRITE_TOKEN")
        if not self.token:
            raise BlobError(
                "No token found. Either configure the `BLOB_READ_WRITE_TOKEN` environment variable, or pass a `token` option to your calls."
            )

    async def put(
        self,
        path: str,
        body: Any,
        *,
        access: str = "public",
        content_type: str | None = None,
        add_random_suffix: bool = False,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
        multipart: bool = False,
        on_upload_progress: Callable[[UploadProgressEvent], None]
        | Callable[[UploadProgressEvent], Awaitable[None]]
        | None = None,
    ) -> PutBlobResultType:
        return await put_async(
            path=path,
            body=body,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=self.token,
            multipart=multipart,
            on_upload_progress=on_upload_progress,
        )

    async def get(self, url_or_path: str, *, timeout: float | None = None) -> bytes:
        return await get_async(url_or_path, token=self.token, timeout=timeout)

    async def head(self, url_or_path: str) -> HeadBlobResultType:
        return await head_async(url_or_path, token=self.token)

    async def delete(self, url_or_path: str | Iterable[str]) -> None:
        return await delete_async(url_or_path, token=self.token)

    async def iter_objects(
        self,
        *,
        prefix: str | None = None,
        mode: str | None = None,
        batch_size: int | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ):
        return iter_objects_async(
            prefix=prefix,
            mode=mode,
            token=self.token,
            batch_size=batch_size,
            limit=limit,
            cursor=cursor,
        )

    async def list_objects(
        self,
        *,
        limit: int | None = None,
        prefix: str | None = None,
        cursor: str | None = None,
        mode: str | None = None,
    ) -> ListBlobResultType:
        return await list_objects_async(
            limit=limit, prefix=prefix, cursor=cursor, mode=mode, token=self.token
        )

    async def create_folder(self, path: str, *, overwrite: bool = False) -> CreateFolderResultType:
        return await create_folder_async(path, token=self.token, overwrite=overwrite)

    async def copy(
        self,
        src_path: str,
        dst_path: str,
        *,
        access: str = "public",
        content_type: str | None = None,
        add_random_suffix: bool = False,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
    ) -> PutBlobResultType:
        return await copy_async(
            src_path,
            dst_path,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=self.token,
        )

    async def download_file(
        self,
        url_or_path: str,
        local_path: str | PathLike,
        *,
        timeout: float | None = None,
        overwrite: bool = True,
        create_parents: bool = True,
        progress: Callable[[int, int | None], None]
        | Callable[[int, int | None], Awaitable[None]]
        | None = None,
    ) -> str:
        return await download_file_async(
            url_or_path,
            local_path,
            token=self.token,
            timeout=timeout,
            overwrite=overwrite,
            create_parents=create_parents,
            progress=progress,
        )

    async def upload_file(
        self,
        local_path: str | PathLike,
        path: str,
        *,
        access: str = "public",
        content_type: str | None = None,
        add_random_suffix: bool = False,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
        multipart: bool = False,
        on_upload_progress: Callable[[UploadProgressEvent], None]
        | Callable[[UploadProgressEvent], Awaitable[None]]
        | None = None,
    ) -> PutBlobResultType:
        return await upload_file_async(
            local_path,
            path,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=self.token,
            multipart=multipart,
            on_upload_progress=on_upload_progress,
        )
