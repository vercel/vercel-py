from __future__ import annotations

import os
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from os import PathLike
from typing import Any

from .._iter_coroutine import iter_coroutine
from ._core import (
    _AsyncBlobOpsClient,
    _SyncBlobOpsClient,
    normalize_delete_urls,
)
from .errors import BlobError, BlobNoTokenProvidedError
from .multipart.api import (
    AsyncMultipartUploader,
    MultipartUploader,
    create_multipart_uploader,
    create_multipart_uploader_async,
)
from .multipart.core import _AsyncMultipartClient, _SyncMultipartClient
from .types import (
    CreateFolderResult as CreateFolderResultType,
    GetBlobResult as GetBlobResultType,
    HeadBlobResult as HeadBlobResultType,
    ListBlobItem,
    ListBlobResult as ListBlobResultType,
    PutBlobResult as PutBlobResultType,
)
from .utils import Access, UploadProgressEvent, ensure_token


class BlobClient:
    def __init__(self, token: str | None = None):
        resolved_token = (
            token or os.getenv("BLOB_READ_WRITE_TOKEN") or os.getenv("VERCEL_BLOB_READ_WRITE_TOKEN")
        )
        if not resolved_token:
            raise BlobNoTokenProvidedError()
        self.token = ensure_token(resolved_token)

        self._ops_client = _SyncBlobOpsClient()
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise BlobError("Client is closed")

    def close(self) -> None:
        if self._closed:
            return
        self._ops_client.close()
        self._closed = True

    def __enter__(self) -> BlobClient:
        self._ensure_open()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def put(
        self,
        path: str,
        body: Any,
        *,
        access: Access = "public",
        content_type: str | None = None,
        add_random_suffix: bool = False,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
        multipart: bool = False,
        on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
    ) -> PutBlobResultType:
        self._ensure_open()
        result, _ = iter_coroutine(
            self._ops_client.put_blob(
                path,
                body,
                access=access,
                content_type=content_type,
                add_random_suffix=add_random_suffix,
                overwrite=overwrite,
                cache_control_max_age=cache_control_max_age,
                token=self.token,
                multipart=multipart,
                on_upload_progress=on_upload_progress,
            )
        )
        return result

    def get(
        self,
        url_or_path: str,
        *,
        access: Access = "public",
        timeout: float | None = None,
        use_cache: bool = True,
        if_none_match: str | None = None,
    ) -> GetBlobResultType:
        self._ensure_open()
        return iter_coroutine(
            self._ops_client.get_blob(
                url_or_path,
                access=access,
                token=self.token,
                timeout=timeout,
                use_cache=use_cache,
                if_none_match=if_none_match,
                default_timeout=30.0,
            )
        )

    def head(self, url_or_path: str) -> HeadBlobResultType:
        self._ensure_open()
        return iter_coroutine(
            self._ops_client.head_blob(
                url_or_path,
                token=self.token,
            )
        )

    def delete(self, url_or_path: str | Iterable[str]) -> None:
        self._ensure_open()
        normalized_urls = normalize_delete_urls(url_or_path)
        iter_coroutine(
            self._ops_client.delete_blob(
                normalized_urls,
                token=self.token,
            )
        )

    def list_objects(
        self,
        *,
        limit: int | None = None,
        prefix: str | None = None,
        cursor: str | None = None,
        mode: str | None = None,
    ) -> ListBlobResultType:
        self._ensure_open()
        return self._ops_client.list_objects(
            limit=limit,
            prefix=prefix,
            cursor=cursor,
            mode=mode,
            token=self.token,
        )

    def iter_objects(
        self,
        *,
        prefix: str | None = None,
        mode: str | None = None,
        batch_size: int | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Iterator[ListBlobItem]:
        self._ensure_open()
        return self._ops_client.iter_objects(
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
        access: Access = "public",
        content_type: str | None = None,
        add_random_suffix: bool = False,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
    ) -> PutBlobResultType:
        self._ensure_open()
        return iter_coroutine(
            self._ops_client.copy_blob(
                src_path,
                dst_path,
                access=access,
                content_type=content_type,
                add_random_suffix=add_random_suffix,
                overwrite=overwrite,
                cache_control_max_age=cache_control_max_age,
                token=self.token,
            )
        )

    def create_folder(self, path: str, *, overwrite: bool = False) -> CreateFolderResultType:
        self._ensure_open()
        return iter_coroutine(
            self._ops_client.create_folder(
                path,
                token=self.token,
                overwrite=overwrite,
            )
        )

    def download_file(
        self,
        url_or_path: str,
        local_path: str | PathLike,
        *,
        access: Access = "public",
        timeout: float | None = None,
        overwrite: bool = True,
        create_parents: bool = True,
        progress: Callable[[int, int | None], None] | None = None,
    ) -> str:
        self._ensure_open()
        return iter_coroutine(
            self._ops_client.download_file(
                url_or_path,
                local_path,
                access=access,
                token=self.token,
                timeout=timeout,
                overwrite=overwrite,
                create_parents=create_parents,
                progress=progress,
            )
        )

    def upload_file(
        self,
        local_path: str | PathLike,
        path: str,
        *,
        access: Access = "public",
        content_type: str | None = None,
        add_random_suffix: bool = False,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
        multipart: bool = False,
        on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
    ) -> PutBlobResultType:
        self._ensure_open()
        return iter_coroutine(
            self._ops_client.upload_file(
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
                missing_local_path_error="src_path is required",
            )
        )

    def create_multipart_uploader(
        self,
        path: str,
        *,
        access: Access = "public",
        content_type: str | None = None,
        add_random_suffix: bool = True,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
    ) -> MultipartUploader:
        """Create a multipart uploader bound to this client's token."""
        self._ensure_open()
        return create_multipart_uploader(
            path,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=self.token,
            multipart_client=_SyncMultipartClient(self._ops_client.request_api),
        )


class AsyncBlobClient:
    def __init__(self, token: str | None = None):
        resolved_token = (
            token or os.getenv("BLOB_READ_WRITE_TOKEN") or os.getenv("VERCEL_BLOB_READ_WRITE_TOKEN")
        )
        if not resolved_token:
            raise BlobNoTokenProvidedError()
        self.token = ensure_token(resolved_token)

        self._ops_client = _AsyncBlobOpsClient()
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise BlobError("Client is closed")

    async def aclose(self) -> None:
        if self._closed:
            return
        await self._ops_client.aclose()
        self._closed = True

    async def __aenter__(self) -> AsyncBlobClient:
        self._ensure_open()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def put(
        self,
        path: str,
        body: Any,
        *,
        access: Access = "public",
        content_type: str | None = None,
        add_random_suffix: bool = False,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
        multipart: bool = False,
        on_upload_progress: (
            Callable[[UploadProgressEvent], None]
            | Callable[[UploadProgressEvent], Awaitable[None]]
            | None
        ) = None,
    ) -> PutBlobResultType:
        self._ensure_open()
        result, _ = await self._ops_client.put_blob(
            path,
            body,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=self.token,
            multipart=multipart,
            on_upload_progress=on_upload_progress,
        )
        return result

    async def get(
        self,
        url_or_path: str,
        *,
        access: Access = "public",
        timeout: float | None = None,
        use_cache: bool = True,
        if_none_match: str | None = None,
    ) -> GetBlobResultType:
        self._ensure_open()
        return await self._ops_client.get_blob(
            url_or_path,
            access=access,
            token=self.token,
            timeout=timeout,
            use_cache=use_cache,
            if_none_match=if_none_match,
            default_timeout=30.0,
        )

    async def head(self, url_or_path: str) -> HeadBlobResultType:
        self._ensure_open()
        return await self._ops_client.head_blob(
            url_or_path,
            token=self.token,
        )

    async def delete(self, url_or_path: str | Iterable[str]) -> None:
        self._ensure_open()
        normalized_urls = normalize_delete_urls(url_or_path)
        await self._ops_client.delete_blob(
            normalized_urls,
            token=self.token,
        )

    async def iter_objects(
        self,
        *,
        prefix: str | None = None,
        mode: str | None = None,
        batch_size: int | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> AsyncIterator[ListBlobItem]:
        self._ensure_open()
        return self._ops_client.iter_objects(
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
        self._ensure_open()
        return await self._ops_client.list_objects(
            limit=limit,
            prefix=prefix,
            cursor=cursor,
            mode=mode,
            token=self.token,
        )

    async def create_folder(self, path: str, *, overwrite: bool = False) -> CreateFolderResultType:
        self._ensure_open()
        return await self._ops_client.create_folder(
            path,
            token=self.token,
            overwrite=overwrite,
        )

    async def copy(
        self,
        src_path: str,
        dst_path: str,
        *,
        access: Access = "public",
        content_type: str | None = None,
        add_random_suffix: bool = False,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
    ) -> PutBlobResultType:
        self._ensure_open()
        return await self._ops_client.copy_blob(
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
        access: Access = "public",
        timeout: float | None = None,
        overwrite: bool = True,
        create_parents: bool = True,
        progress: (
            Callable[[int, int | None], None] | Callable[[int, int | None], Awaitable[None]] | None
        ) = None,
    ) -> str:
        self._ensure_open()
        return await self._ops_client.download_file(
            url_or_path,
            local_path,
            access=access,
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
        access: Access = "public",
        content_type: str | None = None,
        add_random_suffix: bool = False,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
        multipart: bool = False,
        on_upload_progress: (
            Callable[[UploadProgressEvent], None]
            | Callable[[UploadProgressEvent], Awaitable[None]]
            | None
        ) = None,
    ) -> PutBlobResultType:
        self._ensure_open()
        return await self._ops_client.upload_file(
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
            missing_local_path_error="local_path is required",
        )

    async def create_multipart_uploader(
        self,
        path: str,
        *,
        access: Access = "public",
        content_type: str | None = None,
        add_random_suffix: bool = True,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
    ) -> AsyncMultipartUploader:
        """Create an async multipart uploader bound to this client's token."""
        self._ensure_open()
        return await create_multipart_uploader_async(
            path,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=self.token,
            multipart_client=_AsyncMultipartClient(self._ops_client.request_api),
        )
