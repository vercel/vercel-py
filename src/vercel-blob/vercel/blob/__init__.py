"""Asynchronous Vercel Blob SDK surface."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager
from typing import Literal, overload

from vercel._internal.core.session import get_active_session

from . import sync
from ._internal.api_client import _PutRequest
from ._internal.download import AsyncBlobDownload, AsyncDownloadContext
from ._internal.multipart import AsyncMultipartUpload
from ._internal.options import BlobServiceOptions
from ._internal.pagination import (
    query_blob_entries as _query_blob_entries,
    query_blobs as _query_blobs,
)
from ._internal.service import get_blob_service
from .errors import (
    BlobAccessError,
    BlobContentLengthError,
    BlobContentTypeNotAllowedError,
    BlobCredentialsError,
    BlobError,
    BlobFileTooLargeError,
    BlobNotFoundError,
    BlobPathnameMismatchError,
    BlobPreconditionFailedError,
    BlobServiceNotAvailable,
    BlobServiceRateLimited,
    BlobStoreNotFoundError,
    BlobStoreSuspendedError,
    BlobStreamError,
    BlobUnknownError,
)
from .models import (
    Access,
    AsyncByteReader,
    BlobCredentials,
    BlobCredentialsFactory,
    BlobPrefix,
    CopyResult,
    CredentialKind,
    DownloadMetadata,
    GetResult,
    HeadResult,
    ListBlob,
    PutBody,
    PutResult,
    SyncBlobCredentialsFactory,
    SyncByteReader,
    SyncPutBody,
)


@overload
async def put_blob(
    pathname: str,
    body: bytes | bytearray | memoryview,
    *,
    access: Literal["public", "private"],
    content_length: int | None = None,
    content_type: str | None = None,
    add_random_suffix: bool = False,
    allow_overwrite: bool = False,
    cache_control_max_age: int | None = None,
) -> PutResult: ...


@overload
async def put_blob(
    pathname: str,
    body: PutBody,
    *,
    access: Literal["public", "private"],
    content_length: int,
    content_type: str | None = None,
    add_random_suffix: bool = False,
    allow_overwrite: bool = False,
    cache_control_max_age: int | None = None,
) -> PutResult: ...


async def put_blob(
    pathname: str,
    body: bytes | bytearray | memoryview | PutBody,
    *,
    access: Literal["public", "private"],
    content_length: int | None = None,
    content_type: str | None = None,
    add_random_suffix: bool = False,
    allow_overwrite: bool = False,
    cache_control_max_age: int | None = None,
) -> PutResult:
    """Store an object in Vercel Blob with bytes or streaming payload."""
    service = get_blob_service(get_active_session())
    return await service.put(
        pathname,
        body,
        access=access,
        content_length=content_length,
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        allow_overwrite=allow_overwrite,
        cache_control_max_age=cache_control_max_age,
    )


async def get_blob(
    url_or_pathname: str,
    *,
    access: Literal["public", "private"],
) -> GetResult:
    """Download the complete object into memory and close its response."""
    async with stream_blob(url_or_pathname, access=access) as download:
        body = b"".join([chunk async for chunk in download])
    return GetResult(metadata=download.metadata, body=body)


def multipart_upload_blob(
    pathname: str,
    *,
    access: Literal["public", "private"],
    content_type: str | None = None,
    add_random_suffix: bool = False,
    allow_overwrite: bool = False,
    cache_control_max_age: int | None = None,
) -> AsyncMultipartUpload:
    """Create a scoped upload that publishes on successful context exit."""
    request = _PutRequest(
        pathname=pathname,
        access=access,
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        allow_overwrite=allow_overwrite,
        cache_control_max_age=cache_control_max_age,
    )
    service = get_blob_service(get_active_session())
    return AsyncMultipartUpload(service, request)


def stream_blob(
    url_or_pathname: str,
    *,
    access: Literal["public", "private"],
) -> AbstractAsyncContextManager[AsyncBlobDownload]:
    """Open an asynchronous streaming download context manager."""
    session = get_active_session()

    async def opener() -> AsyncBlobDownload:
        service = get_blob_service(session)
        stream, metadata = await service.open_download(url_or_pathname, access=access)
        return AsyncBlobDownload(stream, metadata, check_session=service.check_open)

    return AsyncDownloadContext(opener)


async def head_blob(
    url_or_pathname: str,
) -> HeadResult:
    """Retrieve catalog metadata for a Blob object."""
    service = get_blob_service(get_active_session())
    return await service.head(url_or_pathname)


def query_blobs(
    *,
    prefix: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> AsyncIterator[ListBlob]:
    """Iterate over Blobs, transparently following pagination cursors.

    Args:
        prefix: Literal pathname prefix to filter objects.
        page_size: Maximum number of objects fetched per API request (1–1,000).
        cursor: Cursor at which to begin pagination.
    """
    return _query_blobs(
        get_blob_service(get_active_session()),
        prefix=prefix,
        page_size=page_size,
        cursor=cursor,
    )


def query_blob_entries(
    *,
    prefix: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> AsyncIterator[ListBlob | BlobPrefix]:
    """Iterate over objects and virtual folders at one directory level.

    Nonempty prefixes must end with '/'. Objects precede folder prefixes
    within each page; no global sort order is guaranteed. Pagination follows
    cursors lazily, including pages containing only folders.
    """
    return _query_blob_entries(
        get_blob_service(get_active_session()),
        prefix=prefix,
        page_size=page_size,
        cursor=cursor,
    )


async def copy_blob(
    source: str,
    pathname: str,
    *,
    access: Literal["public", "private"],
    content_type: str | None = None,
    add_random_suffix: bool = False,
    allow_overwrite: bool = False,
    cache_control_max_age: int | None = None,
    if_match: str | None = None,
) -> CopyResult:
    """Copy within a store; if_match checks the source ETag."""
    service = get_blob_service(get_active_session())
    return await service.copy(
        source,
        pathname,
        access=access,
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        allow_overwrite=allow_overwrite,
        cache_control_max_age=cache_control_max_age,
        if_match=if_match,
    )


async def delete_blob(
    url_or_pathname: str | Sequence[str],
    *,
    if_match: str | None = None,
) -> None:
    """Delete 1–1000 objects in one request; if_match requires a single target."""
    service = get_blob_service(get_active_session())
    await service.delete(url_or_pathname, if_match=if_match)


__all__ = [
    "Access",
    "AsyncByteReader",
    "AsyncMultipartUpload",
    "BlobAccessError",
    "BlobContentLengthError",
    "BlobContentTypeNotAllowedError",
    "BlobCredentials",
    "BlobCredentialsError",
    "BlobCredentialsFactory",
    "BlobError",
    "BlobFileTooLargeError",
    "BlobNotFoundError",
    "BlobPathnameMismatchError",
    "BlobPreconditionFailedError",
    "BlobPrefix",
    "BlobServiceNotAvailable",
    "BlobServiceOptions",
    "BlobServiceRateLimited",
    "BlobStoreNotFoundError",
    "BlobStoreSuspendedError",
    "BlobStreamError",
    "BlobUnknownError",
    "CopyResult",
    "CredentialKind",
    "DownloadMetadata",
    "GetResult",
    "HeadResult",
    "ListBlob",
    "PutBody",
    "PutResult",
    "SyncBlobCredentialsFactory",
    "SyncByteReader",
    "SyncPutBody",
    "copy_blob",
    "delete_blob",
    "get_blob",
    "head_blob",
    "multipart_upload_blob",
    "put_blob",
    "query_blobs",
    "query_blob_entries",
    "stream_blob",
    "sync",
]
