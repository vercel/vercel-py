"""Synchronous Vercel Blob SDK surface."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Literal

from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel._internal.core.session import get_active_sync_session

from .._internal.download import SyncBlobDownload, SyncDownloadContext
from .._internal.options import SyncBlobServiceOptions
from .._internal.service import get_sync_blob_service
from ..errors import (
    BlobAccessError,
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
from ..models import (
    Access,
    BlobCredentials,
    BlobCredentialsFactory,
    CredentialKind,
    DownloadMetadata,
    GetResult,
    HeadResult,
    PutResult,
    SyncBlobCredentialsFactory,
)


def put(
    pathname: str,
    body: bytes,
    *,
    access: Literal["public", "private"],
    content_type: str | None = None,
    add_random_suffix: bool = False,
    allow_overwrite: bool = False,
    cache_control_max_age: int | None = None,
) -> PutResult:
    """Store an object in Vercel Blob synchronously."""
    service = get_sync_blob_service(get_active_sync_session())
    return iter_coroutine(
        service.put(
            pathname,
            body,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            allow_overwrite=allow_overwrite,
            cache_control_max_age=cache_control_max_age,
        )
    )


def get(
    url_or_pathname: str,
    *,
    access: Literal["public", "private"],
) -> GetResult:
    """Download the complete object into memory and close its response."""
    with stream(url_or_pathname, access=access) as download:
        body = b"".join(download)
    return GetResult(metadata=download.metadata, body=body)


def stream(
    url_or_pathname: str,
    *,
    access: Literal["public", "private"],
) -> AbstractContextManager[SyncBlobDownload]:
    """Open a synchronous streaming download context manager."""
    session = get_active_sync_session()

    def opener() -> SyncBlobDownload:
        service = get_sync_blob_service(session)
        stream, metadata = iter_coroutine(service.open_download(url_or_pathname, access=access))
        return SyncBlobDownload(stream, metadata, check_session=service.check_open)

    return SyncDownloadContext(opener)


def head(
    url_or_pathname: str,
) -> HeadResult:
    """Retrieve catalog metadata for a Blob object synchronously."""
    service = get_sync_blob_service(get_active_sync_session())
    return iter_coroutine(service.head(url_or_pathname))


def delete(
    url_or_pathname: str,
) -> None:
    """Delete a single object from Vercel Blob synchronously."""
    service = get_sync_blob_service(get_active_sync_session())
    iter_coroutine(service.delete(url_or_pathname))


__all__ = [
    "Access",
    "BlobAccessError",
    "BlobContentTypeNotAllowedError",
    "BlobCredentials",
    "BlobCredentialsError",
    "BlobCredentialsFactory",
    "BlobError",
    "BlobFileTooLargeError",
    "BlobNotFoundError",
    "BlobPathnameMismatchError",
    "BlobPreconditionFailedError",
    "BlobServiceNotAvailable",
    "BlobServiceRateLimited",
    "BlobStoreNotFoundError",
    "BlobStoreSuspendedError",
    "BlobStreamError",
    "BlobUnknownError",
    "CredentialKind",
    "DownloadMetadata",
    "GetResult",
    "HeadResult",
    "PutResult",
    "SyncBlobCredentialsFactory",
    "SyncBlobServiceOptions",
    "delete",
    "get",
    "head",
    "put",
    "stream",
]
