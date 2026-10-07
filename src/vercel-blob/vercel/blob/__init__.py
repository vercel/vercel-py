"""Asynchronous Vercel Blob SDK surface."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Literal

from vercel._internal.core.session import get_active_session

from . import sync
from ._internal.download import AsyncBlobDownload, AsyncDownloadContext
from ._internal.options import BlobServiceOptions
from ._internal.service import get_blob_service
from .errors import (
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
from .models import (
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


async def put(
    pathname: str,
    body: bytes,
    *,
    access: Literal["public", "private"],
    content_type: str | None = None,
    add_random_suffix: bool = False,
    allow_overwrite: bool = False,
    cache_control_max_age: int | None = None,
) -> PutResult:
    """Store an object in Vercel Blob with bytes payload."""
    service = get_blob_service(get_active_session())
    return await service.put(
        pathname,
        body,
        access=access,
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        allow_overwrite=allow_overwrite,
        cache_control_max_age=cache_control_max_age,
    )


async def get(
    url_or_pathname: str,
    *,
    access: Literal["public", "private"],
) -> GetResult:
    """Download the complete object into memory and close its response."""
    async with stream(url_or_pathname, access=access) as download:
        body = b"".join([chunk async for chunk in download])
    return GetResult(metadata=download.metadata, body=body)


def stream(
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


async def head(
    url_or_pathname: str,
) -> HeadResult:
    """Retrieve catalog metadata for a Blob object."""
    service = get_blob_service(get_active_session())
    return await service.head(url_or_pathname)


async def delete(
    url_or_pathname: str,
) -> None:
    """Delete a single object from Vercel Blob."""
    service = get_blob_service(get_active_session())
    await service.delete(url_or_pathname)


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
    "BlobServiceOptions",
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
    "delete",
    "get",
    "head",
    "put",
    "stream",
    "sync",
]
