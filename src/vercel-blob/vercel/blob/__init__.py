"""Asynchronous, file-oriented Vercel Blob SDK."""

from __future__ import annotations

import os
from typing import Any, Literal, overload

from vercel._internal.core.session import get_active_session
from vercel.blob._internal.async_runtime import (
    AsyncBlobBinaryStream,
    AsyncBlobBinaryWriter,
    AsyncBlobTextStream,
    AsyncBlobTextWriter,
    OpenBlobOperation,
    open_async_stream,
)
from vercel.blob._internal.models import (
    Access,
    BlobCredentials,
    BlobCredentialsFactory,
    BlobMetadata,
    BlobStatResult,
    CredentialKind,
)
from vercel.blob._internal.options import BlobServiceOptions
from vercel.blob._internal.service import get_blob_service
from vercel.blob._internal.validation import normalize_path, validate_open
from vercel.blob.errors import (
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

from . import sync

StrPath = str | os.PathLike[str]


@overload
def open(
    pathname: StrPath,
    mode: Literal["r"] = "r",
    *,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    metadata: BlobMetadata | None = None,
    access: Access | None = None,
) -> OpenBlobOperation[AsyncBlobTextStream]: ...


@overload
def open(
    pathname: StrPath,
    mode: Literal["rb"],
    *,
    encoding: None = None,
    errors: None = None,
    newline: None = None,
    metadata: BlobMetadata | None = None,
    access: Access | None = None,
) -> OpenBlobOperation[AsyncBlobBinaryStream]: ...


@overload
def open(
    pathname: StrPath,
    mode: Literal["wb"],
    *,
    encoding: None = None,
    errors: None = None,
    newline: None = None,
    metadata: BlobMetadata | None = None,
    access: Access | None = None,
) -> OpenBlobOperation[AsyncBlobBinaryWriter]: ...


@overload
def open(
    pathname: StrPath,
    mode: Literal["w"],
    *,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    metadata: BlobMetadata | None = None,
    access: Access | None = None,
) -> OpenBlobOperation[AsyncBlobTextWriter]: ...


def open(
    pathname: StrPath,
    mode: str = "r",
    *,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    metadata: BlobMetadata | None = None,
    access: Access | None = None,
) -> OpenBlobOperation[Any]:
    """Return a deferred, single-use operation that opens a Blob stream."""
    metadata = {} if metadata is None else metadata
    content_type = metadata.get("content_type")
    cache_control_max_age = metadata.get("cache_control_max_age")
    path, parsed_mode, _, normalized_cache_control_max_age = validate_open(
        pathname,
        mode,
        encoding=encoding,
        errors=errors,
        newline=newline,
        content_type=content_type,
        cache_control_max_age=cache_control_max_age,
    )
    if access is not None:
        try:
            access = Access(access)
        except ValueError:
            raise ValueError("access must be 'public' or 'private'") from None
    session = get_active_session()

    async def opener():
        service = get_blob_service(session)
        resolved_access = service.options.default_access if access is None else access
        return await open_async_stream(
            service,
            path,
            parsed_mode,
            access=resolved_access,
            encoding=encoding or "utf-8",
            errors=errors or "strict",
            newline=newline,
            content_type=content_type,
            cache_control_max_age=normalized_cache_control_max_age,
        )

    return OpenBlobOperation(opener)


async def stat(pathname: StrPath) -> BlobStatResult:
    path = normalize_path(pathname)
    return await get_blob_service(get_active_session()).stat(path)


async def remove(pathname: StrPath, *, missing_ok: bool = False) -> None:
    if not isinstance(missing_ok, bool):
        raise TypeError("missing_ok must be bool")
    path = normalize_path(pathname)
    await get_blob_service(get_active_session()).remove(path, missing_ok=missing_ok)


__all__ = [
    "AsyncBlobBinaryStream",
    "AsyncBlobBinaryWriter",
    "AsyncBlobTextStream",
    "AsyncBlobTextWriter",
    "Access",
    "BlobAccessError",
    "BlobContentTypeNotAllowedError",
    "BlobCredentials",
    "BlobCredentialsError",
    "BlobCredentialsFactory",
    "BlobMetadata",
    "CredentialKind",
    "BlobError",
    "BlobFileTooLargeError",
    "BlobNotFoundError",
    "BlobPathnameMismatchError",
    "BlobPreconditionFailedError",
    "BlobServiceNotAvailable",
    "BlobServiceRateLimited",
    "BlobServiceOptions",
    "BlobStatResult",
    "BlobStoreNotFoundError",
    "BlobStoreSuspendedError",
    "BlobStreamError",
    "BlobUnknownError",
    "OpenBlobOperation",
    "open",
    "remove",
    "stat",
    "sync",
]
