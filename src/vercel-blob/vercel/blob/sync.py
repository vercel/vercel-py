"""Synchronous, file-oriented Vercel Blob SDK."""

from __future__ import annotations

import os
from typing import Any, Literal, overload

from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel._internal.core.session import get_active_sync_session
from vercel.blob._internal.models import (
    Access,
    BlobCredentials,
    BlobStatResult,
    DurationInput,
    SyncBlobCredentialsFactory,
)
from vercel.blob._internal.options import SyncBlobServiceOptions as BlobServiceOptions
from vercel.blob._internal.service import get_sync_blob_service
from vercel.blob._internal.sync_runtime import (
    SyncBlobBinaryStream,
    SyncBlobBinaryWriter,
    SyncBlobTextStream,
    SyncBlobTextWriter,
    open_sync_stream,
)
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

StrPath = str | os.PathLike[str]


@overload
def open(
    pathname: StrPath,
    mode: Literal["r"] = "r",
    *,
    access: Access | None = None,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    content_type: str | None = None,
    cache_control_max_age: DurationInput = None,
) -> SyncBlobTextStream: ...


@overload
def open(
    pathname: StrPath,
    mode: Literal["rb"],
    *,
    access: Access | None = None,
    encoding: None = None,
    errors: None = None,
    newline: None = None,
    content_type: str | None = None,
    cache_control_max_age: DurationInput = None,
) -> SyncBlobBinaryStream: ...


@overload
def open(
    pathname: StrPath,
    mode: Literal["wb"],
    *,
    access: Access | None = None,
    encoding: None = None,
    errors: None = None,
    newline: None = None,
    content_type: str | None = None,
    cache_control_max_age: DurationInput = None,
) -> SyncBlobBinaryWriter: ...


@overload
def open(
    pathname: StrPath,
    mode: Literal["w"],
    *,
    access: Access | None = None,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    content_type: str | None = None,
    cache_control_max_age: DurationInput = None,
) -> SyncBlobTextWriter: ...


def open(
    pathname: StrPath,
    mode: str = "r",
    *,
    access: Access | None = None,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    content_type: str | None = None,
    cache_control_max_age: DurationInput = None,
) -> Any:
    """Open a Blob object as a synchronous file-like stream."""
    path, parsed_mode, _, normalized_cache_control_max_age = validate_open(
        pathname,
        mode,
        encoding=encoding,
        errors=errors,
        newline=newline,
        content_type=content_type,
        cache_control_max_age=cache_control_max_age,
    )
    if access is not None and access not in ("public", "private"):
        raise ValueError("access must be 'public' or 'private'")
    service = get_sync_blob_service(get_active_sync_session())
    resolved_access = service.options.default_access if access is None else access
    return open_sync_stream(
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


def stat(pathname: StrPath) -> BlobStatResult:
    path = normalize_path(pathname)
    return iter_coroutine(get_sync_blob_service(get_active_sync_session()).stat(path))


def remove(pathname: StrPath, *, missing_ok: bool = False) -> None:
    if not isinstance(missing_ok, bool):
        raise TypeError("missing_ok must be bool")
    path = normalize_path(pathname)
    iter_coroutine(
        get_sync_blob_service(get_active_sync_session()).remove(path, missing_ok=missing_ok)
    )


__all__ = [
    "BlobAccessError",
    "BlobContentTypeNotAllowedError",
    "BlobCredentials",
    "BlobCredentialsError",
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
    "SyncBlobBinaryStream",
    "SyncBlobBinaryWriter",
    "SyncBlobCredentialsFactory",
    "SyncBlobTextStream",
    "SyncBlobTextWriter",
    "open",
    "remove",
    "stat",
]
