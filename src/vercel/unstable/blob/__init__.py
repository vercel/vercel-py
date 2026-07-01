"""Experimental asynchronous Vercel Blob API."""

import os
from collections.abc import AsyncIterator, Sequence
from datetime import timedelta
from typing import Any, Literal, TypeAlias, overload

from vercel._internal.blob.types import Access as _Access
from vercel._internal.unstable import session as _session_impl
from vercel._internal.unstable.blob import models as _models, service as _service_impl
from vercel._internal.unstable.blob.async_runtime import (
    AsyncBlobBinaryStream,
    AsyncBlobBinaryWriter,
    AsyncBlobTextStream,
    AsyncBlobTextWriter,
    OpenBlobOperation,
    open_blob_stream as _open_blob_stream,
    validate_text_options as _validate_text_options,
)
from vercel._internal.unstable.blob.duration import (
    DurationInput,
    parse_duration_seconds,
    parse_required_duration_seconds,
)
from vercel._internal.unstable.blob.errors import (  # noqa: F401
    BlobAccessError,
    BlobAlreadyExistsError,
    BlobClientTokenExpiredError,
    BlobContentTypeNotAllowedError,
    BlobCredentialsError,
    BlobError,
    BlobFileTooLargeError,
    BlobInvalidResponseJSONError,
    BlobNotFoundError,
    BlobNoTokenProvidedError,
    BlobPathnameMismatchError,
    BlobPreconditionFailedError,
    BlobRecursiveDeleteError,
    BlobRequestAbortedError,
    BlobServiceNotAvailable,
    BlobServiceRateLimited,
    BlobStoreNotFoundError,
    BlobStoreSuspendedError,
    BlobStreamError,
    BlobUnexpectedResponseContentTypeError,
    BlobUnknownError,
    __all__ as _error_exports,
)
from vercel._internal.unstable.blob.models import (
    BlobStatResult,
    PresignedOperation,
    PresignedUrl,
    ScandirMode,
)
from vercel._internal.unstable.blob.options import (
    BlobCredentials,
    BlobCredentialsFactory,
    BlobServiceOptions,
    SyncBlobCredentialsFactory,
)
from vercel._internal.unstable.blob.runtime_common import (
    BlobEntry,
    BlobObjectEntry,
    BlobPrefixEntry,
)

from . import sync

StrPath: TypeAlias = str | os.PathLike[str]


def _service() -> _service_impl.BlobService:
    return _service_impl.get_blob_service(_session_impl.get_active_session())


def _resolved_access(service: _service_impl.BlobService, access: _Access | None) -> _Access:
    return service.options.default_access if access is None else access


@overload
def open(
    pathname: StrPath,
    mode: Literal["r"] = "r",
    *,
    access: _Access | None = None,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    content_type: str | None = None,
    cache_control_max_age: DurationInput = None,
) -> OpenBlobOperation[AsyncBlobTextStream]: ...


@overload
def open(
    pathname: StrPath,
    mode: Literal["rb", "wb", "xb", "ab", "r+b", "rb+", "w+b", "wb+", "x+b", "xb+", "a+b", "ab+"],
    *,
    access: _Access | None = None,
    encoding: None = None,
    errors: None = None,
    newline: None = None,
    content_type: str | None = None,
    cache_control_max_age: DurationInput = None,
) -> OpenBlobOperation[AsyncBlobBinaryStream]: ...


@overload
def open(
    pathname: StrPath,
    mode: Literal["w", "x", "a", "r+", "w+", "x+", "a+"],
    *,
    access: _Access | None = None,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    content_type: str | None = None,
    cache_control_max_age: DurationInput = None,
) -> OpenBlobOperation[AsyncBlobTextStream]: ...


def open(
    pathname: StrPath,
    mode: str = "r",
    *,
    access: _Access | None = None,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    content_type: str | None = None,
    cache_control_max_age: DurationInput = None,
) -> OpenBlobOperation[Any]:
    """Open a Blob object as an asynchronous file-like stream.

    Opening is deferred until the returned operation is awaited or entered as an
    async context manager. Reader streams pin the object ETag observed at open
    time. Writer streams stage mutations locally and publish the replacement
    Blob when the stream closes successfully.

    Args:
        pathname: Store-relative object pathname.
        mode: Python file mode. Binary and text read, write, exclusive create,
            append, and update modes are supported.
        access: Delivery access for the object. Defaults to the active
            service's configured access.
        encoding: Text encoding for text modes.
        errors: Text codec error handler for text modes.
        newline: Text newline handling for text modes.
        content_type: Content type to publish with write modes.
        cache_control_max_age: Cache control max-age to publish with write
            modes, as seconds or a ``timedelta``.

    Returns:
        A single-use async operation that yields the opened Blob stream.
    """
    parsed_mode = _models._parse_file_mode(mode)
    if parsed_mode.binary and any(value is not None for value in (encoding, errors, newline)):
        raise ValueError("encoding, errors, and newline are invalid in binary mode")
    if not parsed_mode.writing and any(
        value is not None for value in (content_type, cache_control_max_age)
    ):
        raise ValueError("content_type and cache_control_max_age are invalid in read mode")

    if not parsed_mode.binary:
        encoding, errors = _validate_text_options(
            encoding=encoding,
            errors=errors,
            newline=newline,
        )
    normalized_cache_control_max_age = parse_duration_seconds(
        cache_control_max_age, name="cache_control_max_age"
    )

    session = _session_impl.get_active_session()

    async def opener() -> Any:
        service = _service_impl.get_blob_service(session)
        return await _open_blob_stream(
            service,
            pathname,
            mode=parsed_mode,
            access=_resolved_access(service, access),
            encoding=encoding,
            errors=errors,
            newline=newline,
            content_type=content_type,
            cache_control_max_age=normalized_cache_control_max_age,
        )

    return OpenBlobOperation(opener)


async def stat(pathname: StrPath) -> BlobStatResult:
    """Return complete metadata for one Blob object.

    Args:
        pathname: Store-relative object pathname.

    Returns:
        Metadata for the object.
    """
    return await _service().stat(pathname)


def scandir(
    prefix: StrPath = "",
    *,
    mode: ScandirMode = ScandirMode.FOLDED,
    page_size: int | None = None,
    cursor: str | None = None,
) -> AsyncIterator[BlobEntry]:
    """Iterate Blob objects and common prefixes under a prefix.

    Args:
        prefix: Store-relative pathname prefix to list. An empty prefix lists
            from the store root.
        mode: Whether to fold common prefixes or expand all matching objects.
        page_size: Optional backend page size hint.
        cursor: Optional continuation cursor for resuming a listing.

    Yields:
        Object and prefix entries observed in the listing.
    """
    service = _service()

    async def entries() -> AsyncIterator[BlobEntry]:
        async for entry in service.scandir(
            prefix=prefix, mode=mode, page_size=page_size, cursor=cursor
        ):
            if isinstance(entry, _models.BlobListItemState):
                yield BlobObjectEntry(entry, service)
            elif isinstance(entry, _models.BlobPrefixState):
                yield BlobPrefixEntry(entry, service)
            else:
                raise TypeError("Blob listing returned an unknown entry type")

    return entries()


async def remove(pathname: StrPath, *, missing_ok: bool = False) -> None:
    """Delete one exact Blob object.

    Args:
        pathname: Store-relative object pathname.
        missing_ok: Whether a missing object should be ignored.
    """
    await _service().remove(pathname, missing_ok=missing_ok)


async def rmtree(pathname: StrPath, *, missing_ok: bool = False) -> None:
    """Delete every Blob object below a prefix.

    Args:
        pathname: Store-relative prefix to remove recursively.
        missing_ok: Whether an empty or missing prefix should be ignored.
    """
    await _service().rmtree(pathname, missing_ok=missing_ok)


async def mkdir(
    pathname: StrPath,
    *,
    access: _Access | None = None,
    exist_ok: bool = False,
) -> None:
    """Create a zero-byte Blob marker for a prefix.

    Args:
        pathname: Store-relative prefix pathname.
        access: Delivery access for the marker. Defaults to the active
            service's configured access.
        exist_ok: Whether an existing marker should be accepted.
    """
    service = _service()
    await service.mkdir(
        pathname,
        access=_resolved_access(service, access),
        exist_ok=exist_ok,
    )


async def presign(
    pathname: StrPath,
    *,
    operation: PresignedOperation = PresignedOperation.GET,
    access: _Access | None = None,
    expires_in: DurationInput = timedelta(hours=1),
    maximum_size: int | None = None,
    allowed_content_types: Sequence[str] | None = None,
    allow_overwrite: bool | None = None,
    cache_control_max_age: DurationInput = None,
    if_match: str | None = None,
) -> PresignedUrl:
    """Create a presigned URL for a Blob operation.

    Args:
        pathname: Store-relative object pathname.
        operation: Blob operation the URL may perform.
        access: Delivery access for read URLs. Defaults to the active
            service's configured access.
        expires_in: Maximum lifetime for the URL, as seconds or a
            ``timedelta``.
        maximum_size: Optional maximum upload size for PUT URLs.
        allowed_content_types: Optional content type allow-list for PUT URLs.
        allow_overwrite: Optional overwrite policy for PUT URLs.
        cache_control_max_age: Optional cache max-age for PUT URLs, as seconds
            or a ``timedelta``.
        if_match: Optional ETag precondition for PUT or DELETE URLs.

    Returns:
        The presigned URL and its effective expiry.
    """
    service = _service()
    return await service.presign(
        pathname,
        operation=operation,
        access=_resolved_access(service, access),
        expires_in=parse_required_duration_seconds(expires_in, name="expires_in"),
        maximum_size=maximum_size,
        allowed_content_types=allowed_content_types,
        allow_overwrite=allow_overwrite,
        cache_control_max_age=parse_duration_seconds(
            cache_control_max_age, name="cache_control_max_age"
        ),
        if_match=if_match,
    )


__all__ = [
    "sync",
    "open",
    "stat",
    "scandir",
    "remove",
    "rmtree",
    "mkdir",
    "presign",
    "BlobServiceOptions",
    "BlobCredentials",
    "BlobCredentialsFactory",
    "SyncBlobCredentialsFactory",
    "ScandirMode",
    "PresignedOperation",
    "BlobStatResult",
    "PresignedUrl",
    "BlobEntry",
    "BlobObjectEntry",
    "BlobPrefixEntry",
    "AsyncBlobBinaryStream",
    "AsyncBlobBinaryWriter",
    "AsyncBlobTextStream",
    "AsyncBlobTextWriter",
    "OpenBlobOperation",
    *_error_exports,
]
