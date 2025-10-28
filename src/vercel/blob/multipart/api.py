from __future__ import annotations

from typing import Any, Callable, Awaitable, cast
import inspect

from ..utils import (
    UploadProgressEvent,
    PutHeaders,
    create_put_headers,
    validate_path,
    require_public_access,
)
from ..errors import BlobError
from .core import (
    call_create_multipart_upload,
    call_create_multipart_upload_async,
    call_upload_part,
    call_upload_part_async,
    call_complete_multipart_upload,
    call_complete_multipart_upload_async,
)
from ..types import MultipartCreateResult, MultipartPart, PutBlobResult
from ..utils import ensure_token


def create_multipart_upload(
    path: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> MultipartCreateResult:
    token = ensure_token(token)
    validate_path(path)
    require_public_access(access)

    headers = create_put_headers(
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        allow_overwrite=overwrite,
        cache_control_max_age=cache_control_max_age,
    )

    resp = call_create_multipart_upload(path, headers, token=token)
    return MultipartCreateResult(upload_id=resp["uploadId"], key=resp["key"])


async def create_multipart_upload_async(
    path: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> MultipartCreateResult:
    token = ensure_token(token)
    validate_path(path)
    require_public_access(access)
    headers = create_put_headers(
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        allow_overwrite=overwrite,
        cache_control_max_age=cache_control_max_age,
    )
    resp = await call_create_multipart_upload_async(path, headers, token=token)
    return MultipartCreateResult(upload_id=resp["uploadId"], key=resp["key"])


def upload_part(
    path: str,
    body: Any,
    *,
    access: str = "public",
    token: str | None = None,
    upload_id: str,
    key: str,
    part_number: int,
    content_type: str | None = None,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
) -> MultipartPart:
    token = ensure_token(token)
    validate_path(path)
    require_public_access(access)

    headers = create_put_headers(content_type=content_type)
    resp = call_upload_part(
        upload_id=upload_id,
        key=key,
        path=path,
        headers=headers,
        token=token,
        part_number=part_number,
        body=body,
        on_upload_progress=on_upload_progress,
    )
    return MultipartPart(part_number=part_number, etag=resp["etag"])


async def upload_part_async(
    path: str,
    body: Any,
    *,
    access: str = "public",
    token: str | None = None,
    upload_id: str,
    key: str,
    part_number: int,
    content_type: str | None = None,
    on_upload_progress: (
        Callable[[UploadProgressEvent], None]
        | Callable[[UploadProgressEvent], Awaitable[None]]
        | None
    ) = None,
) -> MultipartPart:
    token = ensure_token(token)
    validate_path(path)
    require_public_access(access)

    headers = create_put_headers(content_type=content_type)
    resp = await call_upload_part_async(
        upload_id=upload_id,
        key=key,
        path=path,
        headers=headers,
        token=token,
        part_number=part_number,
        body=body,
        on_upload_progress=on_upload_progress,
    )
    return MultipartPart(part_number=part_number, etag=resp["etag"])


def complete_multipart_upload(
    path: str,
    parts: list[MultipartPart],
    *,
    access: str = "public",
    content_type: str | None = None,
    token: str | None = None,
    upload_id: str,
    key: str,
) -> PutBlobResult:
    token = ensure_token(token)
    validate_path(path)
    require_public_access(access)
    headers = create_put_headers(content_type=content_type)

    resp = call_complete_multipart_upload(
        upload_id=upload_id,
        key=key,
        path=path,
        headers=headers,
        token=token,
        parts=[{"partNumber": p.part_number, "etag": p.etag} for p in parts],
    )
    return PutBlobResult(
        url=resp["url"],
        download_url=resp["downloadUrl"],
        pathname=resp["pathname"],
        content_type=resp["contentType"],
        content_disposition=resp["contentDisposition"],
    )


async def complete_multipart_upload_async(
    path: str,
    parts: list[MultipartPart],
    *,
    access: str = "public",
    content_type: str | None = None,
    token: str | None = None,
    upload_id: str,
    key: str,
) -> PutBlobResult:
    token = ensure_token(token)
    validate_path(path)
    require_public_access(access)

    headers = create_put_headers(content_type=content_type)
    resp = await call_complete_multipart_upload_async(
        upload_id=upload_id,
        key=key,
        path=path,
        headers=headers,
        token=token,
        parts=[{"partNumber": p.part_number, "etag": p.etag} for p in parts],
    )
    return PutBlobResult(
        url=resp["url"],
        download_url=resp["downloadUrl"],
        pathname=resp["pathname"],
        content_type=resp["contentType"],
        content_disposition=resp["contentDisposition"],
    )


class MultipartUploader:
    """
    A convenience wrapper for multipart uploads that encapsulates the upload context.

    This provides a cleaner API than the manual approach where you have to pass
    upload_id, key, pathname, etc. to every function call, while still giving you
    control over when and how parts are uploaded (unlike the automatic flow).

    Example:
        >>> uploader = create_multipart_uploader("path/to/file.bin")
        >>> part1 = uploader.upload_part(1, b"data chunk 1")
        >>> part2 = uploader.upload_part(2, b"data chunk 2")
        >>> result = uploader.complete([part1, part2])
    """

    def __init__(
        self,
        path: str,
        upload_id: str,
        key: str,
        headers: PutHeaders | dict[str, str],
        token: str | None,
    ):
        self._path = path
        self._upload_id = upload_id
        self._key = key
        self._headers: dict[str, str] = cast(dict[str, str], headers)
        self._token = token

    @property
    def upload_id(self) -> str:
        """The upload ID for this multipart upload."""
        return self._upload_id

    @property
    def key(self) -> str:
        """The key (blob identifier) for this multipart upload."""
        return self._key

    def upload_part(
        self,
        part_number: int,
        body: Any,
        *,
        on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
        per_part_progress: Callable[[int, UploadProgressEvent], None] | None = None,
    ) -> MultipartPart:
        """
        Upload a single part of the multipart upload.

        Args:
            part_number: The part number (must be between 1 and 10,000)
            body: The content to upload for this part (bytes, str, or file-like object)
            on_upload_progress: Optional callback for upload progress tracking

        Returns:
            A dict with 'partNumber' and 'etag' fields to pass to complete()

        Raises:
            BlobError: If body is a plain dict/object
        """
        if part_number < 1 or part_number > 10000:
            raise BlobError("part_number must be between 1 and 10,000")

        if isinstance(body, dict) and not hasattr(body, "read"):
            raise BlobError(
                "Body must be a string, bytes, or file-like object. "
                "You sent a plain dictionary, double check what you're trying to upload."
            )

        # Compose per-part progress if provided
        effective = on_upload_progress
        if per_part_progress is not None and on_upload_progress is None:

            def effective(evt: UploadProgressEvent) -> None:
                per_part_progress(part_number, evt)

        result = call_upload_part(
            upload_id=self._upload_id,
            key=self._key,
            path=self._path,
            headers=self._headers,
            part_number=part_number,
            body=body,
            on_upload_progress=effective,
            token=self._token,
        )

        return MultipartPart(part_number=part_number, etag=result["etag"])

    def complete(self, parts: list[MultipartPart]) -> PutBlobResult:
        """
        Complete the multipart upload by assembling the uploaded parts.

        Args:
            parts: List of parts returned from upload_part() calls.
                   Each part should have 'partNumber' and 'etag' fields.

        Returns:
            The result of the completed upload with URL and metadata
        """
        resp = call_complete_multipart_upload(
            upload_id=self._upload_id,
            key=self._key,
            path=self._path,
            headers=self._headers,
            parts=[{"partNumber": p.part_number, "etag": p.etag} for p in parts],
            token=self._token,
        )
        return PutBlobResult(
            url=resp["url"],
            download_url=resp["downloadUrl"],
            pathname=resp["pathname"],
            content_type=resp["contentType"],
            content_disposition=resp["contentDisposition"],
        )


class AsyncMultipartUploader:
    """
    An async convenience wrapper for multipart uploads that encapsulates the upload context.

    This provides a cleaner API than the manual approach where you have to pass
    upload_id, key, pathname, etc. to every function call, while still giving you
    control over when and how parts are uploaded (unlike the automatic flow).

    Example:
        >>> uploader = await create_multipart_uploader_async("path/to/file.bin")
        >>> part1 = await uploader.upload_part(1, b"data chunk 1")
        >>> part2 = await uploader.upload_part(2, b"data chunk 2")
        >>> result = await uploader.complete([part1, part2])
    """

    def __init__(
        self,
        path: str,
        upload_id: str,
        key: str,
        headers: PutHeaders | dict[str, str],
        token: str | None,
    ):
        self._path = path
        self._upload_id = upload_id
        self._key = key
        self._headers: dict[str, str] = cast(dict[str, str], headers)
        self._token = token

    @property
    def upload_id(self) -> str:
        """The upload ID for this multipart upload."""
        return self._upload_id

    @property
    def key(self) -> str:
        """The key (blob identifier) for this multipart upload."""
        return self._key

    async def upload_part(
        self,
        part_number: int,
        body: Any,
        *,
        on_upload_progress: (
            Callable[[UploadProgressEvent], None]
            | Callable[[UploadProgressEvent], Awaitable[None]]
            | None
        ) = None,
        per_part_progress: (
            Callable[[int, UploadProgressEvent], None]
            | Callable[[int, UploadProgressEvent], Awaitable[None]]
            | None
        ) = None,
    ) -> MultipartPart:
        """
        Upload a single part of the multipart upload.

        Args:
            part_number: The part number (must be between 1 and 10,000)
            body: The content to upload for this part (bytes, str, or file-like object)
            on_upload_progress: Optional callback for upload progress tracking

        Returns:
            A dict with 'partNumber' and 'etag' fields to pass to complete()

        Raises:
            BlobError: If body is a plain dict/object
        """
        if part_number < 1 or part_number > 10000:
            raise BlobError("part_number must be between 1 and 10,000")

        if isinstance(body, dict) and not hasattr(body, "read"):
            raise BlobError(
                "Body must be a string, bytes, or file-like object. "
                "You sent a plain dictionary, double check what you're trying to upload."
            )

        # Compose per-part progress if provided
        effective_progress = on_upload_progress
        if per_part_progress is not None and on_upload_progress is None:

            async def effective_progress(evt: UploadProgressEvent):
                res = per_part_progress(part_number, evt)
                if inspect.isawaitable(res):
                    await res

        result = await call_upload_part_async(
            upload_id=self._upload_id,
            key=self._key,
            path=self._path,
            headers=self._headers,
            part_number=part_number,
            body=body,
            on_upload_progress=effective_progress,
            token=self._token,
        )

        return MultipartPart(part_number=part_number, etag=result["etag"])

    async def complete(self, parts: list[MultipartPart]) -> PutBlobResult:
        """
        Complete the multipart upload by assembling the uploaded parts.

        Args:
            parts: List of parts returned from upload_part() calls.
                   Each part should have 'partNumber' and 'etag' fields.

        Returns:
            The result of the completed upload with URL and metadata
        """
        resp = await call_complete_multipart_upload_async(
            upload_id=self._upload_id,
            key=self._key,
            path=self._path,
            headers=self._headers,
            parts=[{"partNumber": p.part_number, "etag": p.etag} for p in parts],
            token=self._token,
        )
        return PutBlobResult(
            url=resp["url"],
            download_url=resp["downloadUrl"],
            pathname=resp["pathname"],
            content_type=resp["contentType"],
            content_disposition=resp["contentDisposition"],
        )


def create_multipart_uploader(
    path: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = True,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> MultipartUploader:
    """
    Create a multipart uploader with a cleaner API than the manual approach.

    It provides more control than the automatic approach (you control part creation
    and concurrency) while being cleaner than the manual approach (no need to pass
    upload_id, key, pathname to every call).

    Args:
        path: The path inside the blob store (includes filename and extension)
        access: Access level, defaults to "public"
        content_type: The media type for the file (auto-detected from extension if not provided)
        add_random_suffix: Whether to add a random suffix to the pathname (default: True)
        overwrite: Whether to allow overwriting existing files (default: False)
        cache_control_max_age: Cache duration in seconds (default: one year)
        token: Authentication token (defaults to BLOB_READ_WRITE_TOKEN or
               VERCEL_BLOB_READ_WRITE_TOKEN env var)

    Returns:
        A MultipartUploader instance with upload_part() and complete() methods

    Example:
        >>> uploader = create_multipart_uploader("large-file.bin")
        >>> parts = []
        >>> for i, chunk in enumerate(chunks, start=1):
        ...     part = uploader.upload_part(i, chunk)
        ...     parts.append(part)
        >>> result = uploader.complete(parts)
    """
    token = ensure_token(token)
    validate_path(path)
    require_public_access(access)

    headers = create_put_headers(
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        allow_overwrite=overwrite,
        cache_control_max_age=cache_control_max_age,
    )
    create_resp = call_create_multipart_upload(path, headers, token=token)

    return MultipartUploader(
        path=path,
        upload_id=create_resp["uploadId"],
        key=create_resp["key"],
        headers=headers,
        token=token,
    )


async def create_multipart_uploader_async(
    path: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = True,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> AsyncMultipartUploader:
    """
    Create an async multipart uploader with a cleaner API than the manual approach.

    It provides more control than the automatic approach (you control part creation
    and concurrency) while being cleaner than the manual approach (no need to pass
    upload_id, key, pathname to every call).

    Args:
        path: The path inside the blob store (includes filename and extension)
        access: Access level, defaults to "public"
        content_type: The media type for the file (auto-detected from extension if not provided)
        add_random_suffix: Whether to add a random suffix to the pathname (default: True)
        overwrite: Whether to allow overwriting existing files (default: False)
        cache_control_max_age: Cache duration in seconds (default: one year)
        token: Authentication token (defaults to BLOB_READ_WRITE_TOKEN or
            VERCEL_BLOB_READ_WRITE_TOKEN env var)

    Returns:
        An AsyncMultipartUploader instance with upload_part() and complete() methods

    Example:
        >>> uploader = await create_multipart_uploader_async("large-file.bin")
        >>> parts = []
        >>> for i, chunk in enumerate(chunks, start=1):
        ...     part = await uploader.upload_part(i, chunk)
        ...     parts.append(part)
        >>> result = await uploader.complete(parts)
    """
    token = ensure_token(token)
    validate_path(path)
    require_public_access(access)
    headers = create_put_headers(
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        allow_overwrite=overwrite,
        cache_control_max_age=cache_control_max_age,
    )

    create_resp = await call_create_multipart_upload_async(path, headers, token=token)

    return AsyncMultipartUploader(
        path=path,
        upload_id=create_resp["uploadId"],
        key=create_resp["key"],
        headers=headers,
        token=token,
    )
