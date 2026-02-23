from __future__ import annotations

import contextvars
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from os import PathLike
from typing import Any, TypeVar
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx

from .._iter_coroutine import iter_coroutine
from .._telemetry.tracker import telemetry, track
from ._core import (
    _AsyncBlobOpsClient,
    _SyncBlobOpsClient,
    get_telemetry_size_bytes,
    normalize_delete_urls,
)
from .errors import BlobError
from .types import (
    CreateFolderResult as CreateFolderResultType,
    GetBlobResult as GetBlobResultType,
    HeadBlobResult as HeadBlobResultType,
    ListBlobItem,
    ListBlobResult as ListBlobResultType,
    PutBlobResult as PutBlobResultType,
)
from .utils import (
    Access,
    UploadProgressEvent,
    construct_blob_url,
    ensure_token,
    extract_store_id_from_token,
    get_download_url,
    is_url,
    parse_datetime,
    validate_access,
)

# Context variable to store the delete count for telemetry
# This allows the derive function to access the count after the iterable is consumed
_delete_count_context: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "_delete_count", default=None
)

_T = TypeVar("_T")


def _run_sync_blob_operation(
    operation: Callable[[_SyncBlobOpsClient], Awaitable[_T]],
) -> _T:
    with _SyncBlobOpsClient() as client:
        # Keep exactly one sync bridge at the wrapper boundary.
        return iter_coroutine(operation(client))


def _resolve_blob_url(url_or_path: str, token: str, access: Access) -> tuple[str, str]:
    if is_url(url_or_path):
        parsed = urlparse(url_or_path)
        pathname = parsed.path.lstrip("/")
        return url_or_path, pathname

    store_id = extract_store_id_from_token(token)
    if not store_id:
        raise BlobError(
            "Unable to extract store ID from token. "
            "When using a pathname instead of a full URL, "
            "a valid token with an embedded store ID is required."
        )
    pathname = url_or_path.lstrip("/")
    blob_url = construct_blob_url(store_id, pathname, access)
    return blob_url, pathname


def _parse_last_modified(value: str | None) -> datetime:
    if not value:
        return datetime.now(tz=timezone.utc)
    try:
        return parsedate_to_datetime(value)
    except (ValueError, TypeError):
        pass
    try:
        return parse_datetime(value)
    except (ValueError, TypeError):
        return datetime.now(tz=timezone.utc)


def _build_get_result(
    resp: httpx.Response, blob_url: str, pathname: str
) -> GetBlobResultType:
    if resp.status_code == 304:
        return GetBlobResultType(
            url=blob_url,
            download_url=get_download_url(blob_url),
            pathname=pathname,
            content_type=None,
            size=None,
            content_disposition=resp.headers.get("content-disposition", ""),
            cache_control=resp.headers.get("cache-control", ""),
            uploaded_at=_parse_last_modified(resp.headers.get("last-modified")),
            etag=resp.headers.get("etag", ""),
            content=b"",
            status_code=304,
        )

    content_length = resp.headers.get("content-length")
    return GetBlobResultType(
        url=blob_url,
        download_url=get_download_url(blob_url),
        pathname=pathname,
        content_type=resp.headers.get("content-type", "application/octet-stream"),
        size=int(content_length) if content_length else len(resp.content),
        content_disposition=resp.headers.get("content-disposition", ""),
        cache_control=resp.headers.get("cache-control", ""),
        uploaded_at=_parse_last_modified(resp.headers.get("last-modified")),
        etag=resp.headers.get("etag", ""),
        content=resp.content,
        status_code=resp.status_code,
    )


def _build_cache_bypass_url(blob_url: str) -> str:
    parsed = urlparse(blob_url)
    params = parse_qs(parsed.query)
    params["cache"] = ["0"]
    query = urlencode(params, doseq=True)
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            query,
            parsed.fragment,
        )
    )


def put(
    path: str,
    body: Any,
    *,
    access: Access = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    multipart: bool = False,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
) -> PutBlobResultType:
    token = ensure_token(token)
    result, used_multipart = _run_sync_blob_operation(
        lambda client: client._put_blob(
            path,
            body,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
            multipart=multipart,
            on_upload_progress=on_upload_progress,
        )
    )
    track(
        "blob_put",
        token=token,
        access=access,
        content_type=content_type,
        multipart=used_multipart,
        size_bytes=get_telemetry_size_bytes(body),
    )
    return result


async def put_async(
    path: str,
    body: Any,
    *,
    access: Access = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    multipart: bool = False,
    on_upload_progress: (
        Callable[[UploadProgressEvent], None]
        | Callable[[UploadProgressEvent], Awaitable[None]]
        | None
    ) = None,
) -> PutBlobResultType:
    token = ensure_token(token)
    async with _AsyncBlobOpsClient() as client:
        result, used_multipart = await client._put_blob(
            path,
            body,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
            multipart=multipart,
            on_upload_progress=on_upload_progress,
        )
    track(
        "blob_put",
        token=token,
        access=access,
        content_type=content_type,
        multipart=used_multipart,
        size_bytes=get_telemetry_size_bytes(body),
    )
    return result


def _derive_delete_count(args: tuple, kwargs: dict, result: Any) -> int:
    del args, kwargs, result
    count = _delete_count_context.get()
    _delete_count_context.set(None)
    return count or 1


@telemetry(
    event="blob_delete",
    capture=["token"],
    derive={"count": _derive_delete_count},
    when="after",
)
def delete(
    url_or_path: str | Iterable[str],
    *,
    token: str | None = None,
) -> None:
    token = ensure_token(token)
    normalized_urls = normalize_delete_urls(url_or_path)
    _delete_count_context.set(len(normalized_urls))
    _run_sync_blob_operation(
        lambda client: client._delete_blob(
            normalized_urls,
            token=token,
        )
    )


@telemetry(
    event="blob_delete",
    capture=["token"],
    derive={"count": _derive_delete_count},
    when="after",
)
async def delete_async(
    url_or_path: str | Iterable[str],
    *,
    token: str | None = None,
) -> None:
    token = ensure_token(token)
    normalized_urls = normalize_delete_urls(url_or_path)
    _delete_count_context.set(len(normalized_urls))
    async with _AsyncBlobOpsClient() as client:
        await client._delete_blob(
            normalized_urls,
            token=token,
        )


def head(url_or_path: str, *, token: str | None = None) -> HeadBlobResultType:
    token = ensure_token(token)
    return _run_sync_blob_operation(
        lambda client: client._head_blob(
            url_or_path,
            token=token,
        )
    )


async def head_async(url_or_path: str, *, token: str | None = None) -> HeadBlobResultType:
    token = ensure_token(token)
    async with _AsyncBlobOpsClient() as client:
        return await client._head_blob(
            url_or_path,
            token=token,
        )


def get(
    url_or_path: str,
    *,
    access: Access = "public",
    token: str | None = None,
    timeout: float | None = None,
    use_cache: bool = True,
    if_none_match: str | None = None,
) -> bytes:
    token = ensure_token(token)
    validate_access(access)
    return _run_sync_blob_operation(
        lambda client: client._get_blob(
            url_or_path,
            access=access,
            token=token,
            timeout=timeout,
            use_cache=use_cache,
            if_none_match=if_none_match,
            default_timeout=30.0,
        )
    )


async def get_async(
    url_or_path: str,
    *,
    access: Access = "public",
    token: str | None = None,
    timeout: float | None = None,
    use_cache: bool = True,
    if_none_match: str | None = None,
) -> bytes:
    token = ensure_token(token)
    validate_access(access)
    async with _AsyncBlobOpsClient() as client:
        return await client._get_blob(
            url_or_path,
            access=access,
            token=token,
            timeout=timeout,
            use_cache=use_cache,
            if_none_match=if_none_match,
            default_timeout=120.0,
        )


def list_objects(
    *,
    limit: int | None = None,
    prefix: str | None = None,
    cursor: str | None = None,
    mode: str | None = None,
    token: str | None = None,
) -> ListBlobResultType:
    token = ensure_token(token)
    return _run_sync_blob_operation(
        lambda client: client._list_objects(
            limit=limit,
            prefix=prefix,
            cursor=cursor,
            mode=mode,
            token=token,
        )
    )


async def list_objects_async(
    *,
    limit: int | None = None,
    prefix: str | None = None,
    cursor: str | None = None,
    mode: str | None = None,
    token: str | None = None,
) -> ListBlobResultType:
    token = ensure_token(token)
    async with _AsyncBlobOpsClient() as client:
        return await client._list_objects(
            limit=limit,
            prefix=prefix,
            cursor=cursor,
            mode=mode,
            token=token,
        )


def iter_objects(
    *,
    prefix: str | None = None,
    mode: str | None = None,
    token: str | None = None,
    batch_size: int | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> Iterator[ListBlobItem]:
    token = ensure_token(token)
    with _SyncBlobOpsClient() as client:
        yield from client._iter_objects_sync(
            prefix=prefix,
            mode=mode,
            token=token,
            batch_size=batch_size,
            limit=limit,
            cursor=cursor,
        )


async def iter_objects_async(
    *,
    prefix: str | None = None,
    mode: str | None = None,
    token: str | None = None,
    batch_size: int | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> AsyncIterator[ListBlobItem]:
    token = ensure_token(token)
    async with _AsyncBlobOpsClient() as client:
        async for item in client._iter_objects(
            prefix=prefix,
            mode=mode,
            token=token,
            batch_size=batch_size,
            limit=limit,
            cursor=cursor,
        ):
            yield item


def copy(
    src_path: str,
    dst_path: str,
    *,
    access: Access = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> PutBlobResultType:
    token = ensure_token(token)
    return _run_sync_blob_operation(
        lambda client: client._copy_blob(
            src_path,
            dst_path,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
        )
    )


async def copy_async(
    src_path: str,
    dst_path: str,
    *,
    access: Access = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> PutBlobResultType:
    token = ensure_token(token)
    async with _AsyncBlobOpsClient() as client:
        return await client._copy_blob(
            src_path,
            dst_path,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
        )


def create_folder(
    path: str,
    *,
    token: str | None = None,
    overwrite: bool = False,
) -> CreateFolderResultType:
    token = ensure_token(token)
    return _run_sync_blob_operation(
        lambda client: client._create_folder(
            path,
            token=token,
            overwrite=overwrite,
        )
    )


async def create_folder_async(
    path: str,
    *,
    token: str | None = None,
    overwrite: bool = False,
) -> CreateFolderResultType:
    token = ensure_token(token)
    async with _AsyncBlobOpsClient() as client:
        return await client._create_folder(
            path,
            token=token,
            overwrite=overwrite,
        )


def upload_file(
    local_path: str | PathLike,
    path: str,
    *,
    access: Access = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    multipart: bool = False,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
) -> PutBlobResultType:
    return _run_sync_blob_operation(
        lambda client: client._upload_file(
            local_path,
            path,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
            multipart=multipart,
            on_upload_progress=on_upload_progress,
            missing_local_path_error="src_path is required",
        )
    )


async def upload_file_async(
    local_path: str | PathLike,
    path: str,
    *,
    access: Access = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    multipart: bool = False,
    on_upload_progress: (
        Callable[[UploadProgressEvent], None]
        | Callable[[UploadProgressEvent], Awaitable[None]]
        | None
    ) = None,
) -> PutBlobResultType:
    async with _AsyncBlobOpsClient() as client:
        return await client._upload_file(
            local_path,
            path,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
            multipart=multipart,
            on_upload_progress=on_upload_progress,
            missing_local_path_error="local_path is required",
        )


def download_file(
    url_or_path: str,
    local_path: str | PathLike,
    *,
    access: Access = "public",
    token: str | None = None,
    timeout: float | None = None,
    overwrite: bool = True,
    create_parents: bool = True,
    progress: Callable[[int, int | None], None] | None = None,
) -> str:
    token = ensure_token(token)
    validate_access(access)
    return _run_sync_blob_operation(
        lambda client: client._download_file(
            url_or_path,
            local_path,
            access=access,
            token=token,
            timeout=timeout,
            overwrite=overwrite,
            create_parents=create_parents,
            progress=progress,
        )
    )


async def download_file_async(
    url_or_path: str,
    local_path: str | PathLike,
    *,
    access: Access = "public",
    token: str | None = None,
    timeout: float | None = None,
    overwrite: bool = True,
    create_parents: bool = True,
    progress: (
        Callable[[int, int | None], None] | Callable[[int, int | None], Awaitable[None]] | None
    ) = None,
) -> str:
    token = ensure_token(token)
    validate_access(access)
    async with _AsyncBlobOpsClient() as client:
        return await client._download_file(
            url_or_path,
            local_path,
            access=access,
            token=token,
            timeout=timeout,
            overwrite=overwrite,
            create_parents=create_parents,
            progress=progress,
        )
