from __future__ import annotations

import contextvars
import inspect
import os
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
from .errors import BlobError, BlobNotFoundError
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
    try:
        blob_url, pathname = _resolve_blob_url(url_or_path, token, access)
    except BlobError:
        if is_url(url_or_path):
            raise
        meta = head(url_or_path, token=token)
        blob_url = meta.url
        pathname = meta.pathname

    headers: dict[str, str] = {}
    if access == "private":
        headers["authorization"] = f"Bearer {token}"
    if if_none_match:
        headers["if-none-match"] = if_none_match
    fetch_url = _build_cache_bypass_url(blob_url) if not use_cache else blob_url

    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=httpx.Timeout(timeout or 30.0),
        ) as client:
            response = client.get(fetch_url, headers=headers)
            if response.status_code == 404:
                raise BlobNotFoundError()
            if response.status_code == 304:
                return b""
            response.raise_for_status()
            return _build_get_result(response, blob_url, pathname).content
    except httpx.HTTPStatusError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            raise BlobNotFoundError() from exc
        raise


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
    try:
        blob_url, pathname = _resolve_blob_url(url_or_path, token, access)
    except BlobError:
        if is_url(url_or_path):
            raise
        meta = await head_async(url_or_path, token=token)
        blob_url = meta.url
        pathname = meta.pathname

    headers: dict[str, str] = {}
    if access == "private":
        headers["authorization"] = f"Bearer {token}"
    if if_none_match:
        headers["if-none-match"] = if_none_match
    fetch_url = _build_cache_bypass_url(blob_url) if not use_cache else blob_url

    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(timeout or 120.0),
        ) as client:
            response = await client.get(fetch_url, headers=headers)
            if response.status_code == 404:
                raise BlobNotFoundError()
            if response.status_code == 304:
                return b""
            response.raise_for_status()
            return _build_get_result(response, blob_url, pathname).content
    except httpx.HTTPStatusError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            raise BlobNotFoundError() from exc
        raise


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

    try:
        blob_url, _ = _resolve_blob_url(url_or_path, token, access)
        target_url = get_download_url(blob_url)
    except BlobError:
        if is_url(url_or_path):
            raise
        meta = head(url_or_path, token=token)
        target_url = meta.download_url or meta.url
    dst = os.fspath(local_path)

    if not overwrite and os.path.exists(dst):
        raise BlobError("destination exists; pass overwrite=True to replace it")
    if create_parents:
        os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)

    tmp = dst + ".part"
    bytes_read = 0
    request_headers: dict[str, str] = {}
    if access == "private":
        request_headers["authorization"] = f"Bearer {token}"

    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=httpx.Timeout(timeout or 120.0),
        ) as client:
            with client.stream("GET", target_url, headers=request_headers) as response:
                if response.status_code == 404:
                    raise BlobNotFoundError()
                response.raise_for_status()
                total = int(response.headers.get("Content-Length", "0")) or None
                with open(tmp, "wb") as f:
                    for chunk in response.iter_bytes():
                        if not chunk:
                            continue
                        f.write(chunk)
                        bytes_read += len(chunk)
                        if progress:
                            progress(bytes_read, total)
        os.replace(tmp, dst)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise

    return dst


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

    try:
        blob_url, _ = _resolve_blob_url(url_or_path, token, access)
        target_url = get_download_url(blob_url)
    except BlobError:
        if is_url(url_or_path):
            raise
        meta = await head_async(url_or_path, token=token)
        target_url = meta.download_url or meta.url
    dst = os.fspath(local_path)

    if not overwrite and os.path.exists(dst):
        raise BlobError("destination exists; pass overwrite=True to replace it")
    if create_parents:
        os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)

    tmp = dst + ".part"
    bytes_read = 0
    request_headers: dict[str, str] = {}
    if access == "private":
        request_headers["authorization"] = f"Bearer {token}"

    try:
        async with (
            httpx.AsyncClient(
                follow_redirects=True,
                timeout=httpx.Timeout(timeout or 120.0),
            ) as client,
            client.stream("GET", target_url, headers=request_headers) as response,
        ):
            if response.status_code == 404:
                raise BlobNotFoundError()
            response.raise_for_status()
            total = int(response.headers.get("Content-Length", "0")) or None
            with open(tmp, "wb") as f:
                async for chunk in response.aiter_bytes():
                    if not chunk:
                        continue
                    f.write(chunk)
                    bytes_read += len(chunk)
                    if progress:
                        maybe = progress(bytes_read, total)
                        if inspect.isawaitable(maybe):
                            await maybe
        os.replace(tmp, dst)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise

    return dst
