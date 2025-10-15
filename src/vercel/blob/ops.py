from __future__ import annotations

import inspect
from os import PathLike
import os
import httpx
from typing import Any, Callable, Awaitable, Iterable, Iterator, AsyncIterator

from .utils import (
    UploadProgressEvent,
    parse_datetime,
    is_url,
    create_put_headers,
    create_put_options,
    PUT_OPTION_HEADER_MAP,
)
from .api import request_api, request_api_async
from .types import (
    PutBlobResult as PutBlobResultType,
    HeadBlobResult as HeadBlobResultType,
    ListBlobItem,
    ListBlobResult as ListBlobResultType,
    CreateFolderResult as CreateFolderResultType,
)
from .errors import BlobError, BlobNotFoundError
from .multipart import auto_multipart_upload, auto_multipart_upload_async


def put(
    path: str,
    body: Any,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    multipart: bool = False,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
) -> PutBlobResultType:
    if body is None:
        raise BlobError("body is required")

    if isinstance(body, dict):
        raise BlobError(
            "Body must be a string, buffer or stream. You sent a plain object, double check what you're trying to upload."
        )

    options: dict[str, Any] = {
        "access": access,
        "contentType": content_type,
        "addRandomSuffix": add_random_suffix,
        "allowOverwrite": overwrite,
        "cacheControlMaxAge": cache_control_max_age,
        "token": token,
        "multipart": multipart,
    }

    opts = create_put_options(path=path, options=options)
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )

    if opts.get("multipart") is True:
        raw = auto_multipart_upload(
            path,
            body,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
            on_upload_progress=on_upload_progress,
        )
        return PutBlobResultType(
            url=raw["url"],
            download_url=raw["downloadUrl"],
            pathname=raw["pathname"],
            content_type=raw["contentType"],
            content_disposition=raw["contentDisposition"],
        )

    params = {"pathname": path}
    raw = request_api(
        "",
        "PUT",
        options=opts,
        headers=headers,
        params=params,
        body=body,
        on_upload_progress=on_upload_progress,
    )
    return PutBlobResultType(
        url=raw["url"],
        download_url=raw["downloadUrl"],
        pathname=raw["pathname"],
        content_type=raw["contentType"],
        content_disposition=raw["contentDisposition"],
    )


async def put_async(
    path: str,
    body: Any,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    multipart: bool = False,
    on_upload_progress: Callable[[UploadProgressEvent], None]
    | Callable[[UploadProgressEvent], Awaitable[None]]
    | None = None,
) -> PutBlobResultType:
    if body is None:
        raise BlobError("body is required")

    # Reject plain dict (JS plain object equivalent) to match TS error semantics
    if isinstance(body, dict):
        raise BlobError(
            "Body must be a string, buffer or stream. You sent a plain object, double check what you're trying to upload."
        )

    options: dict[str, Any] = {
        "access": access,
        "contentType": content_type,
        "addRandomSuffix": add_random_suffix,
        "allowOverwrite": overwrite,
        "cacheControlMaxAge": cache_control_max_age,
        "token": token,
        "multipart": multipart,
    }

    opts = create_put_options(path=path, options=options, extra_checks=None, get_token=None)
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )

    # Multipart auto support
    if opts.get("multipart") is True:
        raw = await auto_multipart_upload_async(
            path,
            body,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
            on_upload_progress=on_upload_progress,
        )
        return PutBlobResultType(
            url=raw["url"],
            download_url=raw["downloadUrl"],
            pathname=raw["pathname"],
            content_type=raw["contentType"],
            content_disposition=raw["contentDisposition"],
        )

    params = {"pathname": path}
    raw = await request_api_async(
        "",
        "PUT",
        options=opts,
        headers=headers,
        params=params,
        body=body,
        on_upload_progress=on_upload_progress,
    )
    return PutBlobResultType(
        url=raw["url"],
        download_url=raw["downloadUrl"],
        pathname=raw["pathname"],
        content_type=raw["contentType"],
        content_disposition=raw["contentDisposition"],
    )


def delete(
    url_or_path: str | Iterable[str],
    *,
    token: str | None = None,
) -> None:
    if isinstance(url_or_path, Iterable) and not isinstance(url_or_path, (str, bytes)):
        urls = [str(u) for u in url_or_path]
    else:
        urls = [str(url_or_path)]
    request_api(
        "/delete",
        "POST",
        options={"token": token} if token else {},
        headers={"content-type": "application/json"},
        body={"urls": urls},
    )


async def delete_async(
    url_or_path: str | Iterable[str],
    *,
    token: str | None = None,
) -> None:
    if isinstance(url_or_path, Iterable) and not isinstance(url_or_path, (str, bytes)):
        urls = [str(u) for u in url_or_path]
    else:
        urls = [str(url_or_path)]
    await request_api_async(
        "/delete",
        "POST",
        options={"token": token} if token else {},
        headers={"content-type": "application/json"},
        body={"urls": urls},
    )


def head(url_or_path: str, *, token: str | None = None) -> HeadBlobResultType:
    params = {"url": url_or_path}
    resp = request_api(
        "",
        "GET",
        options={"token": token} if token else {},
        params=params,
    )
    uploaded_at = (
        parse_datetime(resp["uploadedAt"])
        if isinstance(resp.get("uploadedAt"), str)
        else resp["uploadedAt"]
    )
    return HeadBlobResultType(
        size=resp["size"],
        uploaded_at=uploaded_at,
        pathname=resp["pathname"],
        content_type=resp["contentType"],
        content_disposition=resp["contentDisposition"],
        url=resp["url"],
        download_url=resp["downloadUrl"],
        cache_control=resp["cacheControl"],
    )


async def head_async(url_or_path: str, *, token: str | None = None) -> HeadBlobResultType:
    params = {"url": url_or_path}
    resp = await request_api_async(
        "",
        "GET",
        options={"token": token} if token else {},
        params=params,
    )
    uploaded_at = (
        parse_datetime(resp["uploadedAt"])
        if isinstance(resp.get("uploadedAt"), str)
        else resp["uploadedAt"]
    )
    return HeadBlobResultType(
        size=resp["size"],
        uploaded_at=uploaded_at,
        pathname=resp["pathname"],
        content_type=resp["contentType"],
        content_disposition=resp["contentDisposition"],
        url=resp["url"],
        download_url=resp["downloadUrl"],
        cache_control=resp["cacheControl"],
    )


def get(
    url_or_path: str,
    *,
    token: str | None = None,
    timeout: float | None = None,
) -> bytes:
    target_url: str
    if is_url(url_or_path):
        target_url = url_or_path
    else:
        metadata = head(url_or_path, token=token)
        target_url = metadata.url

    try:
        with httpx.Client(follow_redirects=True, timeout=httpx.Timeout(timeout or 30.0)) as client:
            resp = client.get(target_url)
            if resp.status_code == 404:
                raise BlobNotFoundError()
            resp.raise_for_status()
            return resp.content
    except httpx.HTTPStatusError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            raise BlobNotFoundError() from exc
        raise
    except httpx.HTTPError:
        raise


async def get_async(
    url_or_path: str,
    *,
    token: str | None = None,
    timeout: float | None = None,
) -> bytes:
    target_url: str
    if is_url(url_or_path):
        target_url = url_or_path
    else:
        metadata = await head_async(url_or_path, token=token)
        target_url = metadata.url

    try:
        async with httpx.AsyncClient(
            follow_redirects=True, timeout=httpx.Timeout(timeout or 120.0)
        ) as client:
            resp = await client.get(target_url)
            if resp.status_code == 404:
                raise BlobNotFoundError()
            resp.raise_for_status()
            return resp.content
    except httpx.HTTPStatusError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            raise BlobNotFoundError() from exc
        raise
    except httpx.HTTPError:
        raise


def list_objects(
    *,
    limit: int | None = None,
    prefix: str | None = None,
    cursor: str | None = None,
    mode: str | None = None,
    token: str | None = None,
) -> ListBlobResultType:
    params: dict[str, Any] = {}
    if limit is not None:
        params["limit"] = int(limit)
    if prefix is not None:
        params["prefix"] = prefix
    if cursor is not None:
        params["cursor"] = cursor
    if mode is not None:
        params["mode"] = mode

    resp = request_api(
        "",
        "GET",
        options={"token": token} if token else {},
        params=params,
    )
    blobs_list: list[ListBlobItem] = []
    for b in resp.get("blobs", []):
        uploaded_at = (
            parse_datetime(b["uploadedAt"])
            if isinstance(b.get("uploadedAt"), str)
            else b["uploadedAt"]
        )
        blobs_list.append(
            ListBlobItem(
                url=b["url"],
                download_url=b["downloadUrl"],
                pathname=b["pathname"],
                size=b["size"],
                uploaded_at=uploaded_at,
            )
        )
    return ListBlobResultType(
        blobs=blobs_list,
        cursor=resp.get("cursor"),
        has_more=resp.get("hasMore", False),
        folders=resp.get("folders"),
    )


async def list_objects_async(
    *,
    limit: int | None = None,
    prefix: str | None = None,
    cursor: str | None = None,
    mode: str | None = None,
    token: str | None = None,
) -> ListBlobResultType:
    params: dict[str, Any] = {}
    if limit is not None:
        params["limit"] = int(limit)
    if prefix is not None:
        params["prefix"] = prefix
    if cursor is not None:
        params["cursor"] = cursor
    if mode is not None:
        params["mode"] = mode

    resp = await request_api_async(
        "",
        "GET",
        options={"token": token} if token else {},
        params=params,
    )
    blobs_list: list[ListBlobItem] = []
    for b in resp.get("blobs", []):
        uploaded_at = (
            parse_datetime(b["uploadedAt"])
            if isinstance(b.get("uploadedAt"), str)
            else b["uploadedAt"]
        )
        blobs_list.append(
            ListBlobItem(
                url=b["url"],
                download_url=b["downloadUrl"],
                pathname=b["pathname"],
                size=b["size"],
                uploaded_at=uploaded_at,
            )
        )
    return ListBlobResultType(
        blobs=blobs_list,
        cursor=resp.get("cursor"),
        has_more=resp.get("hasMore", False),
        folders=resp.get("folders"),
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
    next_cursor = cursor
    yielded_count = 0
    while True:
        effective_limit: int | None = batch_size
        if limit is not None:
            remaining = limit - yielded_count
            if remaining <= 0:
                break
            if effective_limit is None or effective_limit > remaining:
                effective_limit = remaining
        page = list_objects(
            limit=effective_limit,
            prefix=prefix,
            cursor=next_cursor,
            mode=mode,
            token=token,
        )
        for item in page.blobs:
            yield item
            if limit is not None:
                yielded_count += 1
                if yielded_count >= limit:
                    return
        if not page.has_more or not page.cursor:
            break
        next_cursor = page.cursor


async def iter_objects_async(
    *,
    prefix: str | None = None,
    mode: str | None = None,
    token: str | None = None,
    batch_size: int | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> AsyncIterator[ListBlobItem]:
    next_cursor = cursor
    yielded_count = 0
    while True:
        effective_limit: int | None = batch_size
        if limit is not None:
            remaining = limit - yielded_count
            if remaining <= 0:
                break
            if effective_limit is None or effective_limit > remaining:
                effective_limit = remaining
        page = await list_objects_async(
            limit=effective_limit,
            prefix=prefix,
            cursor=next_cursor,
            mode=mode,
            token=token,
        )
        for item in page.blobs:
            yield item
            if limit is not None:
                yielded_count += 1
                if yielded_count >= limit:
                    return
        if not page.has_more or not page.cursor:
            break
        next_cursor = page.cursor


def copy(
    src_path: str,
    dst_path: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> PutBlobResultType:
    if not is_url(src_path):
        meta = head(src_path, token=token)
        src_path = meta.url

    options: dict[str, Any] = {
        "access": access,
        "contentType": content_type,
        "addRandomSuffix": add_random_suffix,
        "allowOverwrite": overwrite,
        "cacheControlMaxAge": cache_control_max_age,
        "token": token,
    }
    opts = create_put_options(path=dst_path, options=options)
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )
    params = {"pathname": dst_path, "fromUrl": src_path}
    raw = request_api(
        "",
        "PUT",
        options=opts,
        headers=headers,
        params=params,
    )
    return PutBlobResultType(
        url=raw["url"],
        download_url=raw["downloadUrl"],
        pathname=raw["pathname"],
        content_type=raw["contentType"],
        content_disposition=raw["contentDisposition"],
    )


async def copy_async(
    src_path: str,
    dst_path: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> PutBlobResultType:
    if not is_url(src_path):
        meta = head(src_path, token=token)
        src_path = meta.url
    dst_path = str(dst_path)

    options: dict[str, Any] = {
        "access": access,
        "contentType": content_type,
        "addRandomSuffix": add_random_suffix,
        "allowOverwrite": overwrite,
        "cacheControlMaxAge": cache_control_max_age,
        "token": token,
    }
    opts = create_put_options(path=dst_path, options=options)
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )
    params = {"pathname": dst_path, "fromUrl": src_path}
    raw = await request_api_async(
        "",
        "PUT",
        options=opts,
        headers=headers,
        params=params,
    )
    return PutBlobResultType(
        url=raw["url"],
        download_url=raw["downloadUrl"],
        pathname=raw["pathname"],
        content_type=raw["contentType"],
        content_disposition=raw["contentDisposition"],
    )


def create_folder(
    path: str,
    *,
    token: str | None = None,
    overwrite: bool = False,
) -> CreateFolderResultType:
    folder_path = path if path.endswith("/") else path + "/"
    headers = {PUT_OPTION_HEADER_MAP["addRandomSuffix"]: "0"}
    if overwrite:
        headers[PUT_OPTION_HEADER_MAP["allowOverwrite"]] = "1"
    params = {"pathname": folder_path}
    raw = request_api(
        "",
        "PUT",
        options={"token": token} if token else {},
        headers=headers,
        params=params,
    )
    return CreateFolderResultType(pathname=raw["pathname"], url=raw["url"])


async def create_folder_async(
    path: str,
    *,
    token: str | None = None,
    overwrite: bool = False,
) -> CreateFolderResultType:
    folder_path = path if path.endswith("/") else path + "/"
    headers = {PUT_OPTION_HEADER_MAP["addRandomSuffix"]: "0"}
    if overwrite:
        headers[PUT_OPTION_HEADER_MAP["allowOverwrite"]] = "1"
    params = {"pathname": folder_path}
    raw = await request_api_async(
        "",
        "PUT",
        options={"token": token} if token else {},
        headers=headers,
        params=params,
    )
    return CreateFolderResultType(pathname=raw["pathname"], url=raw["url"])


def upload_file(
    local_path: str | PathLike,
    path: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    multipart: bool = False,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
) -> PutBlobResultType:
    if not local_path:
        raise BlobError("src_path is required")
    if not path:
        raise BlobError("path is required")
    if not os.path.exists(os.fspath(local_path)):
        raise BlobError("local_path does not exist")
    if not os.path.isfile(os.fspath(local_path)):
        raise BlobError("local_path is not a file")

    # Auto-enable multipart if file size exceeds 5 MiB
    size_bytes = os.path.getsize(os.fspath(local_path))
    use_multipart = multipart or (size_bytes > 5 * 1024 * 1024)

    with open(os.fspath(local_path), "rb") as f:
        return put(
            path,
            f,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
            multipart=use_multipart,
            on_upload_progress=on_upload_progress,
        )


async def upload_file_async(
    local_path: str | PathLike,
    path: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    multipart: bool = False,
    on_upload_progress: Callable[[UploadProgressEvent], None]
    | Callable[[UploadProgressEvent], Awaitable[None]]
    | None = None,
) -> PutBlobResultType:
    if not local_path:
        raise BlobError("local_path is required")
    if not path:
        raise BlobError("path is required")
    if not os.path.exists(os.fspath(local_path)):
        raise BlobError("local_path does not exist")
    if not os.path.isfile(os.fspath(local_path)):
        raise BlobError("local_path is not a file")

    # Auto-enable multipart if file size exceeds 5 MiB
    size_bytes = os.path.getsize(os.fspath(local_path))
    use_multipart = multipart or (size_bytes > 5 * 1024 * 1024)

    with open(os.fspath(local_path), "rb") as f:
        return await put_async(
            path,
            f,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
            multipart=use_multipart,
            on_upload_progress=on_upload_progress,
        )


def download_file(
    url_or_path: str,
    local_path: str | PathLike,
    *,
    token: str | None = None,
    timeout: float | None = None,
    overwrite: bool = True,
    create_parents: bool = True,
    progress: Callable[[int, int | None], None] | None = None,
) -> str:
    # Resolve remote URL from url_or_path
    if is_url(url_or_path):
        target_url = url_or_path
    else:
        meta = head(url_or_path, token=token)
        target_url = meta.download_url or meta.url

    # Prepare destination
    dst = os.fspath(local_path)
    if not overwrite and os.path.exists(dst):
        raise BlobError("destination exists; pass overwrite=True to replace it")
    if create_parents:
        os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)

    tmp = dst + ".part"
    bytes_read = 0

    try:
        with httpx.Client(follow_redirects=True, timeout=httpx.Timeout(timeout or 120.0)) as client:
            with client.stream("GET", target_url) as resp:
                if resp.status_code == 404:
                    raise BlobNotFoundError()
                resp.raise_for_status()
                total = int(resp.headers.get("Content-Length", "0")) or None
                with open(tmp, "wb") as f:
                    for chunk in resp.iter_bytes():
                        if chunk:
                            f.write(chunk)
                            bytes_read += len(chunk)
                            if progress:
                                progress(bytes_read, total)

        os.replace(tmp, dst)  # atomic finalize
    except Exception:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        finally:
            raise
    return dst


async def download_file_async(
    url_or_path: str,
    local_path: str | PathLike,
    *,
    token: str | None = None,
    timeout: float | None = None,
    overwrite: bool = True,
    create_parents: bool = True,
    progress: Callable[[int, int | None], None]
    | Callable[[int, int | None], Awaitable[None]]
    | None = None,
) -> str:
    # Resolve remote URL from url_or_path
    if is_url(url_or_path):
        target_url = url_or_path
    else:
        meta = await head_async(url_or_path, token=token)
        target_url = meta.download_url or meta.url

    # Prepare destination
    dst = os.fspath(local_path)
    if not overwrite and os.path.exists(dst):
        raise BlobError("destination exists; pass overwrite=True to replace it")
    if create_parents:
        os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)

    tmp = dst + ".part"
    bytes_read = 0

    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(timeout or 120.0),
        ) as client:
            async with client.stream("GET", target_url) as resp:
                if resp.status_code == 404:
                    raise BlobNotFoundError()
                resp.raise_for_status()
                total = int(resp.headers.get("Content-Length", "0")) or None
                with open(tmp, "wb") as f:
                    async for chunk in resp.aiter_bytes():
                        if chunk:
                            f.write(chunk)
                            bytes_read += len(chunk)
                            if progress:
                                maybe = progress(bytes_read, total)
                                if inspect.isawaitable(maybe):
                                    await maybe

        os.replace(tmp, dst)  # atomic finalize
    except Exception:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        finally:
            raise
    return dst
