from __future__ import annotations

from typing import Any, Callable

from ._helpers import UploadProgressEvent, parse_datetime
from ._put_helpers import create_put_headers, create_put_options, PUT_OPTION_HEADER_MAP
from ._request import request_api
from .types import (
    PutBlobResult as PutBlobResultType,
    HeadBlobResult as HeadBlobResultType,
    ListBlobItem,
    ListBlobResult as ListBlobResultType,
    CreateFolderResult as CreateFolderResultType,
)
from .errors import BlobError, BlobNotFoundError
from .multipart import uncontrolled_multipart_upload


async def put(
    pathname: str,
    body: Any,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    allow_overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    multipart: bool = False,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
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
        "allowOverwrite": allow_overwrite,
        "cacheControlMaxAge": cache_control_max_age,
        "token": token,
        "multipart": multipart,
    }

    opts = await create_put_options(
        pathname=pathname, options=options, extra_checks=None, get_token=None
    )
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )

    # Multipart uncontrolled support
    if opts.get("multipart") is True:
        raw = await uncontrolled_multipart_upload(
            pathname,
            body,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            allow_overwrite=allow_overwrite,
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

    params = {"pathname": pathname}
    raw = await request_api(
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


async def delete(url_or_pathname: Any, *, token: str | None = None) -> None:
    urls: list[str]
    if isinstance(url_or_pathname, (list, tuple)):
        urls = [str(u) for u in url_or_pathname]
    else:
        urls = [str(url_or_pathname)]

    await request_api(
        "/delete",
        "POST",
        options={"token": token} if token else {},
        headers={"content-type": "application/json"},
        body={"urls": urls},
    )


async def head(url_or_pathname: str, *, token: str | None = None) -> HeadBlobResultType:
    params = {"url": url_or_pathname}
    resp = await request_api(
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


async def list_blobs(
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

    resp = await request_api(
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


async def copy(
    from_url_or_pathname: str,
    to_pathname: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    allow_overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> PutBlobResultType:
    options: dict[str, Any] = {
        "access": access,
        "contentType": content_type,
        "addRandomSuffix": add_random_suffix,
        "allowOverwrite": allow_overwrite,
        "cacheControlMaxAge": cache_control_max_age,
        "token": token,
    }
    opts = await create_put_options(
        pathname=to_pathname, options=options, extra_checks=None, get_token=None
    )
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )
    params = {"pathname": to_pathname, "fromUrl": from_url_or_pathname}
    raw = await request_api(
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


async def create_folder(
    pathname: str,
    *,
    token: str | None = None,
    allow_overwrite: bool = False,
) -> CreateFolderResultType:
    folder_pathname = pathname if pathname.endswith("/") else pathname + "/"
    headers = {PUT_OPTION_HEADER_MAP["addRandomSuffix"]: "0"}
    if allow_overwrite:
        headers[PUT_OPTION_HEADER_MAP["allowOverwrite"]] = "1"
    params = {"pathname": folder_pathname}
    raw = await request_api(
        "",
        "PUT",
        options={"token": token} if token else {},
        headers=headers,
        params=params,
    )
    return CreateFolderResultType(pathname=raw["pathname"], url=raw["url"])
