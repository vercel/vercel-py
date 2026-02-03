"""Core business logic for Vercel Blob operations."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from .._http import (
    AsyncTransport,
    BaseTransport,
    BlockingTransport,
    HTTPConfig,
    JSONBody,
)
from .types import (
    CreateFolderResult as CreateFolderResultType,
    HeadBlobResult as HeadBlobResultType,
    ListBlobItem,
    ListBlobResult as ListBlobResultType,
    PutBlobResult as PutBlobResultType,
)
from .utils import (
    PutHeaders,
    create_put_headers,
    ensure_token,
    get_api_url,
    is_url,
    parse_datetime,
    require_public_access,
    validate_path,
)


def build_put_blob_result(raw: dict[str, Any]) -> PutBlobResultType:
    """Build PutBlobResult from raw API response."""
    return PutBlobResultType(
        url=raw["url"],
        download_url=raw["downloadUrl"],
        pathname=raw["pathname"],
        content_type=raw["contentType"],
        content_disposition=raw["contentDisposition"],
    )


def build_head_blob_result(resp: dict[str, Any]) -> HeadBlobResultType:
    """Build HeadBlobResult from raw API response."""
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


def build_list_blob_result(resp: dict[str, Any]) -> ListBlobResultType:
    """Build ListBlobResult from raw API response."""
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


def build_create_folder_result(raw: dict[str, Any]) -> CreateFolderResultType:
    """Build CreateFolderResult from raw API response."""
    return CreateFolderResultType(pathname=raw["pathname"], url=raw["url"])


def build_list_params(
    limit: int | None = None,
    prefix: str | None = None,
    cursor: str | None = None,
    mode: str | None = None,
) -> dict[str, Any]:
    """Build query parameters for list_objects."""
    params: dict[str, Any] = {}
    if limit is not None:
        params["limit"] = int(limit)
    if prefix is not None:
        params["prefix"] = prefix
    if cursor is not None:
        params["cursor"] = cursor
    if mode is not None:
        params["mode"] = mode
    return params


def normalize_delete_urls(url_or_path: str | list[str] | tuple[str, ...]) -> list[str]:
    """Normalize delete URL input to a list of URL strings."""
    if isinstance(url_or_path, (list, tuple)):
        return [str(u) for u in url_or_path]
    return [str(url_or_path)]


__all__ = [
    "build_put_blob_result",
    "build_head_blob_result",
    "build_list_blob_result",
    "build_create_folder_result",
    "build_list_params",
    "normalize_delete_urls",
]
