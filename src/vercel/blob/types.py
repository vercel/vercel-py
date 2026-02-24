from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Literal


@dataclass(slots=True)
class PutBlobResult:
    url: str
    download_url: str
    pathname: str
    content_type: str
    content_disposition: str


@dataclass(slots=True)
class HeadBlobResult:
    size: int
    uploaded_at: datetime
    pathname: str
    content_type: str
    content_disposition: str
    url: str
    download_url: str
    cache_control: str


@dataclass(slots=True)
class ListBlobItem:
    url: str
    download_url: str
    pathname: str
    size: int
    uploaded_at: datetime


@dataclass(slots=True)
class ListBlobResult:
    blobs: list[ListBlobItem]
    cursor: str | None
    has_more: bool
    folders: list[str] | None = None


@dataclass(slots=True)
class CreateFolderResult:
    pathname: str
    url: str


@dataclass(slots=True)
class MultipartCreateResult:
    upload_id: str
    key: str


@dataclass(slots=True)
class GetBlobResult:
    url: str
    download_url: str
    pathname: str
    content_type: str | None
    size: int | None
    content_disposition: str
    cache_control: str
    uploaded_at: datetime
    etag: str
    content: bytes
    status_code: int


@dataclass(slots=True)
class MultipartPart:
    part_number: int
    etag: str


Access = Literal["public", "private"]


@dataclass
class UploadProgressEvent:
    loaded: int
    total: int
    percentage: float


OnUploadProgressCallback = (
    Callable[[UploadProgressEvent], None] | Callable[[UploadProgressEvent], Awaitable[None]]
)


def get_download_url(blob_url: str) -> str:
    try:
        from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

        parsed = urlparse(blob_url)
        q = dict(parse_qsl(parsed.query))
        q["download"] = "1"
        new_query = urlencode(q)
        return urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                new_query,
                parsed.fragment,
            )
        )
    except Exception:
        # Fallback: naive append
        sep = "&" if "?" in blob_url else "?"
        return f"{blob_url}{sep}download=1"
