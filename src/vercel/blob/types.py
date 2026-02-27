from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


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

    def __bytes__(self) -> bytes:
        return self.content


@dataclass(slots=True)
class MultipartPart:
    part_number: int
    etag: str
