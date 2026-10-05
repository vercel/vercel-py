"""Domain models and result types for Vercel Blob."""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Protocol, TypeAlias

from vercel._internal.core.polyfills import StrEnum

Access: TypeAlias = Literal["public", "private"]


class CredentialKind(StrEnum):
    READ_WRITE = "read_write"
    OIDC = "oidc"


@dataclass(frozen=True, slots=True)
class BlobCredentials:
    token: str
    store_id: str
    kind: CredentialKind = CredentialKind.READ_WRITE

    def __repr__(self) -> str:
        return f"BlobCredentials(token='***', store_id={self.store_id!r}, kind={self.kind!r})"


BlobCredentialsFactory: TypeAlias = (
    Callable[[], Coroutine[Any, Any, BlobCredentials]] | Callable[[], BlobCredentials]
)


class SyncBlobCredentialsFactory(Protocol):
    def __call__(self) -> BlobCredentials: ...


@dataclass(frozen=True, slots=True)
class PutResult:
    """Metadata returned immediately from a successful PUT operation."""

    url: str
    download_url: str
    pathname: str
    content_type: str
    content_disposition: str
    etag: str


@dataclass(frozen=True, slots=True)
class HeadResult:
    """Complete object metadata returned by HEAD (via API query)."""

    url: str
    download_url: str
    pathname: str
    size: int
    etag: str
    uploaded_at: datetime
    content_type: str | None
    content_disposition: str
    cache_control: str


@dataclass(frozen=True, slots=True)
class DownloadMetadata:
    """Response-derived metadata available immediately on GET entry."""

    url: str
    status_code: int
    size: int | None = None
    content_type: str | None = None
    content_disposition: str | None = None
    cache_control: str | None = None
    etag: str | None = None
    last_modified: datetime | None = None
    headers: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class GetResult:
    """A fully buffered object and its response-derived metadata."""

    metadata: DownloadMetadata
    body: bytes
