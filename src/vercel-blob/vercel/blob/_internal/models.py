"""Shared Blob API types and result models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol, TypeAlias, TypedDict

from vercel._internal.core.polyfills import StrEnum


class Access(StrEnum):
    PUBLIC = "public"
    PRIVATE = "private"


class CredentialKind(StrEnum):
    READ_WRITE = "read_write"
    OIDC = "oidc"


DurationInput: TypeAlias = int | float | timedelta | None


class BlobMetadata(TypedDict, total=False):
    """Metadata to publish with a writable Blob."""

    content_type: str | None
    cache_control_max_age: DurationInput


@dataclass(frozen=True, slots=True)
class BlobCredentials:
    token: str
    store_id: str
    kind: CredentialKind


class BlobCredentialsFactory(Protocol):
    async def __call__(self) -> BlobCredentials: ...


class SyncBlobCredentialsFactory(Protocol):
    def __call__(self) -> BlobCredentials: ...


@dataclass(frozen=True, slots=True)
class BlobStatResult:
    pathname: str
    url: str
    download_url: str
    size: int
    etag: str
    uploaded_at: datetime
    content_type: str | None
    content_disposition: str
    cache_control: str
