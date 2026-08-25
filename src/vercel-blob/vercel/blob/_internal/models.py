"""Shared Blob API types and result models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal, Protocol, TypeAlias

Access: TypeAlias = Literal["public", "private"]
CredentialKind: TypeAlias = Literal["read_write", "oidc"]
DurationInput: TypeAlias = int | float | timedelta | None


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
