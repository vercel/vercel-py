"""Session configuration options for Vercel Blob."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta

from vercel._internal.core.options import ServiceOptions
from vercel.blob.models import BlobCredentialsFactory, SyncBlobCredentialsFactory

from .credentials import default_async_credentials, default_sync_credentials

_DEFAULT_TRANSFER_TIMEOUT = timedelta(minutes=5)


class _BlobServiceOptionsKey(ServiceOptions):
    """Shared registry key for sync and async Blob service options."""

    __slots__ = ()

    @classmethod
    def service_options_key(cls) -> type[ServiceOptions]:
        return _BlobServiceOptionsKey


@dataclass(frozen=True, slots=True, init=False)
class BlobServiceOptions(_BlobServiceOptionsKey):
    """Configure Blob for one asynchronous SDK session.

    `transfer_timeout` replaces the session's HTTP timeout for uploads, multipart
    parts and completion, and copies, whose duration grows with object size.
    """

    base_url: str
    credentials_factory: BlobCredentialsFactory
    transfer_timeout: timedelta

    def __init__(
        self,
        *,
        base_url: str | None = None,
        credentials_factory: BlobCredentialsFactory | None = None,
        transfer_timeout: timedelta | None = None,
    ) -> None:
        raw_url = (
            base_url
            if base_url is not None
            else os.environ.get("VERCEL_BLOB_API_URL")
            or os.environ.get("NEXT_PUBLIC_VERCEL_BLOB_API_URL")
            or "https://vercel.com/api/blob"
        )
        object.__setattr__(self, "base_url", raw_url.rstrip("/"))
        object.__setattr__(
            self,
            "credentials_factory",
            credentials_factory or default_async_credentials,
        )
        object.__setattr__(
            self,
            "transfer_timeout",
            transfer_timeout if transfer_timeout is not None else _DEFAULT_TRANSFER_TIMEOUT,
        )


@dataclass(frozen=True, slots=True, init=False)
class SyncBlobServiceOptions(_BlobServiceOptionsKey):
    """Configure Blob for one synchronous SDK session.

    `transfer_timeout` replaces the session's HTTP timeout for uploads, multipart
    parts and completion, and copies, whose duration grows with object size.
    """

    base_url: str
    credentials_factory: SyncBlobCredentialsFactory
    transfer_timeout: timedelta

    def __init__(
        self,
        *,
        base_url: str | None = None,
        credentials_factory: SyncBlobCredentialsFactory | None = None,
        transfer_timeout: timedelta | None = None,
    ) -> None:
        raw_url = (
            base_url
            if base_url is not None
            else os.environ.get("VERCEL_BLOB_API_URL")
            or os.environ.get("NEXT_PUBLIC_VERCEL_BLOB_API_URL")
            or "https://vercel.com/api/blob"
        )
        object.__setattr__(self, "base_url", raw_url.rstrip("/"))
        object.__setattr__(
            self,
            "credentials_factory",
            credentials_factory or default_sync_credentials,
        )
        object.__setattr__(
            self,
            "transfer_timeout",
            transfer_timeout if transfer_timeout is not None else _DEFAULT_TRANSFER_TIMEOUT,
        )
