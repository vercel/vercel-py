"""Blob session configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass

from vercel._internal.core.options import ServiceOptions

from .credentials import _default_async_credentials, _default_sync_credentials
from .models import (
    Access,
    BlobCredentialsFactory,
    SyncBlobCredentialsFactory,
)


class _BlobServiceOptionsKey(ServiceOptions):
    """Shared registry key for sync and async Blob service options."""

    __slots__ = ()

    @classmethod
    def service_options_key(cls) -> type[ServiceOptions]:
        return _BlobServiceOptionsKey


@dataclass(frozen=True, slots=True, init=False)
class BlobServiceOptions(_BlobServiceOptionsKey):
    """Configure Blob for one SDK session."""

    base_url: str
    credentials_factory: BlobCredentialsFactory
    default_access: Access
    read_buffer_size: int

    def __init__(
        self,
        *,
        base_url: str | None = None,
        credentials_factory: BlobCredentialsFactory | None = None,
        default_access: Access = "public",
        read_buffer_size: int = 256 * 1024,
    ) -> None:
        if default_access not in ("public", "private"):
            raise ValueError("default_access must be 'public' or 'private'")
        if isinstance(read_buffer_size, bool) or not isinstance(read_buffer_size, int):
            raise TypeError("read_buffer_size must be an integer")
        if read_buffer_size <= 0:
            raise ValueError("read_buffer_size must be positive")
        object.__setattr__(
            self,
            "base_url",
            os.environ.get("VERCEL_BLOB_API_URL")
            or os.environ.get("NEXT_PUBLIC_VERCEL_BLOB_API_URL")
            or "https://vercel.com/api/blob"
            if base_url is None
            else base_url,
        )
        object.__setattr__(
            self,
            "credentials_factory",
            credentials_factory or _default_async_credentials,
        )
        object.__setattr__(self, "default_access", default_access)
        object.__setattr__(self, "read_buffer_size", read_buffer_size)


@dataclass(frozen=True, slots=True, init=False)
class SyncBlobServiceOptions(_BlobServiceOptionsKey):
    """Configure synchronous Blob calls for one SDK session."""

    base_url: str
    credentials_factory: SyncBlobCredentialsFactory
    default_access: Access
    read_buffer_size: int

    def __init__(
        self,
        *,
        base_url: str | None = None,
        credentials_factory: SyncBlobCredentialsFactory | None = None,
        default_access: Access = "public",
        read_buffer_size: int = 256 * 1024,
    ) -> None:
        if default_access not in ("public", "private"):
            raise ValueError("default_access must be 'public' or 'private'")
        if isinstance(read_buffer_size, bool) or not isinstance(read_buffer_size, int):
            raise TypeError("read_buffer_size must be an integer")
        if read_buffer_size <= 0:
            raise ValueError("read_buffer_size must be positive")
        object.__setattr__(
            self,
            "base_url",
            os.environ.get("VERCEL_BLOB_API_URL")
            or os.environ.get("NEXT_PUBLIC_VERCEL_BLOB_API_URL")
            or "https://vercel.com/api/blob"
            if base_url is None
            else base_url,
        )
        object.__setattr__(
            self,
            "credentials_factory",
            credentials_factory or _default_sync_credentials,
        )
        object.__setattr__(self, "default_access", default_access)
        object.__setattr__(self, "read_buffer_size", read_buffer_size)
