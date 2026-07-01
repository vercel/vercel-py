"""Service options and credential resolution for experimental Blob calls."""

import os
from collections.abc import Awaitable
from dataclasses import dataclass
from inspect import isawaitable
from typing import Any, Literal, Protocol, cast

import vercel.oidc as oidc
import vercel.oidc.aio as oidc_aio
from vercel._internal.blob.errors import BlobError
from vercel._internal.blob.multipart import DEFAULT_PART_SIZE, validate_part_size
from vercel._internal.blob.types import Access
from vercel._internal.unstable.blob.errors import BlobCredentialsError
from vercel._internal.unstable.blob.headers import extract_store_id_from_token, get_api_url
from vercel._internal.unstable.options import ServiceOptions


@dataclass(frozen=True, slots=True)
class BlobCredentials:
    """Credentials for Blob data-plane and control-plane requests."""

    token: str
    store_id: str
    kind: Literal["read_write", "oidc"]


class BlobCredentialsFactory(Protocol):
    """Asynchronously resolve fresh credentials for one Blob request."""

    async def __call__(self) -> BlobCredentials: ...


class SyncBlobCredentialsFactory(Protocol):
    """Resolve fresh credentials without returning an awaitable."""

    def __call__(self) -> BlobCredentials: ...


class _BlobCredentialsResolver(Protocol):
    async def resolve(self) -> BlobCredentials: ...


class _AsyncBlobCredentialsResolver:
    def __init__(self, factory: BlobCredentialsFactory) -> None:
        self.factory = factory

    async def resolve(self) -> BlobCredentials:
        result = self.factory()
        if isawaitable(result):
            return await cast(Awaitable[BlobCredentials], result)
        return cast(BlobCredentials, result)


class _SyncBlobCredentialsResolver:
    def __init__(self, factory: SyncBlobCredentialsFactory) -> None:
        self.factory = factory

    async def resolve(self) -> BlobCredentials:
        result: Any = self.factory()
        if isawaitable(result):
            error = BlobCredentialsError(
                "Synchronous Blob sessions require sync_credentials_factory to return "
                "BlobCredentials without awaiting"
            )
            close = getattr(result, "close", None)
            if callable(close):
                try:
                    close()
                except BaseException as cleanup:
                    raise error from cleanup
            raise error
        return cast(BlobCredentials, result)


def _extract_read_write_store_id(token: str) -> str:
    prefix = "vercel_blob_rw_"
    if not token.startswith(prefix):
        raise BlobCredentialsError("Blob read-write token has an invalid format")
    store_id, separator, remainder = token[len(prefix) :].partition("_")
    if not separator or not store_id.strip() or not remainder.strip().strip("_"):
        raise BlobCredentialsError("Blob read-write token has an invalid format")
    return extract_store_id_from_token(token)


def _normalize_store_id(store_id: str) -> str:
    normalized = store_id.removeprefix("store_")
    if not normalized.strip():
        raise BlobCredentialsError("Blob credentials must identify a non-empty store")
    return normalized


def _normalize_blob_credentials(credentials: BlobCredentials) -> BlobCredentials:
    if not isinstance(credentials.token, str) or not credentials.token.strip():
        raise BlobCredentialsError("Blob credentials must include a non-empty token")
    if credentials.kind not in ("read_write", "oidc"):
        raise BlobCredentialsError(f"Unknown Blob credential kind: {credentials.kind!r}")

    if not isinstance(credentials.store_id, str):
        raise BlobCredentialsError("Blob credentials must identify a string store ID")
    store_id = _normalize_store_id(credentials.store_id)
    if credentials.kind == "read_write":
        embedded_store_id = _extract_read_write_store_id(credentials.token)
        embedded_store_id = _normalize_store_id(embedded_store_id)
        if store_id != embedded_store_id:
            raise BlobCredentialsError(
                "Blob read-write token store ID does not match the supplied store ID"
            )
        store_id = embedded_store_id
    return BlobCredentials(token=credentials.token, store_id=store_id, kind=credentials.kind)


def _credentials_from_oidc_token(oidc_token: str | None) -> BlobCredentials | None:
    if not oidc_token:
        return None

    store_id = os.getenv("BLOB_STORE_ID")
    if store_id:
        try:
            return _normalize_blob_credentials(
                BlobCredentials(token=oidc_token, store_id=store_id, kind="oidc")
            )
        except BlobCredentialsError:
            pass
    return None


def _credentials_from_read_write_token() -> BlobCredentials | None:
    read_write_token = os.getenv("BLOB_READ_WRITE_TOKEN") or os.getenv(
        "VERCEL_BLOB_READ_WRITE_TOKEN"
    )
    if not read_write_token:
        return None

    embedded_store_id = _extract_read_write_store_id(read_write_token)
    return _normalize_blob_credentials(
        BlobCredentials(
            token=read_write_token,
            store_id=embedded_store_id,
            kind="read_write",
        )
    )


def _finish_default_blob_credentials(oidc_token: str | None) -> BlobCredentials:
    oidc_credentials = _credentials_from_oidc_token(oidc_token)
    if oidc_credentials is not None:
        return oidc_credentials

    read_write_credentials = _credentials_from_read_write_token()
    if read_write_credentials is not None:
        return read_write_credentials

    if oidc_token:
        raise BlobCredentialsError("OIDC Blob credentials require BLOB_STORE_ID")
    raise BlobCredentialsError(
        "Missing Blob credentials: configure OIDC with BLOB_STORE_ID or a Blob read-write token"
    )


async def _default_blob_credentials() -> BlobCredentials:
    oidc_token: str | None = None
    try:
        oidc_token = await oidc_aio.get_vercel_oidc_token()
    except oidc.VercelOidcTokenError:
        pass

    return _finish_default_blob_credentials(oidc_token)


def _default_sync_blob_credentials() -> BlobCredentials:
    oidc_token: str | None = None
    try:
        oidc_token = oidc.get_vercel_oidc_token_sync()
    except oidc.VercelOidcTokenError:
        pass

    return _finish_default_blob_credentials(oidc_token)


def _default_blob_credentials_factory() -> BlobCredentialsFactory:
    async def _factory() -> BlobCredentials:
        return await _default_blob_credentials()

    return _factory


def _default_sync_blob_credentials_factory() -> SyncBlobCredentialsFactory:
    return _default_sync_blob_credentials


def _validate_positive_size(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


@dataclass(frozen=True, slots=True, init=False)
class BlobServiceOptions(ServiceOptions):
    """Configuration for the session-scoped unstable Blob service."""

    base_url: str
    credentials_factory: BlobCredentialsFactory
    sync_credentials_factory: SyncBlobCredentialsFactory | None
    default_access: Access
    read_buffer_size: int
    multipart_threshold: int
    multipart_part_size: int

    def __init__(
        self,
        *,
        base_url: str | None = None,
        credentials_factory: BlobCredentialsFactory | None = None,
        sync_credentials_factory: SyncBlobCredentialsFactory | None = None,
        default_access: Access = "public",
        read_buffer_size: int = 256 * 1024,
        multipart_threshold: int = 8 * 1024 * 1024,
        multipart_part_size: int = DEFAULT_PART_SIZE,
    ) -> None:
        normalized_read_buffer_size = _validate_positive_size("read_buffer_size", read_buffer_size)
        normalized_threshold = _validate_positive_size("multipart_threshold", multipart_threshold)
        normalized_part_size_input = _validate_positive_size(
            "multipart_part_size", multipart_part_size
        )
        try:
            normalized_part_size = validate_part_size(normalized_part_size_input)
        except BlobError as exc:
            raise ValueError(str(exc)) from exc
        if normalized_threshold < normalized_part_size:
            raise ValueError(
                "multipart_threshold must be greater than or equal to multipart_part_size"
            )
        if default_access not in ("public", "private"):
            raise ValueError('default_access must be "public" or "private"')

        object.__setattr__(self, "base_url", get_api_url("") if base_url is None else base_url)
        object.__setattr__(
            self,
            "credentials_factory",
            (
                _default_blob_credentials_factory()
                if credentials_factory is None
                else credentials_factory
            ),
        )
        object.__setattr__(
            self,
            "sync_credentials_factory",
            (
                _default_sync_blob_credentials_factory()
                if credentials_factory is None and sync_credentials_factory is None
                else sync_credentials_factory
            ),
        )
        object.__setattr__(self, "default_access", default_access)
        object.__setattr__(self, "read_buffer_size", normalized_read_buffer_size)
        object.__setattr__(self, "multipart_threshold", normalized_threshold)
        object.__setattr__(self, "multipart_part_size", normalized_part_size)
