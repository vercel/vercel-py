"""Blob credential resolution and validation."""

from __future__ import annotations

import inspect
import os

from vercel.blob.errors import BlobCredentialsError
from vercel.blob.models import (
    BlobCredentials,
    BlobCredentialsFactory,
    CredentialKind,
    SyncBlobCredentialsFactory,
)

from .validation import normalize_store_id

_RW_PREFIX = "vercel_blob_rw_"


def store_from_read_write_token(token: str) -> str:
    if not token.startswith(_RW_PREFIX):
        raise BlobCredentialsError("Blob read-write token has an invalid format")
    store_id, separator, secret = token[len(_RW_PREFIX) :].partition("_")
    if not separator or not store_id.strip() or not secret.strip().strip("_"):
        raise BlobCredentialsError("Blob read-write token has an invalid format")
    return normalize_store_id(store_id)


def normalize_credentials(credentials: BlobCredentials) -> BlobCredentials:
    if not isinstance(credentials, BlobCredentials):
        raise BlobCredentialsError("credential factory must return BlobCredentials")
    if not isinstance(credentials.token, str) or not credentials.token.strip():
        raise BlobCredentialsError("Blob credentials must include a non-empty token")
    if not isinstance(credentials.store_id, str):
        raise BlobCredentialsError("Blob credentials must identify a string store ID")

    store_id = normalize_store_id(credentials.store_id)
    try:
        kind = CredentialKind(credentials.kind)
    except ValueError:
        raise BlobCredentialsError(f"Unknown Blob credential kind: {credentials.kind!r}") from None

    if kind == CredentialKind.READ_WRITE:
        embedded_store = store_from_read_write_token(credentials.token)
        if embedded_store != store_id:
            raise BlobCredentialsError("Blob read-write token store ID does not match credentials")

    return BlobCredentials(credentials.token, store_id, kind)


async def _async_oidc_token() -> str | None:
    try:
        from vercel.oidc import VercelOidcTokenError
        from vercel.oidc.aio import get_vercel_oidc_token
    except ImportError:
        return None
    try:
        return await get_vercel_oidc_token()
    except VercelOidcTokenError:
        return None


def _sync_oidc_token() -> str | None:
    try:
        from vercel.oidc import VercelOidcTokenError, get_vercel_oidc_token_sync
    except ImportError:
        return None
    try:
        return get_vercel_oidc_token_sync()
    except VercelOidcTokenError:
        return None


def _read_write_credentials() -> BlobCredentials | None:
    token = os.environ.get("BLOB_READ_WRITE_TOKEN") or os.environ.get(
        "VERCEL_BLOB_READ_WRITE_TOKEN"
    )
    if not token:
        return None
    token = token.strip()
    return BlobCredentials(token, store_from_read_write_token(token), CredentialKind.READ_WRITE)


def _missing_credentials(oidc_token: str | None) -> BlobCredentialsError:
    if oidc_token:
        return BlobCredentialsError("BLOB_STORE_ID is required with Vercel OIDC credentials")
    return BlobCredentialsError("Missing Blob credentials")


# OIDC is only usable with BLOB_STORE_ID, so the token is fetched only when the store ID
# is set. Fetching it can refresh the token over the network and write it to disk and
# the environment, which is wasted work when a read-write token will be used anyway.
async def default_async_credentials() -> BlobCredentials:
    raw_store = os.environ.get("BLOB_STORE_ID", "").strip()
    if raw_store:
        oidc_token = await _async_oidc_token()
        if oidc_token:
            return BlobCredentials(oidc_token, normalize_store_id(raw_store), CredentialKind.OIDC)

    read_write = _read_write_credentials()
    if read_write is not None:
        return read_write

    raise _missing_credentials(None if raw_store else await _async_oidc_token())


def default_sync_credentials() -> BlobCredentials:
    raw_store = os.environ.get("BLOB_STORE_ID", "").strip()
    if raw_store:
        oidc_token = _sync_oidc_token()
        if oidc_token:
            return BlobCredentials(oidc_token, normalize_store_id(raw_store), CredentialKind.OIDC)

    read_write = _read_write_credentials()
    if read_write is not None:
        return read_write

    raise _missing_credentials(None if raw_store else _sync_oidc_token())


def adapt_sync_credentials_factory(
    factory: SyncBlobCredentialsFactory,
) -> BlobCredentialsFactory:
    async def credentials_factory() -> BlobCredentials:
        creds = factory()
        if inspect.isawaitable(creds):
            close = getattr(creds, "close", None)
            if callable(close):
                try:
                    close()
                except BaseException:
                    pass
            raise BlobCredentialsError(
                "synchronous credential factory must not return an awaitable"
            )
        return creds

    return credentials_factory
