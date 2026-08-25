"""Blob credential discovery and validation."""

from __future__ import annotations

import os

from vercel.blob.errors import BlobCredentialsError

from .models import BlobCredentials


def _store_from_read_write_token(token: str) -> str:
    prefix = "vercel_blob_rw_"
    if not token.startswith(prefix):
        raise BlobCredentialsError("Blob read-write token has an invalid format")
    store_id, separator, secret = token[len(prefix) :].partition("_")
    if not separator or not store_id.strip() or not secret.strip().strip("_"):
        raise BlobCredentialsError("Blob read-write token has an invalid format")
    return store_id


def _normalize_credentials(credentials: BlobCredentials) -> BlobCredentials:
    if not isinstance(credentials, BlobCredentials):
        raise BlobCredentialsError("credential factory must return BlobCredentials")
    if not isinstance(credentials.token, str) or not credentials.token.strip():
        raise BlobCredentialsError("Blob credentials must include a non-empty token")
    if not isinstance(credentials.store_id, str):
        raise BlobCredentialsError("Blob credentials must identify a string store ID")
    store_id = credentials.store_id.removeprefix("store_")
    if not store_id.strip():
        raise BlobCredentialsError("Blob credentials must identify a non-empty store")
    if credentials.kind == "read_write":
        embedded = _store_from_read_write_token(credentials.token)
        if embedded != store_id:
            raise BlobCredentialsError("Blob read-write token store ID does not match credentials")
    elif credentials.kind != "oidc":
        raise BlobCredentialsError(f"Unknown Blob credential kind: {credentials.kind!r}")
    return BlobCredentials(credentials.token, store_id, credentials.kind)


async def _default_async_credentials() -> BlobCredentials:
    oidc_token: str | None = None
    try:
        from vercel.oidc.aio import get_vercel_oidc_token

        oidc_token = await get_vercel_oidc_token()
    except Exception as exc:
        from vercel.oidc import VercelOidcTokenError

        if not isinstance(exc, VercelOidcTokenError):
            raise
    store_id = os.environ.get("BLOB_STORE_ID", "").removeprefix("store_")
    if oidc_token and store_id.strip():
        return BlobCredentials(oidc_token, store_id, "oidc")
    token = os.environ.get("BLOB_READ_WRITE_TOKEN") or os.environ.get(
        "VERCEL_BLOB_READ_WRITE_TOKEN"
    )
    if token:
        return BlobCredentials(token, _store_from_read_write_token(token), "read_write")
    if oidc_token:
        raise BlobCredentialsError("BLOB_STORE_ID is required with Vercel OIDC credentials")
    raise BlobCredentialsError("Missing Blob credentials")


def _default_sync_credentials() -> BlobCredentials:
    oidc_token: str | None = None
    try:
        from vercel.oidc import VercelOidcTokenError, get_vercel_oidc_token_sync

        oidc_token = get_vercel_oidc_token_sync()
    except Exception as exc:
        if not isinstance(exc, VercelOidcTokenError):
            raise
    store_id = os.environ.get("BLOB_STORE_ID", "").removeprefix("store_")
    if oidc_token and store_id.strip():
        return BlobCredentials(oidc_token, store_id, "oidc")
    token = os.environ.get("BLOB_READ_WRITE_TOKEN") or os.environ.get(
        "VERCEL_BLOB_READ_WRITE_TOKEN"
    )
    if token:
        return BlobCredentials(token, _store_from_read_write_token(token), "read_write")
    if oidc_token:
        raise BlobCredentialsError("BLOB_STORE_ID is required with Vercel OIDC credentials")
    raise BlobCredentialsError("Missing Blob credentials")
