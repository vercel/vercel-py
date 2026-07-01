import pytest

import vercel.oidc as oidc
import vercel.oidc.aio as oidc_aio
from vercel._internal.blob.multipart import DEFAULT_PART_SIZE, MIN_PART_SIZE
from vercel._internal.unstable.blob.errors import BlobCredentialsError
from vercel._internal.unstable.blob.options import (
    BlobCredentials,
    BlobServiceOptions,
    _default_blob_credentials_factory,
    _default_sync_blob_credentials_factory,
    _normalize_blob_credentials,
)


def _clear_blob_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "VERCEL_OIDC_TOKEN",
        "BLOB_STORE_ID",
        "BLOB_READ_WRITE_TOKEN",
        "VERCEL_BLOB_READ_WRITE_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)


def _disable_oidc(monkeypatch: pytest.MonkeyPatch) -> None:
    async def raise_async() -> str:
        raise oidc.VercelOidcTokenError("no oidc")

    def raise_sync() -> str:
        raise oidc.VercelOidcTokenError("no oidc")

    monkeypatch.setattr(oidc_aio, "get_vercel_oidc_token", raise_async)
    monkeypatch.setattr(oidc, "get_vercel_oidc_token_sync", raise_sync)


def _set_oidc(monkeypatch: pytest.MonkeyPatch, token: str) -> None:
    async def get_async() -> str:
        return token

    monkeypatch.setattr(oidc_aio, "get_vercel_oidc_token", get_async)
    monkeypatch.setattr(oidc, "get_vercel_oidc_token_sync", lambda: token)


class _FalseyCredentialsFactory:
    def __bool__(self) -> bool:
        return False

    async def __call__(self) -> BlobCredentials:
        return BlobCredentials("oidc-token", "store-id", "oidc")


def test_blob_service_options_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VERCEL_BLOB_API_URL", raising=False)
    monkeypatch.delenv("NEXT_PUBLIC_VERCEL_BLOB_API_URL", raising=False)

    options = BlobServiceOptions()

    assert options.base_url == "https://vercel.com/api/blob"
    assert options.default_access == "public"
    assert options.read_buffer_size == 256 * 1024
    assert options.multipart_threshold == 8 * 1024 * 1024
    assert options.multipart_part_size == DEFAULT_PART_SIZE


def test_blob_service_options_preserves_base_url_environment_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL_BLOB_API_URL", "https://blob.example.test")

    assert BlobServiceOptions().base_url == "https://blob.example.test"


def test_blob_service_options_preserves_explicit_empty_base_url() -> None:
    assert BlobServiceOptions(base_url="").base_url == ""


def test_blob_service_options_preserves_explicit_falsey_credentials_factory() -> None:
    factory = _FalseyCredentialsFactory()

    assert BlobServiceOptions(credentials_factory=factory).credentials_factory is factory


@pytest.mark.parametrize(
    "kwargs",
    [
        {"read_buffer_size": 0},
        {"read_buffer_size": True},
        {"multipart_threshold": 0},
        {"multipart_threshold": True},
        {"multipart_part_size": MIN_PART_SIZE - 1},
        {"multipart_part_size": True},
        {"multipart_threshold": DEFAULT_PART_SIZE - 1},
    ],
)
def test_blob_service_options_rejects_invalid_sizes(kwargs: dict[str, object]) -> None:
    with pytest.raises((TypeError, ValueError)):
        BlobServiceOptions(**kwargs)  # type: ignore[arg-type]


def test_normalize_blob_credentials_validates_and_normalizes_store_id() -> None:
    assert _normalize_blob_credentials(
        BlobCredentials("token", "store_abc", "oidc")
    ) == BlobCredentials("token", "abc", "oidc")

    with pytest.raises(BlobCredentialsError, match="token"):
        _normalize_blob_credentials(BlobCredentials("", "abc", "oidc"))
    with pytest.raises(BlobCredentialsError, match="store"):
        _normalize_blob_credentials(BlobCredentials("token", "", "oidc"))
    with pytest.raises(BlobCredentialsError, match="store"):
        _normalize_blob_credentials(BlobCredentials("not-embedded", "", "read_write"))


def test_read_write_credentials_reject_mismatched_store_id() -> None:
    with pytest.raises(BlobCredentialsError, match="does not match"):
        _normalize_blob_credentials(
            BlobCredentials("vercel_blob_rw_token-store_secret", "other-store", "read_write")
        )


@pytest.mark.asyncio
async def test_default_credentials_prefers_complete_oidc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_blob_credentials(monkeypatch)
    _set_oidc(monkeypatch, "oidc-token")
    monkeypatch.setenv("BLOB_STORE_ID", "store_oidc-store")
    monkeypatch.setenv("BLOB_READ_WRITE_TOKEN", "vercel_blob_rw_rw-store_secret")

    credentials = await _default_blob_credentials_factory()()

    assert credentials == BlobCredentials("oidc-token", "oidc-store", "oidc")


def test_default_sync_credentials_use_public_sync_oidc_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_blob_credentials(monkeypatch)
    monkeypatch.setenv("BLOB_STORE_ID", "store_sync-oidc-store")
    monkeypatch.setattr(oidc_aio, "get_vercel_oidc_token", None)
    monkeypatch.setattr(oidc, "get_vercel_oidc_token_sync", lambda: "sync-oidc-token")

    credentials = _default_sync_blob_credentials_factory()()

    assert credentials == BlobCredentials("sync-oidc-token", "sync-oidc-store", "oidc")


@pytest.mark.asyncio
async def test_incomplete_oidc_falls_back_to_read_write_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_blob_credentials(monkeypatch)
    _set_oidc(monkeypatch, "oidc-token")
    monkeypatch.setenv("BLOB_READ_WRITE_TOKEN", "vercel_blob_rw_rw-store_secret")

    credentials = await _default_blob_credentials_factory()()

    assert credentials == BlobCredentials(
        "vercel_blob_rw_rw-store_secret", "rw-store", "read_write"
    )


@pytest.mark.asyncio
async def test_blob_read_write_token_precedes_vercel_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_blob_credentials(monkeypatch)
    _disable_oidc(monkeypatch)
    monkeypatch.setenv("BLOB_READ_WRITE_TOKEN", "vercel_blob_rw_primary_secret")
    monkeypatch.setenv("VERCEL_BLOB_READ_WRITE_TOKEN", "vercel_blob_rw_alias_secret")

    credentials = await _default_blob_credentials_factory()()

    assert credentials == BlobCredentials("vercel_blob_rw_primary_secret", "primary", "read_write")


@pytest.mark.asyncio
async def test_vercel_token_is_not_blob_default_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_blob_credentials(monkeypatch)
    _disable_oidc(monkeypatch)
    monkeypatch.setenv("VERCEL_TOKEN", "vercel-token")

    with pytest.raises(BlobCredentialsError, match="Missing Blob credentials"):
        await _default_blob_credentials_factory()()


@pytest.mark.asyncio
async def test_unexpected_oidc_resolver_error_does_not_fall_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_blob_credentials(monkeypatch)
    monkeypatch.setenv("BLOB_READ_WRITE_TOKEN", "vercel_blob_rw_rw-store_secret")

    def raise_unexpected_error() -> str:
        raise RuntimeError("request context failed")

    monkeypatch.setattr(oidc_aio, "get_vercel_oidc_token", raise_unexpected_error)

    with pytest.raises(RuntimeError, match="request context failed"):
        await _default_blob_credentials_factory()()


@pytest.mark.parametrize("store_id", ["store_", "   "])
@pytest.mark.asyncio
async def test_invalid_oidc_store_falls_back_to_read_write_token(
    monkeypatch: pytest.MonkeyPatch,
    store_id: str,
) -> None:
    _clear_blob_credentials(monkeypatch)
    _set_oidc(monkeypatch, "oidc-token")
    monkeypatch.setenv("BLOB_STORE_ID", store_id)
    monkeypatch.setenv("BLOB_READ_WRITE_TOKEN", "vercel_blob_rw_rw-store_secret")

    credentials = await _default_blob_credentials_factory()()

    assert credentials == BlobCredentials(
        "vercel_blob_rw_rw-store_secret", "rw-store", "read_write"
    )


@pytest.mark.asyncio
async def test_incomplete_oidc_without_fallback_names_missing_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_blob_credentials(monkeypatch)
    _set_oidc(monkeypatch, "oidc-token")

    with pytest.raises(BlobCredentialsError, match="BLOB_STORE_ID"):
        await _default_blob_credentials_factory()()


@pytest.mark.parametrize(
    "token",
    [
        "malformed",
        "other_blob_rw_store_secret",
        "vercel_blob_ro_store_secret",
        "vercel_blob_rw__secret",
        "vercel_blob_rw_store",
        "vercel_blob_rw_store_",
        "vercel_blob_rw_store__",
    ],
)
@pytest.mark.asyncio
async def test_read_write_token_requires_embedded_store(
    monkeypatch: pytest.MonkeyPatch,
    token: str,
) -> None:
    _clear_blob_credentials(monkeypatch)
    _disable_oidc(monkeypatch)
    monkeypatch.setenv("BLOB_READ_WRITE_TOKEN", token)

    with pytest.raises(BlobCredentialsError, match="read-write token"):
        await _default_blob_credentials_factory()()
