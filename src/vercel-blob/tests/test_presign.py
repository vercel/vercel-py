"""Behavioral tests for signed tokens and presigned Blob URLs."""

from __future__ import annotations

import base64
import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx2 as httpx
import pytest

from vercel import blob
from vercel.api import session
from vercel.blob import BlobCredentials, BlobError, BlobServiceOptions, BlobSignedToken
from vercel.blob.sync import SyncBlobServiceOptions

API_URL = "https://vercel.com/api/blob"
NOW_MS = 1_800_000_000_000
TEST_TOKEN = "vercel_blob_rw_teststore123_secretkeyabc"

# Expected URLs below were produced by `presignUrl` in the TypeScript SDK with
# this token and the clock fixed at NOW_MS, so they pin cross-SDK signatures.
DELEGATION = (
    "eyJzdG9yZUlkIjoic3RvcmVfQWJDZEVmMTIzIiwib3duZXJJZCI6Im8iLCJwYXRobmFtZSI6IioiLCJvcGVy"
    "YXRpb25zIjpbImdldCIsImhlYWQiLCJwdXQiLCJkZWxldGUiXSwidmFsaWRVbnRpbCI6MTgwMDAwMzYwMDAw"
    "MCwiaWF0IjoxODAwMDAwMDAwMDAwLCJtYXhpbXVtU2l6ZUluQnl0ZXMiOjEwMDAwMDAwLCJhbGxvd2VkQ29u"
    "dGVudFR5cGVzIjpbImltYWdlLyoiLCJ0ZXh0L3BsYWluIl19.-u1qYtB3fbGsdfGoGc_DHB3Akcwyp63JQjZu"
    "bs9r6W0"
)
SIGNED = BlobSignedToken(
    delegation_token=DELEGATION,
    client_signing_token="client-signing-key",
    valid_until=datetime.fromtimestamp((NOW_MS + 3_600_000) / 1000, tz=UTC),
)
_AUTH = f"vercel-blob-delegation={DELEGATION}&vercel-blob-signature="


@pytest.fixture(autouse=True)
def _fixed_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("vercel.blob._internal.service._now_ms", lambda: NOW_MS)


def _credentials() -> BlobCredentials:
    return BlobCredentials(token=TEST_TOKEN, store_id="teststore123")


def _no_requests(request: httpx.Request) -> httpx.Response:
    raise AssertionError(f"unexpected request: {request.method} {request.url}")


@contextmanager
def _sync_session(
    handler: Callable[[httpx.Request], httpx.Response] = _no_requests,
    *,
    credentials_factory: Callable[[], BlobCredentials] = _credentials,
) -> Iterator[None]:
    options = SyncBlobServiceOptions(base_url=API_URL, credentials_factory=credentials_factory)
    with session(
        service_options=[options],
        httpx_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    ):
        yield


def _delegation(**overrides: Any) -> BlobSignedToken:
    payload: dict[str, Any] = {
        "storeId": "store_abc",
        "ownerId": "o",
        "pathname": "*",
        "operations": ["get", "head", "put", "delete"],
        "validUntil": NOW_MS + 3_600_000,
        "iat": NOW_MS,
    }
    payload.update(overrides)
    segment = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    return BlobSignedToken(f"{segment}.sig", "key", datetime.now(UTC))


@pytest.mark.parametrize(
    ("pathname", "kwargs", "expected"),
    [
        (
            "docs/report ü.pdf",
            {"operation": "get", "access": "private"},
            "https://abcdef123.private.blob.vercel-storage.com/docs/report%20%C3%BC.pdf?"
            f"{_AUTH}lmKMhRcnk3-Qtv0rYqjbgz2m4wWgsi0nSP6b7_RW1-U",
        ),
        (
            "a.txt",
            {"operation": "get", "access": "private", "use_cache": False, "expires_in": 60},
            "https://abcdef123.private.blob.vercel-storage.com/a.txt?cache=0&"
            f"vercel-blob-valid-until=1800000060000&{_AUTH}"
            "FZmgHnaUaOvfc9uzmq6zFrjvf2kACDd-294bg3tmB70",
        ),
        (
            "a.txt",
            {"operation": "head", "access": "public"},
            "https://abcdef123.public.blob.vercel-storage.com/a.txt?"
            f"{_AUTH}Bc7HaaIVnWRy99O3uBIqHOBw8YzOmerU-qbu9RX7IEI",
        ),
        (
            "uploads/photo.png",
            {
                "operation": "put",
                "expires_in": timedelta(minutes=2),
                "allowed_content_types": ["text/plain", "image/png"],
                "maximum_size_in_bytes": 5000,
                "add_random_suffix": True,
                "allow_overwrite": False,
                "cache_control_max_age": 60,
                "if_match": '"etag-1"',
                "ttl_days": 30,
                "callback_url": "https://example.com/cb?x=1&y=2",
                "callback_token_payload": '{"user":"ü 1"}',
            },
            f"{API_URL}/?pathname=uploads%2Fphoto.png&vercel-blob-valid-until=1800000120000"
            "&vercel-blob-allowed-content-types=image%2Fpng%2Ctext%2Fplain"
            "&vercel-blob-maximum-size-in-bytes=5000&vercel-blob-add-random-suffix=true"
            "&vercel-blob-allow-overwrite=false&vercel-blob-cache-control-max-age=60"
            "&vercel-blob-if-match=%22etag-1%22&vercel-blob-ttl-days=30"
            "&vercel-blob-callback-url=https%3A%2F%2Fexample.com%2Fcb%3Fx%3D1%26y%3D2"
            "&vercel-blob-callback-token-payload=%7B%22user%22%3A%22%C3%BC+1%22%7D"
            f"&{_AUTH}c1JyChzVIH3g3xq2okpEnG3mw0fWP0_XFg6dHa5pj98",
        ),
        (
            "old.txt",
            {"operation": "delete", "if_match": "abc"},
            f"{API_URL}/?pathname=old.txt&vercel-blob-if-match=abc&"
            f"{_AUTH}abyHVvIFG0oU9CGTVfZAEd9yBfwxfmbxbINA1FXH6aI",
        ),
    ],
    ids=["get-private", "get-no-cache", "head-public", "put-all-options", "delete-if-match"],
)
def test_presigned_urls_match_typescript_sdk(
    pathname: str, kwargs: dict[str, Any], expected: str
) -> None:
    with _sync_session():
        assert blob.sync.presign_blob_url(SIGNED, pathname, **kwargs) == expected


@pytest.mark.anyio
async def test_async_presign_matches_sync_and_sends_no_request() -> None:
    options = BlobServiceOptions(base_url=API_URL, credentials_factory=_credentials)
    async with session(
        service_options=[options],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(_no_requests)),
    ):
        url = blob.presign_blob_url(SIGNED, "/a.txt", operation="head", access="public")
    assert url.endswith("Bc7HaaIVnWRy99O3uBIqHOBw8YzOmerU-qbu9RX7IEI")


def test_expiry_past_token_is_clamped_and_omitted() -> None:
    with _sync_session():
        url = blob.sync.presign_blob_url(
            SIGNED, "a.txt", operation="head", access="public", expires_in=timedelta(days=1)
        )
    assert "vercel-blob-valid-until" not in url


@pytest.mark.parametrize(
    ("token", "pathname", "kwargs", "match"),
    [
        (_delegation(pathname="only.txt"), "other.txt", {}, "does not match the signed token"),
        (_delegation(operations=["put"]), "a.txt", {}, "does not allow 'get'"),
        (_delegation(validUntil=NOW_MS - 1), "a.txt", {}, "has expired"),
        (
            BlobSignedToken("not-a-token", "key", datetime.now(UTC)),
            "a.txt",
            {},
            "Invalid delegation token format",
        ),
        (_delegation(), "a.txt", {"access": None}, "access is required"),
        (_delegation(), "a.txt", {"ttl_days": 3}, "only applies to 'put'"),
        (_delegation(), "a.txt", {"if_match": "e"}, "only applies to 'put' and 'delete'"),
        (_delegation(), "a.txt", {"expires_in": -5}, "expires_in must be positive"),
        (_delegation(), "a//b", {}, "cannot contain"),
    ],
    ids=[
        "pathname-scope",
        "operation-scope",
        "expired-token",
        "malformed-token",
        "get-needs-access",
        "upload-option-on-get",
        "if-match-on-get",
        "nonpositive-expiry",
        "invalid-pathname",
    ],
)
def test_get_presign_rejects_invalid_requests(
    token: BlobSignedToken, pathname: str, kwargs: dict[str, Any], match: str
) -> None:
    arguments: dict[str, Any] = {"operation": "get", "access": "private"} | kwargs
    with _sync_session(), pytest.raises(BlobError, match=match):
        blob.sync.presign_blob_url(token, pathname, **arguments)


@pytest.mark.parametrize(
    ("token", "kwargs", "match"),
    [
        (
            _delegation(allowedContentTypes=["image/*"]),
            {"allowed_content_types": ["text/plain"]},
            "not permitted by the signed token",
        ),
        (
            _delegation(maximumSizeInBytes=100),
            {"maximum_size_in_bytes": 101},
            "cannot exceed the signed token's limit",
        ),
        (_delegation(), {"access": "public"}, "access does not apply"),
        (_delegation(), {"allowed_content_types": []}, "cannot be empty"),
        (_delegation(), {"ttl_days": 731}, "ttl_days must be an integer"),
        (_delegation(), {"cache_control_max_age": -1}, "cache_control_max_age must be"),
        (_delegation(), {"callback_url": "/relative"}, "absolute URL"),
        (_delegation(), {"callback_token_payload": "x"}, "requires callback_url"),
        (_delegation(), {"add_random_suffix": "yes"}, "must be bool"),
    ],
    ids=[
        "content-type-scope",
        "size-scope",
        "access-on-put",
        "empty-content-types",
        "ttl-range",
        "cache-age-range",
        "relative-callback",
        "payload-without-callback",
        "non-bool-flag",
    ],
)
def test_put_presign_rejects_invalid_requests(
    token: BlobSignedToken, kwargs: dict[str, Any], match: str
) -> None:
    with _sync_session(), pytest.raises((BlobError, TypeError), match=match):
        blob.sync.presign_blob_url(token, "a.png", operation="put", **kwargs)


def test_put_presign_accepts_wildcard_content_type_within_scope() -> None:
    token = _delegation(allowedContentTypes=["image/*"], maximumSizeInBytes=100)
    with _sync_session():
        url = blob.sync.presign_blob_url(
            token,
            "a.png",
            operation="put",
            allowed_content_types=["image/png"],
            maximum_size_in_bytes=100,
        )
    assert "vercel-blob-allowed-content-types=image%2Fpng" in url


def test_issue_signed_token_sends_scope_and_parses_response() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "delegationToken": DELEGATION,
                "clientSigningToken": "client-signing-key",
                "validUntil": NOW_MS + 600_000,
            },
        )

    with _sync_session(handler):
        token = blob.sync.issue_blob_signed_token(
            pathname="/uploads/a.png",
            operations=["put", "get", "put"],
            expires_in=timedelta(minutes=10),
            maximum_size_in_bytes=1024,
            allowed_content_types=["image/*"],
        )

    (request,) = requests
    assert request.method == "POST"
    assert str(request.url) == f"{API_URL}/signed-token"
    assert request.headers["authorization"] == f"Bearer {TEST_TOKEN}"
    assert json.loads(request.content) == {
        "pathname": "uploads/a.png",
        "operations": ["put", "get"],
        "validUntil": NOW_MS + 600_000,
        "maximumSizeInBytes": 1024,
        "allowedContentTypes": ["image/*"],
    }
    assert token == BlobSignedToken(
        DELEGATION, "client-signing-key", datetime.fromtimestamp(1_800_000_600, tz=UTC)
    )
    assert "client-signing-key" not in repr(token)


@pytest.mark.anyio
async def test_async_issue_signed_token_with_oidc_sends_store_header() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={"delegationToken": "d.s", "clientSigningToken": "c", "validUntil": NOW_MS},
        )

    def oidc() -> BlobCredentials:
        return BlobCredentials("oidc-jwt", "store_abc", blob.CredentialKind.OIDC)

    options = BlobServiceOptions(base_url=API_URL, credentials_factory=oidc)
    async with session(
        service_options=[options],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        await blob.issue_blob_signed_token()

    (request,) = requests
    assert request.headers["authorization"] == "Bearer oidc-jwt"
    assert request.headers["x-vercel-blob-store-id"] == "abc"
    assert json.loads(request.content) == {}


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"operations": []}, "operations cannot be empty"),
        ({"operations": ["list"]}, "Unknown presign operation"),
        ({"maximum_size_in_bytes": 0}, "positive integer"),
        ({"expires_in": 0}, "expires_in must be positive"),
    ],
)
def test_issue_signed_token_validates_before_requesting(kwargs: dict[str, Any], match: str) -> None:
    with _sync_session(), pytest.raises(BlobError, match=match):
        blob.sync.issue_blob_signed_token(**kwargs)


def test_issue_signed_token_rejects_malformed_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"delegationToken": "d.s"})

    with _sync_session(handler), pytest.raises(BlobError, match="signed token"):
        blob.sync.issue_blob_signed_token()
