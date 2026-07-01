import base64
import json
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qs, urlparse

import anyio
import httpx
import pytest
import respx

from vercel._internal.blob.errors import (
    BlobAccessError,
    BlobError,
    BlobServiceNotAvailable,
    BlobServiceRateLimited,
    BlobStoreNotFoundError,
)
from vercel._internal.http import (
    AsyncTransport,
    BaseTransport,
    JSONBody,
    StreamingRequest,
    StreamingResponse,
)
from vercel._internal.unstable.blob.api_client import (
    BlobApiClient,
    _canonical,
    _sign,
)
from vercel._internal.unstable.blob.errors import (
    BlobAlreadyExistsError,
    BlobCredentialsError,
    BlobNotFoundError,
    BlobPreconditionFailedError,
    BlobStreamError,
)
from vercel._internal.unstable.blob.models import (
    BlobRangeResponse,
    BlobStatResult,
    MultipartPartState,
    MultipartUploadState,
    PresignedOperation,
    ScandirMode,
)
from vercel._internal.unstable.blob.options import BlobCredentials

UTC = timezone.utc
BASE_URL = "https://blob.example.test"
TOKEN = "oidc-token"
STORE_ID = "store-id"


async def credentials() -> BlobCredentials:
    return BlobCredentials(TOKEN, STORE_ID, "oidc")


def client(transport: BaseTransport) -> BlobApiClient:
    return BlobApiClient(
        base_url=BASE_URL,
        credentials_factory=credentials,
        transport=transport,
    )


@pytest.fixture
async def transport():
    async with httpx.AsyncClient() as http:
        yield AsyncTransport(http)


@respx.mock
async def test_stat_sends_v12_oidc_store_and_parses_complete_state(transport) -> None:
    route = respx.get(BASE_URL, params={"url": "folder/blob.txt"}).mock(
        return_value=httpx.Response(
            200,
            json={
                "pathname": "folder/blob.txt",
                "url": "https://store-id.public.blob.vercel-storage.com/folder/blob.txt",
                "downloadUrl": "https://store-id.public.blob.vercel-storage.com/folder/blob.txt?download=1",
                "size": 5,
                "etag": '"etag-1"',
                "uploadedAt": "2026-06-11T12:00:00Z",
                "contentType": "text/plain",
                "contentDisposition": 'inline; filename="blob.txt"',
                "cacheControl": "public, max-age=60",
            },
        )
    )

    result = await client(transport).stat("folder/blob.txt")

    assert result.size == 5
    assert result.etag == '"etag-1"'
    request = route.calls.last.request
    assert request.headers["authorization"] == f"Bearer {TOKEN}"
    assert request.headers["x-api-version"] == "12"
    assert request.headers["x-vercel-blob-store-id"] == STORE_ID


@respx.mock
async def test_list_page_encodes_folded_and_expanded_entries(transport) -> None:
    route = respx.get(BASE_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "blobs": [
                    {
                        "pathname": "a/file",
                        "url": "https://delivery/a/file",
                        "downloadUrl": "https://delivery/a/file?download=1",
                        "size": 1,
                        "etag": "etag",
                        "uploadedAt": "2026-06-11T12:00:00Z",
                    }
                ],
                "folders": ["a/nested/"],
                "cursor": "next",
                "hasMore": True,
            },
        )
    )

    page = await client(transport).list_page(
        prefix="a/", mode=ScandirMode.FOLDED, page_size=20, cursor="cursor"
    )

    assert [entry.pathname for entry in page.entries] == ["a/file", "a/nested/"]
    assert page.cursor == "next"
    assert dict(route.calls.last.request.url.params) == {
        "prefix": "a/",
        "mode": "folded",
        "limit": "20",
        "cursor": "cursor",
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("pathname", 1),
        ("pathname", ""),
        ("url", 1),
        ("url", "not-a-url"),
        ("downloadUrl", 1),
        ("downloadUrl", ""),
        ("size", True),
        ("size", -1),
        ("etag", 1),
        ("etag", ""),
        ("uploadedAt", "2026-06-11T12:00:00"),
        ("uploadedAt", "not-a-timestamp"),
        ("contentType", 1),
        ("contentDisposition", 1),
        ("cacheControl", 1),
    ],
)
def list_payload() -> dict[str, object]:
    return {
        "blobs": [stat_payload()],
        "folders": ["folder/"],
        "cursor": "next",
        "hasMore": True,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("blobs", {}),
        ("blobs", ["blob"]),
        ("folders", {}),
        ("folders", [1]),
        ("folders", [""]),
        ("folders", ["folder"]),
        ("folders", ["folder/\n"]),
        ("cursor", 1),
        ("hasMore", "false"),
    ],
)
@respx.mock
async def test_list_rejects_malformed_page_shapes(transport, field, value) -> None:
    payload = list_payload()
    payload[field] = value
    respx.get(BASE_URL).mock(return_value=httpx.Response(200, json=payload))

    with pytest.raises(BlobStreamError, match="malformed list metadata"):
        await client(transport).list_page(
            prefix="", mode=ScandirMode.FOLDED, page_size=None, cursor=None
        )


@respx.mock
async def test_expanded_list_rejects_folded_folder_entries(transport) -> None:
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(
            200,
            json={"blobs": [], "folders": ["folder/"], "hasMore": False},
        )
    )

    with pytest.raises(BlobStreamError, match="malformed list metadata"):
        await client(transport).list_page(
            prefix="", mode=ScandirMode.EXPANDED, page_size=None, cursor=None
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("pathname", 1),
        ("url", "not-a-url"),
        ("downloadUrl", None),
        ("size", True),
        ("size", -1),
        ("etag", ""),
        ("uploadedAt", "2026-06-11T12:00:00"),
        ("uploadedAt", "invalid"),
    ],
)
def _delegation(payload: dict[str, object]) -> str:
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    return f"{encoded}.opaque-signature"


@respx.mock
async def test_presign_put_uses_canonical_hmac_vector(transport, monkeypatch) -> None:
    now = datetime.fromtimestamp(1_700_000_000, UTC)
    monkeypatch.setattr("vercel._internal.unstable.blob.api_client._now", lambda: now)
    delegated_until = 1_700_000_060_000
    token = _delegation(
        {
            "storeId": STORE_ID,
            "ownerId": "owner",
            "pathname": "folder/a b.txt",
            "operations": ["put"],
            "validUntil": delegated_until,
            "iat": 1_700_000_000_000,
            "maximumSizeInBytes": 4096,
        }
    )
    route = respx.post(f"{BASE_URL}/signed-token").mock(
        return_value=httpx.Response(
            200,
            json={
                "delegationToken": token,
                "clientSigningToken": "client-signing-token-test",
                "validUntil": delegated_until,
            },
        )
    )

    result = await client(transport).presign(
        "folder/a b.txt",
        operation=PresignedOperation.PUT,
        access="private",
        expires_at=datetime.fromtimestamp(1_700_000_060, UTC),
        maximum_size=2048,
        allow_overwrite=True,
        cache_control_max_age=timedelta(seconds=120),
        if_match='"etag-1"',
    )

    assert "vercel-blob-signature=" in result.url
    assert result.required_headers == {}
    assert json.loads(route.calls.last.request.content) == {
        "pathname": "folder/a b.txt",
        "operations": ["put"],
        "validUntil": 1_700_000_060_000,
        "maximumSizeInBytes": 2048,
    }
    assert route.calls.last.request.headers["authorization"] == f"Bearer {TOKEN}"
    assert route.calls.last.request.headers["x-api-version"] == "12"
    assert route.calls.last.request.headers["x-vercel-blob-store-id"] == STORE_ID


def test_canonical_hmac_vector_uses_literal_key_and_utf8_sorting() -> None:
    canonical = _canonical(
        operation=PresignedOperation.PUT,
        pathname="folder/a b.txt",
        constraints={
            "vercel-blob-valid-until": "1700000060000",
            "vercel-blob-maximum-size-in-bytes": "2048",
            "vercel-blob-allow-overwrite": "true",
            "vercel-blob-cache-control-max-age": "120",
            "vercel-blob-if-match": '"etag-1"',
        },
    )
    signature = _sign(
        "client-signing-token-test",
        operation=PresignedOperation.PUT,
        pathname="folder/a b.txt",
        constraints={
            "vercel-blob-valid-until": "1700000060000",
            "vercel-blob-maximum-size-in-bytes": "2048",
            "vercel-blob-allow-overwrite": "true",
            "vercel-blob-cache-control-max-age": "120",
            "vercel-blob-if-match": '"etag-1"',
        },
    )

    assert canonical == "\n".join(
        [
            "operation=put",
            "pathname=folder/a b.txt",
            "vercel-blob-allow-overwrite=true",
            "vercel-blob-cache-control-max-age=120",
            'vercel-blob-if-match="etag-1"',
            "vercel-blob-maximum-size-in-bytes=2048",
            "vercel-blob-valid-until=1700000060000",
        ]
    )
    assert signature == "zAQ1Xad9gxMxBJstz13icosc_sZ8vOqLcUN4VQg2i9U"
    assert "=" not in signature


def test_signature_is_stable_across_constraint_insertion_order_and_utf8() -> None:
    first = {
        "vercel-blob-if-match": "é",
        "vercel-blob-cache-control-max-age": "0",
    }
    second = dict(reversed(first.items()))
    assert _canonical(
        operation=PresignedOperation.PUT, pathname="路径", constraints=first
    ) == _canonical(operation=PresignedOperation.PUT, pathname="路径", constraints=second)
    assert _sign(
        "not-base64+/=",
        operation=PresignedOperation.PUT,
        pathname="路径",
        constraints=first,
    ) == _sign(
        "not-base64+/=",
        operation=PresignedOperation.PUT,
        pathname="路径",
        constraints=second,
    )
    assert _sign("key", operation=PresignedOperation.GET, pathname="blob", constraints={}) != _sign(
        "key", operation=PresignedOperation.HEAD, pathname="blob", constraints={}
    )


@pytest.mark.parametrize(
    "allowed",
    ["text/plain", [""], ["text/plain\x00"], ["text/plain\x7f"]],
)
def _signed_token_response(
    *,
    pathname: str,
    operation: PresignedOperation,
    valid_until: int,
    store_id: str = STORE_ID,
    **scope: object,
) -> httpx.Response:
    payload = {
        "storeId": store_id,
        "ownerId": "owner",
        "pathname": pathname,
        "operations": [operation.value],
        "validUntil": valid_until,
        "iat": 1_700_000_000_000,
        **scope,
    }
    return httpx.Response(
        200,
        json={
            "delegationToken": _delegation(payload),
            "clientSigningToken": "client-signing-token-test",
            "validUntil": valid_until,
        },
    )


@respx.mock
async def test_presign_issuance_omits_absent_fields_and_sends_rw_auth(
    transport, monkeypatch
) -> None:
    now = datetime.fromtimestamp(1_700_000_000, UTC)
    monkeypatch.setattr("vercel._internal.unstable.blob.api_client._now", lambda: now)
    until = 1_700_000_060_000
    route = respx.post(f"{BASE_URL}/signed-token").mock(
        return_value=_signed_token_response(
            pathname="blob", operation=PresignedOperation.GET, valid_until=until
        )
    )

    async def rw_credentials() -> BlobCredentials:
        return BlobCredentials("vercel_blob_rw_store-id_secret", STORE_ID, "read_write")

    api = BlobApiClient(base_url=BASE_URL, credentials_factory=rw_credentials, transport=transport)
    await api.presign(
        "blob",
        operation=PresignedOperation.GET,
        access="public",
        expires_at=datetime.fromtimestamp(until / 1000, UTC),
    )

    request = route.calls.last.request
    assert request.headers["authorization"] == "Bearer vercel_blob_rw_store-id_secret"
    assert request.headers["x-api-version"] == "12"
    assert request.headers["x-vercel-blob-store-id"] == STORE_ID
    assert json.loads(request.content) == {
        "pathname": "blob",
        "operations": ["get"],
        "validUntil": until,
    }


@respx.mock
async def test_presign_caps_to_earlier_delegation_and_omits_equal_expiry(
    transport, monkeypatch
) -> None:
    now = datetime.fromtimestamp(1_700_000_000, UTC)
    monkeypatch.setattr("vercel._internal.unstable.blob.api_client._now", lambda: now)
    requested = 1_700_000_120_000
    delegated = 1_700_000_060_000
    respx.post(f"{BASE_URL}/signed-token").mock(
        return_value=_signed_token_response(
            pathname="blob", operation=PresignedOperation.GET, valid_until=delegated
        )
    )

    result = await client(transport).presign(
        "blob",
        operation=PresignedOperation.GET,
        access="private",
        expires_at=datetime.fromtimestamp(requested / 1000, UTC),
    )

    assert result.expires_at == datetime.fromtimestamp(delegated / 1000, UTC)
    assert "vercel-blob-valid-until" not in parse_qs(urlparse(result.url).query)


@respx.mock
async def test_presign_allows_mime_wildcard_delegation_and_sorts_encoded_query(
    transport, monkeypatch
) -> None:
    now = datetime.fromtimestamp(1_700_000_000, UTC)
    monkeypatch.setattr("vercel._internal.unstable.blob.api_client._now", lambda: now)
    until = 1_700_000_060_000
    route = respx.post(f"{BASE_URL}/signed-token").mock(
        return_value=_signed_token_response(
            pathname="a b+c.txt",
            operation=PresignedOperation.PUT,
            valid_until=until,
            allowedContentTypes=["image/*", "text/plain"],
        )
    )

    result = await client(transport).presign(
        "a b+c.txt",
        operation=PresignedOperation.PUT,
        access="private",
        expires_at=datetime.fromtimestamp(until / 1000, UTC),
        allowed_content_types=["text/plain", "image/png"],
    )

    parsed = urlparse(result.url)
    query = parse_qs(parsed.query)
    assert parsed.path == "/"
    assert query["pathname"] == ["a b+c.txt"]
    assert query["vercel-blob-allowed-content-types"] == ["image/png,text/plain"]
    assert query["vercel-blob-allowed-content-types"][0].split(",") == [
        "image/png",
        "text/plain",
    ]
    assert json.loads(route.calls.last.request.content)["allowedContentTypes"] == [
        "text/plain",
        "image/png",
    ]


@pytest.mark.parametrize("operation", [PresignedOperation.GET, PresignedOperation.HEAD])
@respx.mock
async def test_presign_read_hosts_and_method_signatures_differ(
    transport, monkeypatch, operation
) -> None:
    now = datetime.fromtimestamp(1_700_000_000, UTC)
    monkeypatch.setattr("vercel._internal.unstable.blob.api_client._now", lambda: now)
    until = 1_700_000_060_000
    respx.post(f"{BASE_URL}/signed-token").mock(
        return_value=_signed_token_response(
            pathname="folder/a b", operation=operation, valid_until=until
        )
    )
    result = await client(transport).presign(
        "folder/a b",
        operation=operation,
        access="public",
        expires_at=datetime.fromtimestamp(until / 1000, UTC),
    )
    assert result.url.startswith(f"https://{STORE_ID}.public.blob.vercel-storage.com/folder/a%20b?")
    assert result.operation is operation


@pytest.mark.parametrize(
    "response_body",
    [
        {},
        {"delegationToken": "", "clientSigningToken": "signing", "validUntil": 1},
        {"delegationToken": "x.y", "clientSigningToken": "", "validUntil": 1},
        {"delegationToken": "x.y", "clientSigningToken": "signing", "validUntil": True},
        {"delegationToken": "x.y", "clientSigningToken": "signing", "validUntil": "1"},
    ],
)
@respx.mock
async def test_presign_rejects_malformed_outer_response(
    transport, monkeypatch, response_body
) -> None:
    now = datetime.fromtimestamp(1_700_000_000, UTC)
    monkeypatch.setattr("vercel._internal.unstable.blob.api_client._now", lambda: now)
    respx.post(f"{BASE_URL}/signed-token").mock(
        return_value=httpx.Response(200, json=response_body)
    )
    with pytest.raises(BlobStreamError):
        await client(transport).presign(
            "blob",
            operation=PresignedOperation.GET,
            access="public",
            expires_at=now + timedelta(minutes=1),
        )


@pytest.mark.parametrize(
    ("status", "code", "error", "message"),
    [
        (400, "bad_request", BlobError, "Bad input"),
        (403, "client_token_not_allowed", BlobError, "Cannot issue"),
        (403, "forbidden", BlobAccessError, "Access denied"),
        (404, "store_not_found", BlobStoreNotFoundError, "does not exist"),
        (412, "precondition_failed", BlobPreconditionFailedError, "precondition"),
        (429, "rate_limited", BlobServiceRateLimited, "Too many requests"),
        (503, "service_unavailable", BlobServiceNotAvailable, "not available"),
    ],
)
@respx.mock
async def test_signed_token_errors_use_blob_hierarchy_and_messages(
    transport, status, code, error, message
) -> None:
    respx.post(f"{BASE_URL}/signed-token").mock(
        return_value=httpx.Response(
            status,
            json={
                "error": {"code": code, "message": "Cannot issue" if status == 403 else "Bad input"}
            },
            headers={"retry-after": "3"},
        )
    )
    with pytest.raises(error, match=message):
        await client(transport).presign(
            "blob",
            operation=PresignedOperation.GET,
            access="public",
            expires_at=datetime.now(UTC) + timedelta(minutes=1),
        )


@respx.mock
async def test_presigned_delete_uses_control_root_and_delete_operation(
    transport, monkeypatch
) -> None:
    now = datetime.fromtimestamp(1_700_000_000, UTC)
    monkeypatch.setattr("vercel._internal.unstable.blob.api_client._now", lambda: now)
    until = 1_700_000_060_000
    respx.post(f"{BASE_URL}/signed-token").mock(
        return_value=_signed_token_response(
            pathname="a b", operation=PresignedOperation.DELETE, valid_until=until
        )
    )
    result = await client(transport).presign(
        "a b",
        operation=PresignedOperation.DELETE,
        access="private",
        expires_at=datetime.fromtimestamp(until / 1000, UTC),
    )
    assert result.operation is PresignedOperation.DELETE
    assert result.url.startswith(f"{BASE_URL}/?pathname=a+b&")


@pytest.mark.parametrize(
    "scope_override",
    [
        {"storeId": "different"},
        {"pathname": "other"},
        {"validUntil": 1_700_000_059_999},
        {"validUntil": 1_700_000_061_000},
        {"iat": "1700000000000"},
    ],
)
@respx.mock
async def test_presign_rejects_delegation_scope_mismatches(
    transport, monkeypatch, scope_override
) -> None:
    now = datetime.fromtimestamp(1_700_000_000, UTC)
    monkeypatch.setattr("vercel._internal.unstable.blob.api_client._now", lambda: now)
    until = 1_700_000_060_000
    payload = {
        "storeId": STORE_ID,
        "ownerId": "owner",
        "pathname": "blob",
        "operations": ["get"],
        "validUntil": until,
        "iat": 1_700_000_000_000,
        **scope_override,
    }
    respx.post(f"{BASE_URL}/signed-token").mock(
        return_value=httpx.Response(
            200,
            json={
                "delegationToken": _delegation(payload),
                "clientSigningToken": "signing",
                "validUntil": until,
            },
        )
    )
    with pytest.raises(BlobCredentialsError):
        await client(transport).presign(
            "blob",
            operation=PresignedOperation.GET,
            access="public",
            expires_at=datetime.fromtimestamp(until / 1000, UTC),
        )


@pytest.mark.parametrize(
    ("delegated", "concrete"),
    [
        ({"maximumSizeInBytes": 10}, {"maximum_size": 11}),
        ({"allowedContentTypes": ["image/*"]}, {"allowed_content_types": ["text/plain"]}),
    ],
)
@respx.mock
async def test_presign_rejects_concrete_constraints_that_widen_delegation(
    transport, monkeypatch, delegated, concrete
) -> None:
    now = datetime.fromtimestamp(1_700_000_000, UTC)
    monkeypatch.setattr("vercel._internal.unstable.blob.api_client._now", lambda: now)
    until = 1_700_000_060_000
    respx.post(f"{BASE_URL}/signed-token").mock(
        return_value=_signed_token_response(
            pathname="blob",
            operation=PresignedOperation.PUT,
            valid_until=until,
            **delegated,
        )
    )
    with pytest.raises(BlobCredentialsError):
        await client(transport).presign(
            "blob",
            operation=PresignedOperation.PUT,
            access="private",
            expires_at=datetime.fromtimestamp(until / 1000, UTC),
            **concrete,
        )


@pytest.mark.parametrize("size", [True, -1])
async def test_put_rejects_invalid_size_before_credentials(transport, size) -> None:
    called = False

    async def tracking_credentials() -> BlobCredentials:
        nonlocal called
        called = True
        return await credentials()

    api = BlobApiClient(
        base_url=BASE_URL,
        credentials_factory=tracking_credentials,
        transport=transport,
    )

    with pytest.raises((TypeError, ValueError)):
        await api.put(
            "blob",
            object(),  # type: ignore[arg-type]
            size=size,
            access="public",
            content_type=None,
            cache_control_max_age=None,
            exclusive=False,
        )
    assert called is False


class Source:
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.read_sizes: list[int] = []
        self.closed = False

    async def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        return self.chunks.pop(0) if self.chunks else b""


class InstrumentedResponse(StreamingResponse):
    def __init__(self, body: bytes = b"{}", *, block_read: bool = False) -> None:
        self.response = httpx.Response(200)
        self.body = body
        self.block_read = block_read
        self.read_started = anyio.Event()
        self.closed = False

    async def read(self) -> bytes:
        self.read_started.set()
        if self.block_read:
            await anyio.sleep_forever()
        return self.body

    async def __anext__(self) -> bytes:
        raise StopAsyncIteration

    async def aiter_lines(self) -> AsyncIterator[str]:
        if False:
            yield ""

    async def aclose(self) -> None:
        self.closed = True


class InstrumentedRequest(StreamingRequest):
    def __init__(
        self,
        response: InstrumentedResponse,
        *,
        write_error: BaseException | None = None,
        finish_error: BaseException | None = None,
        block_write: bool = False,
    ) -> None:
        self.response = response
        self.write_error = write_error
        self.finish_error = finish_error
        self.block_write = block_write
        self.writes: list[bytes] = []
        self.aborted = False
        self.finished = False

    async def write(self, data: bytes) -> None:
        if self.write_error is not None:
            raise self.write_error
        if self.block_write:
            await anyio.sleep_forever()
        self.writes.append(bytes(data))

    async def finish(self) -> StreamingResponse:
        if self.finish_error is not None:
            raise self.finish_error
        self.finished = True
        return self.response

    async def abort(self) -> None:
        self.aborted = True


class InstrumentedTransport(BaseTransport):
    def __init__(
        self,
        *,
        stream_body: bytes = b"{}",
        write_error: BaseException | None = None,
        finish_error: BaseException | None = None,
        block_write: bool = False,
        block_response_read: bool = False,
        send_responses: list[httpx.Response] | None = None,
    ) -> None:
        self.stream_response = InstrumentedResponse(stream_body, block_read=block_response_read)
        self.request = InstrumentedRequest(
            self.stream_response,
            write_error=write_error,
            finish_error=finish_error,
            block_write=block_write,
        )
        self.stream_calls: list[dict[str, Any]] = []
        self.send_calls: list[dict[str, Any]] = []
        self.send_responses = list(send_responses or [])

    async def send(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        self.send_calls.append({"method": method, "path": path, **kwargs})
        if not self.send_responses:
            raise AssertionError("unexpected buffered request")
        return self.send_responses.pop(0)

    @asynccontextmanager
    async def request_stream(
        self, method: str, path: str, **kwargs: Any
    ) -> AsyncIterator[StreamingRequest]:
        self.stream_calls.append({"method": method, "path": path, **kwargs})
        try:
            yield self.request
        except BaseException:
            with anyio.CancelScope(shield=True):
                await self.request.abort()
            raise
        else:
            if not self.request.finished:
                await self.request.abort()

    async def open_response_stream(
        self, method: str, path: str, **kwargs: Any
    ) -> StreamingResponse:
        raise AssertionError("unexpected delivery request")


class SequentialSource(Source):
    def __init__(self, chunks: list[bytes], request: InstrumentedRequest) -> None:
        super().__init__(chunks)
        self.request = request
        self.read_count = 0

    async def read(self, size: int = -1) -> bytes:
        assert len(self.request.writes) == self.read_count
        self.read_count += 1
        return await super().read(size)


def stat_payload(pathname: str = "blob") -> dict[str, object]:
    return {
        "pathname": pathname,
        "url": f"https://delivery/{pathname}",
        "downloadUrl": f"https://delivery/{pathname}?download=1",
        "size": 6,
        "etag": '"etag"',
        "uploadedAt": "2026-06-11T12:00:00Z",
        "contentType": "application/octet-stream",
        "contentDisposition": "inline",
        "cacheControl": "public, max-age=60",
    }


@respx.mock
async def test_put_streams_exact_body_then_stats_without_closing_source(transport) -> None:
    put_route = respx.put(BASE_URL, params={"pathname": "blob"}).mock(
        return_value=httpx.Response(200, json={"pathname": "blob", "etag": '"etag"'})
    )
    respx.get(BASE_URL, params={"url": "blob"}).mock(
        return_value=httpx.Response(200, json=stat_payload())
    )
    source = Source([b"ab", b"cdef", b""])

    result = await client(transport).put(
        "blob",
        source,
        size=6,
        access="public",
        content_type=None,
        cache_control_max_age=None,
        exclusive=True,
    )

    assert put_route.calls.last.request.content == b"abcdef"
    assert put_route.calls.last.request.headers["content-length"] == "6"
    assert put_route.calls.last.request.headers["x-allow-overwrite"] == "0"
    assert source.closed is False
    assert result.etag == '"etag"'


@pytest.mark.parametrize("etag", [None, 1, [], ""])
async def test_put_rejects_missing_or_malformed_publication_etag(etag: object) -> None:
    payload = {} if etag is None else {"etag": etag}
    transport = InstrumentedTransport(stream_body=json.dumps(payload).encode())

    with pytest.raises(BlobStreamError, match="Malformed put response"):
        await client(transport).put(
            "blob",
            Source([b"x", b""]),
            size=1,
            access="public",
            content_type=None,
            cache_control_max_age=None,
            exclusive=False,
        )

    assert transport.send_calls == []
    assert transport.stream_response.closed


async def test_put_rejects_stat_for_concurrent_replacement() -> None:
    replacement = stat_payload()
    replacement["etag"] = '"replacement"'
    transport = InstrumentedTransport(
        stream_body=b'{"etag":"\\"published\\""}',
        send_responses=[httpx.Response(200, json=replacement)],
    )

    with pytest.raises(BlobPreconditionFailedError, match="provenance"):
        await client(transport).put(
            "blob",
            Source([b"x", b""]),
            size=1,
            access="public",
            content_type=None,
            cache_control_max_age=None,
            exclusive=False,
        )


async def test_put_returns_immutable_matching_stat_snapshot() -> None:
    published = stat_payload()
    replacement = stat_payload()
    replacement["etag"] = '"replacement"'
    transport = InstrumentedTransport(
        stream_body=b'{"etag":"\\"etag\\""}',
        send_responses=[
            httpx.Response(200, json=published),
            httpx.Response(200, json=replacement),
        ],
    )
    api = client(transport)

    result = await api.put(
        "blob",
        Source([b"x", b""]),
        size=1,
        access="public",
        content_type=None,
        cache_control_max_age=None,
        exclusive=False,
    )
    await api.stat("blob")

    assert result.etag == '"etag"'


@pytest.mark.parametrize("chunks", [[b"abc", b""], [b"abcdef", b"x"]])
@respx.mock
async def test_put_rejects_underwrite_and_overwrite(transport, chunks) -> None:
    respx.put(BASE_URL).mock(return_value=httpx.Response(200, json={}))
    source = Source(chunks)

    with pytest.raises(BlobStreamError):
        await client(transport).put(
            "blob",
            source,
            size=6,
            access="public",
            content_type=None,
            cache_control_max_age=None,
            exclusive=False,
        )
    assert source.closed is False


@respx.mock
async def test_marker_is_bodyless_exact_and_create_only(transport) -> None:
    route = respx.put(BASE_URL, params={"pathname": "folder/"}).mock(
        return_value=httpx.Response(200, json={"pathname": "folder/"})
    )
    respx.get(BASE_URL, params={"url": "folder/"}).mock(
        return_value=httpx.Response(200, json=stat_payload("folder/"))
    )

    await client(transport).create_marker("folder/", access="private", exist_ok=False)

    request = route.calls.last.request
    assert request.content == b""
    assert request.headers["x-add-random-suffix"] == "0"
    assert request.headers["x-allow-overwrite"] == "0"


@respx.mock
async def test_read_range_validates_headers_and_streams_private_body(transport) -> None:
    stat = BlobStatResult(
        pathname="blob",
        url="https://delivery/blob",
        download_url="https://delivery/blob?download=1",
        size=6,
        etag='"etag"',
        uploaded_at=datetime.now(UTC),
        content_type=None,
        content_disposition="inline",
        cache_control="",
    )
    route = respx.get(stat.url).mock(
        return_value=httpx.Response(
            206,
            headers={"content-range": "bytes 1-3/6", "etag": '"etag"'},
            content=b"bcd",
        )
    )

    response = await client(transport).read_range(stat, access="private", start=1, end=3)
    body = b"".join([chunk async for chunk in response])
    await response.aclose()

    assert body == b"bcd"
    assert route.calls.last.request.headers["range"] == "bytes=1-3"
    assert route.calls.last.request.headers["if-match"] == '"etag"'
    assert route.calls.last.request.headers["authorization"] == f"Bearer {TOKEN}"


async def test_range_response_metadata_is_read_only_and_close_detaches_stream() -> None:
    stream = InstrumentedResponse()
    response = BlobRangeResponse(stream, start=1, end=3, total=6)

    with pytest.raises(AttributeError):
        response.start = 2  # type: ignore[misc]
    await response.aclose()
    await response.aclose()

    assert stream.closed
    assert b"".join([chunk async for chunk in response]) == b""


class ConcurrentCloseResponse(InstrumentedResponse):
    def __init__(self) -> None:
        super().__init__()
        self.close_count = 0

    async def aclose(self) -> None:
        self.close_count += 1
        await super().aclose()


@respx.mock
async def test_upload_part_streams_to_multipart_endpoint(transport) -> None:
    route = respx.post(f"{BASE_URL}/mpu", params={"pathname": "blob"}).mock(
        return_value=httpx.Response(200, json={"etag": "part-etag"})
    )

    part = await client(transport).upload_part(
        MultipartUploadState("blob", "upload-id", "object-key"),
        part_number=2,
        source=Source([b"part", b""]),
        size=4,
    )

    assert part == MultipartPartState(2, "part-etag")
    assert route.calls.last.request.content == b"part"
    assert route.calls.last.request.headers["x-mpu-action"] == "upload"


@respx.mock
async def test_exclusive_multipart_completion_maps_conflict(transport) -> None:
    respx.post(f"{BASE_URL}/mpu").mock(
        return_value=httpx.Response(
            409, json={"error": {"code": "already_exists", "message": "exists"}}
        )
    )

    with pytest.raises(BlobAlreadyExistsError):
        await client(transport).complete_multipart_upload(
            MultipartUploadState("blob", "upload-id", "object-key"),
            [MultipartPartState(1, "etag")],
            exclusive=True,
            if_match=None,
        )


@pytest.mark.parametrize(
    ("start", "end"),
    [(-1, 1), (2, 1), (0, 6), (True, 1), (0, False)],
)
async def test_read_range_validates_interval_before_credentials(transport, start, end) -> None:
    called = False

    async def tracking() -> BlobCredentials:
        nonlocal called
        called = True
        return await credentials()

    api = BlobApiClient(base_url=BASE_URL, credentials_factory=tracking, transport=transport)
    with pytest.raises((TypeError, ValueError)):
        await api.read_range(
            BlobStatResult(
                "blob",
                "https://delivery/blob",
                "https://delivery/blob?download=1",
                6,
                '"etag"',
                datetime.now(UTC),
                None,
                "inline",
                "",
            ),
            access="public",
            start=start,
            end=end,
        )
    assert called is False


@pytest.mark.parametrize(
    ("status", "error"),
    [(404, BlobNotFoundError), (412, BlobPreconditionFailedError), (416, BlobStreamError)],
)
@respx.mock
async def test_delivery_errors_are_translated_and_closed(transport, status, error) -> None:
    raw = httpx.Response(status, content=b"error")
    respx.get("https://delivery/blob").mock(return_value=raw)
    stat = BlobStatResult(
        "blob",
        "https://delivery/blob",
        "https://delivery/blob?download=1",
        6,
        '"etag"',
        datetime.now(UTC),
        None,
        "inline",
        "",
    )

    with pytest.raises(error):
        await client(transport).read_range(stat, access="public", start=5, end=5)
    assert raw.is_closed


@respx.mock
async def test_delete_batch_is_unconditioned_and_all_or_error(transport) -> None:
    route = respx.post(f"{BASE_URL}/delete").mock(return_value=httpx.Response(200))

    await client(transport).delete_batch(["a", "b"])

    assert json.loads(route.calls.last.request.content) == {"urls": ["a", "b"]}
    assert "x-if-match" not in route.calls.last.request.headers


@pytest.mark.parametrize("operation", ["put", "part"])
async def test_stream_upload_is_incremental_and_has_exact_length(operation: str) -> None:
    first = b"a" * (64 * 1024)
    transport = InstrumentedTransport(
        stream_body=b'{"etag":"\\"etag\\""}',
        send_responses=[httpx.Response(200, json=stat_payload())],
    )
    source = SequentialSource([first, b"b", b""], transport.request)
    api = client(transport)

    if operation == "put":
        await api.put(
            "blob",
            source,
            size=len(first) + 1,
            access="public",
            content_type=None,
            cache_control_max_age=None,
            exclusive=False,
        )
    else:
        await api.upload_part(
            MultipartUploadState("blob", "upload-id", "key"),
            part_number=1,
            source=source,
            size=len(first) + 1,
        )

    assert transport.request.writes == [first, b"b"]
    assert source.read_sizes == [64 * 1024, 1, 1]
    assert transport.stream_calls[0]["headers"]["content-length"] == str(len(first) + 1)
    if operation == "part":
        assert transport.stream_calls[0]["method"] == "POST"
        assert transport.stream_calls[0]["path"] == f"{BASE_URL}/mpu"
        assert transport.stream_calls[0]["params"] == {"pathname": "blob"}
        headers = dict(transport.stream_calls[0]["headers"])
        assert headers.pop("content-length") == str(len(first) + 1)
        assert headers == {
            "x-mpu-action": "upload",
            "x-mpu-key": "key",
            "x-mpu-upload-id": "upload-id",
            "x-mpu-part-number": "1",
            "x-api-version": "12",
            "x-vercel-blob-store-id": STORE_ID,
        }
    assert source.closed is False


@pytest.mark.parametrize("operation", ["put", "part"])
@pytest.mark.parametrize("chunks", [[b"abc", b""], [b"abcdef", b"x"]])
async def test_stream_upload_rejects_length_mismatch_and_aborts(
    operation: str, chunks: list[bytes]
) -> None:
    transport = InstrumentedTransport(stream_body=b'{"etag":"part-etag"}')
    source = Source(chunks)
    api = client(transport)

    with pytest.raises(BlobStreamError):
        if operation == "put":
            await api.put(
                "blob",
                source,
                size=6,
                access="public",
                content_type=None,
                cache_control_max_age=None,
                exclusive=False,
            )
        else:
            await api.upload_part(
                MultipartUploadState("blob", "upload-id", "key"),
                part_number=1,
                source=source,
                size=6,
            )

    assert transport.request.aborted
    assert source.closed is False


async def test_put_sends_exclusive_and_if_match_conditions() -> None:
    transport = InstrumentedTransport(
        stream_body=b'{"etag":"\\"etag\\""}',
        send_responses=[httpx.Response(200, json=stat_payload())],
    )

    await client(transport).put(
        "blob",
        Source([b"x", b""]),
        size=1,
        access="private",
        content_type=None,
        cache_control_max_age=None,
        exclusive=True,
        if_match='"etag"',
    )

    headers = transport.stream_calls[0]["headers"]
    assert headers["x-allow-overwrite"] == "0"
    assert headers["x-if-match"] == '"etag"'


async def test_create_multipart_upload_sends_exact_request_and_parses_state() -> None:
    transport = InstrumentedTransport(
        send_responses=[httpx.Response(200, json={"uploadId": "upload-id", "key": "a/b"})]
    )

    state = await client(transport).create_multipart_upload(
        "blob",
        access="private",
        content_type="text/plain",
        cache_control_max_age=timedelta(seconds=60),
    )

    assert state == MultipartUploadState("blob", "upload-id", "a/b")
    call = transport.send_calls[0]
    assert (call["method"], call["path"], call["params"]) == (
        "POST",
        f"{BASE_URL}/mpu",
        {"pathname": "blob"},
    )
    assert call["headers"] == {
        "x-api-version": "12",
        "x-vercel-blob-store-id": STORE_ID,
        "x-content-type": "text/plain",
        "x-add-random-suffix": "0",
        "x-cache-control-max-age": "60",
        "x-vercel-blob-access": "private",
        "x-mpu-action": "create",
    }


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"uploadId": None, "key": "key"},
        {"uploadId": 1, "key": "key"},
        {"uploadId": "", "key": "key"},
        {"uploadId": "id", "key": []},
        {"uploadId": "id", "key": ""},
    ],
)
async def test_create_multipart_upload_rejects_malformed_state(payload: object) -> None:
    transport = InstrumentedTransport(send_responses=[httpx.Response(200, json=payload)])
    with pytest.raises(BlobStreamError, match="Malformed multipart create response"):
        await client(transport).create_multipart_upload(
            "blob", access="public", content_type=None, cache_control_max_age=None
        )


@pytest.mark.parametrize("etag", [None, 1, [], ""])
async def test_upload_part_rejects_malformed_etag(etag: object) -> None:
    transport = InstrumentedTransport(stream_body=json.dumps({"etag": etag}).encode())
    source = Source([b"x", b""])
    with pytest.raises(BlobStreamError, match="Malformed multipart part response"):
        await client(transport).upload_part(
            MultipartUploadState("blob", "upload-id", "key"),
            part_number=1,
            source=source,
            size=1,
        )
    assert source.closed is False


async def test_complete_multipart_orders_parts_sets_conditions_and_stats() -> None:
    transport = InstrumentedTransport(
        send_responses=[
            httpx.Response(200, json={"etag": '"etag"'}),
            httpx.Response(200, json=stat_payload()),
        ]
    )

    result = await client(transport).complete_multipart_upload(
        MultipartUploadState("blob", "upload-id", "a/b"),
        [MultipartPartState(2, "etag-2"), MultipartPartState(1, "etag-1")],
        exclusive=True,
        if_match='"etag"',
    )

    complete, stat = transport.send_calls
    assert complete["headers"]["x-allow-overwrite"] == "0"
    assert complete["headers"]["x-if-match"] == '"etag"'
    assert complete["headers"]["x-mpu-key"] == "a%2Fb"
    assert isinstance(complete["body"], JSONBody)
    assert complete["body"].data == [
        {"partNumber": 1, "etag": "etag-1"},
        {"partNumber": 2, "etag": "etag-2"},
    ]
    assert stat["params"] == {"url": "blob"}
    assert result.etag == '"etag"'


@pytest.mark.parametrize(
    "parts",
    [
        [MultipartPartState(1, "a"), MultipartPartState(1, "b")],
        [MultipartPartState(0, "a")],
        [MultipartPartState(-1, "a")],
    ],
)
async def test_complete_multipart_rejects_invalid_part_ordering(
    parts: list[MultipartPartState],
) -> None:
    transport = InstrumentedTransport()
    with pytest.raises(ValueError):
        await client(transport).complete_multipart_upload(
            MultipartUploadState("blob", "upload-id", "key"),
            parts,
            exclusive=False,
            if_match=None,
        )
    assert transport.send_calls == []


@respx.mock
async def test_delete_batch_translates_structured_request_failure(transport) -> None:
    respx.post(f"{BASE_URL}/delete").mock(
        return_value=httpx.Response(
            503, json={"error": {"code": "service_unavailable", "message": "retry"}}
        )
    )
    with pytest.raises(BlobServiceNotAvailable):
        await client(transport).delete_batch(["a", "b"])


@pytest.mark.parametrize(
    ("credential", "expected_headers"),
    [
        (
            BlobCredentials("vercel_blob_rw_store-id_secret", STORE_ID, "read_write"),
            {"x-api-version": "12"},
        ),
        (
            BlobCredentials(TOKEN, STORE_ID, "oidc"),
            {"x-api-version": "12", "x-vercel-blob-store-id": STORE_ID},
        ),
    ],
)
async def test_credentials_send_exact_auth_and_store_headers(
    credential: BlobCredentials, expected_headers: Mapping[str, str]
) -> None:
    async def factory() -> BlobCredentials:
        return credential

    transport = InstrumentedTransport(send_responses=[httpx.Response(200, json=stat_payload())])
    api = BlobApiClient(base_url=BASE_URL, credentials_factory=factory, transport=transport)

    await api.stat("blob")

    assert transport.send_calls[0]["token"] == credential.token
    assert transport.send_calls[0]["headers"] == expected_headers
