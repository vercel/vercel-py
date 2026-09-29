"""Behavioral tests for Vercel Blob object operations."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterator
from typing import Any

import anyio
import httpx2 as httpx
import pytest

from vercel import blob
from vercel._internal.core.errors import VercelSessionClosedError
from vercel.api import session
from vercel.blob import (
    BlobAccessError,
    BlobCredentials,
    BlobCredentialsError,
    BlobError,
    BlobNotFoundError,
    BlobPreconditionFailedError,
    BlobServiceNotAvailable,
    BlobServiceOptions,
    BlobServiceRateLimited,
    BlobStoreNotFoundError,
    BlobStreamError,
    BlobUnknownError,
    DownloadMetadata,
    HeadResult,
    PutResult,
)
from vercel.blob.sync import SyncBlobServiceOptions


class _TrackingSyncStream(httpx.SyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.closed = False
        self.yielded_count = 0

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self.chunks:
            self.yielded_count += 1
            yield chunk

    def close(self) -> None:
        self.closed = True


class _TrackingAsyncStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.closed = False
        self.yielded_count = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            self.yielded_count += 1
            yield chunk

    async def aclose(self) -> None:
        await anyio.lowlevel.checkpoint()
        self.closed = True


class _FailingAsyncStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"chunk1"
        raise RuntimeError("network failure during streaming")

    async def aclose(self) -> None:
        await anyio.lowlevel.checkpoint()
        self.closed = True


class _FailingSyncStream(httpx.SyncByteStream):
    def __init__(self) -> None:
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        yield b"chunk1"
        raise RuntimeError("network failure during streaming")

    def close(self) -> None:
        self.closed = True


TEST_TOKEN = "vercel_blob_rw_teststore123_secretkeyabc"
TEST_STORE = "teststore123"


def test_public_and_sync_reexports() -> None:
    """Public types and errors must be importable from vercel.blob and vercel.blob.sync."""
    import vercel.blob as async_blob
    import vercel.blob.sync as sync_blob

    for name in (
        "put",
        "get",
        "head",
        "delete",
        "PutResult",
        "HeadResult",
        "DownloadMetadata",
        "BlobCredentials",
        "BlobError",
        "BlobNotFoundError",
        "BlobAccessError",
        "BlobStoreNotFoundError",
        "BlobServiceRateLimited",
        "BlobServiceNotAvailable",
        "BlobPreconditionFailedError",
        "BlobStreamError",
    ):
        assert hasattr(async_blob, name), f"{name} missing from vercel.blob"
        assert hasattr(sync_blob, name), f"{name} missing from vercel.blob.sync"

    assert hasattr(async_blob, "BlobServiceOptions")
    assert hasattr(sync_blob, "SyncBlobServiceOptions")


@pytest.mark.anyio
async def test_async_put_success() -> None:
    captured_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        return httpx.Response(
            200,
            json={
                "url": "https://teststore123.public.blob.vercel-storage.com/folder/doc.txt",
                "downloadUrl": "https://teststore123.public.blob.vercel-storage.com/folder/doc.txt?download=1",
                "pathname": "folder/doc.txt",
                "contentType": "text/plain",
                "contentDisposition": 'inline; filename="doc.txt"',
                "etag": "etag-xyz-123",
            },
        )

    creds = BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)
    opt = BlobServiceOptions(credentials_factory=lambda: creds)  # type: ignore[return-value]

    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        result = await blob.put(
            "folder/doc.txt",
            b"hello world",
            access="public",
            content_type="text/plain",
            add_random_suffix=False,
            allow_overwrite=True,
            cache_control_max_age=3600,
        )

    assert isinstance(result, PutResult)
    assert result.pathname == "folder/doc.txt"
    assert result.etag == "etag-xyz-123"
    assert result.url == "https://teststore123.public.blob.vercel-storage.com/folder/doc.txt"

    assert len(captured_requests) == 1
    req = captured_requests[0]
    assert req.method == "PUT"
    assert req.headers["authorization"] == f"Bearer {TEST_TOKEN}"
    assert req.headers["x-api-version"] == "12"
    assert req.headers["x-vercel-blob-access"] == "public"
    assert req.headers["x-add-random-suffix"] == "0"
    assert req.headers["x-allow-overwrite"] == "1"
    assert req.headers["x-content-type"] == "text/plain"
    assert req.headers["x-cache-control-max-age"] == "3600"
    assert req.content == b"hello world"


def test_sync_put_success() -> None:
    captured_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        return httpx.Response(
            200,
            json={
                "url": "https://teststore123.private.blob.vercel-storage.com/secret.bin",
                "downloadUrl": "https://teststore123.private.blob.vercel-storage.com/secret.bin?download=1",
                "pathname": "secret.bin",
                "contentType": "application/octet-stream",
                "contentDisposition": "attachment",
                "etag": "etag-sec-999",
            },
        )

    creds = BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)
    opt = SyncBlobServiceOptions(credentials_factory=lambda: creds)

    with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    ):
        result = blob.sync.put(
            "secret.bin",
            b"\x00\x01\x02",
            access="private",
            add_random_suffix=True,
            allow_overwrite=False,
        )

    assert isinstance(result, PutResult)
    assert result.pathname == "secret.bin"
    assert result.etag == "etag-sec-999"
    assert len(captured_requests) == 1
    req = captured_requests[0]
    assert req.headers["x-vercel-blob-access"] == "private"
    assert req.headers["x-add-random-suffix"] == "1"
    assert req.headers["x-allow-overwrite"] == "0"


@pytest.mark.anyio
async def test_put_invalid_input_zero_io() -> None:
    calls = 0

    def credential_factory() -> BlobCredentials:
        nonlocal calls
        calls += 1
        return BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)

    opt = BlobServiceOptions(credentials_factory=credential_factory)  # type: ignore[return-value]

    async with session(service_options=[opt]):
        # Non-bytes body
        with pytest.raises(TypeError, match="put body must be bytes"):
            await blob.put("valid/path.txt", "not bytes", access="public")  # type: ignore[arg-type]

        # Empty pathname
        with pytest.raises(BlobError, match="pathname cannot be empty"):
            await blob.put("", b"bytes", access="public")

        # Invalid access
        with pytest.raises(BlobError, match="access must be 'public' or 'private'"):
            await blob.put("valid/path.txt", b"bytes", access="invalid")  # type: ignore[arg-type]

        # Pathname with control char
        with pytest.raises(BlobError, match="control characters"):
            await blob.put("valid/\x01path.txt", b"bytes", access="public")

    assert calls == 0, "Credential factory must not be invoked on invalid inputs"


@pytest.mark.anyio
async def test_async_get_streaming_lifecycle() -> None:
    stream = _TrackingAsyncStream([b"chunk-1-", b"chunk-2-", b"chunk-3"])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "content-type": "text/plain",
                "content-length": "23",
                "etag": '"etag-stream-1"',
                "cache-control": "public, max-age=3600",
            },
            stream=stream,
        )

    target_url = "https://teststore123.public.blob.vercel-storage.com/file.txt"
    creds_called = False

    def cred_factory() -> BlobCredentials:
        nonlocal creds_called
        creds_called = True
        return BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)

    opt = BlobServiceOptions(credentials_factory=cred_factory)  # type: ignore[return-value]

    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        async with blob.get(target_url, access="public") as download:
            assert isinstance(download.metadata, DownloadMetadata)
            assert download.metadata.size == 23
            assert download.metadata.content_type == "text/plain"
            assert download.metadata.etag == '"etag-stream-1"'
            assert stream.yielded_count == 0, "No body consumed during GET entry"
            assert not stream.closed

            chunks = []
            async for chunk in download:
                chunks.append(chunk)

            assert b"".join(chunks) == b"chunk-1-chunk-2-chunk-3"
            assert stream.closed, "Stream must close upon EOF"

        assert download.is_closed
        assert not creds_called, "Public full-URL GET must not invoke credentials"


def test_sync_get_streaming_lifecycle() -> None:
    stream = _TrackingSyncStream([b"part-a", b"part-b"])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "content-type": "application/octet-stream",
                "content-length": "12",
                "etag": "sync-etag",
            },
            stream=stream,
        )

    creds = BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)
    opt = SyncBlobServiceOptions(credentials_factory=lambda: creds)

    with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    ):
        with blob.sync.get("test.bin", access="private") as download:
            assert download.metadata.size == 12
            assert stream.yielded_count == 0
            chunks = list(download)
            assert b"".join(chunks) == b"part-apart-b"
            assert stream.closed


@pytest.mark.anyio
async def test_get_early_exit_closes_stream() -> None:
    chunk1 = b"c1" * 32768
    chunk2 = b"c2" * 32768
    stream = _TrackingAsyncStream([chunk1, chunk2])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/early.bin"

    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        async with blob.get(url, access="public") as download:
            async for chunk in download:
                assert chunk == chunk1
                break
            assert not stream.closed

        assert stream.closed, "Early exit from async with block must close stream"


def test_sync_get_early_exit_closes_stream() -> None:
    chunk1 = b"c1" * 32768
    chunk2 = b"c2" * 32768
    stream = _TrackingSyncStream([chunk1, chunk2])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/sync-early.bin"

    with session(
        httpx_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    ):
        with blob.sync.get(url, access="public") as download:
            for chunk in download:
                assert chunk == chunk1
                break
            assert not stream.closed

        assert stream.closed, "Early exit from sync with block must close stream"


@pytest.mark.anyio
async def test_get_single_consumer_and_closed_guards() -> None:
    stream = _TrackingAsyncStream([b"data"])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/once.bin"

    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        async with blob.get(url, access="public") as download:
            # First consumer
            iterator = download.__aiter__()
            await anext(iterator)

            # Attempting second iteration on the same download
            with pytest.raises(BlobStreamError, match="consumed once"):
                download.__aiter__()

        # Closed download rejects reads
        with pytest.raises(BlobStreamError, match="Cannot read from closed download"):
            download.__aiter__()


@pytest.mark.anyio
async def test_get_stream_error_closes_stream() -> None:
    stream = _FailingAsyncStream()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/fail.bin"

    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        with pytest.raises(RuntimeError, match="network failure"):
            async with blob.get(url, access="public") as download:
                async for _ in download:
                    pass

        assert stream.closed, "Stream must close on internal read exception"


def test_sync_get_stream_error_closes_stream() -> None:
    stream = _FailingSyncStream()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/sync-fail.bin"

    with session(
        httpx_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    ):
        with pytest.raises(RuntimeError, match="network failure"):
            with blob.sync.get(url, access="public") as download:
                for _ in download:
                    pass

        assert stream.closed, "Sync stream must close on internal read exception"


@pytest.mark.anyio
async def test_get_cancellation_closes_stream() -> None:
    stream = _TrackingAsyncStream([b"infinite1", b"infinite2"])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/cancel.bin"

    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        with anyio.move_on_after(0.01) as cancel_scope:
            async with blob.get(url, access="public") as download:
                async for _ in download:
                    await anyio.sleep(0.05)

        assert cancel_scope.cancel_called
        assert stream.closed, "Cancellation must trigger shielded stream closure"


@pytest.mark.anyio
async def test_delivery_url_security_validations() -> None:
    # Non-HTTPS
    with pytest.raises(BlobError, match="HTTPS scheme"):
        bad_http = "http://teststore123.public.blob.vercel-storage.com/f.txt"
        await blob.get(bad_http, access="public").__aenter__()

    # Non-blob domain
    with pytest.raises(BlobError, match="does not point to a Vercel Blob store"):
        await blob.get("https://attacker.com/f.txt", access="public").__aenter__()

    # Userinfo in URL
    with pytest.raises(BlobError, match="userinfo"):
        userinfo_url = "https://user:pass@teststore123.public.blob.vercel-storage.com/f.txt"
        await blob.get(userinfo_url, access="public").__aenter__()

    # Non-default port
    with pytest.raises(BlobError, match="non-default port"):
        port_url = "https://teststore123.public.blob.vercel-storage.com:8080/f.txt"
        await blob.get(port_url, access="public").__aenter__()

    # Access mismatch in host vs argument
    with pytest.raises(BlobError, match="does not match requested access"):
        mismatch_url = "https://teststore123.private.blob.vercel-storage.com/f.txt"
        await blob.get(mismatch_url, access="public").__aenter__()


@pytest.mark.anyio
async def test_private_get_store_id_mismatch() -> None:
    creds = BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)
    opt = BlobServiceOptions(credentials_factory=lambda: creds)  # type: ignore[return-value]

    async with session(service_options=[opt]):
        # URL identifies store999, but credentials belong to teststore123
        url = "https://store999.private.blob.vercel-storage.com/f.txt"
        with pytest.raises(BlobError, match="does not match credential store ID"):
            await blob.get(url, access="private").__aenter__()


@pytest.mark.anyio
async def test_head_success_and_errors() -> None:
    captured_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        if request.url.params.get("url") == "notfound.txt":
            return httpx.Response(404, json={"error": {"code": "not_found"}})
        return httpx.Response(
            200,
            json={
                "url": "https://teststore123.public.blob.vercel-storage.com/head.txt",
                "downloadUrl": "https://teststore123.public.blob.vercel-storage.com/head.txt?download=1",
                "pathname": "head.txt",
                "size": 42,
                "etag": "head-etag",
                "uploadedAt": "2026-09-29T12:00:00.000Z",
                "contentType": "text/plain",
                "contentDisposition": "inline",
                "cacheControl": "max-age=300",
            },
        )

    creds = BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)
    opt = BlobServiceOptions(credentials_factory=lambda: creds)  # type: ignore[return-value]

    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        res = await blob.head("head.txt")
        assert isinstance(res, HeadResult)
        assert res.size == 42
        assert res.etag == "head-etag"
        assert res.uploaded_at.year == 2026

        with pytest.raises(BlobNotFoundError):
            await blob.head("notfound.txt")


def test_sync_head_and_delete() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "url": "https://teststore123.public.blob.vercel-storage.com/sync-head.txt",
                    "downloadUrl": "https://teststore123.public.blob.vercel-storage.com/sync-head.txt?download=1",
                    "pathname": "sync-head.txt",
                    "size": 10,
                    "etag": "sync-etag",
                    "uploadedAt": "2026-09-29T12:00:00Z",
                    "contentType": "text/plain",
                    "contentDisposition": "inline",
                    "cacheControl": "max-age=300",
                },
            )
        if request.method == "POST":
            # delete endpoint
            return httpx.Response(200, json={})
        return httpx.Response(405)

    creds = BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)
    opt = SyncBlobServiceOptions(credentials_factory=lambda: creds)

    with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    ):
        meta = blob.sync.head("sync-head.txt")
        assert meta.size == 10
        # delete should succeed without error
        blob.sync.delete("sync-head.txt")


@pytest.mark.anyio
async def test_delete_idempotent_and_404_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "missing-404" in str(request.content):
            return httpx.Response(404, json={"error": {"code": "not_found"}})
        return httpx.Response(200, json={})

    creds = BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)
    opt = BlobServiceOptions(credentials_factory=lambda: creds)  # type: ignore[return-value]

    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        # 200 on delete represents backend idempotent deletion
        await blob.delete("existing-or-backend-missing.txt")

        # 404 must raise BlobNotFoundError
        with pytest.raises(BlobNotFoundError):
            await blob.delete("missing-404.txt")


@pytest.mark.anyio
async def test_session_closed_guards() -> None:
    chunk1 = b"d1" * 32768
    chunk2 = b"d2" * 32768
    stream = _TrackingAsyncStream([chunk1, chunk2])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/sess.bin"

    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        download = await blob.get(url, access="public").__aenter__()
        chunk = await anext(download.__aiter__())
        assert chunk == chunk1

    # Session is now closed
    with pytest.raises(VercelSessionClosedError):
        await anext(download)

    assert stream.closed


@pytest.mark.anyio
async def test_error_mappings() -> None:
    def make_handler(status_code: int, code: str, msg: str = "") -> Any:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code, json={"error": {"code": code, "message": msg}})

        return handler

    creds = BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)
    opt = BlobServiceOptions(credentials_factory=lambda: creds)  # type: ignore[return-value]

    # 403 Forbidden -> BlobAccessError
    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(make_handler(403, "forbidden"))
        ),
    ):
        with pytest.raises(BlobAccessError):
            await blob.put("forbidden.txt", b"x", access="public")

    # store_not_found -> BlobStoreNotFoundError
    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(make_handler(400, "store_not_found"))
        ),
    ):
        with pytest.raises(BlobStoreNotFoundError):
            await blob.put("missing_store.txt", b"x", access="public")

    # 412 precondition_failed -> BlobPreconditionFailedError
    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(make_handler(412, "precondition_failed"))
        ),
    ):
        with pytest.raises(BlobPreconditionFailedError):
            await blob.head("precond.txt")

    # 429 rate_limited -> BlobServiceRateLimited
    def rate_limit_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={"retry-after": "5"},
            json={"error": {"code": "rate_limited"}},
        )

    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(rate_limit_handler)
        ),
    ):
        with pytest.raises(BlobServiceRateLimited) as exc_info:
            await blob.head("rate.txt")
        assert exc_info.value.retry_after == 5

    # 503 service_unavailable -> BlobServiceNotAvailable
    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(make_handler(503, "service_unavailable"))
        ),
    ):
        with pytest.raises(BlobServiceNotAvailable):
            await blob.head("unavail.txt")


def test_sync_credentials_factory_returning_awaitable_fails() -> None:
    async def async_factory() -> BlobCredentials:
        return BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)

    opt = SyncBlobServiceOptions(credentials_factory=async_factory)  # type: ignore[arg-type]

    with session(service_options=[opt]):
        with pytest.raises(BlobCredentialsError, match="must not return an awaitable"):
            blob.sync.head("test.txt")


@pytest.mark.anyio
async def test_credentials_factory_store_id_divergence_fails() -> None:
    calls = 0

    def alternating_factory() -> BlobCredentials:
        nonlocal calls
        calls += 1
        store_id = "store1" if calls == 1 else "store2"
        return BlobCredentials(
            token="oidc_jwt_token_sample",
            store_id=store_id,
            kind=blob.CredentialKind.OIDC,
        )

    opt = BlobServiceOptions(credentials_factory=alternating_factory)  # type: ignore[return-value]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "url": "https://store1.public.blob.vercel-storage.com/t.txt",
                "downloadUrl": "https://store1.public.blob.vercel-storage.com/t.txt?download=1",
                "pathname": "t.txt",
                "size": 1,
                "etag": "e",
                "uploadedAt": "2026-09-29T12:00:00Z",
                "contentType": "text/plain",
                "contentDisposition": "inline",
                "cacheControl": "max-age=300",
            },
        )

    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        await blob.head("t.txt")
        with pytest.raises(BlobCredentialsError, match="changed store ID"):
            await blob.head("t.txt")


# --- Regression Tests for Review Findings 1 to 10 ---


@pytest.mark.anyio
async def test_put_strict_types_zero_io() -> None:
    calls = 0

    def cred_factory() -> BlobCredentials:
        nonlocal calls
        calls += 1
        return BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)

    opt = BlobServiceOptions(credentials_factory=cred_factory)

    async with session(service_options=[opt]):
        # Reject bytearray
        with pytest.raises(TypeError, match="put body must be bytes"):
            await blob.put("valid.bin", bytearray(b"123"), access="public")

        # Reject memoryview
        with pytest.raises(TypeError, match="put body must be bytes"):
            await blob.put("valid.bin", memoryview(b"123"), access="public")

        # Reject bool for add_random_suffix
        with pytest.raises(TypeError, match="add_random_suffix must be bool"):
            await blob.put("valid.bin", b"123", access="public", add_random_suffix=1)  # type: ignore[arg-type]

        # Reject bool for allow_overwrite
        with pytest.raises(TypeError, match="allow_overwrite must be bool"):
            await blob.put("valid.bin", b"123", access="public", allow_overwrite=0)  # type: ignore[arg-type]

        # Reject bool for cache_control_max_age
        with pytest.raises(TypeError, match="cache_control_max_age must be an integer, not bool"):
            await blob.put("valid.bin", b"123", access="public", cache_control_max_age=True)  # type: ignore[arg-type]

        # Reject empty content_type
        with pytest.raises(ValueError, match="content_type cannot be empty"):
            await blob.put("valid.bin", b"123", access="public", content_type="")

        # Reject non-ASCII content_type
        with pytest.raises(ValueError, match="content_type must be ASCII"):
            await blob.put("valid.bin", b"123", access="public", content_type="text/plain; café=1")

    assert calls == 0, "No credentials should be called on invalid put arguments"


@pytest.mark.anyio
async def test_open_download_validates_inputs_before_credentials() -> None:
    calls = 0

    def cred_factory() -> BlobCredentials:
        nonlocal calls
        calls += 1
        return BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)

    opt = BlobServiceOptions(credentials_factory=cred_factory)

    async with session(service_options=[opt]):
        # URL with bad scheme
        with pytest.raises(BlobError, match="HTTPS scheme"):
            await blob.get(
                "ftp://teststore123.public.blob.vercel-storage.com/f.txt", access="public"
            ).__aenter__()

        # Private URL with bad scheme
        with pytest.raises(BlobError, match="HTTPS scheme"):
            await blob.get(
                "http://teststore123.private.blob.vercel-storage.com/f.txt", access="private"
            ).__aenter__()

        # Path with dot segments
        with pytest.raises(BlobError, match="dot segments"):
            await blob.get("folder/../secret.txt", access="public").__aenter__()

        with pytest.raises(BlobError, match="dot segments"):
            await blob.get("./secret.txt", access="public").__aenter__()

        # URL with empty userinfo '@'
        with pytest.raises(BlobError, match="userinfo"):
            await blob.get(
                "https://@teststore123.public.blob.vercel-storage.com/f.txt", access="public"
            ).__aenter__()

    # Note: URL validation must happen before credentials, so calls must be 0
    assert calls == 0, "Input validation must occur before credential factory is invoked"

    # Valid private URL checks store against credentials
    async with session(service_options=[opt]):
        with pytest.raises(BlobError, match="does not match credential store ID"):
            await blob.get(
                "https://wrongstore.private.blob.vercel-storage.com/f.txt", access="private"
            ).__aenter__()
    assert calls == 1


@pytest.mark.anyio
async def test_strict_json_and_headers_parsing() -> None:
    creds = BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)
    opt = BlobServiceOptions(credentials_factory=lambda: creds)

    # 1. PUT response with integer for url
    def handler_bad_put(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "url": 12345,  # not a string!
                "downloadUrl": "https://...",
                "pathname": "f.txt",
                "contentType": "text/plain",
                "contentDisposition": "inline",
                "etag": "etag",
            },
        )

    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(handler_bad_put)
        ),
    ):
        with pytest.raises(BlobStreamError, match="must be a string"):
            await blob.put("f.txt", b"x", access="public")

    # 2. HEAD response with boolean for size
    def handler_bad_head(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "url": "https://...",
                "downloadUrl": "https://...",
                "pathname": "f.txt",
                "size": True,  # boolean, not a real integer!
                "etag": "etag",
                "uploadedAt": "2026-09-29T12:00:00Z",
                "contentType": "text/plain",
                "contentDisposition": "inline",
                "cacheControl": "max-age=300",
            },
        )

    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(handler_bad_head)
        ),
    ):
        with pytest.raises(BlobStreamError, match="invalid size"):
            await blob.head("f.txt")

    # 3. GET response with invalid content-length header
    def handler_bad_get_cl(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-length": "not_a_number"},
            stream=_TrackingAsyncStream([b"data"]),
        )

    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(handler_bad_get_cl)
        ),
    ):
        with pytest.raises(BlobStreamError, match="invalid content-length"):
            async with blob.get("f.txt", access="private"):
                pass

    # 4. GET response with invalid last-modified header
    def handler_bad_get_lm(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"last-modified": "garbage-date"},
            stream=_TrackingAsyncStream([b"data"]),
        )

    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(handler_bad_get_lm)
        ),
    ):
        with pytest.raises(BlobStreamError, match="invalid last-modified"):
            async with blob.get("f.txt", access="private"):
                pass


@pytest.mark.anyio
async def test_delete_does_not_swallow_arbitrary_404() -> None:
    creds = BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)
    opt = BlobServiceOptions(credentials_factory=lambda: creds)

    # 404 with store_not_found must raise BlobStoreNotFoundError
    def handler_store_not_found(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404, json={"error": {"code": "store_not_found", "message": "store missing"}}
        )

    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(handler_store_not_found)
        ),
    ):
        with pytest.raises(BlobStoreNotFoundError):
            await blob.delete("f.txt")

    # 404 with blob_not_found must raise BlobNotFoundError
    def handler_blob_not_found(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404, json={"error": {"code": "blob_not_found", "message": "blob missing"}}
        )

    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(handler_blob_not_found)
        ),
    ):
        with pytest.raises(BlobNotFoundError):
            await blob.delete("f.txt")


@pytest.mark.anyio
async def test_context_reentry_and_stream_closed_errors() -> None:
    stream = _TrackingAsyncStream([b"only_chunk"])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/reenter.bin"

    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        ctx = blob.get(url, access="public")
        async with ctx as download:
            chunks = []
            async for chunk in download:
                chunks.append(chunk)
            assert chunks == [b"only_chunk"]

            # Stream has reached EOF. Calling anext() again must raise BlobStreamError,
            # not StopAsyncIteration!
            with pytest.raises(BlobStreamError, match="Cannot read from closed download"):
                await anext(download)

        # Context manager cannot be re-entered
        with pytest.raises(RuntimeError, match="cannot be re-entered"):
            async with ctx:
                pass


def test_sync_context_reentry_and_stream_closed_errors() -> None:
    stream = _TrackingSyncStream([b"sync_chunk"])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/sync-reenter.bin"

    with session(
        httpx_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    ):
        ctx = blob.sync.get(url, access="public")
        with ctx as download:
            chunks = list(download)
            assert chunks == [b"sync_chunk"]

            # Stream has reached EOF. Calling next() again must raise BlobStreamError!
            with pytest.raises(BlobStreamError, match="Cannot read from closed download"):
                next(download)

        # Re-entry must raise RuntimeError
        with pytest.raises(RuntimeError, match="cannot be re-entered"):
            with ctx:
                pass


@pytest.mark.anyio
async def test_explicit_close_errors_preserved() -> None:
    from vercel.blob._internal.download import AsyncBlobDownload, SyncBlobDownload

    class FailingCloseStreamingResponse:
        async def aclose(self) -> None:
            raise OSError("disk flush failure on close")

    meta = DownloadMetadata(
        url="https://teststore123.public.blob.vercel-storage.com/f.txt", status_code=200
    )
    async_dl = AsyncBlobDownload(FailingCloseStreamingResponse(), meta)  # type: ignore[arg-type]

    # Explicit aclose must NOT swallow the OSError!
    with pytest.raises(OSError, match="disk flush failure"):
        await async_dl.aclose()

    sync_dl = SyncBlobDownload(FailingCloseStreamingResponse(), meta)  # type: ignore[arg-type]
    with pytest.raises(OSError, match="disk flush failure"):
        sync_dl.close()


def test_blob_error_public_and_credentials_repr() -> None:
    # BlobError status_code, no response attribute
    err = BlobNotFoundError()
    assert hasattr(err, "status_code")
    assert not hasattr(err, "response")

    # BlobCredentials masks secret in repr
    creds = BlobCredentials(token="super_secret_rw_token_12345", store_id="mystore")
    creds_repr = repr(creds)
    assert "super_secret_rw_token_12345" not in creds_repr
    assert "***" in creds_repr
    assert "mystore" in creds_repr


@pytest.mark.anyio
async def test_full_lifecycle_and_cancellation() -> None:
    storage: dict[str, bytes] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT":
            pathname = request.url.params["pathname"]
            storage[pathname] = request.content
            return httpx.Response(
                200,
                json={
                    "url": f"https://teststore123.public.blob.vercel-storage.com/{pathname}",
                    "downloadUrl": f"https://teststore123.public.blob.vercel-storage.com/{pathname}?download=1",
                    "pathname": pathname,
                    "contentType": "application/octet-stream",
                    "contentDisposition": "inline",
                    "etag": "etag-lifecycle",
                },
            )
        if request.method == "GET" and "/delete" not in request.url.path:
            # Could be head or get
            if "url" in request.url.params:
                # HEAD query
                target = request.url.params["url"]
                pathname = target.split("/")[-1]
                data = storage.get(pathname, b"")
                return httpx.Response(
                    200,
                    json={
                        "url": f"https://teststore123.public.blob.vercel-storage.com/{pathname}",
                        "downloadUrl": f"https://teststore123.public.blob.vercel-storage.com/{pathname}?download=1",
                        "pathname": pathname,
                        "size": len(data),
                        "etag": "etag-lifecycle",
                        "uploadedAt": "2026-09-29T12:00:00Z",
                        "contentType": "application/octet-stream",
                        "contentDisposition": "inline",
                        "cacheControl": "max-age=300",
                    },
                )
            else:
                # Streaming GET
                pathname = request.url.path.lstrip("/")
                data = storage.get(pathname, b"")
                return httpx.Response(
                    200,
                    headers={"content-length": str(len(data))},
                    stream=_TrackingAsyncStream([data]),
                )
        if request.method == "POST" and request.url.path.endswith("/delete"):
            payload = json.loads(request.content)
            assert "urls" in payload and isinstance(payload["urls"], list)
            for u in payload["urls"]:
                name = u.split("/")[-1]
                storage.pop(name, None)
            return httpx.Response(200, json={})
        return httpx.Response(404)

    creds = BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)
    opt = BlobServiceOptions(credentials_factory=lambda: creds)

    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        put_empty = await blob.put("empty.bin", b"", access="public")
        assert put_empty.pathname == "empty.bin"
        async with blob.get("empty.bin", access="public") as dl_empty:
            empty_data = b"".join([c async for c in dl_empty])
            assert empty_data == b""
        head_empty = await blob.head("empty.bin")
        assert head_empty.size == 0
        await blob.delete("empty.bin")
        assert "empty.bin" not in storage

        large_payload = b"x" * (150 * 1024)
        put_large = await blob.put("large.bin", large_payload, access="public")
        assert put_large.pathname == "large.bin"
        async with blob.get("large.bin", access="public") as dl_large:
            large_data = b"".join([c async for c in dl_large])
            assert large_data == large_payload
        head_large = await blob.head("large.bin")
        assert head_large.size == 150 * 1024
        await blob.delete("large.bin")
        assert len(storage) == 0


def test_sync_full_lifecycle() -> None:
    storage: dict[str, bytes] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT":
            pathname = request.url.params["pathname"]
            storage[pathname] = request.content
            return httpx.Response(
                200,
                json={
                    "url": f"https://teststore123.public.blob.vercel-storage.com/{pathname}",
                    "downloadUrl": f"https://teststore123.public.blob.vercel-storage.com/{pathname}?download=1",
                    "pathname": pathname,
                    "contentType": "application/octet-stream",
                    "contentDisposition": "inline",
                    "etag": "etag-sync-life",
                },
            )
        if request.method == "GET" and "/delete" not in request.url.path:
            if "url" in request.url.params:
                target = request.url.params["url"]
                pathname = target.split("/")[-1]
                data = storage.get(pathname, b"")
                return httpx.Response(
                    200,
                    json={
                        "url": f"https://teststore123.public.blob.vercel-storage.com/{pathname}",
                        "downloadUrl": f"https://teststore123.public.blob.vercel-storage.com/{pathname}?download=1",
                        "pathname": pathname,
                        "size": len(data),
                        "etag": "etag-sync-life",
                        "uploadedAt": "2026-09-29T12:00:00Z",
                        "contentType": "application/octet-stream",
                        "contentDisposition": "inline",
                        "cacheControl": "max-age=300",
                    },
                )
            else:
                pathname = request.url.path.lstrip("/")
                data = storage.get(pathname, b"")
                return httpx.Response(
                    200,
                    headers={"content-length": str(len(data))},
                    stream=_TrackingSyncStream([data]),
                )
        if request.method == "POST" and request.url.path.endswith("/delete"):
            payload = json.loads(request.content)
            assert "urls" in payload and isinstance(payload["urls"], list)
            for u in payload["urls"]:
                name = u.split("/")[-1]
                storage.pop(name, None)
            return httpx.Response(200, json={})
        return httpx.Response(404)

    creds = BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)
    opt = SyncBlobServiceOptions(credentials_factory=lambda: creds)

    with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    ):
        blob.sync.put("sync-empty.bin", b"", access="public")
        with blob.sync.get("sync-empty.bin", access="public") as dl_empty:
            assert b"".join(list(dl_empty)) == b""
        head_empty = blob.sync.head("sync-empty.bin")
        assert head_empty.size == 0
        blob.sync.delete("sync-empty.bin")
        assert "sync-empty.bin" not in storage

        large_payload = b"y" * (150 * 1024)
        blob.sync.put("sync-large.bin", large_payload, access="public")
        with blob.sync.get("sync-large.bin", access="public") as dl_large:
            assert b"".join(list(dl_large)) == large_payload
        head_large = blob.sync.head("sync-large.bin")
        assert head_large.size == 150 * 1024
        blob.sync.delete("sync-large.bin")
        assert len(storage) == 0


@pytest.mark.anyio
async def test_cancellation_while_body_read_awaits() -> None:
    read_started = anyio.Event()

    class HangingAsyncStream(httpx.AsyncByteStream):
        def __init__(self) -> None:
            self.closed = False

        async def __aiter__(self) -> AsyncIterator[bytes]:
            read_started.set()
            await anyio.sleep(100.0)  # hanging read
            yield b"never"

        async def aclose(self) -> None:
            self.closed = True

    stream = HangingAsyncStream()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/hang.bin"

    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        with anyio.CancelScope() as cancel_scope:

            async def consumer() -> None:
                async with blob.get(url, access="public") as download:
                    async for _ in download:
                        pass

            async with anyio.create_task_group() as tg:
                tg.start_soon(consumer)
                await read_started.wait()
                cancel_scope.cancel()

        assert stream.closed, "Stream must be closed when task is cancelled during body read"


@pytest.mark.anyio
async def test_read_failure_after_yielding_64kb_no_retry() -> None:
    request_count = 0
    chunk1 = b"z" * (64 * 1024)

    class FailingSecondChunkStream(httpx.AsyncByteStream):
        def __init__(self) -> None:
            self.closed = False

        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield chunk1
            raise RuntimeError("network failure after 64kb")

        async def aclose(self) -> None:
            self.closed = True

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(200, stream=FailingSecondChunkStream())

    url = "https://teststore123.public.blob.vercel-storage.com/drop.bin"

    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        with pytest.raises(RuntimeError, match="network failure after 64kb"):
            async with blob.get(url, access="public") as download:
                async for chunk in download:
                    assert len(chunk) == 64 * 1024

    assert request_count == 1, "Must never retry download after yielding data"


@pytest.mark.anyio
async def test_malformed_response_on_get_entry_closes_response() -> None:
    stream = _TrackingAsyncStream([b"data"])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"content-length": "malformed_number"}, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/bad-header.bin"

    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        with pytest.raises(BlobStreamError, match="invalid content-length"):
            async with blob.get(url, access="public"):
                pass

    assert stream.closed, "Stream must be closed if metadata parsing fails on GET entry"


@pytest.mark.anyio
async def test_oidc_private_get_headers() -> None:
    captured_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        return httpx.Response(200, stream=_TrackingAsyncStream([b"secure"]))

    oidc_creds = BlobCredentials(
        token="oidc_jwt_token_sample",
        store_id=TEST_STORE,
        kind=blob.CredentialKind.OIDC,
    )
    opt = BlobServiceOptions(credentials_factory=lambda: oidc_creds)

    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        async with blob.get("secure.txt", access="private") as download:
            chunks = [c async for c in download]
            assert chunks == [b"secure"]

    assert len(captured_requests) == 1
    req = captured_requests[0]
    assert req.headers["authorization"] == "Bearer oidc_jwt_token_sample"
    assert req.headers["x-vercel-blob-store-id"] == TEST_STORE


def test_default_credentials_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from vercel.blob._internal.credentials import default_sync_credentials

    # 1. BLOB_READ_WRITE_TOKEN
    monkeypatch.setenv("BLOB_READ_WRITE_TOKEN", "vercel_blob_rw_envstore123_sec999")
    monkeypatch.delenv("BLOB_STORE_ID", raising=False)
    monkeypatch.delenv("VERCEL_BLOB_READ_WRITE_TOKEN", raising=False)

    creds = default_sync_credentials()
    assert creds.store_id == "envstore123"
    assert creds.token == "vercel_blob_rw_envstore123_sec999"
    assert creds.kind == blob.CredentialKind.READ_WRITE

    # 2. Missing credentials
    monkeypatch.delenv("BLOB_READ_WRITE_TOKEN", raising=False)
    monkeypatch.delenv("VERCEL_OIDC_TOKEN", raising=False)
    with pytest.raises(BlobCredentialsError, match="Missing Blob credentials"):
        default_sync_credentials()


@pytest.mark.anyio
async def test_mixed_case_oidc_header() -> None:
    captured_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        return httpx.Response(200, stream=_TrackingAsyncStream([b"secure"]))

    mixed_store = "AbCd123"
    oidc_creds = BlobCredentials(
        token="oidc_jwt_token_sample",
        store_id=f"store_{mixed_store}",
        kind=blob.CredentialKind.OIDC,
    )
    opt = BlobServiceOptions(credentials_factory=lambda: oidc_creds)

    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        async with blob.get("secure.txt", access="private") as download:
            chunks = [c async for c in download]
            assert chunks == [b"secure"]

    assert len(captured_requests) == 1
    req = captured_requests[0]
    # Header must preserve exact casing
    assert req.headers["x-vercel-blob-store-id"] == mixed_store
    # Hostname in delivery URL must be lowercased
    assert req.url.host == f"{mixed_store.lower()}.private.blob.vercel-storage.com"


@pytest.mark.anyio
async def test_no_redirect_followed_on_mutation_and_get() -> None:
    captured_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        return httpx.Response(307, headers={"location": "https://attacker.com/target"})

    creds = BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)
    opt = BlobServiceOptions(credentials_factory=lambda: creds)

    # Client configured with follow_redirects=True to test enforcement of follow_redirects=False
    async with session(
        service_options=[opt],
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            follow_redirects=True,
        ),
    ):
        with pytest.raises(BlobUnknownError):
            await blob.put("test.bin", b"data", access="public")
        assert len(captured_requests) == 1
        assert "attacker.com" not in str(captured_requests[-1].url)

        with pytest.raises(BlobUnknownError):
            async with blob.get("test.bin", access="private"):
                pass
        assert len(captured_requests) == 2
        assert "attacker.com" not in str(captured_requests[-1].url)

        with pytest.raises(BlobUnknownError):
            await blob.delete("test.bin")
        assert len(captured_requests) == 3
        assert "attacker.com" not in str(captured_requests[-1].url)


@pytest.mark.anyio
async def test_bounded_sdk_buffering_and_overlapping_reads() -> None:
    chunk_size = 64 * 1024
    read_started = anyio.Event()
    release_read = anyio.Event()

    class PausingStream(_TrackingAsyncStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            for index, chunk in enumerate(self.chunks):
                if index == 1:
                    read_started.set()
                    await release_read.wait()
                self.yielded_count += 1
                yield chunk

    stream = PausingStream([b"a" * chunk_size for _ in range(10)])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/stream-buffer.bin"

    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        async with blob.get(url, access="public") as download:
            # Read first chunk
            first_chunk = await anext(download)
            assert len(first_chunk) == chunk_size
            assert stream.yielded_count == 1, "Must not eagerly buffer subsequent chunks"

            async def read_next() -> None:
                assert len(await anext(download)) == chunk_size

            async with anyio.create_task_group() as group:
                group.start_soon(read_next)
                await read_started.wait()
                try:
                    with pytest.raises(BlobStreamError, match="Concurrent reads"):
                        await anext(download)
                finally:
                    release_read.set()
            assert stream.yielded_count == 2
        assert stream.closed


def test_sync_put_invalid_input_zero_io() -> None:
    calls = 0

    def cred_factory() -> BlobCredentials:
        nonlocal calls
        calls += 1
        return BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)

    opt = SyncBlobServiceOptions(credentials_factory=cred_factory)

    with session(service_options=[opt]):
        with pytest.raises(TypeError, match="put body must be bytes"):
            blob.sync.put("valid.bin", "not bytes", access="public")  # type: ignore[arg-type]

        with pytest.raises(BlobError, match="pathname cannot be empty"):
            blob.sync.put("", b"bytes", access="public")

        with pytest.raises(BlobError, match="access must be 'public' or 'private'"):
            blob.sync.put("valid.bin", b"bytes", access="invalid")  # type: ignore[arg-type]

    assert calls == 0, "Credential factory must not be called on invalid sync put"


@pytest.mark.anyio
async def test_checkpoint_awaiting_close_under_cancellation_on_failed_get_entry() -> None:
    checkpoint_completed = False

    class CheckpointAwaitingAsyncStream(httpx.AsyncByteStream):
        def __init__(self) -> None:
            self.closed = False

        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield b"data"

        async def aclose(self) -> None:
            nonlocal checkpoint_completed
            await anyio.lowlevel.checkpoint()
            checkpoint_completed = True
            self.closed = True

    stream = CheckpointAwaitingAsyncStream()

    def handler(request: httpx.Request) -> httpx.Response:
        # Return 404 so entry fails, triggering cleanup
        return httpx.Response(404, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/entry-fail.bin"

    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        with anyio.CancelScope() as scope:
            scope.cancel()
            with pytest.raises((BlobNotFoundError, anyio.get_cancelled_exc_class())):
                async with blob.get(url, access="public"):
                    pass

    assert checkpoint_completed, "aclose() checkpoint must complete under shielded cleanup"
    assert stream.closed
