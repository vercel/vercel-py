"""Behavioral tests for Vercel Blob object operations."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Any

import anyio
import httpx2 as httpx
import pytest
from hypothesis import example, given, settings, strategies as st

from vercel import blob
from vercel.api import session
from vercel.blob import (
    BlobAccessError,
    BlobCredentials,
    BlobCredentialsError,
    BlobCredentialsFactory,
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
    SyncBlobCredentialsFactory,
)
from vercel.blob.sync import SyncBlobServiceOptions
from vercel.errors import VercelSessionClosedError


class _TrackingSyncStream(httpx.SyncByteStream):
    def __init__(self, chunks: list[bytes], *, fail: bool = False) -> None:
        self.chunks = chunks
        self.fail = fail
        self.closed = False
        self.yielded_count = 0

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self.chunks:
            self.yielded_count += 1
            yield chunk
        if self.fail:
            raise RuntimeError("network failure during streaming")

    def close(self) -> None:
        self.closed = True


class _TrackingAsyncStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes], *, fail: bool = False) -> None:
        self.chunks = chunks
        self.fail = fail
        self.closed = False
        self.yielded_count = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            self.yielded_count += 1
            yield chunk
        if self.fail:
            raise RuntimeError("network failure during streaming")

    async def aclose(self) -> None:
        await anyio.lowlevel.checkpoint()
        self.closed = True


TEST_TOKEN = "vercel_blob_rw_teststore123_secretkeyabc"
TEST_STORE = "teststore123"

_URL = f"https://{TEST_STORE}.public.blob.vercel-storage.com/f.bin"


def _credentials() -> BlobCredentials:
    return BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)


@asynccontextmanager
async def _async_session(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    credentials_factory: BlobCredentialsFactory = _credentials,
    follow_redirects: bool = False,
) -> AsyncIterator[None]:
    async with session(
        service_options=[BlobServiceOptions(credentials_factory=credentials_factory)],
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(handler), follow_redirects=follow_redirects
        ),
    ):
        yield


@contextmanager
def _sync_session(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    credentials_factory: SyncBlobCredentialsFactory = _credentials,
) -> Iterator[None]:
    with session(
        service_options=[SyncBlobServiceOptions(credentials_factory=credentials_factory)],
        httpx_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    ):
        yield


def _object_json(pathname: str, *, size: int | None = None) -> dict[str, Any]:
    url = f"https://{TEST_STORE}.public.blob.vercel-storage.com/{pathname}"
    data: dict[str, Any] = {
        "url": url,
        "downloadUrl": f"{url}?download=1",
        "pathname": pathname,
        "contentType": "text/plain",
        "contentDisposition": "inline",
        "etag": "etag",
    }
    if size is not None:
        data.update(size=size, uploadedAt="2026-09-29T12:00:00Z", cacheControl="max-age=300")
    return data


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

    async with _async_session(handler):
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

    with _sync_session(handler):
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
@pytest.mark.parametrize("cache_age", [None, 0, 60])
async def test_put_valid_input_boundaries(cache_age: int | None) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["x-add-random-suffix"] == "0"
        assert request.url.params["pathname"] == "valid.bin"
        assert request.content == b""
        assert "x-content-type" not in request.headers
        if cache_age is None:
            assert "x-cache-control-max-age" not in request.headers
        else:
            assert request.headers["x-cache-control-max-age"] == str(cache_age)
        return httpx.Response(200, json=_object_json("valid.bin"))

    async with _async_session(handler):
        result = await blob.put(
            "/valid.bin", b"", access="public", content_type=None, cache_control_max_age=cache_age
        )

    assert result.pathname == "valid.bin"


@pytest.mark.parametrize("cache_age", [None, 0, 60])
def test_sync_put_valid_input_boundaries(cache_age: int | None) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["x-add-random-suffix"] == "0"
        assert request.url.params["pathname"] == "valid.bin"
        assert request.content == b""
        assert "x-content-type" not in request.headers
        if cache_age is None:
            assert "x-cache-control-max-age" not in request.headers
        else:
            assert request.headers["x-cache-control-max-age"] == str(cache_age)
        return httpx.Response(200, json=_object_json("valid.bin"))

    with _sync_session(handler):
        result = blob.sync.put(
            "/valid.bin", b"", access="public", content_type=None, cache_control_max_age=cache_age
        )

    assert result.pathname == "valid.bin"


_INVALID_PUT_INPUTS = [
    pytest.param({"body": "not bytes"}, TypeError, "put body must be bytes", id="str-body"),
    pytest.param(
        {"body": bytearray(b"123")}, TypeError, "put body must be bytes", id="bytearray-body"
    ),
    pytest.param(
        {"body": memoryview(b"123")}, TypeError, "put body must be bytes", id="memoryview-body"
    ),
    pytest.param({"pathname": ""}, BlobError, "pathname cannot be empty", id="empty-path"),
    pytest.param(
        {"pathname": "//file.txt"}, BlobError, "cannot contain.*//", id="leading-double-slash"
    ),
    pytest.param(
        {"pathname": "folder//file.txt"}, BlobError, "cannot contain.*//", id="double-slash"
    ),
    pytest.param({"pathname": "folder/../file.txt"}, BlobError, "dot segments", id="dot-path"),
    pytest.param({"pathname": "file\ud800.txt"}, BlobError, "Unicode", id="surrogate-path"),
    pytest.param({"pathname": "x" * 951}, BlobError, "maximum length is 950", id="long-path"),
    pytest.param(
        {"pathname": "😀" * 476}, BlobError, "maximum length is 950", id="long-utf16-path"
    ),
    pytest.param(
        {"pathname": "valid/\x01path.txt"}, BlobError, "control characters", id="control-path"
    ),
    pytest.param(
        {"access": "invalid"}, BlobError, "access must be 'public' or 'private'", id="access"
    ),
    pytest.param(
        {"add_random_suffix": 1}, TypeError, "add_random_suffix must be bool", id="suffix"
    ),
    pytest.param({"allow_overwrite": 0}, TypeError, "allow_overwrite must be bool", id="overwrite"),
    pytest.param(
        {"cache_control_max_age": True},
        TypeError,
        "cache_control_max_age must be an integer, not bool",
        id="bool-age",
    ),
    pytest.param(
        {"cache_control_max_age": -1}, ValueError, "must be nonnegative", id="negative-age"
    ),
    pytest.param({"cache_control_max_age": 1.5}, TypeError, "must be an integer", id="float-age"),
    pytest.param({"cache_control_max_age": "1"}, TypeError, "must be an integer", id="str-age"),
    pytest.param({"add_random_suffix": "true"}, TypeError, "must be bool", id="str-suffix"),
    pytest.param({"allow_overwrite": None}, TypeError, "must be bool", id="null-overwrite"),
    pytest.param({"content_type": 123}, ValueError, "must be a string", id="non-string-type"),
    pytest.param(
        {"content_type": "text/plain\r\n"}, ValueError, "control characters", id="control-type"
    ),
    pytest.param({"content_type": ""}, ValueError, "content_type cannot be empty", id="empty-type"),
    pytest.param(
        {"content_type": "text/plain\x7f"}, ValueError, "control characters", id="delete-type"
    ),
    pytest.param(
        {"content_type": "text/plain; café=1"},
        ValueError,
        "content_type must be ASCII",
        id="non-ascii-type",
    ),
]


@pytest.mark.anyio
@pytest.mark.parametrize("invalid,error,message", _INVALID_PUT_INPUTS)
async def test_put_invalid_input_fails_before_credentials_or_http(
    invalid: dict[str, Any], error: type[Exception], message: str
) -> None:
    credentials_calls = 0

    def credentials() -> BlobCredentials:
        nonlocal credentials_calls
        credentials_calls += 1
        return _credentials()

    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("Invalid arguments must not send HTTP requests")

    async with _async_session(handler, credentials_factory=credentials):
        arguments = {"pathname": "valid.bin", "body": b"123", "access": "public"} | invalid
        with pytest.raises(error, match=message):
            await blob.put(**arguments)

    assert credentials_calls == 0


@pytest.mark.parametrize("invalid,error,message", _INVALID_PUT_INPUTS)
def test_sync_put_invalid_input_fails_before_credentials_or_http(
    invalid: dict[str, Any], error: type[Exception], message: str
) -> None:
    credentials_calls = 0

    def credentials() -> BlobCredentials:
        nonlocal credentials_calls
        credentials_calls += 1
        return _credentials()

    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("Invalid arguments must not send HTTP requests")

    with _sync_session(handler, credentials_factory=credentials):
        arguments = {"pathname": "valid.bin", "body": b"123", "access": "public"} | invalid
        with pytest.raises(error, match=message):
            blob.sync.put(**arguments)

    assert credentials_calls == 0


_BUFFERED_GET_CASES = [
    pytest.param([], False, id="empty"),
    pytest.param([b"a" * (64 * 1024), b"\x00\xfftail"], False, id="multi-chunk"),
    pytest.param([b"a" * (64 * 1024)], True, id="read-error"),
]


@pytest.mark.anyio
@pytest.mark.parametrize("chunks,fail", _BUFFERED_GET_CASES)
async def test_async_get_buffered(chunks: list[bytes], fail: bool) -> None:
    stream = _TrackingAsyncStream(chunks, fail=fail)
    requests: list[httpx.Request] = []
    headers = {"content-type": "application/octet-stream", "etag": "buffered-etag"}

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        return httpx.Response(200, headers=headers, stream=stream)

    async with _async_session(handler):
        if fail:
            with pytest.raises(RuntimeError, match="network failure"):
                await blob.get(_URL, access="public")
        else:
            result = await blob.get(_URL, access="public")
        assert stream.closed
    assert len(requests) == 1
    if not fail:
        assert type(result) is blob.GetResult is blob.sync.GetResult
        assert result.body == b"".join(chunks)
        assert result.metadata == DownloadMetadata(
            url=_URL,
            status_code=200,
            content_type="application/octet-stream",
            etag="buffered-etag",
            headers=headers,
        )


@pytest.mark.parametrize("chunks,fail", _BUFFERED_GET_CASES)
def test_sync_get_buffered(chunks: list[bytes], fail: bool) -> None:
    stream = _TrackingSyncStream(chunks, fail=fail)
    requests: list[httpx.Request] = []
    headers = {"content-type": "application/octet-stream", "etag": "buffered-etag"}

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        return httpx.Response(200, headers=headers, stream=stream)

    with _sync_session(handler):
        if fail:
            with pytest.raises(RuntimeError, match="network failure"):
                blob.sync.get(_URL, access="public")
        else:
            result = blob.sync.get(_URL, access="public")
        assert stream.closed
    assert len(requests) == 1
    if not fail:
        assert type(result) is blob.GetResult is blob.sync.GetResult
        assert result.body == b"".join(chunks)
        assert result.metadata == DownloadMetadata(
            url=_URL,
            status_code=200,
            content_type="application/octet-stream",
            etag="buffered-etag",
            headers=headers,
        )


@pytest.mark.anyio
async def test_async_stream_lifecycle() -> None:
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
        context = blob.stream(target_url, access="public")
        async with context as download:
            assert isinstance(download.metadata, DownloadMetadata)
            assert download.metadata.size == 23
            assert download.metadata.content_type == "text/plain"
            assert download.metadata.etag == '"etag-stream-1"'
            assert stream.yielded_count == 0, "No body consumed during GET entry"
            assert not stream.closed

            iterator = aiter(download)
            assert aiter(iterator) is iterator
            assert b"".join([chunk async for chunk in iterator]) == b"chunk-1-chunk-2-chunk-3"
            assert download.is_closed
            with pytest.raises(BlobStreamError, match="Cannot read from closed download"):
                aiter(download)
            with pytest.raises(BlobStreamError, match="Cannot read from closed download"):
                await anext(download)
            assert stream.closed, "Stream must close upon EOF"

        assert download.is_closed
        assert not creds_called, "Public full-URL GET must not invoke credentials"
        with pytest.raises(RuntimeError, match="cannot be re-entered"):
            async with context:
                pass


def test_sync_stream_lifecycle() -> None:
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

    with _sync_session(handler):
        context = blob.sync.stream("test.bin", access="private")
        with context as download:
            assert download.metadata.size == 12
            assert stream.yielded_count == 0
            assert b"".join(download) == b"part-apart-b"
            assert stream.closed
            assert download.is_closed
            with pytest.raises(BlobStreamError, match="Cannot read from closed download"):
                iter(download)
            with pytest.raises(BlobStreamError, match="Cannot read from closed download"):
                next(download)
        with pytest.raises(RuntimeError, match="cannot be re-entered"):
            with context:
                pass


@pytest.mark.anyio
async def test_stream_early_exit_closes_stream() -> None:
    chunk1 = b"c1" * 32768
    chunk2 = b"c2" * 32768
    stream = _TrackingAsyncStream([chunk1, chunk2])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/early.bin"

    async with _async_session(handler):
        async with blob.stream(url, access="public") as download:
            async for chunk in download:
                assert chunk == chunk1
                break
            assert not stream.closed
            with pytest.raises(BlobStreamError, match="consumed once"):
                aiter(download)
        assert stream.closed
        assert download.is_closed
        with pytest.raises(BlobStreamError, match="Cannot read from closed download"):
            aiter(download)


def test_sync_stream_early_exit_closes_stream() -> None:
    chunk1 = b"c1" * 32768
    chunk2 = b"c2" * 32768
    stream = _TrackingSyncStream([chunk1, chunk2])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/sync-early.bin"

    with _sync_session(handler):
        with blob.sync.stream(url, access="public") as download:
            for chunk in download:
                assert chunk == chunk1
                break
            assert not stream.closed
            with pytest.raises(BlobStreamError, match="consumed once"):
                iter(download)
        assert stream.closed
        assert download.is_closed
        with pytest.raises(BlobStreamError, match="Cannot read from closed download"):
            iter(download)


@pytest.mark.anyio
@pytest.mark.parametrize("chunk_size", [6, 64 * 1024], ids=["buffered", "delivered"])
async def test_stream_read_error_closes_without_retry(chunk_size: int) -> None:
    chunk = b"z" * chunk_size
    stream = _TrackingAsyncStream([chunk], fail=True)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, stream=stream)

    received = []
    async with _async_session(handler):
        with pytest.raises(RuntimeError, match="network failure"):
            async with blob.stream(_URL, access="public") as download:
                async for part in download:
                    received.append(part)
    assert received == ([chunk] if chunk_size == 64 * 1024 else [])
    assert stream.closed
    assert download.is_closed
    assert len(requests) == 1


@pytest.mark.parametrize("chunk_size", [6, 64 * 1024], ids=["buffered", "delivered"])
def test_sync_stream_read_error_closes_without_retry(chunk_size: int) -> None:
    chunk = b"z" * chunk_size
    stream = _TrackingSyncStream([chunk], fail=True)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, stream=stream)

    received = []
    with _sync_session(handler):
        with pytest.raises(RuntimeError, match="network failure"):
            with blob.sync.stream(_URL, access="public") as download:
                for part in download:
                    received.append(part)
    assert received == ([chunk] if chunk_size == 64 * 1024 else [])
    assert stream.closed
    assert download.is_closed
    assert len(requests) == 1


@pytest.mark.anyio
async def test_stream_cancellation_closes_stream() -> None:
    chunk = b"a" * (64 * 1024)
    stream = _TrackingAsyncStream([chunk, b"unread"])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/cancel.bin"

    async with _async_session(handler):
        with anyio.CancelScope() as cancel_scope:
            async with blob.stream(url, access="public") as download:
                async for received in download:
                    assert received == chunk
                    assert stream.yielded_count == 1
                    assert not stream.closed
                    cancel_scope.cancel()
                    await anyio.lowlevel.checkpoint()
                    pytest.fail("Cancellation must interrupt the download body")

        assert cancel_scope.cancelled_caught
        assert stream.closed, "Cancellation must trigger shielded stream closure"
        assert download.is_closed
        assert stream.yielded_count == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    "target,access,message",
    [
        pytest.param(
            f"http://{TEST_STORE}.public.blob.vercel-storage.com/f.txt",
            "public",
            "HTTPS scheme",
            id="http",
        ),
        pytest.param(
            f"ftp://{TEST_STORE}.public.blob.vercel-storage.com/f.txt",
            "public",
            "HTTPS scheme",
            id="ftp",
        ),
        pytest.param(
            f"http://{TEST_STORE}.private.blob.vercel-storage.com/f.txt",
            "private",
            "HTTPS scheme",
            id="private-http",
        ),
        pytest.param(
            "https://attacker.com/f.txt",
            "public",
            "does not point to a Vercel Blob store",
            id="domain",
        ),
        pytest.param(
            f"https://user:pass@{TEST_STORE}.public.blob.vercel-storage.com/f.txt",
            "public",
            "userinfo",
            id="userinfo",
        ),
        pytest.param(
            f"https://@{TEST_STORE}.public.blob.vercel-storage.com/f.txt",
            "public",
            "userinfo",
            id="empty-userinfo",
        ),
        pytest.param(
            f"https://{TEST_STORE}.public.blob.vercel-storage.com:8080/f.txt",
            "public",
            "non-default port",
            id="port",
        ),
        pytest.param(
            f"https://{TEST_STORE}.private.blob.vercel-storage.com/f.txt",
            "public",
            "does not match requested access",
            id="access",
        ),
        pytest.param("folder/../secret.txt", "public", "dot segments", id="parent-segment"),
        pytest.param("./secret.txt", "public", "dot segments", id="current-segment"),
    ],
)
async def test_stream_invalid_input_zero_io(target: str, access: blob.Access, message: str) -> None:
    def credentials() -> BlobCredentials:
        pytest.fail("Invalid target must not resolve credentials")

    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("Invalid target must not send HTTP requests")

    async with _async_session(handler, credentials_factory=credentials):
        with pytest.raises(BlobError, match=message):
            async with blob.stream(target, access=access):
                pass


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("operation", ["stream", "head", "delete"])
@pytest.mark.parametrize("encoded_path", ["folder/%2e%2e/file.txt", "file%00.txt", "file%ff.txt"])
async def test_encoded_invalid_delivery_url_zero_io(
    asynchronous: bool, operation: str, encoded_path: str
) -> None:
    target = f"https://{TEST_STORE}.private.blob.vercel-storage.com/{encoded_path}"

    def credentials() -> BlobCredentials:
        pytest.fail("Invalid URL must not resolve credentials")

    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("Invalid URL must not send HTTP requests")

    if asynchronous:
        async with _async_session(handler, credentials_factory=credentials):
            with pytest.raises(BlobError):
                if operation == "stream":
                    async with blob.stream(target, access="private"):
                        pass
                elif operation == "head":
                    await blob.head(target)
                else:
                    await blob.delete(target)
    else:
        with _sync_session(handler, credentials_factory=credentials):
            with pytest.raises(BlobError):
                if operation == "stream":
                    with blob.sync.stream(target, access="private"):
                        pass
                elif operation == "head":
                    blob.sync.head(target)
                else:
                    blob.sync.delete(target)


@pytest.mark.anyio
async def test_private_stream_store_id_mismatch() -> None:
    calls = 0

    def credentials() -> BlobCredentials:
        nonlocal calls
        calls += 1
        return BlobCredentials(token=TEST_TOKEN, store_id=TEST_STORE)

    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("Store mismatch must fail before HTTP")

    async with _async_session(handler, credentials_factory=credentials):
        with pytest.raises(BlobError, match="does not match credential store ID"):
            async with blob.stream(
                "https://wrongstore.private.blob.vercel-storage.com/f.txt", access="private"
            ):
                pass
    assert calls == 1


@pytest.mark.anyio
async def test_head_success() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.params["url"] == "head.txt"
        return httpx.Response(200, json=_object_json("head.txt", size=42))

    async with _async_session(handler):
        result = await blob.head("head.txt")
    assert isinstance(result, HeadResult)
    assert result.size == 42
    assert result.etag == "etag"
    assert result.uploaded_at.year == 2026


@pytest.mark.anyio
async def test_delete_success() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={})

    async with _async_session(handler):
        await blob.delete("f.txt")
    assert len(captured) == 1
    assert captured[0].method == "POST"
    assert captured[0].url.path.endswith("/delete")
    assert json.loads(captured[0].content) == {"urls": ["f.txt"]}


@pytest.mark.anyio
async def test_session_closed_guards() -> None:
    chunk1 = b"d1" * 32768
    chunk2 = b"d2" * 32768
    stream = _TrackingAsyncStream([chunk1, chunk2])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    url = "https://teststore123.public.blob.vercel-storage.com/sess.bin"

    async with _async_session(handler):
        download = await blob.stream(url, access="public").__aenter__()
        chunk = await anext(download.__aiter__())
        assert chunk == chunk1

    # Session is now closed
    with pytest.raises(VercelSessionClosedError):
        await anext(download)

    assert stream.closed


@pytest.mark.anyio
@pytest.mark.parametrize(
    "operation,status,code,error,retry_after",
    [
        pytest.param("put", 403, "forbidden", BlobAccessError, None, id="forbidden"),
        pytest.param(
            "put", 400, "store_not_found", BlobStoreNotFoundError, None, id="missing-store"
        ),
        pytest.param("head", 404, "not_found", BlobNotFoundError, None, id="head-not-found"),
        pytest.param(
            "head", 412, "precondition_failed", BlobPreconditionFailedError, None, id="precondition"
        ),
        pytest.param("head", 429, "rate_limited", BlobServiceRateLimited, 5, id="rate-limit"),
        pytest.param(
            "head", 503, "service_unavailable", BlobServiceNotAvailable, None, id="unavailable"
        ),
        pytest.param("delete", 404, "not_found", BlobNotFoundError, None, id="delete-not-found"),
        pytest.param(
            "delete", 404, "blob_not_found", BlobNotFoundError, None, id="delete-blob-not-found"
        ),
        pytest.param(
            "delete",
            404,
            "store_not_found",
            BlobStoreNotFoundError,
            None,
            id="delete-store-not-found",
        ),
    ],
)
async def test_error_mappings(
    operation: str, status: int, code: str, error: type[BlobError], retry_after: int | None
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        error_data = {"code": code}
        if retry_after is None:
            error_data["message"] = "backend failure"
        return httpx.Response(status, headers={"retry-after": "5"}, json={"error": error_data})

    async with _async_session(handler):
        with pytest.raises(error) as caught:
            if operation == "put":
                await blob.put("f.txt", b"x", access="public")
            else:
                await getattr(blob, operation)("f.txt")
    assert caught.value.status_code == status
    assert caught.value.code == code
    if retry_after is not None:
        assert isinstance(caught.value, BlobServiceRateLimited)
        assert caught.value.retry_after == retry_after
    else:
        assert "backend failure" in str(caught.value)


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


@pytest.mark.anyio
@pytest.mark.parametrize(
    "operation,field,value,message",
    [
        pytest.param(operation, field, 12345, "must be a string", id=f"{operation}-{field}")
        for operation, fields in (
            (
                "put",
                ("url", "downloadUrl", "pathname", "contentType", "contentDisposition", "etag"),
            ),
            (
                "head",
                (
                    "url",
                    "downloadUrl",
                    "pathname",
                    "etag",
                    "contentType",
                    "contentDisposition",
                    "cacheControl",
                ),
            ),
        )
        for field in fields
    ]
    + [
        pytest.param("head", "size", True, "invalid size", id="head-bool-size"),
        pytest.param("head", "size", -1, "invalid size", id="head-negative-size"),
        pytest.param("head", "uploadedAt", 12345, "uploadedAt", id="head-numeric-date"),
        pytest.param("head", "uploadedAt", "not-a-date", "uploadedAt", id="head-invalid-date"),
    ],
)
async def test_strict_json_parsing(operation: str, field: str, value: object, message: str) -> None:
    payload = _object_json("f.txt", size=1)
    payload[field] = value

    async with _async_session(lambda _: httpx.Response(200, json=payload)):
        with pytest.raises(BlobStreamError, match=message):
            if operation == "put":
                await blob.put("f.txt", b"x", access="public")
            else:
                await blob.head("f.txt")


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["put", "head"])
@pytest.mark.parametrize(
    "content,message",
    [
        pytest.param(b"not-json", "not valid JSON", id="invalid-json"),
        pytest.param(b"[]", "JSON object", id="array"),
        pytest.param(b"null", "JSON object", id="null"),
        pytest.param(b"{}", "must be a string", id="missing-fields"),
    ],
)
async def test_invalid_response_payload(operation: str, content: bytes, message: str) -> None:
    async with _async_session(lambda _: httpx.Response(200, content=content)):
        with pytest.raises(BlobStreamError, match=message):
            if operation == "put":
                await blob.put("f.txt", b"x", access="public")
            else:
                await blob.head("f.txt")


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["put", "head"])
@pytest.mark.parametrize("encoding", ["utf-8-sig", "utf-16", "utf-32"])
async def test_response_json_encoding_compatibility(operation: str, encoding: str) -> None:
    content = json.dumps(_object_json("f.txt", size=1)).encode(encoding)
    result: PutResult | HeadResult
    async with _async_session(lambda _: httpx.Response(200, content=content)):
        if operation == "put":
            result = await blob.put("f.txt", b"x", access="public")
        else:
            result = await blob.head("f.txt")
    assert result.pathname == "f.txt"
    assert result.etag == "etag"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "uploaded_at",
    ["2026-09-29T12:00:00.000Z", "2026-09-29T12:00:00+00:00", "2026-09-29T12:00:00", "2026-09-29"],
)
async def test_head_preserves_timestamp_formats_and_optional_defaults(uploaded_at: str) -> None:
    payload = _object_json("f.txt", size=1)
    payload["uploadedAt"] = uploaded_at
    payload["futureField"] = "ignored"
    for field in ("contentType", "contentDisposition", "cacheControl"):
        del payload[field]
    async with _async_session(lambda _: httpx.Response(200, json=payload)):
        result = await blob.head("f.txt")
    assert result.uploaded_at.year == 2026
    assert result.uploaded_at.month == 9
    assert result.uploaded_at.day == 29
    assert result.content_type is None
    assert result.content_disposition == ""
    assert result.cache_control == ""


_INVALID_DOWNLOAD_HEADERS = [
    pytest.param({"content-length": "not_a_number"}, "invalid content-length", id="length"),
    pytest.param({"content-length": "-1"}, "invalid content-length", id="negative-length"),
    pytest.param({"last-modified": "garbage-date"}, "invalid last-modified", id="modified"),
]


@pytest.mark.anyio
@pytest.mark.parametrize("access", ["public", "private"])
@pytest.mark.parametrize("headers,message", _INVALID_DOWNLOAD_HEADERS)
async def test_malformed_stream_metadata_closes_response(
    access: blob.Access, headers: dict[str, str], message: str
) -> None:
    stream = _TrackingAsyncStream([b"data"])
    async with _async_session(lambda _: httpx.Response(200, headers=headers, stream=stream)):
        with pytest.raises(BlobStreamError, match=message):
            async with blob.stream(
                f"https://{TEST_STORE}.{access}.blob.vercel-storage.com/f.txt", access=access
            ):
                pass
    assert stream.closed
    assert stream.yielded_count == 0


@pytest.mark.parametrize("access", ["public", "private"])
@pytest.mark.parametrize("headers,message", _INVALID_DOWNLOAD_HEADERS)
def test_sync_malformed_stream_metadata_closes_response(
    access: blob.Access, headers: dict[str, str], message: str
) -> None:
    stream = _TrackingSyncStream([b"data"])
    with _sync_session(lambda _: httpx.Response(200, headers=headers, stream=stream)):
        with pytest.raises(BlobStreamError, match=message):
            with blob.sync.stream(
                f"https://{TEST_STORE}.{access}.blob.vercel-storage.com/f.txt", access=access
            ):
                pass
    assert stream.closed
    assert stream.yielded_count == 0


@pytest.mark.anyio
@pytest.mark.xfail(
    strict=True,
    reason="Core streaming-response adapters suppress explicit close errors",
)
async def test_explicit_close_errors_preserved() -> None:
    class FailingCloseAsyncStream(_TrackingAsyncStream):
        async def aclose(self) -> None:
            await super().aclose()
            raise OSError("disk flush failure on close")

    stream = FailingCloseAsyncStream([b"unread"])
    async with _async_session(lambda _: httpx.Response(200, stream=stream)):
        async with blob.stream(_URL, access="public") as download:
            with pytest.raises(OSError, match="disk flush failure"):
                await download.aclose()
            assert download.is_closed
            assert stream.closed
            assert stream.yielded_count == 0


@pytest.mark.xfail(
    strict=True,
    reason="Core streaming-response adapters suppress explicit close errors",
)
def test_sync_explicit_close_errors_preserved() -> None:
    class FailingCloseSyncStream(_TrackingSyncStream):
        def close(self) -> None:
            super().close()
            raise OSError("disk flush failure on close")

    stream = FailingCloseSyncStream([b"unread"])
    with _sync_session(lambda _: httpx.Response(200, stream=stream)):
        with blob.sync.stream(_URL, access="public") as download:
            with pytest.raises(OSError, match="disk flush failure"):
                download.close()
            assert download.is_closed
            assert stream.closed
            assert stream.yielded_count == 0


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


def _lifecycle_handler(
    storage: dict[str, bytes], *, asynchronous: bool
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["x-api-version"] == "12"
        if request.method == "PUT":
            pathname = request.url.params["pathname"]
            storage[pathname] = request.content
            return httpx.Response(200, json=_object_json(pathname))
        if request.method == "POST":
            assert request.url.path.endswith("/delete")
            for target in json.loads(request.content)["urls"]:
                storage.pop(target, None)
            return httpx.Response(200, json={})
        assert request.method == "GET"
        if "url" in request.url.params:
            pathname = request.url.params["url"]
            return httpx.Response(200, json=_object_json(pathname, size=len(storage[pathname])))
        data = storage[request.url.path.lstrip("/")]
        stream = _TrackingAsyncStream([data]) if asynchronous else _TrackingSyncStream([data])
        return httpx.Response(200, headers={"content-length": str(len(data))}, stream=stream)

    return handler


@pytest.mark.anyio
@settings(max_examples=20, deadline=None)
@given(payload=st.binary(max_size=160 * 1024))
@example(payload=b"")
@example(payload=b"x" * (150 * 1024))
async def test_lifecycle_preserves_bytes(payload: bytes) -> None:
    storage: dict[str, bytes] = {}
    async with _async_session(_lifecycle_handler(storage, asynchronous=True)):
        uploaded = await blob.put("f.bin", payload, access="public")
        assert uploaded.pathname == "f.bin"
        async with blob.stream(uploaded.pathname, access="public") as download:
            chunks = [chunk async for chunk in download]
            assert all(0 < len(chunk) <= 64 * 1024 for chunk in chunks)
            assert b"".join(chunks) == payload
        metadata = await blob.head(uploaded.pathname)
        assert metadata.size == len(payload)
        assert metadata.url == uploaded.url
        await blob.delete(uploaded.pathname)
    assert storage == {}


@settings(max_examples=20, deadline=None)
@given(payload=st.binary(max_size=160 * 1024))
@example(payload=b"")
@example(payload=b"y" * (150 * 1024))
def test_sync_lifecycle_preserves_bytes(payload: bytes) -> None:
    storage: dict[str, bytes] = {}
    with _sync_session(_lifecycle_handler(storage, asynchronous=False)):
        uploaded = blob.sync.put("f.bin", payload, access="public")
        assert uploaded.pathname == "f.bin"
        with blob.sync.stream(uploaded.pathname, access="public") as download:
            chunks = list(download)
            assert all(0 < len(chunk) <= 64 * 1024 for chunk in chunks)
            assert b"".join(chunks) == payload
        metadata = blob.sync.head(uploaded.pathname)
        assert metadata.size == len(payload)
        assert metadata.url == uploaded.url
        blob.sync.delete(uploaded.pathname)
    assert storage == {}


@pytest.mark.anyio
@pytest.mark.parametrize("buffered", [False, True], ids=["stream", "get"])
async def test_cancellation_while_body_read_awaits(buffered: bool) -> None:
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

    async with _async_session(handler):
        with anyio.CancelScope() as cancel_scope:

            async def consumer() -> None:
                if buffered:
                    await blob.get(url, access="public")
                else:
                    async with blob.stream(url, access="public") as download:
                        async for _ in download:
                            pass

            async with anyio.create_task_group() as tg:
                tg.start_soon(consumer)
                await read_started.wait()
                cancel_scope.cancel()

        assert stream.closed, "Stream must be closed when task is cancelled during body read"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "store_id,normalized", [(TEST_STORE, TEST_STORE), ("store_AbCd123", "AbCd123")]
)
async def test_oidc_private_stream_headers(store_id: str, normalized: str) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, stream=_TrackingAsyncStream([b"secure"]))

    credentials = BlobCredentials(
        token="oidc_jwt_token_sample", store_id=store_id, kind=blob.CredentialKind.OIDC
    )
    async with _async_session(handler, credentials_factory=lambda: credentials):
        async with blob.stream("secure.txt", access="private") as download:
            assert [chunk async for chunk in download] == [b"secure"]
    assert len(requests) == 1
    request = requests[0]
    assert request.headers["authorization"] == "Bearer oidc_jwt_token_sample"
    assert request.headers["x-vercel-blob-store-id"] == normalized
    assert request.url.host == f"{normalized.lower()}.private.blob.vercel-storage.com"


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("has_credentials", [True, False], ids=["read-write", "missing"])
async def test_default_credentials_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, asynchronous: bool, has_credentials: bool
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
    token = "vercel_blob_rw_envstore123_sec999"
    for variable in (
        "BLOB_READ_WRITE_TOKEN",
        "BLOB_STORE_ID",
        "VERCEL_BLOB_READ_WRITE_TOKEN",
        "VERCEL_OIDC_TOKEN",
    ):
        monkeypatch.delenv(variable, raising=False)
    if has_credentials:
        monkeypatch.setenv("BLOB_READ_WRITE_TOKEN", token)

    requests: list[httpx.Request] = []
    stream = _TrackingAsyncStream([b"data"]) if asynchronous else _TrackingSyncStream([b"data"])
    expected_url = "https://envstore123.private.blob.vercel-storage.com/env.txt"

    def handler(request: httpx.Request) -> httpx.Response:
        if not has_credentials:
            pytest.fail("Missing credentials must fail before HTTP")
        requests.append(request)
        assert request.method == "GET"
        assert str(request.url) == expected_url
        assert request.headers["authorization"] == f"Bearer {token}"
        assert request.headers["x-api-version"] == "12"
        assert "x-vercel-blob-store-id" not in request.headers
        return httpx.Response(200, stream=stream)

    if asynchronous:
        async with session(
            service_options=[BlobServiceOptions()],
            httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        ):
            if has_credentials:
                async with blob.stream("env.txt", access="private") as download:
                    assert download.metadata.url == expected_url
                    assert b"".join([chunk async for chunk in download]) == b"data"
                assert download.is_closed
            else:
                with pytest.raises(BlobCredentialsError, match="Missing Blob credentials"):
                    async with blob.stream("env.txt", access="private"):
                        pass
    else:
        with session(
            service_options=[SyncBlobServiceOptions()],
            httpx_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
        ):
            if has_credentials:
                with blob.sync.stream("env.txt", access="private") as sync_download:
                    assert sync_download.metadata.url == expected_url
                    assert b"".join(sync_download) == b"data"
                assert sync_download.is_closed
            else:
                with pytest.raises(BlobCredentialsError, match="Missing Blob credentials"):
                    with blob.sync.stream("env.txt", access="private"):
                        pass

    assert len(requests) == int(has_credentials)
    assert stream.yielded_count == int(has_credentials)
    assert stream.closed == has_credentials


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["put", "stream", "delete"])
async def test_no_redirect_followed(operation: str) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(307, headers={"location": "https://attacker.com/target"})

    async with _async_session(handler, follow_redirects=True):
        with pytest.raises(BlobUnknownError):
            if operation == "put":
                await blob.put("test.bin", b"data", access="public")
            elif operation == "stream":
                async with blob.stream("test.bin", access="private"):
                    pass
            else:
                await blob.delete("test.bin")
    assert len(requests) == 1
    assert requests[0].url.host != "attacker.com"


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

    async with _async_session(handler):
        async with blob.stream(url, access="public") as download:
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


@pytest.mark.anyio
async def test_checkpoint_awaiting_close_under_cancellation_on_failed_stream_entry() -> None:
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

    async with _async_session(handler):
        with anyio.CancelScope() as scope:
            scope.cancel()
            with pytest.raises((BlobNotFoundError, anyio.get_cancelled_exc_class())):
                async with blob.stream(url, access="public"):
                    pass

    assert checkpoint_completed, "aclose() checkpoint must complete under shielded cleanup"
    assert stream.closed
