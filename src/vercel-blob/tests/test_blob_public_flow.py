from __future__ import annotations

import csv
import io
from dataclasses import FrozenInstanceError
from typing import Any

import anyio
import httpx
import pytest

from vercel._internal.core.session import get_active_session, get_active_sync_session
from vercel.api import session
from vercel.blob import BlobCredentials, BlobServiceOptions
from vercel.blob._internal.service import get_blob_service, get_sync_blob_service
from vercel.blob.sync import BlobServiceOptions as SyncBlobServiceOptions
from vercel.errors import VercelServiceOptionsError

BASE_URL = "https://blob.test/api"


def payload(path: str, data: bytes, *, etag: str = '"v1"') -> dict[str, Any]:
    return {
        "pathname": path,
        "url": f"https://store.public.blob.vercel-storage.com/{path}",
        "downloadUrl": f"https://store.public.blob.vercel-storage.com/{path}?download=1",
        "size": len(data),
        "etag": etag,
        "uploadedAt": "2026-08-24T00:00:00Z",
        "contentType": "application/octet-stream",
        "contentDisposition": "inline",
        "cacheControl": "public, max-age=60",
    }


def options(*, read_buffer_size: int = 2) -> BlobServiceOptions:
    async def credentials() -> BlobCredentials:
        return BlobCredentials("oidc", "store", "oidc")

    return BlobServiceOptions(
        base_url=BASE_URL,
        credentials_factory=credentials,
        read_buffer_size=read_buffer_size,
    )


def sync_options(*, read_buffer_size: int = 2) -> SyncBlobServiceOptions:
    def credentials() -> BlobCredentials:
        return BlobCredentials("oidc", "store", "oidc")

    return SyncBlobServiceOptions(
        base_url=BASE_URL,
        credentials_factory=credentials,
        read_buffer_size=read_buffer_size,
    )


class Store:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.requests: list[httpx.Request] = []

    async def async_handler(self, request: httpx.Request) -> httpx.Response:
        return await self.handle(request, await request.aread())

    def sync_handler(self, request: httpx.Request) -> httpx.Response:
        return anyio.run(self.handle, request, request.read())

    async def handle(self, request: httpx.Request, body: bytes) -> httpx.Response:
        self.requests.append(request)
        if request.url.host == "store.public.blob.vercel-storage.com":
            assert "authorization" not in request.headers
            assert "x-api-version" not in request.headers
            assert "x-vercel-blob-store-id" not in request.headers
            path = request.url.path.lstrip("/")
            data = self.objects[path]
            start, end = request.headers["range"].removeprefix("bytes=").split("-")
            part = data[int(start) : int(end) + 1]
            return httpx.Response(
                206,
                content=part,
                headers={
                    "etag": '"v1"',
                    "content-range": f"bytes {start}-{end}/{len(data)}",
                },
            )
        if request.method == "GET":
            path = request.url.params["url"]
            if path not in self.objects:
                return httpx.Response(404, json={"error": {"code": "blob_not_found"}})
            return httpx.Response(200, json=payload(path, self.objects[path]))
        if request.method == "PUT":
            self.objects[request.url.params["pathname"]] = body
            return httpx.Response(200, json={"etag": '"v1"'})
        pathnames = httpx.Response(200, content=body).json()["urls"]
        for pathname in pathnames:
            if pathname not in self.objects:
                return httpx.Response(404, json={"error": {"code": "blob_not_found"}})
            self.objects.pop(pathname)
        return httpx.Response(200, json={})


def test_options_are_immutable_and_path_validation_is_eager() -> None:
    import vercel.blob as blob

    configured = options()
    with pytest.raises(FrozenInstanceError):
        configured.read_buffer_size = 1  # type: ignore[misc]
    for pathname in ("//double-root", "https://example.com/object", "folder/", "x" * 951):
        with pytest.raises(ValueError):
            blob.open(pathname, "rb")


def test_blob_service_options_are_mode_specific() -> None:
    async_options = options()
    configured_sync_options = sync_options()

    with pytest.raises(VercelServiceOptionsError, match="one object per logical service"):
        with session(service_options=[async_options, configured_sync_options]):
            pass

    with session(service_options=[async_options]):
        with pytest.raises(
            VercelServiceOptionsError,
            match="BlobServiceOptions cannot configure this session mode",
        ):
            get_sync_blob_service(get_active_sync_session())

    async def reject_sync_options() -> None:
        async with session(service_options=[configured_sync_options]):
            with pytest.raises(
                VercelServiceOptionsError,
                match="SyncBlobServiceOptions cannot configure this session mode",
            ):
                get_blob_service(get_active_session())

    anyio.run(reject_sync_options)


@pytest.mark.anyio
async def test_async_binary_lifecycle_is_file_like_and_uses_control_headers() -> None:
    import vercel.blob as blob

    store = Store()
    async with session(
        service_options=[options()],
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(store.async_handler)
        ),
    ):
        operation = blob.open("folder/file.bin", "wb", cache_control_max_age=120)
        assert store.requests == []
        async with operation as writer:
            with pytest.raises(TypeError, match="bytes-like"):
                await writer.write("not bytes")
            assert not writer.closed
            assert writer.mode == "wb"
            assert await writer.write(b"abcd") == 4
            assert writer.tell() == 4
            assert "folder/file.bin" not in store.objects
        assert store.objects["folder/file.bin"] == b"abcd"
        with pytest.raises(ValueError, match="closed"):
            await writer.write(b"late")

        result = await blob.stat("folder/file.bin")
        assert (result.pathname, result.size) == ("folder/file.bin", 4)
        with pytest.raises(FrozenInstanceError):
            result.size = 5  # type: ignore[misc]

        read_operation = blob.open("folder/file.bin", "rb")
        request_count = len(store.requests)
        reader = await read_operation
        assert len(store.requests) > request_count
        async with reader:
            target = bytearray(2)
            assert reader.mode == "rb"
            assert await reader.readinto(target) == 2
            assert target == b"ab"
            assert await reader.readline() == b"cd"
            assert reader.tell() == 4
        with pytest.raises(RuntimeError, match="single-use"):
            await read_operation

        with pytest.raises(RuntimeError, match="abort"):
            async with blob.open("aborted.bin", "wb") as aborted:
                await aborted.write(b"discard")
                raise RuntimeError("abort")
        assert "aborted.bin" not in store.objects

        await blob.remove("folder/file.bin")
        await blob.remove("missing", missing_ok=True)
        with pytest.raises(FileNotFoundError):
            await blob.remove("missing")

    control = [request for request in store.requests if request.url.host == "blob.test"]
    assert all(request.headers["x-api-version"] == "12" for request in control)
    assert all(request.headers.get("x-vercel-blob-store-id") == "store" for request in control)
    put_request = next(request for request in control if request.method == "PUT")
    assert "x-content-type" not in put_request.headers
    assert put_request.headers["x-cache-control-max-age"] == "120"


@pytest.mark.anyio
async def test_async_text_handles_split_codepoints_and_newlines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import vercel.blob as blob
    import vercel.blob._internal.async_runtime as blob_async_runtime

    monkeypatch.setattr(blob_async_runtime.os, "linesep", "\r\n")
    store = Store()
    async with session(
        service_options=[options(read_buffer_size=2)],
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(store.async_handler)
        ),
    ):
        async with blob.open("text.txt", "w", encoding="utf-8", newline=None) as writer:
            assert await writer.write("a€\nb") == 4
            assert writer.tell() == 7
        assert store.objects["text.txt"] == "a€\r\nb".encode()
        async with blob.open("text.txt", encoding="utf-8", newline=None) as reader:
            assert await reader.readline() == "a€\n"
            assert reader.tell() == 6
            assert await reader.read() == "b"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("access", "expected_authorization"),
    [("public", None), ("private", "Bearer secret")],
)
async def test_delivery_auth_is_scoped_to_object_access(
    access: str,
    expected_authorization: str | None,
) -> None:
    import vercel.blob as blob

    delivery_requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "blob.test":
            metadata = payload("object", b"data")
            metadata["url"] = "https://delivery.example/object"
            return httpx.Response(200, json=metadata)
        delivery_requests.append(request)
        return httpx.Response(
            206,
            content=b"data",
            headers={"etag": '"v1"', "content-range": "bytes 0-3/4"},
        )

    async def credentials() -> BlobCredentials:
        return BlobCredentials("secret", "store", "oidc")

    configured = BlobServiceOptions(base_url=BASE_URL, credentials_factory=credentials)
    async with session(
        service_options=[configured],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        async with blob.open("object", "rb", access=access) as reader:  # type: ignore[call-overload]
            assert await reader.read() == b"data"

    headers = delivery_requests[0].headers
    assert headers.get("authorization") == expected_authorization
    assert "x-api-version" not in headers
    assert "x-vercel-blob-store-id" not in headers


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("status", "headers", "error_type"),
    [
        (200, {"etag": '"v1"', "content-range": "bytes 0-3/4"}, "BlobStreamError"),
        (206, {"content-range": "bytes 0-3/4"}, "BlobPreconditionFailedError"),
        (
            206,
            {"etag": '"v2"', "content-range": "bytes 0-3/4"},
            "BlobPreconditionFailedError",
        ),
        (206, {"etag": '"v1"', "content-range": "bytes 1-3/4"}, "BlobStreamError"),
    ],
)
async def test_range_reads_reject_unpinned_or_malformed_responses(
    status: int,
    headers: dict[str, str],
    error_type: str,
) -> None:
    import vercel.blob as blob

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "blob.test":
            return httpx.Response(200, json=payload("object", b"data"))
        return httpx.Response(status, headers=headers, content=b"data")

    async with session(
        service_options=[options()],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        with pytest.raises(getattr(blob, error_type)):
            await blob.open("object", "rb")


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("status", "code", "error_type", "retry_after"),
    [
        (404, "blob_not_found", "BlobNotFoundError", None),
        (404, "store_not_found", "BlobStoreNotFoundError", None),
        (429, "rate_limited", "BlobServiceRateLimited", "7"),
        (503, "service_unavailable", "BlobServiceNotAvailable", None),
    ],
)
async def test_control_errors_preserve_public_identity_and_response(
    status: int,
    code: str,
    error_type: str,
    retry_after: str | None,
) -> None:
    import vercel.blob as blob

    async def handler(request: httpx.Request) -> httpx.Response:
        headers = {"retry-after": retry_after} if retry_after is not None else {}
        return httpx.Response(status, headers=headers, json={"error": {"code": code}})

    async with session(
        service_options=[options()],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        with pytest.raises(getattr(blob, error_type)) as caught:
            await blob.stat("object")
    assert caught.value.response.status_code == status
    if retry_after is not None:
        assert caught.value.retry_after == 7


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("message", "error_type", "expected"),
    [
        (
            'contentType "text/plain" is not allowed',
            "BlobContentTypeNotAllowedError",
            'Vercel Blob: Content type mismatch, contentType "text/plain" is not allowed.',
        ),
        (
            '"pathname" does not match the token payload',
            "BlobPathnameMismatchError",
            'Vercel Blob: Pathname mismatch, "pathname" does not match the token payload. '
            "Check the pathname used in upload() or put() matches the one from the client token.",
        ),
        (
            "the file length cannot be greater than 1 MB",
            "BlobFileTooLargeError",
            "Vercel Blob: File is too large, the file length cannot be greater than 1 MB.",
        ),
    ],
)
async def test_put_errors_preserve_public_identity_and_message(
    message: str,
    error_type: str,
    expected: str,
) -> None:
    import vercel.blob as blob

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": {"message": message}})

    async with session(
        service_options=[options()],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        writer = await blob.open("object", "wb")
        await writer.write(b"data")
        with pytest.raises(getattr(blob, error_type)) as caught:
            await writer.close()
    assert str(caught.value) == expected


@pytest.mark.anyio
async def test_remove_missing_ok_only_suppresses_missing_object() -> None:
    import vercel.blob as blob

    code = "blob_not_found"

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": {"code": code}})

    async with session(
        service_options=[options()],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        await blob.remove("missing", missing_ok=True)
        code = "store_not_found"
        with pytest.raises(blob.BlobStoreNotFoundError):
            await blob.remove("missing", missing_ok=True)


@pytest.mark.anyio
async def test_protocol_and_transport_failures_are_public_blob_errors() -> None:
    import vercel.blob as blob

    async def malformed(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not-json")

    async with session(
        service_options=[options()],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(malformed)),
    ):
        with pytest.raises(blob.BlobStreamError):
            await blob.stat("object")

    async def broken(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("offline", request=request)

    async with session(
        service_options=[options()],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(broken)),
    ):
        with pytest.raises(blob.BlobUnknownError) as caught:
            await blob.stat("object")
    assert isinstance(caught.value.__cause__, httpx.ConnectError)


@pytest.mark.anyio
async def test_async_publication_failure_closes_and_breaks_writer() -> None:
    import vercel.blob as blob

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": {"code": "service_unavailable"}})

    async with session(
        service_options=[options()],
        httpx_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    ):
        writer = await blob.open("object", "wb")
        await writer.write(b"data")
        with pytest.raises(blob.BlobServiceNotAvailable) as caught:
            await writer.close()
        assert writer.closed
        with pytest.raises(blob.BlobServiceNotAvailable) as repeated:
            await writer.close()
        assert repeated.value is caught.value


def test_sync_lifecycle_supports_stdlib_text_and_binary_io() -> None:
    import vercel.blob.sync as blob

    store = Store()
    with session(
        service_options=[sync_options()],
        httpx_client_factory=lambda: httpx.Client(
            transport=httpx.MockTransport(store.sync_handler)
        ),
    ):
        with blob.open("rows.csv", "w", newline="") as writer:
            writer.write("name,value\none,1\n")
        with blob.open("rows.csv", "r", newline="") as reader:
            assert list(csv.reader(reader)) == [["name", "value"], ["one", "1"]]

        with blob.open("copy.bin", "wb") as writer:
            writer.write(b"binary")
        with blob.open("copy.bin", "rb") as reader:
            destination = io.BytesIO()
            destination.write(reader.read())
            assert destination.getvalue() == b"binary"

        with pytest.raises(RuntimeError, match="abort"):
            with blob.open("aborted.bin", "wb") as aborted:
                aborted.write(b"discard")
                raise RuntimeError("abort")
        assert aborted.closed
        assert "aborted.bin" not in store.objects
        blob.remove("copy.bin")


def test_sync_publication_failure_closes_and_breaks_writer() -> None:
    import vercel.blob.sync as blob

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": {"code": "service_unavailable"}})

    with session(
        service_options=[sync_options()],
        httpx_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    ):
        writer = blob.open("failed.bin", "wb")
        writer.write(b"data")
        with pytest.raises(blob.BlobServiceNotAvailable) as caught:
            writer.close()
        assert writer.closed
        with pytest.raises(blob.BlobServiceNotAvailable) as repeated:
            writer.close()
        assert repeated.value is caught.value
        with pytest.raises(ValueError, match="closed"):
            writer.flush()
