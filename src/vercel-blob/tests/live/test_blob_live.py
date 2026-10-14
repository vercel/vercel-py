"""Opt-in round trips against real Blob stores."""

from __future__ import annotations

import io
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import anyio
import httpx2 as httpx
import pytest

from vercel import blob
from vercel.api import session

from .conftest import LiveStore

pytestmark = pytest.mark.live
_PAYLOADS = [b"", bytes(range(256)) * 1024]
_CHUNK_SIZE = 64 * 1024


def _pathname(name: str) -> str:
    return f"vercel-py-live/{uuid4().hex}/{name}"


@pytest.mark.asyncio
async def test_async_multipart(store: LiveStore) -> None:
    async def part() -> AsyncIterator[bytes]:
        for _ in range(80):
            yield b"m" * _CHUNK_SIZE

    async with session():
        async with blob.multipart_upload_blob(
            _pathname("async-multipart.bin"), access=store.access
        ) as upload:
            await upload.upload_part(part(), content_length=5 * 1024 * 1024)
            await upload.upload_part(part(), content_length=5 * 1024 * 1024)
            await upload.upload_part(b"tail")
        try:
            assert (await blob.get_blob(upload.result.url, access=store.access)).body == (
                b"m" * (10 * 1024 * 1024) + b"tail"
            )
            assert (await blob.head_blob(upload.result.url)).etag == upload.result.etag
        finally:
            await blob.delete_blob(upload.result.url)


def test_sync_multipart(store: LiveStore) -> None:
    with session():
        with blob.sync.multipart_upload_blob(
            _pathname("sync-multipart.bin"), access=store.access
        ) as upload:
            for _ in range(2):
                upload.upload_part(
                    (b"m" * _CHUNK_SIZE for _ in range(80)), content_length=5 * 1024 * 1024
                )
            upload.upload_part(b"tail")
        try:
            assert blob.sync.get_blob(upload.result.url, access=store.access).body == (
                b"m" * (10 * 1024 * 1024) + b"tail"
            )
            assert blob.sync.head_blob(upload.result.url).etag == upload.result.etag
        finally:
            blob.sync.delete_blob(upload.result.url)


@pytest.mark.asyncio
async def test_async_failed_multipart_does_not_publish(store: LiveStore) -> None:
    pathname = _pathname("async-failed-multipart.bin")
    async with session():
        try:
            with pytest.raises(ValueError, match="stop"):
                async with blob.multipart_upload_blob(pathname, access=store.access) as upload:
                    await upload.upload_part(b"prefix")
                    raise ValueError("stop")
            with pytest.raises(blob.BlobNotFoundError):
                await blob.head_blob(pathname)
        finally:
            await blob.delete_blob(pathname)


def test_sync_failed_multipart_does_not_publish(store: LiveStore) -> None:
    pathname = _pathname("sync-failed-multipart.bin")
    with session():
        try:
            with pytest.raises(ValueError, match="stop"):
                with blob.sync.multipart_upload_blob(pathname, access=store.access) as upload:
                    upload.upload_part(b"prefix")
                    raise ValueError("stop")
            with pytest.raises(blob.BlobNotFoundError):
                blob.sync.head_blob(pathname)
        finally:
            blob.sync.delete_blob(pathname)


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", _PAYLOADS, ids=["empty", "multi-chunk"])
async def test_async_lifecycle(store: LiveStore, payload: bytes) -> None:
    async with session():
        uploaded = await blob.put_blob(_pathname("async.bin"), payload, access=store.access)
        try:
            result = await blob.get_blob(uploaded.url, access=store.access)
            assert result.body == payload
            assert result.metadata.url == uploaded.url
            assert result.metadata.content_type == uploaded.content_type
            received = bytearray()
            async with blob.stream_blob(uploaded.url, access=store.access) as download:
                async for chunk in download:
                    assert 0 < len(chunk) <= _CHUNK_SIZE
                    received.extend(chunk)
            assert received == payload
            assert download.is_closed
            metadata = await blob.head_blob(uploaded.url)
            assert metadata.url == uploaded.url
            assert metadata.pathname == uploaded.pathname
            assert metadata.size == len(payload)
            assert metadata.etag == uploaded.etag
        finally:
            await blob.delete_blob(uploaded.url)
        await blob.delete_blob(uploaded.url)
        with pytest.raises(blob.BlobNotFoundError):
            await blob.head_blob(uploaded.url)
        with pytest.raises(blob.BlobNotFoundError):
            await blob.get_blob(uploaded.url, access=store.access)
        with pytest.raises(blob.BlobNotFoundError):
            async with blob.stream_blob(uploaded.url, access=store.access):
                pass


@pytest.mark.parametrize("payload", _PAYLOADS, ids=["empty", "multi-chunk"])
def test_sync_lifecycle(store: LiveStore, payload: bytes) -> None:
    with session():
        uploaded = blob.sync.put_blob(_pathname("sync.bin"), payload, access=store.access)
        try:
            result = blob.sync.get_blob(uploaded.url, access=store.access)
            assert result.body == payload
            assert result.metadata.url == uploaded.url
            assert result.metadata.content_type == uploaded.content_type
            received = bytearray()
            with blob.sync.stream_blob(uploaded.url, access=store.access) as download:
                for chunk in download:
                    assert 0 < len(chunk) <= _CHUNK_SIZE
                    received.extend(chunk)
            assert received == payload
            assert download.is_closed
            metadata = blob.sync.head_blob(uploaded.url)
            assert metadata.url == uploaded.url
            assert metadata.pathname == uploaded.pathname
            assert metadata.size == len(payload)
            assert metadata.etag == uploaded.etag
        finally:
            blob.sync.delete_blob(uploaded.url)
        blob.sync.delete_blob(uploaded.url)
        with pytest.raises(blob.BlobNotFoundError):
            blob.sync.head_blob(uploaded.url)
        with pytest.raises(blob.BlobNotFoundError):
            blob.sync.get_blob(uploaded.url, access=store.access)
        with pytest.raises(blob.BlobNotFoundError):
            with blob.sync.stream_blob(uploaded.url, access=store.access):
                pass


def test_encoded_pathname_and_metadata(store: LiveStore) -> None:
    pathname = _pathname("space + ?% café.txt")
    content_type = "text/plain; charset=utf-8"
    with session():
        uploaded = blob.sync.put_blob(
            pathname,
            b"hello",
            access=store.access,
            content_type=content_type,
            add_random_suffix=False,
            cache_control_max_age=120,
        )
        try:
            assert uploaded.pathname == pathname
            assert uploaded.content_type == content_type
            assert uploaded.content_disposition.startswith("attachment")

            metadata = blob.sync.head_blob(pathname)
            assert metadata.url == uploaded.url
            assert metadata.content_type == content_type
            assert "max-age=120" in metadata.cache_control

            for target in (pathname, uploaded.url, uploaded.download_url):
                result = blob.sync.get_blob(target, access=store.access)
                assert result.body == b"hello"
                assert result.metadata.content_type == content_type
                assert result.metadata.etag == uploaded.etag
                assert result.metadata.size == 5
        finally:
            blob.sync.delete_blob(pathname)


def test_overwrite_policy(store: LiveStore) -> None:
    pathname = _pathname("overwrite.txt")
    with session():
        first = blob.sync.put_blob(pathname, b"first", access=store.access, add_random_suffix=False)
        try:
            with pytest.raises(blob.BlobError) as error:
                blob.sync.put_blob(
                    pathname, b"second", access=store.access, add_random_suffix=False
                )
            assert error.value.status_code == 400

            second = blob.sync.put_blob(
                pathname,
                b"second",
                access=store.access,
                add_random_suffix=False,
                allow_overwrite=True,
            )
            assert second.url == first.url
            assert second.etag != first.etag
            assert blob.sync.get_blob(pathname, access=store.access).body == b"second"
        finally:
            blob.sync.delete_blob(pathname)


def test_store_rejects_other_access(store: LiveStore) -> None:
    other: blob.Access = "private" if store.access == "public" else "public"
    pathname = _pathname("wrong-access.txt")
    with session():
        try:
            with pytest.raises(blob.BlobError) as error:
                blob.sync.put_blob(pathname, b"x", access=other, add_random_suffix=False)
            assert error.value.status_code == 400
            with pytest.raises(blob.BlobNotFoundError):
                blob.sync.head_blob(pathname)
        finally:
            blob.sync.delete_blob(pathname)


def test_unauthenticated_delivery(store: LiveStore) -> None:
    with session():
        uploaded = blob.sync.put_blob(_pathname("delivery.txt"), b"hello", access=store.access)
        try:
            response = httpx.get(uploaded.url, trust_env=False)
        finally:
            blob.sync.delete_blob(uploaded.url)
    if store.access == "public":
        assert response.status_code == 200
        assert response.content == b"hello"
    else:
        assert response.status_code in (401, 403)
        assert response.content != b"hello"


@pytest.mark.asyncio
async def test_early_exit_closes_download(store: LiveStore) -> None:
    payload = bytes(range(256)) * 4096
    async with session():
        uploaded = await blob.put_blob(_pathname("early-exit.bin"), payload, access=store.access)
        try:
            for _ in range(3):
                async with blob.stream_blob(uploaded.url, access=store.access) as download:
                    async for chunk in download:
                        assert chunk == payload[: len(chunk)]
                        break
                assert download.is_closed
            metadata = await blob.head_blob(uploaded.url)
            assert metadata.size == len(payload)
        finally:
            await blob.delete_blob(uploaded.url)


@pytest.mark.asyncio
async def test_async_streaming_round_trip(store: LiveStore, tmp_path: Path) -> None:
    payload = bytes(range(256)) * 1024
    pathname_reader = _pathname("async-streaming-reader.bin")
    pathname_iter = _pathname("async-streaming-iter.bin")
    path = tmp_path / "payload.bin"
    path.write_bytes(payload)

    async with session():
        async with await anyio.open_file(path, "rb") as file:
            uploaded_reader = await blob.put_blob(
                pathname_reader, file, access=store.access, content_length=len(payload)
            )
        try:
            result = await blob.get_blob(uploaded_reader.url, access=store.access)
            assert result.body == payload

            size = (await blob.head_blob(uploaded_reader.url)).size
            async with blob.stream_blob(uploaded_reader.url, access=store.access) as download:
                uploaded_copy = await blob.put_blob(
                    _pathname("async-streaming-copy.bin"),
                    download,
                    access=store.access,
                    content_length=size,
                )
            try:
                result = await blob.get_blob(uploaded_copy.url, access=store.access)
                assert result.body == payload
            finally:
                await blob.delete_blob(uploaded_copy.url)
        finally:
            await blob.delete_blob(uploaded_reader.url)

        async def chunks() -> AsyncIterator[bytes]:
            chunk_size = 64 * 1024
            for offset in range(0, len(payload), chunk_size):
                yield payload[offset : offset + chunk_size]

        uploaded_iter = await blob.put_blob(
            pathname_iter, chunks(), access=store.access, content_length=len(payload)
        )
        try:
            result = await blob.get_blob(uploaded_iter.url, access=store.access)
            assert result.body == payload
        finally:
            await blob.delete_blob(uploaded_iter.url)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tag,data,declared",
    [
        ("short", b"abc", 10),
        ("oversized", b"x" * 11, 10),
        ("zero-with-data", b"extra", 0),
    ],
)
async def test_async_streaming_length_mismatch_creates_no_object(
    store: LiveStore, tag: str, data: bytes, declared: int
) -> None:
    pathname = _pathname(f"async-bad-len-{tag}.bin")
    async with session():
        try:
            with pytest.raises(blob.BlobContentLengthError):
                await blob.put_blob(
                    pathname,
                    anyio.wrap_file(io.BytesIO(data)),
                    access=store.access,
                    content_length=declared,
                )
            with pytest.raises(blob.BlobNotFoundError):
                await blob.head_blob(pathname)
        finally:
            await blob.delete_blob(pathname)


def test_sync_streaming_round_trip(store: LiveStore) -> None:
    payload = bytes(range(256)) * 1024
    pathname_reader = _pathname("sync-streaming-reader.bin")
    pathname_iter = _pathname("sync-streaming-iter.bin")

    with session():
        reader = io.BytesIO(payload)
        uploaded_reader = blob.sync.put_blob(
            pathname_reader, reader, access=store.access, content_length=len(payload)
        )
        try:
            result = blob.sync.get_blob(uploaded_reader.url, access=store.access)
            assert result.body == payload

            size = blob.sync.head_blob(uploaded_reader.url).size
            with blob.sync.stream_blob(uploaded_reader.url, access=store.access) as download:
                uploaded_copy = blob.sync.put_blob(
                    _pathname("sync-streaming-copy.bin"),
                    download,
                    access=store.access,
                    content_length=size,
                )
            try:
                result = blob.sync.get_blob(uploaded_copy.url, access=store.access)
                assert result.body == payload
            finally:
                blob.sync.delete_blob(uploaded_copy.url)
        finally:
            blob.sync.delete_blob(uploaded_reader.url)

        def chunks():
            chunk_size = 64 * 1024
            for offset in range(0, len(payload), chunk_size):
                yield payload[offset : offset + chunk_size]

        uploaded_iter = blob.sync.put_blob(
            pathname_iter, chunks(), access=store.access, content_length=len(payload)
        )
        try:
            result = blob.sync.get_blob(uploaded_iter.url, access=store.access)
            assert result.body == payload
        finally:
            blob.sync.delete_blob(uploaded_iter.url)


@pytest.mark.parametrize(
    "tag,data,declared",
    [
        ("short", b"abc", 10),
        ("oversized", b"x" * 11, 10),
        ("zero-with-data", b"extra", 0),
    ],
)
def test_sync_streaming_length_mismatch_creates_no_object(
    store: LiveStore, tag: str, data: bytes, declared: int
) -> None:
    pathname = _pathname(f"sync-bad-len-{tag}.bin")
    with session():
        try:
            with pytest.raises(blob.BlobContentLengthError):
                blob.sync.put_blob(
                    pathname, io.BytesIO(data), access=store.access, content_length=declared
                )
            with pytest.raises(blob.BlobNotFoundError):
                blob.sync.head_blob(pathname)
        finally:
            blob.sync.delete_blob(pathname)


@pytest.mark.asyncio
async def test_async_collections(store: LiveStore) -> None:
    prefix = _pathname("collections/")
    source, destination = prefix + "source.bin", prefix + "nested/copy.bin"
    async with session():
        try:
            original = await blob.put_blob(source, b"collection", access=store.access)
            with pytest.raises(blob.BlobPreconditionFailedError):
                await blob.copy_blob(source, destination, access=store.access, if_match='"wrong"')
            copied = await blob.copy_blob(
                original.url,
                destination,
                access=store.access,
                if_match=original.etag,
                content_type="application/octet-stream",
                cache_control_max_age=300,
            )
            assert (await blob.get_blob(copied.url, access=store.access)).body == b"collection"
            metadata = await blob.head_blob(destination)
            assert metadata.etag == copied.etag
            assert metadata.content_type == "application/octet-stream"
            assert metadata.cache_control == "public, max-age=300"
            paths = {item.pathname async for item in blob.query_blobs(prefix=prefix, page_size=1)}
            assert paths == {source, destination}
            entries = [item async for item in blob.query_blob_entries(prefix=prefix, page_size=1)]
            assert {item.pathname for item in entries if isinstance(item, blob.ListBlob)} == {
                source
            }
            folders = [item for item in entries if isinstance(item, blob.BlobPrefix)]
            assert folders == [blob.BlobPrefix(prefix=prefix + "nested/")]
            nested = [
                item
                async for item in blob.query_blob_entries(prefix=folders[0].prefix, page_size=1)
            ]
            assert len(nested) == 1
            assert isinstance(nested[0], blob.ListBlob)
            assert nested[0].pathname == destination
            with pytest.raises(blob.BlobPreconditionFailedError):
                await blob.delete_blob(source, if_match='"wrong"')
            await blob.delete_blob([source, destination])
            assert not [item async for item in blob.query_blobs(prefix=prefix)]
        finally:
            await blob.delete_blob([source, destination])


def test_sync_collections(store: LiveStore) -> None:
    prefix = _pathname("collections/")
    source, destination = prefix + "source.bin", prefix + "nested/copy.bin"
    with session():
        try:
            original = blob.sync.put_blob(source, b"collection", access=store.access)
            with pytest.raises(blob.BlobPreconditionFailedError):
                blob.sync.copy_blob(source, destination, access=store.access, if_match='"wrong"')
            copied = blob.sync.copy_blob(
                original.url,
                destination,
                access=store.access,
                if_match=original.etag,
                content_type="application/octet-stream",
                cache_control_max_age=300,
            )
            assert (blob.sync.get_blob(copied.url, access=store.access)).body == b"collection"
            metadata = blob.sync.head_blob(destination)
            assert metadata.etag == copied.etag
            assert metadata.content_type == "application/octet-stream"
            assert metadata.cache_control == "public, max-age=300"
            paths = {item.pathname for item in blob.sync.query_blobs(prefix=prefix, page_size=1)}
            assert paths == {source, destination}
            entries = list(blob.sync.query_blob_entries(prefix=prefix, page_size=1))
            assert {item.pathname for item in entries if isinstance(item, blob.ListBlob)} == {
                source
            }
            folders = [item for item in entries if isinstance(item, blob.BlobPrefix)]
            assert folders == [blob.BlobPrefix(prefix=prefix + "nested/")]
            nested = list(blob.sync.query_blob_entries(prefix=folders[0].prefix, page_size=1))
            assert len(nested) == 1
            assert isinstance(nested[0], blob.ListBlob)
            assert nested[0].pathname == destination
            with pytest.raises(blob.BlobPreconditionFailedError):
                blob.sync.delete_blob(source, if_match='"wrong"')
            blob.sync.delete_blob([source, destination])
            assert not list(blob.sync.query_blobs(prefix=prefix))
        finally:
            blob.sync.delete_blob([source, destination])
