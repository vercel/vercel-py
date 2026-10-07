"""Opt-in round trips against real Blob stores."""

from __future__ import annotations

from uuid import uuid4

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
@pytest.mark.parametrize("payload", _PAYLOADS, ids=["empty", "multi-chunk"])
async def test_async_lifecycle(store: LiveStore, payload: bytes) -> None:
    async with session():
        uploaded = await blob.put(_pathname("async.bin"), payload, access=store.access)
        try:
            result = await blob.get(uploaded.url, access=store.access)
            assert result.body == payload
            assert result.metadata.url == uploaded.url
            assert result.metadata.content_type == uploaded.content_type
            received = bytearray()
            async with blob.stream(uploaded.url, access=store.access) as download:
                async for chunk in download:
                    assert 0 < len(chunk) <= _CHUNK_SIZE
                    received.extend(chunk)
            assert received == payload
            assert download.is_closed
            metadata = await blob.head(uploaded.url)
            assert metadata.url == uploaded.url
            assert metadata.pathname == uploaded.pathname
            assert metadata.size == len(payload)
            assert metadata.etag == uploaded.etag
        finally:
            await blob.delete(uploaded.url)
        await blob.delete(uploaded.url)
        with pytest.raises(blob.BlobNotFoundError):
            await blob.head(uploaded.url)
        with pytest.raises(blob.BlobNotFoundError):
            await blob.get(uploaded.url, access=store.access)
        with pytest.raises(blob.BlobNotFoundError):
            async with blob.stream(uploaded.url, access=store.access):
                pass


@pytest.mark.parametrize("payload", _PAYLOADS, ids=["empty", "multi-chunk"])
def test_sync_lifecycle(store: LiveStore, payload: bytes) -> None:
    with session():
        uploaded = blob.sync.put(_pathname("sync.bin"), payload, access=store.access)
        try:
            result = blob.sync.get(uploaded.url, access=store.access)
            assert result.body == payload
            assert result.metadata.url == uploaded.url
            assert result.metadata.content_type == uploaded.content_type
            received = bytearray()
            with blob.sync.stream(uploaded.url, access=store.access) as download:
                for chunk in download:
                    assert 0 < len(chunk) <= _CHUNK_SIZE
                    received.extend(chunk)
            assert received == payload
            assert download.is_closed
            metadata = blob.sync.head(uploaded.url)
            assert metadata.url == uploaded.url
            assert metadata.pathname == uploaded.pathname
            assert metadata.size == len(payload)
            assert metadata.etag == uploaded.etag
        finally:
            blob.sync.delete(uploaded.url)
        blob.sync.delete(uploaded.url)
        with pytest.raises(blob.BlobNotFoundError):
            blob.sync.head(uploaded.url)
        with pytest.raises(blob.BlobNotFoundError):
            blob.sync.get(uploaded.url, access=store.access)
        with pytest.raises(blob.BlobNotFoundError):
            with blob.sync.stream(uploaded.url, access=store.access):
                pass


def test_encoded_pathname_and_metadata(store: LiveStore) -> None:
    pathname = _pathname("space + ?% café.txt")
    content_type = "text/plain; charset=utf-8"
    with session():
        uploaded = blob.sync.put(
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

            metadata = blob.sync.head(pathname)
            assert metadata.url == uploaded.url
            assert metadata.content_type == content_type
            assert "max-age=120" in metadata.cache_control

            for target in (pathname, uploaded.url, uploaded.download_url):
                result = blob.sync.get(target, access=store.access)
                assert result.body == b"hello"
                assert result.metadata.content_type == content_type
                assert result.metadata.etag == uploaded.etag
                assert result.metadata.size == 5
        finally:
            blob.sync.delete(pathname)


def test_overwrite_policy(store: LiveStore) -> None:
    pathname = _pathname("overwrite.txt")
    with session():
        first = blob.sync.put(pathname, b"first", access=store.access, add_random_suffix=False)
        try:
            with pytest.raises(blob.BlobError) as error:
                blob.sync.put(pathname, b"second", access=store.access, add_random_suffix=False)
            assert error.value.status_code == 400

            second = blob.sync.put(
                pathname,
                b"second",
                access=store.access,
                add_random_suffix=False,
                allow_overwrite=True,
            )
            assert second.url == first.url
            assert second.etag != first.etag
            assert blob.sync.get(pathname, access=store.access).body == b"second"
        finally:
            blob.sync.delete(pathname)


def test_store_rejects_other_access(store: LiveStore) -> None:
    other: blob.Access = "private" if store.access == "public" else "public"
    pathname = _pathname("wrong-access.txt")
    with session():
        try:
            with pytest.raises(blob.BlobError) as error:
                blob.sync.put(pathname, b"x", access=other, add_random_suffix=False)
            assert error.value.status_code == 400
            with pytest.raises(blob.BlobNotFoundError):
                blob.sync.head(pathname)
        finally:
            blob.sync.delete(pathname)


def test_unauthenticated_delivery(store: LiveStore) -> None:
    with session():
        uploaded = blob.sync.put(_pathname("delivery.txt"), b"hello", access=store.access)
        try:
            response = httpx.get(uploaded.url, trust_env=False)
        finally:
            blob.sync.delete(uploaded.url)
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
        uploaded = await blob.put(_pathname("early-exit.bin"), payload, access=store.access)
        try:
            for _ in range(3):
                async with blob.stream(uploaded.url, access=store.access) as download:
                    async for chunk in download:
                        assert chunk == payload[: len(chunk)]
                        break
                assert download.is_closed
            metadata = await blob.head(uploaded.url)
            assert metadata.size == len(payload)
        finally:
            await blob.delete(uploaded.url)
