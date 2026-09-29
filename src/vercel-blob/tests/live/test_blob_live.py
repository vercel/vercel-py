"""Opt-in round trips against a real Blob store."""

from __future__ import annotations

import os
from uuid import uuid4

import pytest

from vercel import blob
from vercel.api import session

pytestmark = pytest.mark.live
_PAYLOADS = [b"", bytes(range(256)) * 1024]


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", _PAYLOADS, ids=["empty", "multi-chunk"])
async def test_async_lifecycle(payload: bytes) -> None:
    access: blob.Access = "private" if os.getenv("BLOB_TEST_ACCESS") == "private" else "public"
    pathname = f"vercel-py-live/{uuid4().hex}/async.bin"
    async with session():
        uploaded = await blob.put(pathname, payload, access=access)
        try:
            received = bytearray()
            async with blob.get(uploaded.url, access=access) as download:
                async for chunk in download:
                    received.extend(chunk)
            assert received == payload
            metadata = await blob.head(uploaded.url)
            assert metadata.size == len(payload)
            assert metadata.pathname == uploaded.pathname
        finally:
            await blob.delete(uploaded.url)
        await blob.delete(uploaded.url)


@pytest.mark.parametrize("payload", _PAYLOADS, ids=["empty", "multi-chunk"])
def test_sync_lifecycle(payload: bytes) -> None:
    access: blob.Access = "private" if os.getenv("BLOB_TEST_ACCESS") == "private" else "public"
    pathname = f"vercel-py-live/{uuid4().hex}/sync.bin"
    with session():
        uploaded = blob.sync.put(pathname, payload, access=access)
        try:
            received = bytearray()
            with blob.sync.get(uploaded.url, access=access) as download:
                for chunk in download:
                    received.extend(chunk)
            assert received == payload
            metadata = blob.sync.head(uploaded.url)
            assert metadata.size == len(payload)
            assert metadata.pathname == uploaded.pathname
        finally:
            blob.sync.delete(uploaded.url)
        blob.sync.delete(uploaded.url)
