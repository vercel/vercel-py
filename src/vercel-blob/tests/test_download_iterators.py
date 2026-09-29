"""Download consumers obey Python's iterator protocol without permitting rereads."""

import httpx2 as httpx
import pytest

from vercel import blob
from vercel.api import session

_URL = "https://localstore.public.blob.vercel-storage.com/iterator.bin"


def test_sync_join_accepts_download() -> None:
    with session(
        httpx_client_factory=lambda: httpx.Client(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"payload"))
        )
    ):
        with blob.sync.get(_URL, access="public") as download:
            assert b"".join(download) == b"payload"
            assert download.is_closed
            with pytest.raises(blob.BlobStreamError):
                iter(download)


@pytest.mark.asyncio
async def test_async_iterator_can_be_used_in_async_for() -> None:
    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"payload"))
        )
    ):
        async with blob.get(_URL, access="public") as download:
            iterator = aiter(download)
            assert aiter(iterator) is iterator
            assert [chunk async for chunk in iterator] == [b"payload"]
            assert download.is_closed
            with pytest.raises(blob.BlobStreamError):
                aiter(download)
