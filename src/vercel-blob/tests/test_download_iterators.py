"""Download invariants across arbitrary transport chunks and encoded pathnames."""

from collections.abc import AsyncIterator, Iterator
from urllib.parse import unquote, urlsplit

import httpx2 as httpx
import pytest
from hypothesis import example, given, settings, strategies as st

from vercel import blob
from vercel.api import session
from vercel.blob.sync import SyncBlobServiceOptions

_URL = "https://localstore.public.blob.vercel-storage.com/iterator.bin"
_CHUNKS = st.lists(st.binary(max_size=70 * 1024), max_size=6)


class _FragmentedStream(httpx.SyncByteStream, httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.closed = False
        self.yielded_count = 0

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self.chunks:
            self.yielded_count += 1
            yield chunk

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self:
            yield chunk

    def close(self) -> None:
        self.closed = True

    async def aclose(self) -> None:
        self.close()


@settings(max_examples=30, deadline=None)
@given(chunks=_CHUNKS)
@example(chunks=[])
@example(chunks=[b"a" * 65535, b"", b"b" * 65537])
def test_sync_download_preserves_transport_bytes(chunks: list[bytes]) -> None:
    stream = _FragmentedStream(chunks)
    with session(
        httpx_client_factory=lambda: httpx.Client(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=stream))
        )
    ):
        with blob.sync.stream(_URL, access="public") as download:
            assert stream.yielded_count == 0
            received = list(download)
            assert b"".join(received) == b"".join(chunks)
            assert all(0 < len(chunk) <= 64 * 1024 for chunk in received)
            assert stream.closed
            assert download.is_closed


@pytest.mark.anyio
@settings(max_examples=30, deadline=None)
@given(chunks=_CHUNKS)
@example(chunks=[])
@example(chunks=[b"a" * 65535, b"", b"b" * 65537])
async def test_async_download_preserves_transport_bytes(chunks: list[bytes]) -> None:
    stream = _FragmentedStream(chunks)
    async with session(
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=stream))
        )
    ):
        async with blob.stream(_URL, access="public") as download:
            assert stream.yielded_count == 0
            received = [chunk async for chunk in download]
            assert b"".join(received) == b"".join(chunks)
            assert all(0 < len(chunk) <= 64 * 1024 for chunk in received)
            assert stream.closed
            assert download.is_closed


@settings(max_examples=30, deadline=None)
@given(
    suffix=st.text(
        st.characters(blacklist_categories=("Cc", "Cs"), blacklist_characters="\x7f/"), max_size=40
    )
)
@example(suffix="space + ?% café.txt")
@example(suffix="%2F%20")
def test_download_pathname_encoding_roundtrips(suffix: str) -> None:
    pathname = f"folder/file-{suffix}"
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"")

    credentials = blob.BlobCredentials(
        token="vercel_blob_rw_localstore_secret", store_id="localstore"
    )
    with session(
        service_options=[SyncBlobServiceOptions(credentials_factory=lambda: credentials)],
        httpx_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    ):
        with blob.sync.stream(f"/{pathname}", access="public") as download:
            assert b"".join(download) == b""
    assert len(requests) == 1
    url = urlsplit(str(requests[0].url))
    assert url.hostname == "localstore.public.blob.vercel-storage.com"
    assert unquote(url.path) == f"/{pathname}"
    assert not url.query
    assert not url.fragment
    assert "authorization" not in requests[0].headers
