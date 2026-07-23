"""Tests for response-reading policies in HTTP transports."""

from collections.abc import AsyncIterator, Iterator

import httpx
import pytest

from vercel.internal.core.http import AsyncTransport, ReadResponsePolicy, SyncTransport
from vercel.internal.core.iter_coroutine import iter_coroutine

PAYLOAD = b"response-body"


class _SyncStream(httpx.SyncByteStream):
    def __iter__(self) -> Iterator[bytes]:
        yield PAYLOAD


class _AsyncStream(httpx.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield PAYLOAD


@pytest.mark.parametrize(
    ("read_response", "status_code", "expected_consumed", "expected_closed"),
    [
        (None, 200, False, False),
        (ReadResponsePolicy.ALWAYS, 200, True, True),
        (ReadResponsePolicy.NON_SUCCESS_ONLY, 200, False, False),
        (ReadResponsePolicy.NON_SUCCESS_ONLY, 400, True, True),
    ],
)
def test_sync_transport_read_response_policy(
    read_response: ReadResponsePolicy | None,
    status_code: int,
    expected_consumed: bool,
    expected_closed: bool,
) -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(status_code, stream=_SyncStream())
        )
    )
    transport = SyncTransport(client)
    try:
        if read_response is None:
            response = iter_coroutine(transport.send("GET", "https://example.com", stream=True))
        else:
            response = iter_coroutine(
                transport.send(
                    "GET", "https://example.com", stream=True, read_response=read_response
                )
            )
        assert response.is_stream_consumed is expected_consumed
        assert response.is_closed is expected_closed
        if expected_consumed:
            assert response.content == PAYLOAD
    finally:
        transport.close()


@pytest.mark.parametrize(
    ("read_response", "status_code", "expected_consumed", "expected_closed"),
    [
        (None, 200, False, False),
        (ReadResponsePolicy.ALWAYS, 200, True, True),
        (ReadResponsePolicy.NON_SUCCESS_ONLY, 200, False, False),
        (ReadResponsePolicy.NON_SUCCESS_ONLY, 400, True, True),
    ],
)
async def test_async_transport_read_response_policy(
    read_response: ReadResponsePolicy | None,
    status_code: int,
    expected_consumed: bool,
    expected_closed: bool,
) -> None:
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(status_code, stream=_AsyncStream())
        )
    )
    transport = AsyncTransport(client)
    try:
        if read_response is None:
            response = await transport.send("GET", "https://example.com", stream=True)
        else:
            response = await transport.send(
                "GET", "https://example.com", stream=True, read_response=read_response
            )
        assert response.is_stream_consumed is expected_consumed
        assert response.is_closed is expected_closed
        if expected_consumed:
            assert response.content == PAYLOAD
    finally:
        await transport.aclose()
