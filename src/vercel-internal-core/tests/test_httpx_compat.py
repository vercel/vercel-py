"""Compatibility tests for explicitly supplied legacy HTTPX clients."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, cast

import httpx as legacy_httpx
import pytest

from vercel._internal.core.http import AsyncTransport, ReadResponsePolicy, SyncTransport
from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel._internal.core.session import get_active_session, get_active_sync_session
from vercel.api import session
from vercel.errors import VercelSessionError


def test_sync_transport_supports_legacy_httpx_client() -> None:
    uploads: list[bytes] = []
    timeouts: list[dict[str, float | None]] = []

    def handler(request: legacy_httpx.Request) -> legacy_httpx.Response:
        timeouts.append(request.extensions["timeout"])
        if request.url.path == "/redirect":
            return legacy_httpx.Response(302, headers={"location": "/target"})
        if request.url.path == "/upload":
            uploads.append(request.read())
            return legacy_httpx.Response(201, content=b"uploaded")
        return legacy_httpx.Response(200, content=b"downloaded")

    client = legacy_httpx.Client(
        base_url="https://example.test",
        follow_redirects=False,
        transport=legacy_httpx.MockTransport(handler),
    )
    transport = SyncTransport(cast(Any, client))
    try:
        response = iter_coroutine(transport.send("GET", "/redirect", timeout=timedelta(seconds=9)))
        assert isinstance(response, legacy_httpx.Response)
        assert response.status_code == 302
        assert response.history == []

        redirected = iter_coroutine(transport.send("GET", "/redirect", follow_redirects=True))
        assert redirected.status_code == 200
        assert len(redirected.history) == 1

        async def stream_requests() -> bytes:
            download = await transport.open_response_stream("GET", "/download")
            assert await download.read() == b"downloaded"

            async with transport.request_stream(
                "POST",
                "/upload",
                read_response=ReadResponsePolicy.ALWAYS,
            ) as upload:
                await upload.write(b"legacy ")
                await upload.write(b"upload")
                upload_response = await upload.finish()
                return await upload_response.read()

        assert iter_coroutine(stream_requests()) == b"uploaded"
    finally:
        transport.close()

    assert uploads == [b"legacy upload"]
    assert timeouts[0] == {"connect": 9.0, "read": 9.0, "write": 9.0, "pool": 9.0}
    assert client.is_closed


@pytest.mark.anyio
async def test_async_transport_supports_legacy_httpx_client() -> None:
    uploads: list[bytes] = []
    timeouts: list[dict[str, float | None]] = []

    async def handler(request: legacy_httpx.Request) -> legacy_httpx.Response:
        timeouts.append(request.extensions["timeout"])
        if request.url.path == "/redirect":
            return legacy_httpx.Response(302, headers={"location": "/target"})
        if request.url.path == "/upload":
            uploads.append(await request.aread())
            return legacy_httpx.Response(201, content=b"uploaded")
        return legacy_httpx.Response(200, content=b"downloaded")

    client = legacy_httpx.AsyncClient(
        base_url="https://example.test",
        follow_redirects=False,
        transport=legacy_httpx.MockTransport(handler),
    )
    transport = AsyncTransport(cast(Any, client))
    try:
        response = await transport.send(
            "GET",
            "/redirect",
            timeout=timedelta(seconds=9),
        )
        assert isinstance(response, legacy_httpx.Response)
        assert response.status_code == 302
        assert response.history == []

        redirected = await transport.send("GET", "/redirect", follow_redirects=True)
        assert redirected.status_code == 200
        assert len(redirected.history) == 1

        download = await transport.open_response_stream("GET", "/download")
        assert await download.read() == b"downloaded"

        async with transport.request_stream(
            "POST",
            "/upload",
            read_response=ReadResponsePolicy.ALWAYS,
        ) as upload:
            await upload.write(b"legacy ")
            await upload.write(b"upload")
            upload_response = await upload.finish()
            assert await upload_response.read() == b"uploaded"
    finally:
        await transport.aclose()

    assert uploads == [b"legacy upload"]
    assert timeouts[0] == {"connect": 9.0, "read": 9.0, "write": 9.0, "pool": 9.0}
    assert client.is_closed


def test_sync_session_rejects_and_closes_legacy_async_client() -> None:
    client = legacy_httpx.AsyncClient()

    with session(httpx_client_factory=cast(Any, lambda: client)):
        with pytest.raises(
            VercelSessionError,
            match=r"httpx2\.Client or httpx\.Client",
        ):
            get_active_sync_session().get_transport()

    assert client.is_closed


@pytest.mark.asyncio
async def test_async_session_rejects_and_closes_legacy_sync_client() -> None:
    client = legacy_httpx.Client()

    async with session(httpx_client_factory=cast(Any, lambda: client)):
        with pytest.raises(
            VercelSessionError,
            match=r"httpx2\.AsyncClient or httpx\.AsyncClient",
        ):
            get_active_session().get_transport()

    assert client.is_closed
