"""Tests for per-request timeout handling in HTTP transports."""

from __future__ import annotations

from datetime import timedelta

import httpx
import pytest
import respx
from httpx import Response

from vercel.internal.core.http import AsyncTransport, SyncTransport
from vercel.internal.core.iter_coroutine import iter_coroutine

CLIENT_TIMEOUT = {
    "connect": 1.0,
    "read": 2.0,
    "write": 3.0,
    "pool": 4.0,
}
REQUEST_TIMEOUT = {
    "connect": 9.0,
    "read": 9.0,
    "write": 9.0,
    "pool": 9.0,
}


def _client_timeout() -> httpx.Timeout:
    return httpx.Timeout(
        connect=CLIENT_TIMEOUT["connect"],
        read=CLIENT_TIMEOUT["read"],
        write=CLIENT_TIMEOUT["write"],
        pool=CLIENT_TIMEOUT["pool"],
    )


@respx.mock
def test_sync_request_without_timeout_uses_client_default() -> None:
    base_url = "https://api.example.com"
    route = respx.get(f"{base_url}/ping").mock(return_value=Response(200, json={"ok": True}))
    transport = SyncTransport(httpx.Client(base_url=base_url, timeout=_client_timeout()))

    try:
        response = iter_coroutine(transport.send("GET", "/ping"))
        assert response.status_code == 200
    finally:
        transport.close()

    assert route.calls.last.request.extensions["timeout"] == CLIENT_TIMEOUT


@respx.mock
def test_sync_request_timeout_overrides_client_default() -> None:
    base_url = "https://api.example.com"
    route = respx.get(f"{base_url}/ping").mock(return_value=Response(200, json={"ok": True}))
    transport = SyncTransport(httpx.Client(base_url=base_url, timeout=_client_timeout()))

    try:
        response = iter_coroutine(transport.send("GET", "/ping", timeout=timedelta(seconds=9)))
        assert response.status_code == 200
    finally:
        transport.close()

    assert route.calls.last.request.extensions["timeout"] == REQUEST_TIMEOUT


@respx.mock
@pytest.mark.asyncio
async def test_async_request_without_timeout_uses_client_default() -> None:
    base_url = "https://api.example.com"
    route = respx.get(f"{base_url}/ping").mock(return_value=Response(200, json={"ok": True}))
    transport = AsyncTransport(httpx.AsyncClient(base_url=base_url, timeout=_client_timeout()))

    try:
        response = await transport.send("GET", "/ping")
        assert response.status_code == 200
    finally:
        await transport.aclose()

    assert route.calls.last.request.extensions["timeout"] == CLIENT_TIMEOUT


@respx.mock
@pytest.mark.asyncio
async def test_async_request_timeout_overrides_client_default() -> None:
    base_url = "https://api.example.com"
    route = respx.get(f"{base_url}/ping").mock(return_value=Response(200, json={"ok": True}))
    transport = AsyncTransport(httpx.AsyncClient(base_url=base_url, timeout=_client_timeout()))

    try:
        response = await transport.send("GET", "/ping", timeout=timedelta(seconds=9))
        assert response.status_code == 200
    finally:
        await transport.aclose()

    assert route.calls.last.request.extensions["timeout"] == REQUEST_TIMEOUT
