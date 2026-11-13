"""Tests for per-request timeout handling in HTTP transports."""

from __future__ import annotations

from datetime import timedelta

import httpx2 as httpx
import pytest
from httpx2 import Response

import vendor.respx as respx
from vercel._internal.core.http import NO_READ_TIMEOUT, NO_TIMEOUT, AsyncTransport, SyncTransport
from vercel._internal.core.iter_coroutine import iter_coroutine

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
DISABLED_TIMEOUT = {
    "connect": None,
    "read": None,
    "write": None,
    "pool": None,
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
def test_sync_request_can_disable_client_timeout() -> None:
    base_url = "https://api.example.com"
    route = respx.get(f"{base_url}/ping").mock(return_value=Response(200, json={"ok": True}))
    transport = SyncTransport(httpx.Client(base_url=base_url, timeout=_client_timeout()))

    try:
        response = iter_coroutine(transport.send("GET", "/ping", timeout=NO_TIMEOUT))
        assert response.status_code == 200
    finally:
        transport.close()

    assert route.calls.last.request.extensions["timeout"] == DISABLED_TIMEOUT


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


@respx.mock
@pytest.mark.asyncio
async def test_async_request_can_disable_client_timeout() -> None:
    base_url = "https://api.example.com"
    route = respx.get(f"{base_url}/ping").mock(return_value=Response(200, json={"ok": True}))
    transport = AsyncTransport(httpx.AsyncClient(base_url=base_url, timeout=_client_timeout()))

    try:
        response = await transport.send("GET", "/ping", timeout=NO_TIMEOUT)
        assert response.status_code == 200
    finally:
        await transport.aclose()

    assert route.calls.last.request.extensions["timeout"] == DISABLED_TIMEOUT


@pytest.mark.parametrize("sync", [False, True])
@respx.mock
async def test_disabling_read_timeout_preserves_other_client_limits(sync: bool) -> None:
    route = respx.get("https://api.example.com/ping").respond(200, json={"ok": True})
    if sync:
        with httpx.Client(timeout=_client_timeout()) as client:
            transport = SyncTransport(client)
            iter_coroutine(
                transport.send("GET", "https://api.example.com/ping", timeout=NO_READ_TIMEOUT)
            )
            iter_coroutine(transport.send("GET", "https://api.example.com/ping"))
    else:
        async with httpx.AsyncClient(timeout=_client_timeout()) as async_client:
            async_transport = AsyncTransport(async_client)
            await async_transport.send(
                "GET", "https://api.example.com/ping", timeout=NO_READ_TIMEOUT
            )
            await async_transport.send("GET", "https://api.example.com/ping")

    assert route.calls[0].request.extensions["timeout"] == {**CLIENT_TIMEOUT, "read": None}
    assert route.calls[1].request.extensions["timeout"] == CLIENT_TIMEOUT
