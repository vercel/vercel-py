from __future__ import annotations

import httpx
import pytest

from vercel.stable.client import create_async_client, create_sync_client
from vercel.stable.errors import TransportClosedError


def test_create_sync_client_is_lazy(monkeypatch: pytest.MonkeyPatch) -> None:
    env_calls: list[str] = []
    transport_calls: list[float | None] = []

    def fail_getenv(name: str, default: str | None = None) -> str | None:
        env_calls.append(name)
        return default

    def track_create_base_client(*, timeout: float | None = None) -> httpx.Client:
        transport_calls.append(timeout)
        return httpx.Client()

    monkeypatch.setattr("os.getenv", fail_getenv)
    monkeypatch.setattr("vercel._internal.stable.runtime.create_base_client", track_create_base_client)

    client = create_sync_client(timeout=12.0)
    sdk = client.get_sdk(token="token")
    blob = client.get_blob(token="blob-token")
    cache = client.get_cache(endpoint="https://cache.example.com")
    sandbox = client.get_sandbox(token="sandbox-token")

    assert type(sdk).__name__ == "SyncSdk"
    assert type(blob).__name__ == "SyncBlobClient"
    assert type(cache).__name__ == "SyncCacheClient"
    assert type(sandbox).__name__ == "SyncSandboxClient"
    assert env_calls == []
    assert transport_calls == []


@pytest.mark.asyncio
async def test_create_async_client_is_lazy(monkeypatch: pytest.MonkeyPatch) -> None:
    env_calls: list[str] = []
    transport_calls: list[float | None] = []

    def fail_getenv(name: str, default: str | None = None) -> str | None:
        env_calls.append(name)
        return default

    def track_create_base_async_client(*, timeout: float | None = None) -> httpx.AsyncClient:
        transport_calls.append(timeout)
        return httpx.AsyncClient()

    monkeypatch.setattr("os.getenv", fail_getenv)
    monkeypatch.setattr(
        "vercel._internal.stable.runtime.create_base_async_client",
        track_create_base_async_client,
    )

    client = create_async_client(timeout=14.0)
    sdk = client.get_sdk(token="token")
    blob = client.get_blob(token="blob-token")
    cache = client.get_cache(endpoint="https://cache.example.com")
    sandbox = client.get_sandbox(token="sandbox-token")

    assert type(sdk).__name__ == "AsyncSdk"
    assert type(blob).__name__ == "AsyncBlobClient"
    assert type(cache).__name__ == "AsyncCacheClient"
    assert type(sandbox).__name__ == "AsyncSandboxClient"
    assert env_calls == []
    assert transport_calls == []


def test_with_options_shares_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    transport_calls: list[float | None] = []
    created_clients: list[httpx.Client] = []

    def track_create_base_client(*, timeout: float | None = None) -> httpx.Client:
        transport_calls.append(timeout)
        client = httpx.Client()
        created_clients.append(client)
        return client

    monkeypatch.setattr("vercel._internal.stable.runtime.create_base_client", track_create_base_client)

    client = create_sync_client(timeout=10.0)
    child = client.with_options(timeout=5.0)

    assert child._runtime is client._runtime

    child.ensure_connected()
    client.ensure_connected()

    assert transport_calls == [5.0]

    client.close()
    for http_client in created_clients:
        assert http_client.is_closed


def test_ensure_connected_initializes_transport_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport_calls: list[float | None] = []
    created_clients: list[httpx.Client] = []

    def track_create_base_client(*, timeout: float | None = None) -> httpx.Client:
        transport_calls.append(timeout)
        client = httpx.Client()
        created_clients.append(client)
        return client

    monkeypatch.setattr("vercel._internal.stable.runtime.create_base_client", track_create_base_client)

    client = create_sync_client(timeout=7.0)

    client.ensure_connected()
    client.ensure_connected()
    client.close()
    client.close()

    assert transport_calls == [7.0]
    for http_client in created_clients:
        assert http_client.is_closed


@pytest.mark.asyncio
async def test_async_with_options_shares_runtime_and_aclose_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport_calls: list[float | None] = []
    created_clients: list[httpx.AsyncClient] = []

    def track_create_base_async_client(*, timeout: float | None = None) -> httpx.AsyncClient:
        transport_calls.append(timeout)
        client = httpx.AsyncClient()
        created_clients.append(client)
        return client

    monkeypatch.setattr(
        "vercel._internal.stable.runtime.create_base_async_client",
        track_create_base_async_client,
    )

    client = create_async_client(timeout=11.0)
    child = client.with_options(timeout=6.0)

    assert child._runtime is client._runtime

    await child.ensure_connected()
    await client.ensure_connected()

    assert transport_calls == [6.0]

    await client.aclose()
    await child.aclose()

    for http_client in created_clients:
        assert http_client.is_closed


def test_closed_sync_lineage_rejects_reuse(monkeypatch: pytest.MonkeyPatch) -> None:
    transport_calls: list[float | None] = []

    def track_create_base_client(*, timeout: float | None = None) -> httpx.Client:
        transport_calls.append(timeout)
        return httpx.Client()

    monkeypatch.setattr("vercel._internal.stable.runtime.create_base_client", track_create_base_client)

    client = create_sync_client(timeout=9.0)
    client.ensure_connected()
    client.close()

    with pytest.raises(TransportClosedError):
        client.ensure_connected()

    assert transport_calls == [9.0]


@pytest.mark.asyncio
async def test_closed_async_lineage_rejects_reuse(monkeypatch: pytest.MonkeyPatch) -> None:
    transport_calls: list[float | None] = []

    def track_create_base_async_client(*, timeout: float | None = None) -> httpx.AsyncClient:
        transport_calls.append(timeout)
        return httpx.AsyncClient()

    monkeypatch.setattr(
        "vercel._internal.stable.runtime.create_base_async_client",
        track_create_base_async_client,
    )

    client = create_async_client(timeout=13.0)
    await client.ensure_connected()
    await client.aclose()

    with pytest.raises(TransportClosedError):
        await client.ensure_connected()

    assert transport_calls == [13.0]
