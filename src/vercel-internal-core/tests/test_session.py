from dataclasses import dataclass

import httpx
import pytest

from vercel import session
from vercel.internal.core.errors import VercelSessionClosedError, VercelSessionError
from vercel.internal.core.http import AsyncTransport, SyncTransport
from vercel.internal.core.options import ServiceOptions
from vercel.internal.core.session import (
    SdkSession,
    SyncSdkSession,
    get_active_session,
    get_active_sync_session,
)


@dataclass(frozen=True, slots=True)
class ExampleOptions(ServiceOptions):
    value: str


@dataclass(frozen=True, slots=True)
class OtherOptions(ServiceOptions):
    value: str


class ExampleService:
    pass


class CountingClient(httpx.Client):
    def __init__(self) -> None:
        super().__init__()
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        super().close()


class CountingAsyncClient(httpx.AsyncClient):
    def __init__(self) -> None:
        super().__init__()
        self.close_calls = 0

    async def aclose(self) -> None:
        self.close_calls += 1
        await super().aclose()


def test_sync_scopes_inherit_and_reset_factory_and_replace_exact_options() -> None:
    outer_example = ExampleOptions("outer")
    inner_example = ExampleOptions("inner")
    other = OtherOptions("other")
    clients: list[CountingClient] = []

    def factory() -> httpx.Client:
        client = CountingClient()
        clients.append(client)
        return client

    with session(service_options=[outer_example, other], httpx_client_factory=factory):
        outer = get_active_sync_session()
        assert outer.get_service_option(ExampleOptions) is outer_example
        assert outer.get_service_option(OtherOptions) is other

        with session(service_options=[inner_example]):
            inner = get_active_sync_session()
            assert inner.get_service_option(ExampleOptions) is inner_example
            assert inner.get_service_option(OtherOptions) is other
            assert isinstance(inner.get_transport(), SyncTransport)
            assert len(clients) == 1

        with session(httpx_client_factory=None):
            reset = get_active_sync_session()
            assert isinstance(reset.get_transport(), SyncTransport)
            assert len(clients) == 1

        assert get_active_sync_session() is outer
        assert inner.is_closed
        assert reset.is_closed

    assert outer.is_closed

    async_client = CountingAsyncClient()
    with session(httpx_client_factory=lambda: async_client):
        with pytest.raises(VercelSessionError, match="httpx.Client"):
            get_active_sync_session().get_transport()
    assert async_client.close_calls == 1


@pytest.mark.asyncio
async def test_async_scopes_inherit_and_reset_factory_and_reject_wrong_mode() -> None:
    assert get_active_session() is SdkSession.default()
    assert get_active_sync_session() is SyncSdkSession.default()
    assert get_active_session() is not get_active_sync_session()

    outer_example = ExampleOptions("outer")
    replacement = ExampleOptions("inner")
    unrelated = OtherOptions("other")
    clients: list[CountingAsyncClient] = []

    def factory() -> httpx.AsyncClient:
        client = CountingAsyncClient()
        clients.append(client)
        return client

    async with session(
        service_options=[outer_example, unrelated],
        httpx_client_factory=factory,
    ):
        outer = get_active_session()
        assert outer.get_service_option(ExampleOptions) is outer_example

        async with session(service_options=[replacement]):
            inner = get_active_session()
            assert inner.get_service_option(ExampleOptions) is replacement
            assert inner.get_service_option(OtherOptions) is unrelated
            assert isinstance(inner.get_transport(), AsyncTransport)
            assert len(clients) == 1

        async with session(httpx_client_factory=None):
            reset = get_active_session()
            assert isinstance(reset.get_transport(), AsyncTransport)
            assert len(clients) == 1

        assert get_active_session() is outer
        assert inner.is_closed
        assert reset.is_closed
        with pytest.raises(VercelSessionError, match="Sync Vercel APIs"):
            get_active_sync_session()
        with pytest.raises(VercelSessionError, match="Sync Vercel APIs"):
            with session():
                pass

    assert outer.is_closed

    with session():
        with pytest.raises(VercelSessionError, match="Async Vercel APIs"):
            get_active_session()
        with pytest.raises(VercelSessionError, match="Async Vercel APIs"):
            async with session():
                pass


@pytest.mark.asyncio
async def test_transports_are_lazy_shared_and_closed_exactly_once() -> None:
    sync_clients: list[CountingClient] = []

    def sync_factory() -> httpx.Client:
        client = CountingClient()
        sync_clients.append(client)
        return client

    with session(httpx_client_factory=sync_factory):
        sync_active = get_active_sync_session()
        assert sync_clients == []
        assert sync_active.get_transport() is sync_active.get_transport()
        assert len(sync_clients) == 1
        sync_active.close()
        assert sync_clients[0].close_calls == 1

    assert sync_clients[0].close_calls == 1
    with pytest.raises(VercelSessionClosedError):
        sync_active.get_transport()

    async_clients: list[CountingAsyncClient] = []

    def async_factory() -> httpx.AsyncClient:
        client = CountingAsyncClient()
        async_clients.append(client)
        return client

    async with session(httpx_client_factory=async_factory):
        async_active = get_active_session()
        assert async_clients == []
        assert async_active.get_transport() is async_active.get_transport()
        assert len(async_clients) == 1
        await async_active.aclose()
        assert async_clients[0].close_calls == 1

    assert async_clients[0].close_calls == 1
    with pytest.raises(VercelSessionClosedError):
        async_active.get_transport()


def test_services_and_staging_runtime_are_cached_within_a_session() -> None:
    calls = 0

    def factory() -> ExampleService:
        nonlocal calls
        calls += 1
        return ExampleService()

    with session():
        active = get_active_sync_session()
        service = active.get_or_create_service(ExampleService, factory)
        assert active.get_or_create_service(ExampleService, factory) is service
        assert active.get_staging_file_runtime() is active.get_staging_file_runtime()

    assert calls == 1
    with pytest.raises(VercelSessionClosedError):
        active.get_or_create_service(ExampleService, factory)


@pytest.mark.asyncio
async def test_async_session_rejects_and_closes_sync_factory_client() -> None:
    sync_client = CountingClient()

    async with session(httpx_client_factory=lambda: sync_client):
        with pytest.raises(VercelSessionError, match="httpx.AsyncClient"):
            get_active_session().get_transport()

    assert sync_client.close_calls == 1
