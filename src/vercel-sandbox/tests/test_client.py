import inspect
from typing import cast

import httpx2 as httpx
import pytest
from fake_sandbox import FakeSandboxApi

from vercel import sandbox
from vercel._internal.core.session import get_active_session, get_active_sync_session
from vercel.errors import VercelSessionClosedError
from vercel.sandbox import SandboxClient, SandboxServiceOptions, sync as sandbox_sync
from vercel.sandbox.sync import SyncSandboxClient


async def test_clients_mirror_module_operation_signatures() -> None:
    async_client = SandboxClient.create()
    sync_client = SyncSandboxClient.create()
    try:
        for module, client in (
            (sandbox, async_client),
            (sandbox_sync, sync_client),
        ):
            operations = {
                name: operation
                for name, operation in inspect.getmembers(module, inspect.isfunction)
                if not name.startswith("_") and operation.__module__ == module.__name__
            }
            assert operations
            for name, operation in operations.items():
                assert inspect.signature(getattr(client, name)) == inspect.signature(operation)
    finally:
        await async_client.aclose()
        sync_client.close()


async def test_async_client_is_standalone_and_owns_its_transport(
    fake_api: FakeSandboxApi,
) -> None:
    fake_api.seed("async-client")
    active = get_active_session()
    options = cast(SandboxServiceOptions, fake_api.service_options(sync=False)[0])
    clients: list[httpx.AsyncClient] = []

    def httpx_client_factory() -> httpx.AsyncClient:
        client = fake_api.httpx_client_factory(sync=False)()
        clients.append(client)
        return client

    client = SandboxClient.create(options=options, httpx_client_factory=httpx_client_factory)
    assert get_active_session() is active
    assert clients == []

    instance = await client.get_sandbox(name="async-client")
    assert instance.name == "async-client"
    assert fake_api.requests[-1].params["teamId"] == "team_123"
    assert len(clients) == 1

    await client.aclose()
    await client.aclose()
    assert clients[0].is_closed
    assert get_active_session() is active
    with pytest.raises(VercelSessionClosedError):
        await client.get_sandbox(name="closed")


def test_sync_client_is_standalone_and_owns_its_transport(fake_api: FakeSandboxApi) -> None:
    fake_api.seed("sync-client")
    active = get_active_sync_session()
    options = cast(sandbox_sync.SandboxServiceOptions, fake_api.service_options(sync=True)[0])
    clients: list[httpx.Client] = []

    def httpx_client_factory() -> httpx.Client:
        client = fake_api.httpx_client_factory(sync=True)()
        clients.append(client)
        return client

    client = SyncSandboxClient.create(options=options, httpx_client_factory=httpx_client_factory)
    assert get_active_sync_session() is active
    assert clients == []

    instance = client.get_sandbox(name="sync-client")
    assert instance.name == "sync-client"
    assert fake_api.requests[-1].params["teamId"] == "team_123"
    assert len(clients) == 1

    client.close()
    client.close()
    assert clients[0].is_closed
    assert get_active_sync_session() is active
    with pytest.raises(VercelSessionClosedError):
        client.get_sandbox(name="closed")
