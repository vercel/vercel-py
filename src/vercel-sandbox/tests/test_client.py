import inspect
from typing import Any, cast

import httpx2 as httpx
import pytest
from sandbox_fixtures import sandbox_service_options

import vendor.respx as respx
from vercel import sandbox
from vercel._internal.core.session import get_active_session, get_active_sync_session
from vercel.errors import VercelSessionClosedError
from vercel.sandbox import SandboxClient, SandboxServiceOptions, sync as sandbox_sync
from vercel.sandbox.sync import SyncSandboxClient


def _sandbox_response(*, name: str) -> dict[str, Any]:
    return {
        "sandbox": {
            "name": name,
            "currentSessionId": "sbx_123",
            "image": "vercel/sandbox/universal:latest",
            "status": "running",
            "persistent": True,
            "region": "iad1",
            "failoverRegions": [],
            "timeout": 300000,
            "snapshotExpiration": 0,
            "createdAt": 1,
            "updatedAt": 2,
        },
        "session": {
            "id": "sbx_123",
            "sourceSandboxName": name,
            "projectId": "prj_123",
            "status": "running",
            "cwd": "/vercel/sandbox",
            "region": "iad1",
            "memory": 2048,
            "vcpus": 1,
            "timeout": 300000,
            "requestedAt": 1,
        },
        "routes": [],
    }


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


@respx.mock
async def test_async_client_is_standalone_and_owns_its_transport(
    mock_env_clear: None,
) -> None:
    active = get_active_session()
    options = cast(SandboxServiceOptions, sandbox_service_options(sync=False)[0])
    clients: list[httpx.AsyncClient] = []

    def httpx_client_factory() -> httpx.AsyncClient:
        client = httpx.AsyncClient()
        clients.append(client)
        return client

    route = respx.get("https://sandbox.test/v2/sandboxes/async-client").mock(
        return_value=httpx.Response(200, json=_sandbox_response(name="async-client"))
    )

    client = SandboxClient.create(
        options=options,
        httpx_client_factory=httpx_client_factory,
    )
    assert get_active_session() is active
    assert clients == []

    instance = await client.get_sandbox(name="async-client")
    assert instance.name == "async-client"
    assert route.calls.last.request.url.params["teamId"] == "team_123"
    assert len(clients) == 1

    await client.aclose()
    await client.aclose()
    assert clients[0].is_closed
    assert get_active_session() is active
    with pytest.raises(VercelSessionClosedError):
        await client.get_sandbox(name="closed")


@respx.mock
def test_sync_client_is_standalone_and_owns_its_transport(
    mock_env_clear: None,
) -> None:
    active = get_active_sync_session()
    options = cast(
        sandbox_sync.SandboxServiceOptions,
        sandbox_service_options(sync=True)[0],
    )
    clients: list[httpx.Client] = []

    def httpx_client_factory() -> httpx.Client:
        client = httpx.Client()
        clients.append(client)
        return client

    route = respx.get("https://sandbox.test/v2/sandboxes/sync-client").mock(
        return_value=httpx.Response(200, json=_sandbox_response(name="sync-client"))
    )

    client = SyncSandboxClient.create(
        options=options,
        httpx_client_factory=httpx_client_factory,
    )
    assert get_active_sync_session() is active
    assert clients == []

    instance = client.get_sandbox(name="sync-client")
    assert instance.name == "sync-client"
    assert route.calls.last.request.url.params["teamId"] == "team_123"
    assert len(clients) == 1

    client.close()
    client.close()
    assert clients[0].is_closed
    assert get_active_sync_session() is active
    with pytest.raises(VercelSessionClosedError):
        client.get_sandbox(name="closed")
