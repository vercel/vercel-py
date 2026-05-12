from __future__ import annotations

import inspect
from dataclasses import fields
from typing import Any

import httpx
from pytest import MonkeyPatch

from vercel._internal.fs import create_filesystem_client
from vercel._internal.http.transport import BaseTransport
from vercel._internal.sandbox.core import BaseSandboxOpsClient, SandboxRequestClient
from vercel.oidc.types import Credentials
from vercel.sandbox.command import AsyncCommand, Command
from vercel.sandbox.sandbox import AsyncSandbox, Sandbox
from vercel.sandbox.snapshot import AsyncSnapshot, Snapshot


class RecordingTransport(BaseTransport):
    def __init__(self, responses: list[dict[str, Any]] | None = None) -> None:
        self.requests: list[dict[str, Any]] = []
        self._responses = responses or []

    async def send(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: Any = None,
        body: Any = None,
        headers: Any = None,
        timeout: Any = None,
        follow_redirects: bool | None = None,
        stream: bool = False,
    ) -> httpx.Response:
        self.requests.append(
            {
                "method": method,
                "path": path,
                "token": token,
                "params": params,
                "body": body,
                "headers": headers,
                "stream": stream,
            }
        )
        data = self._responses.pop(0) if self._responses else {}
        return httpx.Response(200, json=data)


async def test_request_client_resolves_fresh_token_for_each_default_request() -> None:
    calls = 0

    async def factory() -> Credentials:
        nonlocal calls
        calls += 1
        return Credentials(
            token=f"token-{calls}",
            project_id=f"project-{calls}",
            team_id=f"team-{calls}",
        )

    transport = RecordingTransport([{"ok": True}, {"ok": True}])
    client = SandboxRequestClient(transport=transport, credentials_factory=factory)

    await client.request_json("GET", "/one")
    await client.request_json("GET", "/two")

    assert calls == 2
    assert [request["token"] for request in transport.requests] == [
        "token-1",
        "token-2",
    ]


async def test_request_client_token_override_skips_credentials_factory() -> None:
    calls = 0

    async def factory() -> Credentials:
        nonlocal calls
        calls += 1
        return Credentials(token="fallback-token", project_id="project", team_id="team")

    transport = RecordingTransport([{"ok": True}])
    client = SandboxRequestClient(transport=transport, credentials_factory=factory)

    await client.request_json("GET", "/one", token="pinned-token")

    assert calls == 0
    assert transport.requests[0]["token"] == "pinned-token"


async def test_request_client_default_token_skips_credentials_factory() -> None:
    calls = 0

    async def factory() -> Credentials:
        nonlocal calls
        calls += 1
        return Credentials(token="fallback-token", project_id="project", team_id="team")

    transport = RecordingTransport([{"ok": True}, {"ok": True}])
    client = SandboxRequestClient(
        transport=transport,
        credentials_factory=factory,
        default_token="pinned-token",
    )

    await client.request_json("GET", "/one")
    await client.request_json("GET", "/two")

    assert calls == 0
    assert [request["token"] for request in transport.requests] == [
        "pinned-token",
        "pinned-token",
    ]


def test_public_facade_surfaces_do_not_store_credentials() -> None:
    for cls in (Sandbox, AsyncSandbox, Command, AsyncCommand, Snapshot, AsyncSnapshot):
        assert "credentials" not in {field.name for field in fields(cls)}


async def test_public_facade_signatures_do_not_expose_team_id() -> None:
    callables = (
        Sandbox.create,
        Sandbox.get,
        Sandbox.list,
        AsyncSandbox.create,
        AsyncSandbox.get,
        AsyncSandbox.list,
        Snapshot.get,
        Snapshot.list,
        AsyncSnapshot.get,
        AsyncSnapshot.list,
    )
    for fn in callables:
        assert "team_id" not in inspect.signature(fn).parameters


async def test_list_sandboxes_omits_team_id_query_param() -> None:
    async def factory() -> Credentials:
        return Credentials(token="token", project_id="project", team_id="team")

    transport = RecordingTransport(
        [{"sandboxes": [], "pagination": {"count": 0, "next": None, "prev": None}}]
    )
    client = SandboxRequestClient(transport=transport, credentials_factory=factory)
    ops = BaseSandboxOpsClient(
        request_client=client,
        filesystem_client=create_filesystem_client(),
    )

    await ops.list_sandboxes(project_id="project", limit=10)

    assert transport.requests[0]["params"] == {"project": "project", "limit": 10}


async def test_list_snapshots_omits_team_id_query_param() -> None:
    async def factory() -> Credentials:
        return Credentials(token="token", project_id="project", team_id="team")

    transport = RecordingTransport(
        [{"snapshots": [], "pagination": {"count": 0, "next": None, "prev": None}}]
    )
    client = SandboxRequestClient(transport=transport, credentials_factory=factory)
    ops = BaseSandboxOpsClient(
        request_client=client,
        filesystem_client=create_filesystem_client(),
    )

    await ops.list_snapshots(project_id="project", limit=10)

    assert transport.requests[0]["params"] == {"project": "project", "limit": 10}


def test_sandbox_create_uses_fresh_credentials_without_storing(
    monkeypatch: MonkeyPatch,
) -> None:
    import vercel.sandbox.sandbox as sandbox_module
    from vercel._internal.sandbox.models import (
        Sandbox as SandboxModel,
        SandboxAndRoutesResponse,
        SandboxStatus,
    )

    class RecordingClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        async def resolve_credentials(self) -> Credentials:
            self.calls.append({"method": "resolve_credentials"})
            return Credentials(token="fresh-token", project_id="fresh-project", team_id="team")

        async def create_sandbox(self, **kwargs: Any) -> SandboxAndRoutesResponse:
            self.calls.append({"method": "create_sandbox", **kwargs})
            return SandboxAndRoutesResponse(
                sandbox=SandboxModel(
                    id="sbx_1",
                    memory=1024,
                    vcpus=1,
                    region="iad1",
                    runtime="node22",
                    timeout=300_000,
                    status=SandboxStatus.RUNNING,
                    requestedAt=1,
                    createdAt=1,
                    cwd="/vercel/sandbox",
                    updatedAt=1,
                ),
                routes=[],
            )

    client = RecordingClient()
    monkeypatch.setattr(sandbox_module, "SyncSandboxOpsClient", lambda **_: client)

    sandbox = Sandbox.create(token="pinned-token")

    assert "credentials" not in sandbox.__dict__
    assert client.calls == [
        {"method": "resolve_credentials"},
        {
            "method": "create_sandbox",
            "project_id": "fresh-project",
            "token": "pinned-token",
            "source": None,
            "ports": None,
            "timeout": None,
            "resources": None,
            "runtime": None,
            "interactive": False,
            "env": None,
            "network_policy": None,
        },
    ]


async def test_sync_sandbox_list_resolves_fresh_auth_per_page(
    monkeypatch: MonkeyPatch,
) -> None:
    import vercel.sandbox.sandbox as sandbox_module
    from vercel._internal.sandbox.models import Pagination, SandboxesResponse

    class RecordingClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def __enter__(self) -> RecordingClient:
            return self

        def __exit__(self, *args: object) -> None:
            pass

        async def list_sandboxes(self, **kwargs: Any) -> SandboxesResponse:
            self.calls.append(kwargs)
            next_cursor = 10 if len(self.calls) == 1 else None
            return SandboxesResponse(
                sandboxes=[],
                pagination=Pagination(count=0, next=next_cursor),
            )

    client = RecordingClient()
    monkeypatch.setattr(sandbox_module, "SyncSandboxOpsClient", lambda **_: client)

    sandboxes = Sandbox.list(project_id="fixed-project", token="pinned", _internal_page_size=1)
    assert list(sandboxes) == []

    assert client.calls == [
        {
            "project_id": "fixed-project",
            "token": "pinned",
            "limit": 1,
            "since": None,
            "until": None,
        },
        {
            "project_id": "fixed-project",
            "token": "pinned",
            "limit": 1,
            "since": None,
            "until": 9,
        },
    ]
