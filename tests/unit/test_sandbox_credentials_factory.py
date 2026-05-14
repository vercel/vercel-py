from __future__ import annotations

import inspect
from dataclasses import fields
from typing import Any, cast

import httpx
from pytest import MonkeyPatch

from vercel._internal.fs import create_filesystem_client
from vercel._internal.http.transport import BaseTransport
from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.sandbox.core import BaseSandboxOpsClient, SandboxRequestClient
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


def _sandbox_payload(sandbox_id: str = "sbx_1") -> dict[str, Any]:
    return {
        "id": sandbox_id,
        "memory": 1024,
        "vcpus": 1,
        "region": "iad1",
        "runtime": "node22",
        "timeout": 300_000,
        "status": "running",
        "requestedAt": 1,
        "createdAt": 1,
        "cwd": "/vercel/sandbox",
        "updatedAt": 1,
    }


def _command_payload(command_id: str = "cmd_1", *, exit_code: int | None = None) -> dict[str, Any]:
    payload = {
        "id": command_id,
        "name": "echo",
        "args": [],
        "cwd": "/vercel/sandbox",
        "sandboxId": "sbx_1",
        "startedAt": 1,
    }
    if exit_code is not None:
        payload["exitCode"] = exit_code
    return payload


def _snapshot_payload(snapshot_id: str = "snap_1", *, status: str = "created") -> dict[str, Any]:
    return {
        "id": snapshot_id,
        "sourceSandboxId": "sbx_1",
        "region": "iad1",
        "status": status,
        "sizeBytes": 1,
        "createdAt": 1,
        "updatedAt": 1,
    }


async def test_request_client_resolves_fresh_token_for_each_default_request() -> None:
    calls = 0

    async def provider() -> str:
        nonlocal calls
        calls += 1
        return f"token-{calls}"

    transport = RecordingTransport([{"ok": True}, {"ok": True}])
    client = SandboxRequestClient(transport=transport, token_provider=provider)

    await client.request_json("GET", "/one")
    await client.request_json("GET", "/two")

    assert calls == 2
    assert [request["token"] for request in transport.requests] == [
        "token-1",
        "token-2",
    ]


async def test_request_client_token_override_skips_token_provider() -> None:
    calls = 0

    async def provider() -> str:
        nonlocal calls
        calls += 1
        return "fallback-token"

    transport = RecordingTransport([{"ok": True}])
    client = SandboxRequestClient(transport=transport, token_provider=provider)

    await client.request_json("GET", "/one", token="pinned-token")

    assert calls == 0
    assert transport.requests[0]["token"] == "pinned-token"


async def test_request_client_token_override_is_per_call_only() -> None:
    calls = 0

    async def provider() -> str:
        nonlocal calls
        calls += 1
        return "fallback-token"

    transport = RecordingTransport([{"ok": True}, {"ok": True}])
    client = SandboxRequestClient(transport=transport, token_provider=provider)

    await client.request_json("GET", "/one", token="pinned-token")
    await client.request_json("GET", "/two")

    assert calls == 1
    assert [request["token"] for request in transport.requests] == [
        "pinned-token",
        "fallback-token",
    ]


def test_sync_handles_use_token_provider_after_override_call() -> None:
    from vercel._internal.sandbox.models import Snapshot as SnapshotModel

    calls = 0

    async def provider() -> str:
        nonlocal calls
        calls += 1
        return f"ambient-{calls}"

    async def project_id_provider() -> str:
        return "project"

    transport = RecordingTransport(
        [
            {"sandbox": _sandbox_payload(), "routes": []},
            {"sandbox": _sandbox_payload(), "routes": []},
            {"command": _command_payload()},
            {"command": _command_payload(exit_code=0)},
            {"snapshot": _snapshot_payload(status="deleted")},
        ]
    )
    request_client = SandboxRequestClient(transport=transport, token_provider=provider)
    ops = BaseSandboxOpsClient(
        request_client=request_client,
        filesystem_client=create_filesystem_client(),
        project_id_provider=project_id_provider,
    )

    created = iter_coroutine(ops.create_sandbox(project_id="project", token="pinned-token"))
    sandbox = Sandbox(
        client=cast(Any, ops),
        sandbox=created.sandbox,
        routes=[r.model_dump() for r in created.routes],
    )

    sandbox.refresh()
    command = sandbox.run_command_detached("echo")
    command.wait()
    snapshot = Snapshot(
        client=cast(Any, ops),
        snapshot=SnapshotModel.model_validate(_snapshot_payload()),
    )
    snapshot.delete()

    assert calls == 4
    assert [request["token"] for request in transport.requests] == [
        "pinned-token",
        "ambient-1",
        "ambient-2",
        "ambient-3",
        "ambient-4",
    ]


async def test_async_handles_use_token_provider_after_override_call() -> None:
    from vercel._internal.sandbox.models import Snapshot as SnapshotModel

    calls = 0

    async def provider() -> str:
        nonlocal calls
        calls += 1
        return f"ambient-{calls}"

    async def project_id_provider() -> str:
        return "project"

    transport = RecordingTransport(
        [
            {"sandbox": _sandbox_payload(), "routes": []},
            {"sandbox": _sandbox_payload(), "routes": []},
            {"command": _command_payload()},
            {"command": _command_payload(exit_code=0)},
            {"snapshot": _snapshot_payload(status="deleted")},
        ]
    )
    request_client = SandboxRequestClient(transport=transport, token_provider=provider)
    ops = BaseSandboxOpsClient(
        request_client=request_client,
        filesystem_client=create_filesystem_client(),
        project_id_provider=project_id_provider,
    )

    created = await ops.create_sandbox(project_id="project", token="pinned-token")
    sandbox = AsyncSandbox(
        client=cast(Any, ops),
        sandbox=created.sandbox,
        routes=[r.model_dump() for r in created.routes],
    )

    await sandbox.refresh()
    command = await sandbox.run_command_detached("echo")
    await command.wait()
    snapshot = AsyncSnapshot(
        client=cast(Any, ops),
        snapshot=SnapshotModel.model_validate(_snapshot_payload()),
    )
    await snapshot.delete()

    assert calls == 4
    assert [request["token"] for request in transport.requests] == [
        "pinned-token",
        "ambient-1",
        "ambient-2",
        "ambient-3",
        "ambient-4",
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
    async def provider() -> str:
        return "token"

    transport = RecordingTransport(
        [{"sandboxes": [], "pagination": {"count": 0, "next": None, "prev": None}}]
    )
    client = SandboxRequestClient(transport=transport, token_provider=provider)
    ops = BaseSandboxOpsClient(
        request_client=client,
        filesystem_client=create_filesystem_client(),
    )

    await ops.list_sandboxes(project_id="project", limit=10)

    assert transport.requests[0]["params"] == {"project": "project", "limit": 10}


async def test_list_snapshots_omits_team_id_query_param() -> None:
    async def provider() -> str:
        return "token"

    transport = RecordingTransport(
        [{"snapshots": [], "pagination": {"count": 0, "next": None, "prev": None}}]
    )
    client = SandboxRequestClient(transport=transport, token_provider=provider)
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

        async def resolve_project_id(self) -> str:
            self.calls.append({"method": "resolve_project_id"})
            return "fresh-project"

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
    constructor_kwargs: list[dict[str, Any]] = []

    def make_client(**kwargs: Any) -> RecordingClient:
        constructor_kwargs.append(kwargs)
        return client

    monkeypatch.setattr(sandbox_module, "SyncSandboxOpsClient", make_client)

    sandbox = Sandbox.create(token="pinned-token")

    assert "credentials" not in sandbox.__dict__
    assert constructor_kwargs == [{}]
    assert client.calls == [
        {"method": "resolve_project_id"},
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

        async def resolve_project_id(self) -> str:
            self.calls.append({"method": "resolve_project_id"})
            return "fixed-project"

        async def list_sandboxes(self, **kwargs: Any) -> SandboxesResponse:
            self.calls.append({"method": "list_sandboxes", **kwargs})
            page_count = len([call for call in self.calls if call["method"] == "list_sandboxes"])
            next_cursor = 10 if page_count == 1 else None
            return SandboxesResponse(
                sandboxes=[],
                pagination=Pagination(count=0, next=next_cursor),
            )

    client = RecordingClient()
    monkeypatch.setattr(sandbox_module, "SyncSandboxOpsClient", lambda **_: client)

    sandboxes = Sandbox.list(token="pinned", _internal_page_size=1)
    assert list(sandboxes) == []

    assert client.calls == [
        {"method": "resolve_project_id"},
        {
            "method": "list_sandboxes",
            "project_id": "fixed-project",
            "token": "pinned",
            "limit": 1,
            "since": None,
            "until": None,
        },
        {
            "method": "list_sandboxes",
            "project_id": "fixed-project",
            "token": "pinned",
            "limit": 1,
            "since": None,
            "until": 9,
        },
    ]
