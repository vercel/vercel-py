from __future__ import annotations

import base64
import json
from collections.abc import Callable
from typing import Any, cast

import httpx
import respx

from vercel._internal.fs import create_filesystem_client
from vercel._internal.http.transport import BaseTransport, ReadResponsePolicy
from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.sandbox.core import (
    BaseSandboxOpsClient,
    SandboxCredentials,
    SandboxRequestClient,
    make_public_sandbox_credentials_factory,
)
from vercel.sandbox.sandbox import AsyncSandbox, Sandbox
from vercel.sandbox.snapshot import AsyncSnapshot, Snapshot

SANDBOX_API_BASE = "https://api.vercel.com"


def _credentials_factory(
    provider: Callable[[], Any],
    *,
    project_id: str | None = None,
    team_id: str | None = None,
):
    async def factory() -> SandboxCredentials:
        return SandboxCredentials(
            token=await provider(),
            project_id=project_id,
            team_id=team_id,
        )

    return factory


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
        read_response: ReadResponsePolicy = ReadResponsePolicy.NEVER,
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


def _record_auth_response(
    auth_headers: list[str | None], response: dict[str, Any]
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        auth_headers.append(request.headers.get("authorization"))
        return httpx.Response(200, json=response)

    return handler


def _set_project_env(monkeypatch: Any) -> None:
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_from_env")
    monkeypatch.setenv("VERCEL_TEAM_ID", "team_from_env")


async def test_request_client_resolves_fresh_token_for_each_default_request() -> None:
    calls = 0

    async def provider() -> str:
        nonlocal calls
        calls += 1
        return f"token-{calls}"

    transport = RecordingTransport([{"ok": True}, {"ok": True}])
    client = SandboxRequestClient(
        transport=transport,
        credentials_factory=_credentials_factory(provider),
    )

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
    client = SandboxRequestClient(
        transport=transport,
        credentials_factory=_credentials_factory(provider),
    )

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
    client = SandboxRequestClient(
        transport=transport,
        credentials_factory=_credentials_factory(provider),
    )

    await client.request_json("GET", "/one", token="pinned-token")
    await client.request_json("GET", "/two")

    assert calls == 1
    assert [request["token"] for request in transport.requests] == [
        "pinned-token",
        "fallback-token",
    ]


async def test_request_client_merges_user_agent_base_header() -> None:
    async def provider() -> str:
        return "token"

    transport = RecordingTransport([{"ok": True}])
    client = SandboxRequestClient(
        transport=transport,
        credentials_factory=_credentials_factory(provider),
        base_headers={"user-agent": "vercel/sandbox/test"},
    )

    await client.request_json("POST", "/one", headers={"x-custom": "1"})

    assert transport.requests[0]["headers"] == {
        "user-agent": "vercel/sandbox/test",
        "x-custom": "1",
        "content-type": "application/json",
    }


def test_sync_handles_use_token_provider_after_override_call() -> None:
    from vercel._internal.sandbox.models import Snapshot as SnapshotModel

    calls = 0

    async def provider() -> str:
        nonlocal calls
        calls += 1
        return f"ambient-{calls}"

    transport = RecordingTransport(
        [
            {"sandbox": _sandbox_payload(), "routes": []},
            {"sandbox": _sandbox_payload(), "routes": []},
            {"command": _command_payload()},
            {"command": _command_payload(exit_code=0)},
            {"snapshot": _snapshot_payload(status="deleted")},
        ]
    )
    request_client = SandboxRequestClient(
        transport=transport,
        credentials_factory=_credentials_factory(provider, project_id="project"),
    )
    ops = BaseSandboxOpsClient(
        request_client=request_client,
        filesystem_client=create_filesystem_client(),
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

    transport = RecordingTransport(
        [
            {"sandbox": _sandbox_payload(), "routes": []},
            {"sandbox": _sandbox_payload(), "routes": []},
            {"command": _command_payload()},
            {"command": _command_payload(exit_code=0)},
            {"snapshot": _snapshot_payload(status="deleted")},
        ]
    )
    request_client = SandboxRequestClient(
        transport=transport,
        credentials_factory=_credentials_factory(provider, project_id="project"),
    )
    ops = BaseSandboxOpsClient(
        request_client=request_client,
        filesystem_client=create_filesystem_client(),
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


@respx.mock
def test_public_sync_sandbox_reuses_token_provider_for_handle_requests() -> None:
    auth_headers: list[str | None] = []
    calls = 0

    async def provider() -> str:
        nonlocal calls
        calls += 1
        return f"provider-{calls}"

    respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
        side_effect=_record_auth_response(
            auth_headers, {"sandbox": _sandbox_payload(), "routes": []}
        )
    )
    respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/sbx_1").mock(
        side_effect=_record_auth_response(
            auth_headers, {"sandbox": _sandbox_payload(), "routes": []}
        )
    )
    respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/sbx_1/cmd").mock(
        side_effect=_record_auth_response(auth_headers, {"command": _command_payload()})
    )
    respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/sbx_1/cmd/cmd_1").mock(
        side_effect=_record_auth_response(auth_headers, {"command": _command_payload(exit_code=0)})
    )

    sandbox = Sandbox.create(token=provider, project_id="project")
    sandbox.refresh()
    command = sandbox.run_command_detached("echo")
    command.wait()
    sandbox.client.close()

    assert calls == 4
    assert auth_headers == [
        "Bearer provider-1",
        "Bearer provider-2",
        "Bearer provider-3",
        "Bearer provider-4",
    ]


@respx.mock
async def test_public_async_sandbox_reuses_token_provider_for_handle_requests() -> None:
    auth_headers: list[str | None] = []
    calls = 0

    async def provider() -> str:
        nonlocal calls
        calls += 1
        return f"provider-{calls}"

    respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
        side_effect=_record_auth_response(
            auth_headers, {"sandbox": _sandbox_payload(), "routes": []}
        )
    )
    respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/sbx_1").mock(
        side_effect=_record_auth_response(
            auth_headers, {"sandbox": _sandbox_payload(), "routes": []}
        )
    )
    respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/sbx_1/cmd").mock(
        side_effect=_record_auth_response(auth_headers, {"command": _command_payload()})
    )
    respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/sbx_1/cmd/cmd_1").mock(
        side_effect=_record_auth_response(auth_headers, {"command": _command_payload(exit_code=0)})
    )

    sandbox = await AsyncSandbox.create(token=provider, project_id="project")
    await sandbox.refresh()
    command = await sandbox.run_command_detached("echo")
    await command.wait()
    await sandbox.client.aclose()

    assert calls == 4
    assert auth_headers == [
        "Bearer provider-1",
        "Bearer provider-2",
        "Bearer provider-3",
        "Bearer provider-4",
    ]


@respx.mock
def test_public_lookup_and_list_methods_use_token_provider() -> None:
    auth_headers: list[str | None] = []
    calls = 0

    async def provider() -> str:
        nonlocal calls
        calls += 1
        return f"provider-{calls}"

    respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/sbx_1").mock(
        side_effect=_record_auth_response(
            auth_headers, {"sandbox": _sandbox_payload(), "routes": []}
        )
    )
    respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
        side_effect=_record_auth_response(
            auth_headers,
            {"sandboxes": [_sandbox_payload()], "pagination": {"count": 1, "next": None}},
        )
    )
    respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/snapshots/snap_1").mock(
        side_effect=_record_auth_response(auth_headers, {"snapshot": _snapshot_payload()})
    )
    respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/snapshots").mock(
        side_effect=_record_auth_response(
            auth_headers,
            {"snapshots": [_snapshot_payload()], "pagination": {"count": 1, "next": None}},
        )
    )

    sandbox = Sandbox.get(sandbox_id="sbx_1", token=provider)
    sandboxes = list(Sandbox.list(token=provider, project_id="project"))
    snapshot = Snapshot.get(snapshot_id="snap_1", token=provider)
    snapshots = list(Snapshot.list(token=provider, project_id="project"))

    sandbox.client.close()
    snapshot.client.close()

    assert sandbox.sandbox_id == "sbx_1"
    assert [item.id for item in sandboxes] == ["sbx_1"]
    assert snapshot.snapshot_id == "snap_1"
    assert [item.id for item in snapshots] == ["snap_1"]
    assert calls == 4
    assert auth_headers == [
        "Bearer provider-1",
        "Bearer provider-2",
        "Bearer provider-3",
        "Bearer provider-4",
    ]


async def test_list_sandboxes_includes_team_id_query_param() -> None:
    async def provider() -> str:
        return "token"

    transport = RecordingTransport(
        [{"sandboxes": [], "pagination": {"count": 0, "next": None, "prev": None}}]
    )

    client = SandboxRequestClient(
        transport=transport,
        credentials_factory=_credentials_factory(
            provider,
            project_id="project",
            team_id="team",
        ),
    )
    ops = BaseSandboxOpsClient(
        request_client=client,
        filesystem_client=create_filesystem_client(),
    )

    await ops.list_sandboxes(project_id="project", limit=10)

    assert transport.requests[0]["params"] == {
        "project": "project",
        "limit": 10,
        "teamId": "team",
    }


async def test_list_snapshots_includes_team_id_query_param() -> None:
    async def provider() -> str:
        return "token"

    transport = RecordingTransport(
        [{"snapshots": [], "pagination": {"count": 0, "next": None, "prev": None}}]
    )

    client = SandboxRequestClient(
        transport=transport,
        credentials_factory=_credentials_factory(
            provider,
            project_id="project",
            team_id="team",
        ),
    )
    ops = BaseSandboxOpsClient(
        request_client=client,
        filesystem_client=create_filesystem_client(),
    )

    await ops.list_snapshots(project_id="project", limit=10)

    assert transport.requests[0]["params"] == {
        "project": "project",
        "limit": 10,
        "teamId": "team",
    }


async def test_request_client_uses_oidc_owner_as_team_id(mock_env_clear: None) -> None:
    payload = base64.urlsafe_b64encode(json.dumps({"owner_id": "team_oidc"}).encode())
    token = f"header.{payload.decode().rstrip('=')}.signature"

    transport = RecordingTransport([{"ok": True}])
    client = SandboxRequestClient(
        transport=transport,
        credentials_factory=make_public_sandbox_credentials_factory(token=token),
    )

    await client.request_json("GET", "/one")

    assert transport.requests[0]["params"] == {"teamId": "team_oidc"}


@respx.mock
def test_public_sync_create_uses_explicit_token_for_default_project_id(
    mock_env_clear: None, monkeypatch: Any
) -> None:
    _set_project_env(monkeypatch)
    route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
        return_value=httpx.Response(200, json={"sandbox": _sandbox_payload(), "routes": []})
    )

    sandbox = Sandbox.create(token="tok", team_id="team_explicit")
    sandbox.client.close()

    request = route.calls.last.request
    assert request.headers["authorization"] == "Bearer tok"
    assert request.url.params["teamId"] == "team_explicit"
    assert json.loads(request.content)["projectId"] == "prj_from_env"


@respx.mock
async def test_public_async_create_uses_explicit_token_for_default_project_id(
    mock_env_clear: None, monkeypatch: Any
) -> None:
    _set_project_env(monkeypatch)
    route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
        return_value=httpx.Response(200, json={"sandbox": _sandbox_payload(), "routes": []})
    )

    sandbox = await AsyncSandbox.create(token="tok")
    await sandbox.client.aclose()

    request = route.calls.last.request
    assert request.headers["authorization"] == "Bearer tok"
    assert request.url.params["teamId"] == "team_from_env"
    assert json.loads(request.content)["projectId"] == "prj_from_env"


@respx.mock
async def test_public_token_provider_resolves_default_project_id(
    mock_env_clear: None, monkeypatch: Any
) -> None:
    _set_project_env(monkeypatch)
    calls = 0

    async def provider() -> str:
        nonlocal calls
        calls += 1
        return f"tok-{calls}"

    route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
        return_value=httpx.Response(200, json={"sandbox": _sandbox_payload(), "routes": []})
    )

    sandbox = await AsyncSandbox.create(token=provider)
    await sandbox.client.aclose()

    request = route.calls.last.request
    assert calls == 1
    assert request.headers["authorization"] == "Bearer tok-1"
    assert json.loads(request.content)["projectId"] == "prj_from_env"


@respx.mock
def test_public_sync_lists_use_explicit_token_for_default_project_id(
    mock_env_clear: None, monkeypatch: Any
) -> None:
    _set_project_env(monkeypatch)
    sandbox_route = respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json={"sandboxes": [_sandbox_payload()], "pagination": {"count": 1, "next": None}},
        )
    )
    snapshot_route = respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/snapshots").mock(
        return_value=httpx.Response(
            200,
            json={"snapshots": [_snapshot_payload()], "pagination": {"count": 1, "next": None}},
        )
    )

    sandboxes = list(Sandbox.list(token="tok", limit=1))
    snapshots = list(Snapshot.list(token="tok", limit=1))

    sandbox_request = sandbox_route.calls.last.request
    snapshot_request = snapshot_route.calls.last.request
    assert [sandbox.id for sandbox in sandboxes] == ["sbx_1"]
    assert [snapshot.id for snapshot in snapshots] == ["snap_1"]
    assert sandbox_request.headers["authorization"] == "Bearer tok"
    assert snapshot_request.headers["authorization"] == "Bearer tok"
    assert sandbox_request.url.params["project"] == "prj_from_env"
    assert snapshot_request.url.params["project"] == "prj_from_env"
    assert sandbox_request.url.params["teamId"] == "team_from_env"
    assert snapshot_request.url.params["teamId"] == "team_from_env"


@respx.mock
async def test_public_async_lists_use_explicit_token_for_default_project_id(
    mock_env_clear: None, monkeypatch: Any
) -> None:
    _set_project_env(monkeypatch)
    sandbox_route = respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json={"sandboxes": [_sandbox_payload()], "pagination": {"count": 1, "next": None}},
        )
    )
    snapshot_route = respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/snapshots").mock(
        return_value=httpx.Response(
            200,
            json={"snapshots": [_snapshot_payload()], "pagination": {"count": 1, "next": None}},
        )
    )

    sandboxes = [sandbox async for sandbox in AsyncSandbox.list(token="tok", limit=1)]
    snapshots = [snapshot async for snapshot in AsyncSnapshot.list(token="tok", limit=1)]

    sandbox_request = sandbox_route.calls.last.request
    snapshot_request = snapshot_route.calls.last.request
    assert [sandbox.id for sandbox in sandboxes] == ["sbx_1"]
    assert [snapshot.id for snapshot in snapshots] == ["snap_1"]
    assert sandbox_request.headers["authorization"] == "Bearer tok"
    assert snapshot_request.headers["authorization"] == "Bearer tok"
    assert sandbox_request.url.params["project"] == "prj_from_env"
    assert snapshot_request.url.params["project"] == "prj_from_env"
    assert sandbox_request.url.params["teamId"] == "team_from_env"
    assert snapshot_request.url.params["teamId"] == "team_from_env"
