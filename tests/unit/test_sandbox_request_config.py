from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

import vercel.sandbox.sandbox as sandbox_module
from vercel._internal.fs import FilesystemClient
from vercel._internal.http import RetryPolicy
from vercel._internal.sandbox import core as sandbox_core
from vercel._internal.sandbox.models import (
    Sandbox as SandboxModel,
    SandboxAndRoutesResponse,
    SandboxStatus,
)
from vercel.oidc.types import Credentials
from vercel.sandbox import AsyncSandbox, Sandbox, SandboxRequestConfig


def _sandbox_model(sandbox_id: str = "sbx_123") -> SandboxModel:
    return SandboxModel(
        id=sandbox_id,
        memory=4096,
        vcpus=2,
        region="iad1",
        runtime="node22",
        timeout=60_000,
        status=SandboxStatus.RUNNING,
        requestedAt=0,
        createdAt=0,
        cwd="/",
        updatedAt=0,
    )


def _sandbox_response(sandbox_id: str = "sbx_123") -> SandboxAndRoutesResponse:
    return SandboxAndRoutesResponse(sandbox=_sandbox_model(sandbox_id), routes=[])


class _RecordingSyncSandboxOpsClient:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, object]] = []
        self.get_calls: list[dict[str, object]] = []
        self.list_calls: list[dict[str, object]] = []

    async def create_sandbox(self, **kwargs: object) -> SandboxAndRoutesResponse:
        self.create_calls.append(kwargs)
        return _sandbox_response()

    async def get_sandbox(self, **kwargs: object) -> SandboxAndRoutesResponse:
        self.get_calls.append(kwargs)
        return _sandbox_response(cast(str, kwargs["sandbox_id"]))

    async def list_sandboxes(self, **kwargs: object) -> SimpleNamespace:
        self.list_calls.append(kwargs)
        return SimpleNamespace(
            sandboxes=[_sandbox_model("sbx_list_1")],
            pagination=SimpleNamespace(next=None),
        )

    def close(self) -> None:
        return None

    def __enter__(self) -> _RecordingSyncSandboxOpsClient:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _RecordingAsyncSandboxOpsClient:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, object]] = []
        self.get_calls: list[dict[str, object]] = []
        self.list_calls: list[dict[str, object]] = []

    async def create_sandbox(self, **kwargs: object) -> SandboxAndRoutesResponse:
        self.create_calls.append(kwargs)
        return _sandbox_response()

    async def get_sandbox(self, **kwargs: object) -> SandboxAndRoutesResponse:
        self.get_calls.append(kwargs)
        return _sandbox_response(cast(str, kwargs["sandbox_id"]))

    async def list_sandboxes(self, **kwargs: object) -> SimpleNamespace:
        self.list_calls.append(kwargs)
        return SimpleNamespace(
            sandboxes=[_sandbox_model("sbx_list_1")],
            pagination=SimpleNamespace(next=None),
        )

    async def aclose(self) -> None:
        return None

    async def __aenter__(self) -> _RecordingAsyncSandboxOpsClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _StubRequestClient:
    def close(self) -> None:
        return None

    async def aclose(self) -> None:
        return None


def _patch_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sandbox_module,
        "get_credentials",
        lambda **_: Credentials(
            token="token_123",
            project_id="project_123",
            team_id="team_123",
        ),
    )


def test_sandbox_request_config_is_forwarded_from_sync_entry_points(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_credentials(monkeypatch)
    client = _RecordingSyncSandboxOpsClient()
    init_kwargs: list[dict[str, object]] = []

    def build_client(**kwargs: object) -> _RecordingSyncSandboxOpsClient:
        init_kwargs.append(kwargs)
        return client

    monkeypatch.setattr(sandbox_module, "SyncSandboxOpsClient", build_client)

    retry = RetryPolicy(retries=2, retry_on_network_error=False)
    request_config = SandboxRequestConfig(timeout=12.5, retry=retry)

    created = Sandbox.create(request_config=request_config)
    fetched = Sandbox.get(sandbox_id="sbx_get", request_config=request_config)
    listed = list(Sandbox.list(request_config=request_config))

    assert created.sandbox_id == "sbx_123"
    assert fetched.sandbox_id == "sbx_get"
    assert [sandbox.id for sandbox in listed] == ["sbx_list_1"]
    assert init_kwargs == [
        {"team_id": "team_123", "token": "token_123", "request_config": request_config},
        {"team_id": "team_123", "token": "token_123", "request_config": request_config},
        {"team_id": "team_123", "token": "token_123", "request_config": request_config},
    ]


@pytest.mark.asyncio
async def test_sandbox_request_config_is_forwarded_from_async_entry_points(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_credentials(monkeypatch)
    client = _RecordingAsyncSandboxOpsClient()
    init_kwargs: list[dict[str, object]] = []

    def build_client(**kwargs: object) -> _RecordingAsyncSandboxOpsClient:
        init_kwargs.append(kwargs)
        return client

    monkeypatch.setattr(sandbox_module, "AsyncSandboxOpsClient", build_client)

    retry = RetryPolicy(retries=1, retry_on_network_error=True)
    request_config = SandboxRequestConfig(timeout=9.0, retry=retry)

    created = await AsyncSandbox.create(request_config=request_config)
    fetched = await AsyncSandbox.get(sandbox_id="sbx_get", request_config=request_config)
    listed = [sandbox async for sandbox in AsyncSandbox.list(request_config=request_config)]

    assert created.sandbox_id == "sbx_123"
    assert fetched.sandbox_id == "sbx_get"
    assert [sandbox.id for sandbox in listed] == ["sbx_list_1"]
    assert init_kwargs == [
        {"team_id": "team_123", "token": "token_123", "request_config": request_config},
        {"team_id": "team_123", "token": "token_123", "request_config": request_config},
        {"team_id": "team_123", "token": "token_123", "request_config": request_config},
    ]


def test_sync_sandbox_ops_client_uses_default_request_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_create_request_client(**kwargs: object) -> _StubRequestClient:
        captured.update(kwargs)
        return _StubRequestClient()

    monkeypatch.setattr(sandbox_core, "create_request_client", fake_create_request_client)

    sandbox_core.SyncSandboxOpsClient(
        team_id="team_123",
        token="token_123",
        filesystem_client=cast(FilesystemClient[Any], SimpleNamespace()),
    )

    assert captured["timeout"] == 180.0
    assert captured["retry"] is None


def test_sync_sandbox_ops_client_uses_custom_request_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_create_request_client(**kwargs: object) -> _StubRequestClient:
        captured.update(kwargs)
        return _StubRequestClient()

    monkeypatch.setattr(sandbox_core, "create_request_client", fake_create_request_client)
    retry = RetryPolicy(retries=3, retry_on_network_error=False)
    request_config = SandboxRequestConfig(timeout=42.0, retry=retry)

    sandbox_core.SyncSandboxOpsClient(
        team_id="team_123",
        token="token_123",
        request_config=request_config,
        filesystem_client=cast(FilesystemClient[Any], SimpleNamespace()),
    )

    assert captured["timeout"] == 42.0
    assert captured["retry"] is retry


@pytest.mark.asyncio
async def test_async_sandbox_ops_client_uses_default_request_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_create_async_request_client(**kwargs: object) -> _StubRequestClient:
        captured.update(kwargs)
        return _StubRequestClient()

    monkeypatch.setattr(
        sandbox_core, "create_async_request_client", fake_create_async_request_client
    )

    client = sandbox_core.AsyncSandboxOpsClient(
        team_id="team_123",
        token="token_123",
        filesystem_client=cast(FilesystemClient[Any], SimpleNamespace()),
    )

    assert captured["timeout"] == 180.0
    assert captured["retry"] is None
    await client.aclose()


@pytest.mark.asyncio
async def test_async_sandbox_ops_client_uses_custom_request_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_create_async_request_client(**kwargs: object) -> _StubRequestClient:
        captured.update(kwargs)
        return _StubRequestClient()

    monkeypatch.setattr(
        sandbox_core, "create_async_request_client", fake_create_async_request_client
    )
    retry = RetryPolicy(retries=4, retry_on_network_error=True)
    request_config = SandboxRequestConfig(timeout=21.0, retry=retry)

    client = sandbox_core.AsyncSandboxOpsClient(
        team_id="team_123",
        token="token_123",
        request_config=request_config,
        filesystem_client=cast(FilesystemClient[Any], SimpleNamespace()),
    )

    assert captured["timeout"] == 21.0
    assert captured["retry"] is retry
    await client.aclose()
