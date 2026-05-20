from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import httpx
import pytest

from tests.unstable.fake_sandbox_api import FakeSandboxAPI
from vercel._internal.http import BaseTransport, RequestBody, RetryPolicy
from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.unstable.errors import SessionClosedError
from vercel._internal.unstable.sandbox import api_client as api_client_module
from vercel._internal.unstable.sandbox.api_client import (
    _USER_AGENT,
    SandboxApiClient,
    create_sandbox_credentials_resolver,
)
from vercel.sandbox import NetworkPolicyCustom, NetworkPolicySubnets, Resources, SnapshotSource
from vercel.unstable import Session, SyncSession, VercelError
from vercel.unstable.auth import (
    AccessTokenCredentials,
    StaticCredentialProvider,
    SyncStaticCredentialProvider,
)
from vercel.unstable.sandbox import (
    SandboxAPIError,
    SandboxCreateParams,
    SandboxError,
    SandboxOperationTimeoutError,
    SandboxOptions,
    SandboxRoute,
    SandboxStatus,
)


@pytest.fixture
def sandbox_payload() -> dict[str, object]:
    return {
        "sandbox": {
            "name": "my-sandbox",
            "persistent": True,
        },
        "session": {
            "id": "sbx_test123",
            "memory": 1024,
            "vcpus": 2,
            "region": "iad1",
            "runtime": "python3.13",
            "timeout": 300000,
            "status": "running",
            "requestedAt": 1,
            "startedAt": 2,
            "cwd": "/vercel/sandbox",
        },
        "routes": [],
    }


def _sandbox_options() -> SandboxOptions:
    return SandboxOptions(
        credential_provider=StaticCredentialProvider(
            AccessTokenCredentials(
                token="token_1",
                project_id="project_1",
                team_id="team_1",
            )
        ),
        request_timeout=timedelta(seconds=9),
    )


async def test_create_sends_authenticated_payload_and_returns_handle(
    fake_sandbox_api: FakeSandboxAPI,
    sandbox_payload: dict[str, object],
) -> None:
    fake_sandbox_api.script_response(status_code=201, json=sandbox_payload)
    session = Session()
    fake_sandbox_api.install(session)
    accessor = session.sandbox.with_options(_sandbox_options())

    sandbox = await accessor.create(
        SandboxCreateParams(
            runtime="python3.13",
            ports=[3000],
            interactive=False,
            name="my-sandbox",
            env={"EXAMPLE": "1"},
            timeout=timedelta(seconds=60),
            resources=Resources(vcpus=2, memory=4096),
            source=SnapshotSource(snapshot_id="snap_source_123"),
            network_policy=NetworkPolicyCustom(
                allow=["api.example.com"],
                subnets=NetworkPolicySubnets(
                    allow=["10.0.0.0/24"],
                    deny=["10.0.1.0/24"],
                ),
            ),
            persistent=False,
            snapshot_expiration=timedelta(hours=1),
            tags=["ci", "unstable"],
        )
    )

    assert sandbox.name == "my-sandbox"
    assert sandbox.persistent is True
    assert sandbox.current_session is not None
    assert sandbox.current_session.id == "sbx_test123"
    assert sandbox.current_session.status == SandboxStatus.RUNNING
    assert sandbox.routes == []
    assert len(fake_sandbox_api.requests) == 1
    request = fake_sandbox_api.requests[0]
    assert request.method == "POST"
    assert request.path == "/v2/sandboxes"
    assert request.query == {}
    assert request.timeout == timedelta(seconds=9)
    assert request.headers["authorization"] == "Bearer token_1"
    assert request.headers["content-type"] == "application/json"
    assert request.headers["user-agent"] == _USER_AGENT
    assert request.body == {
        "projectId": "project_1",
        "ports": [3000],
        "name": "my-sandbox",
        "source": {"type": "snapshot", "snapshotId": "snap_source_123"},
        "timeout": 60000,
        "resources": {"vcpus": 2, "memory": 4096},
        "runtime": "python3.13",
        "networkPolicy": {
            "mode": "custom",
            "allowedDomains": ["api.example.com"],
            "allowedCIDRs": ["10.0.0.0/24"],
            "deniedCIDRs": ["10.0.1.0/24"],
        },
        "__interactive": False,
        "env": {"EXAMPLE": "1"},
        "persistent": False,
        "snapshotExpiration": 3600000,
        "tags": ["ci", "unstable"],
    }


def test_sync_create_returns_handle_smoke(
    fake_sandbox_api: FakeSandboxAPI,
    sandbox_payload: dict[str, object],
) -> None:
    fake_sandbox_api.script_response(status_code=201, json=sandbox_payload)
    session = SyncSession()
    fake_sandbox_api.install(session)
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=SyncStaticCredentialProvider(
                AccessTokenCredentials(
                    token="token_1",
                    project_id="project_1",
                    team_id="team_1",
                )
            )
        )
    )

    sandbox = accessor.create(SandboxCreateParams(runtime="python3.13", name="my-sandbox"))

    assert sandbox.name == "my-sandbox"
    assert sandbox.current_session is not None
    assert sandbox.current_session.status == SandboxStatus.RUNNING
    assert fake_sandbox_api.requests[0].method == "POST"


async def test_create_maps_v2_response_shape(fake_sandbox_api: FakeSandboxAPI) -> None:
    fake_sandbox_api.script_response(
        status_code=201,
        json={
            "sandbox": {
                "name": "my-sandbox",
                "persistent": True,
                "currentSnapshotId": "snap_current_123",
            },
            "session": {
                "id": "sbx_test123",
                "memory": 1024,
                "vcpus": 2,
                "region": "iad1",
                "runtime": "python3.13",
                "timeout": 300000,
                "status": "running",
                "requestedAt": 1,
                "startedAt": 2,
                "cwd": "/vercel/sandbox",
                "projectId": "project_1",
                "sourceSandboxName": "source-sandbox",
                "sourceSnapshotId": "snap_source_456",
                "activeCpuDurationMs": 1234,
                "networkTransfer": 5678,
            },
            "routes": [
                {
                    "url": "https://my-sandbox.vercel.run",
                    "subdomain": "my-sandbox",
                    "port": 3000,
                }
            ],
        },
    )
    session = Session()
    fake_sandbox_api.install(session)

    sandbox = await session.sandbox.with_options(_sandbox_options()).create(
        SandboxCreateParams(runtime="python3.13")
    )

    assert sandbox.name == "my-sandbox"
    assert sandbox.persistent is True
    assert sandbox.current_snapshot_id == "snap_current_123"
    assert sandbox.routes == [
        SandboxRoute(
            url="https://my-sandbox.vercel.run",
            subdomain="my-sandbox",
            port=3000,
        )
    ]
    assert sandbox.current_session is not None
    assert sandbox.current_session.project_id == "project_1"
    assert sandbox.current_session.source_sandbox_name == "source-sandbox"
    assert sandbox.current_session.source_snapshot_id == "snap_source_456"
    assert sandbox.current_session.active_cpu_duration_ms == 1234
    assert sandbox.current_session.network_transfer == 5678


async def test_create_falls_back_to_session_id_when_name_is_missing(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    fake_sandbox_api.script_response(
        status_code=201,
        json={
            "sandbox": {"persistent": False},
            "session": {"id": "sbx_fallback", "status": "running"},
            "routes": [],
        },
    )
    session = Session()
    fake_sandbox_api.install(session)

    sandbox = await session.sandbox.with_options(_sandbox_options()).create(
        SandboxCreateParams(runtime="python3.13")
    )

    assert sandbox.name == "sbx_fallback"
    assert sandbox.current_session is not None
    assert sandbox.current_session.id == "sbx_fallback"


async def test_create_rejects_malformed_success_response(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    fake_sandbox_api.script_response(status_code=201, json={"sandbox": {"name": "missing-session"}})
    session = Session()
    fake_sandbox_api.install(session)

    with pytest.raises(SandboxAPIError, match="response validation failed") as raised:
        await session.sandbox.with_options(_sandbox_options()).create(
            SandboxCreateParams(runtime="python3.13")
        )

    assert raised.value.status_code == 200
    assert raised.value.data == {"sandbox": {"name": "missing-session"}}


async def test_create_rejects_unknown_success_status(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    payload = {
        "sandbox": {"name": "my-sandbox"},
        "session": {"id": "sbx_test123", "status": "mystery"},
        "routes": [],
    }
    fake_sandbox_api.script_response(status_code=201, json=payload)
    session = Session()
    fake_sandbox_api.install(session)

    with pytest.raises(SandboxAPIError, match="response validation failed") as raised:
        await session.sandbox.with_options(_sandbox_options()).create(
            SandboxCreateParams(runtime="python3.13")
        )

    assert raised.value.status_code == 200
    assert raised.value.data == payload


async def test_create_translates_api_errors(fake_sandbox_api: FakeSandboxAPI) -> None:
    session = Session()
    fake_sandbox_api.install(session)
    fake_sandbox_api.script_response(
        status_code=429,
        json={"error": {"code": "rate_limited", "message": "too many requests"}},
        headers={"retry-after": "7"},
    )

    with pytest.raises(SandboxAPIError) as raised:
        await session.sandbox.with_options(_sandbox_options()).create(
            SandboxCreateParams(runtime="python3.13")
        )

    error = raised.value
    assert isinstance(error, VercelError)
    assert error.status_code == 429
    assert error.retry_after == 7
    assert error.data == {"error": {"code": "rate_limited", "message": "too many requests"}}
    assert "too many requests" in str(error)


def test_sync_create_translates_api_errors_smoke(fake_sandbox_api: FakeSandboxAPI) -> None:
    session = SyncSession()
    fake_sandbox_api.install(session)
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=SyncStaticCredentialProvider(
                AccessTokenCredentials(
                    token="token",
                    project_id="project_1",
                    team_id="team_1",
                )
            )
        )
    )
    fake_sandbox_api.script_response(
        status_code=500,
        json={"message": "server failed"},
    )

    with pytest.raises(SandboxAPIError, match="server failed"):
        accessor.create(SandboxCreateParams(runtime="python3.13"))


async def test_create_rejects_closed_session_before_request(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    session = Session()
    fake_sandbox_api.install(session)
    await session.aclose()

    with pytest.raises(SessionClosedError):
        await session.sandbox.with_options(_sandbox_options()).create(
            SandboxCreateParams(runtime="python3.13")
        )

    assert fake_sandbox_api.requests == []


async def test_create_timeout_exceeded_raises_timeout_error(
    fake_sandbox_api: FakeSandboxAPI,
    sandbox_payload: dict[str, object],
) -> None:
    fake_sandbox_api.script_response(status_code=201, json=sandbox_payload, delay=0.5)
    session = Session()
    fake_sandbox_api.install(session)

    with pytest.raises(SandboxOperationTimeoutError) as raised:
        await session.sandbox.with_options(_sandbox_options()).create(
            SandboxCreateParams(runtime="python3.13"),
            timeout=timedelta(seconds=0.1),
        )

    assert isinstance(raised.value, SandboxError)
    assert isinstance(raised.value, VercelError)
    assert len(fake_sandbox_api.requests) == 1


@dataclass
class _ScriptedTransport(BaseTransport):
    outcomes: list[httpx.Response | httpx.TransportError]
    requests: int = 0

    async def send(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: Any = None,
        body: RequestBody = None,
        headers: Any = None,
        timeout: timedelta | None = None,
        follow_redirects: bool | None = None,
        stream: bool = False,
    ) -> httpx.Response:
        _ = (method, path, token, params, body, headers, timeout, follow_redirects, stream)
        self.requests += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, httpx.TransportError):
            raise outcome
        return outcome


def _success_response() -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "sandbox": {"name": "my-sandbox", "persistent": True},
            "session": {"id": "sbx_test123", "status": "running"},
            "routes": [],
        },
        request=httpx.Request("POST", "https://api.vercel.com/v2/sandboxes"),
    )


async def test_api_client_retries_transport_errors() -> None:
    transport = _ScriptedTransport([httpx.ConnectError("temporary failure"), _success_response()])
    sleeps: list[float] = []
    client = SandboxApiClient(
        transport=transport,
        credentials_resolver=create_sandbox_credentials_resolver(_sandbox_options()),
        sleep_fn=sleeps.append,
        retry_attempts=1,
    )

    sandbox = await client.create(SandboxCreateParams(runtime="python3.13"))

    assert sandbox.name == "my-sandbox"
    assert transport.requests == 2
    assert sleeps == [0.1]


async def test_api_client_retries_retryable_responses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retryable = httpx.Response(
        503,
        json={"message": "try again"},
        request=httpx.Request("POST", "https://api.vercel.com/v2/sandboxes"),
    )
    transport = _ScriptedTransport([retryable, _success_response()])
    sleeps: list[float] = []

    def retry_policy(retry_attempts: int | None) -> RetryPolicy | None:
        return RetryPolicy(
            retries=retry_attempts or 0,
            retry_on_response=lambda response: response.status_code == 503,
        )

    monkeypatch.setattr(api_client_module, "_retry_policy", retry_policy)
    client = SandboxApiClient(
        transport=transport,
        credentials_resolver=create_sandbox_credentials_resolver(_sandbox_options()),
        sleep_fn=sleeps.append,
        retry_attempts=1,
    )

    sandbox = await client.create(SandboxCreateParams(runtime="python3.13"))

    assert sandbox.name == "my-sandbox"
    assert transport.requests == 2
    assert sleeps == [0.1]


def test_sync_api_client_create_completes_with_iter_coroutine(
    sandbox_payload: dict[str, object],
) -> None:
    transport = _ScriptedTransport(
        [
            httpx.Response(
                200,
                json=sandbox_payload,
                request=httpx.Request("POST", "https://api.vercel.com/v2/sandboxes"),
            )
        ]
    )
    client = SandboxApiClient(
        transport=transport,
        credentials_resolver=create_sandbox_credentials_resolver(_sandbox_options()),
        sleep_fn=lambda delay: None,
    )

    sandbox = iter_coroutine(client.create(SandboxCreateParams(runtime="python3.13")))

    assert sandbox.name == "my-sandbox"
    assert transport.requests == 1
