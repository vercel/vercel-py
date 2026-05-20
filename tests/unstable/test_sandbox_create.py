from __future__ import annotations

from datetime import timedelta

import pytest

from tests.unstable.fake_sandbox_api import FakeSandboxAPI
from vercel._internal.unstable.errors import SessionClosedError
from vercel._internal.unstable.sandbox.request_client import _USER_AGENT
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
            "runtime": "python3.12",
            "timeout": 300000,
            "status": "running",
            "requestedAt": 1,
            "startedAt": 2,
            "cwd": "/vercel/sandbox",
        },
        "routes": [],
    }


def test_create_request_plumbing_is_lazy_until_io(fake_sandbox_api: FakeSandboxAPI) -> None:
    session = Session()
    session._sandbox_transport = fake_sandbox_api
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=StaticCredentialProvider(
                AccessTokenCredentials(
                    token="token",
                    project_id="project_123",
                    team_id="team_123",
                )
            )
        )
    )

    assert accessor._ops_client is None
    assert session._close_hooks == []
    assert fake_sandbox_api.requests == []


def test_sync_create_request_plumbing_is_lazy_until_io(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    session = SyncSession()
    session._sandbox_transport = fake_sandbox_api
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=SyncStaticCredentialProvider(
                AccessTokenCredentials(
                    token="token",
                    project_id="project_123",
                    team_id="team_123",
                )
            )
        )
    )

    assert accessor._ops_client is None
    assert session._close_hooks == []
    assert fake_sandbox_api.requests == []


async def test_create_sends_authenticated_post_and_returns_handle(
    fake_sandbox_api: FakeSandboxAPI,
    sandbox_payload: dict[str, object],
) -> None:
    fake_sandbox_api.script_response(status_code=201, json=sandbox_payload)
    session = Session()
    session._sandbox_transport = fake_sandbox_api
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=StaticCredentialProvider(
                AccessTokenCredentials(
                    token="token_1",
                    project_id="project_1",
                    team_id="team_1",
                )
            )
        )
    )

    sandbox = await accessor.create(
        SandboxCreateParams(
            runtime="python3.12",
            ports=[],
            interactive=False,
            name="my-sandbox",
            env={"EXAMPLE": "1"},
        )
    )

    assert sandbox.name == "my-sandbox"
    assert sandbox.persistent is True
    assert sandbox.current_session is not None
    assert sandbox.current_session.id == "sbx_test123"
    assert sandbox.current_session.status == SandboxStatus.RUNNING
    assert sandbox._session is session
    assert len(fake_sandbox_api.requests) == 1
    request = fake_sandbox_api.requests[0]
    assert request.method == "POST"
    assert request.path == "/v2/sandboxes"
    assert request.query == {"teamId": "team_1"}
    assert request.headers["authorization"] == "Bearer token_1"
    assert request.headers["content-type"] == "application/json"
    assert request.headers["user-agent"] == _USER_AGENT
    assert request.body == {
        "projectId": "project_1",
        "ports": [],
        "name": "my-sandbox",
        "runtime": "python3.12",
        "__interactive": False,
        "env": {"EXAMPLE": "1"},
    }
    assert accessor._ops_client is not None
    assert len(session._close_hooks) == 1


def test_sync_create_sends_authenticated_post_and_returns_handle(
    fake_sandbox_api: FakeSandboxAPI,
    sandbox_payload: dict[str, object],
) -> None:
    fake_sandbox_api.script_response(status_code=201, json=sandbox_payload)
    session = SyncSession()
    session._sandbox_transport = fake_sandbox_api
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

    sandbox = accessor.create(
        SandboxCreateParams(
            runtime="python3.12",
            ports=[],
            interactive=False,
            name="my-sandbox",
            env={"EXAMPLE": "1"},
        )
    )

    assert sandbox.name == "my-sandbox"
    assert sandbox.persistent is True
    assert sandbox.current_session is not None
    assert sandbox.current_session.id == "sbx_test123"
    assert sandbox.current_session.status == SandboxStatus.RUNNING
    assert sandbox._session is session
    assert len(fake_sandbox_api.requests) == 1
    request = fake_sandbox_api.requests[0]
    assert request.method == "POST"
    assert request.path == "/v2/sandboxes"
    assert request.query == {"teamId": "team_1"}
    assert request.headers["authorization"] == "Bearer token_1"
    assert request.headers["content-type"] == "application/json"
    assert request.headers["user-agent"] == _USER_AGENT
    assert request.body == {
        "projectId": "project_1",
        "ports": [],
        "name": "my-sandbox",
        "runtime": "python3.12",
        "__interactive": False,
        "env": {"EXAMPLE": "1"},
    }
    assert sandbox._raw is not None
    assert accessor._ops_client is not None
    assert len(session._close_hooks) == 1


async def test_create_resolves_provider_at_request_time(
    fake_sandbox_api: FakeSandboxAPI,
    sandbox_payload: dict[str, object],
) -> None:
    session = Session()
    session._sandbox_transport = fake_sandbox_api

    class RotatingProvider:
        def __init__(self) -> None:
            self.credentials = AccessTokenCredentials(
                token="token_before",
                project_id="project_before",
                team_id="team_before",
            )

        async def resolve(self) -> AccessTokenCredentials:
            return self.credentials

    provider = RotatingProvider()
    accessor = session.sandbox.with_options(SandboxOptions(credential_provider=provider))
    provider.credentials = AccessTokenCredentials(
        token="token_after",
        project_id="project_after",
        team_id="team_after",
    )
    fake_sandbox_api.script_response(status_code=201, json=sandbox_payload)

    await accessor.create(SandboxCreateParams(runtime="python3.12"))

    request = fake_sandbox_api.requests[0]
    assert request.headers["authorization"] == "Bearer token_after"
    assert request.query == {"teamId": "team_after"}
    assert request.body["projectId"] == "project_after"


def test_sync_create_resolves_provider_at_request_time(
    fake_sandbox_api: FakeSandboxAPI,
    sandbox_payload: dict[str, object],
) -> None:
    session = SyncSession()
    session._sandbox_transport = fake_sandbox_api

    class RotatingProvider:
        def __init__(self) -> None:
            self.credentials = AccessTokenCredentials(
                token="token_before",
                project_id="project_before",
                team_id="team_before",
            )

        def resolve(self) -> AccessTokenCredentials:
            return self.credentials

    provider = RotatingProvider()
    accessor = session.sandbox.with_options(SandboxOptions(credential_provider=provider))
    provider.credentials = AccessTokenCredentials(
        token="token_after",
        project_id="project_after",
        team_id="team_after",
    )
    fake_sandbox_api.script_response(status_code=201, json=sandbox_payload)

    accessor.create(SandboxCreateParams(runtime="python3.12"))

    request = fake_sandbox_api.requests[0]
    assert request.headers["authorization"] == "Bearer token_after"
    assert request.query == {"teamId": "team_after"}
    assert request.body["projectId"] == "project_after"


async def test_create_uses_clone_specific_options(
    fake_sandbox_api: FakeSandboxAPI,
    sandbox_payload: dict[str, object],
) -> None:
    session = Session()
    session._sandbox_transport = fake_sandbox_api
    parent = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=StaticCredentialProvider(
                AccessTokenCredentials(
                    token="parent",
                    project_id="project_parent",
                    team_id="team_parent",
                )
            )
        )
    )
    assert parent._get_ops_client() is parent._ops_client
    clone = parent.with_options(
        SandboxOptions(
            credential_provider=StaticCredentialProvider(
                AccessTokenCredentials(
                    token="clone",
                    project_id="project_clone",
                    team_id="team_clone",
                )
            )
        )
    )
    fake_sandbox_api.script_response(
        status_code=201,
        json=replace_payload_id(sandbox_payload, "sbx_parent"),
    )
    fake_sandbox_api.script_response(
        status_code=201,
        json=replace_payload_id(sandbox_payload, "sbx_clone"),
    )

    parent_sandbox = await parent.create(SandboxCreateParams(runtime="python3.12"))
    clone_sandbox = await clone.create(SandboxCreateParams(runtime="python3.12"))

    assert parent_sandbox.current_session is not None
    assert parent_sandbox.current_session.id == "sbx_parent"
    assert clone_sandbox.current_session is not None
    assert clone_sandbox.current_session.id == "sbx_clone"
    assert fake_sandbox_api.requests[0].headers["authorization"] == "Bearer parent"
    assert fake_sandbox_api.requests[1].headers["authorization"] == "Bearer clone"


def test_sync_create_uses_clone_specific_options(
    fake_sandbox_api: FakeSandboxAPI,
    sandbox_payload: dict[str, object],
) -> None:
    session = SyncSession()
    session._sandbox_transport = fake_sandbox_api
    parent = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=SyncStaticCredentialProvider(
                AccessTokenCredentials(
                    token="parent",
                    project_id="project_parent",
                    team_id="team_parent",
                )
            )
        )
    )
    assert parent._get_ops_client() is parent._ops_client
    clone = parent.with_options(
        SandboxOptions(
            credential_provider=SyncStaticCredentialProvider(
                AccessTokenCredentials(
                    token="clone",
                    project_id="project_clone",
                    team_id="team_clone",
                )
            )
        )
    )
    fake_sandbox_api.script_response(
        status_code=201,
        json=replace_payload_id(sandbox_payload, "sbx_parent"),
    )
    fake_sandbox_api.script_response(
        status_code=201,
        json=replace_payload_id(sandbox_payload, "sbx_clone"),
    )

    parent_sandbox = parent.create(SandboxCreateParams(runtime="python3.12"))
    clone_sandbox = clone.create(SandboxCreateParams(runtime="python3.12"))

    assert parent_sandbox.current_session is not None
    assert parent_sandbox.current_session.id == "sbx_parent"
    assert clone_sandbox.current_session is not None
    assert clone_sandbox.current_session.id == "sbx_clone"
    assert fake_sandbox_api.requests[0].headers["authorization"] == "Bearer parent"
    assert fake_sandbox_api.requests[1].headers["authorization"] == "Bearer clone"


async def test_create_translates_api_errors(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    session = Session()
    session._sandbox_transport = fake_sandbox_api
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=StaticCredentialProvider(
                AccessTokenCredentials(
                    token="token",
                    project_id="project_1",
                    team_id="team_1",
                )
            )
        )
    )
    fake_sandbox_api.script_response(
        status_code=429,
        json={"error": {"code": "rate_limited", "message": "too many requests"}},
        headers={"retry-after": "7"},
    )

    with pytest.raises(SandboxAPIError) as raised:
        await accessor.create(SandboxCreateParams(runtime="python3.12"))

    error = raised.value
    assert isinstance(error, VercelError)
    assert error.status_code == 429
    assert error.retry_after == 7
    assert error.data == {"error": {"code": "rate_limited", "message": "too many requests"}}
    assert "too many requests" in str(error)


def test_sync_create_translates_api_errors(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    session = SyncSession()
    session._sandbox_transport = fake_sandbox_api
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
        status_code=429,
        json={"error": {"code": "rate_limited", "message": "too many requests"}},
        headers={"retry-after": "7"},
    )

    with pytest.raises(SandboxAPIError) as raised:
        accessor.create(SandboxCreateParams(runtime="python3.12"))

    error = raised.value
    assert isinstance(error, VercelError)
    assert error.status_code == 429
    assert error.retry_after == 7
    assert error.data == {"error": {"code": "rate_limited", "message": "too many requests"}}
    assert "too many requests" in str(error)


async def test_create_rejects_closed_session_before_request(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    session = Session()
    session._sandbox_transport = fake_sandbox_api
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=StaticCredentialProvider(
                AccessTokenCredentials(
                    token="token",
                    project_id="project_1",
                    team_id="team_1",
                )
            )
        )
    )
    await session.aclose()

    with pytest.raises(SessionClosedError):
        await accessor.create(SandboxCreateParams(runtime="python3.12"))

    assert fake_sandbox_api.requests == []


def test_sync_create_rejects_closed_session_before_request(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    session = SyncSession()
    session._sandbox_transport = fake_sandbox_api
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
    session.close()

    with pytest.raises(SessionClosedError):
        accessor.create(SandboxCreateParams(runtime="python3.12"))

    assert fake_sandbox_api.requests == []


async def test_session_aclose_closes_initialized_sandbox_ops_client(
    fake_sandbox_api: FakeSandboxAPI,
    sandbox_payload: dict[str, object],
) -> None:
    session = Session()
    session._sandbox_transport = fake_sandbox_api
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=StaticCredentialProvider(
                AccessTokenCredentials(
                    token="token",
                    project_id="project_1",
                    team_id="team_1",
                )
            )
        )
    )
    fake_sandbox_api.script_response(status_code=201, json=sandbox_payload)

    await accessor.create(SandboxCreateParams(runtime="python3.12"))
    await session.aclose()

    assert accessor._ops_client is None


def test_sync_session_close_closes_initialized_sandbox_ops_client(
    fake_sandbox_api: FakeSandboxAPI,
    sandbox_payload: dict[str, object],
) -> None:
    session = SyncSession()
    session._sandbox_transport = fake_sandbox_api
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
    fake_sandbox_api.script_response(status_code=201, json=sandbox_payload)

    accessor.create(SandboxCreateParams(runtime="python3.12"))
    session.close()
    session.close()

    assert accessor._ops_client is None
    assert session._close_hooks == []

    with pytest.raises(SessionClosedError):
        accessor.create(SandboxCreateParams(runtime="python3.12"))

    assert len(fake_sandbox_api.requests) == 1


async def test_create_omits_interactive_when_none(
    fake_sandbox_api: FakeSandboxAPI,
    sandbox_payload: dict[str, object],
) -> None:
    session = Session()
    session._sandbox_transport = fake_sandbox_api
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=StaticCredentialProvider(
                AccessTokenCredentials(
                    token="token",
                    project_id="project_1",
                    team_id="team_1",
                )
            )
        )
    )
    fake_sandbox_api.script_response(status_code=201, json=sandbox_payload)

    await accessor.create(SandboxCreateParams(runtime="python3.12", interactive=None))

    assert "__interactive" not in fake_sandbox_api.requests[0].body


def replace_payload_id(payload: dict[str, object], sandbox_id: str) -> dict[str, object]:
    session = dict(cast_dict(payload.get("session", {})))
    session["id"] = sandbox_id
    return {**payload, "session": session}


def cast_dict(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return value


async def test_create_timeout_exceeded_raises_timeout_error(
    fake_sandbox_api: FakeSandboxAPI,
    sandbox_payload: dict[str, object],
) -> None:
    fake_sandbox_api.script_response(status_code=201, json=sandbox_payload, delay=0.5)
    session = Session()
    session._sandbox_transport = fake_sandbox_api
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=StaticCredentialProvider(
                AccessTokenCredentials(
                    token="token",
                    project_id="project_1",
                    team_id="team_1",
                )
            )
        )
    )

    with pytest.raises(SandboxOperationTimeoutError) as raised:
        await accessor.create(
            SandboxCreateParams(runtime="python3.12"),
            timeout=timedelta(seconds=0.1),
        )

    assert isinstance(raised.value, SandboxError)
    assert isinstance(raised.value, VercelError)
    assert len(fake_sandbox_api.requests) == 1


async def test_create_timeout_not_exceeded_returns_handle(
    fake_sandbox_api: FakeSandboxAPI,
    sandbox_payload: dict[str, object],
) -> None:
    fake_sandbox_api.script_response(status_code=201, json=sandbox_payload)
    session = Session()
    session._sandbox_transport = fake_sandbox_api
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=StaticCredentialProvider(
                AccessTokenCredentials(
                    token="token",
                    project_id="project_1",
                    team_id="team_1",
                )
            )
        )
    )

    sandbox = await accessor.create(
        SandboxCreateParams(runtime="python3.12"),
        timeout=timedelta(seconds=60),
    )

    assert sandbox.name == "my-sandbox"
    assert sandbox.current_session is not None
    assert sandbox.current_session.id == "sbx_test123"
    assert len(fake_sandbox_api.requests) == 1


async def test_create_without_timeout_works_normally(
    fake_sandbox_api: FakeSandboxAPI,
    sandbox_payload: dict[str, object],
) -> None:
    fake_sandbox_api.script_response(status_code=201, json=sandbox_payload)
    session = Session()
    session._sandbox_transport = fake_sandbox_api
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=StaticCredentialProvider(
                AccessTokenCredentials(
                    token="token",
                    project_id="project_1",
                    team_id="team_1",
                )
            )
        )
    )

    sandbox = await accessor.create(SandboxCreateParams(runtime="python3.12"))

    assert sandbox.name == "my-sandbox"
    assert sandbox.current_session is not None
    assert sandbox.current_session.id == "sbx_test123"
    assert len(fake_sandbox_api.requests) == 1


def test_sync_create_has_no_timeout_parameter(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    session = SyncSession()
    session._sandbox_transport = fake_sandbox_api
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

    import inspect

    sig = inspect.signature(accessor.create)
    assert "timeout" not in sig.parameters
