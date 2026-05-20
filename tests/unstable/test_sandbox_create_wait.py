from __future__ import annotations

from datetime import timedelta

import pytest

from tests.unstable.fake_sandbox_api import FakeSandboxAPI
from vercel.unstable import Session, SyncSession
from vercel.unstable.auth import (
    AccessTokenCredentials,
    StaticCredentialProvider,
    SyncStaticCredentialProvider,
)
from vercel.unstable.sandbox import (
    SandboxCreateParams,
    SandboxOperationTimeoutError,
    SandboxOptions,
    SandboxStatus,
    SandboxTerminalStateError,
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


def _pending_payload(name: str = "my-sandbox") -> dict[str, object]:
    return {
        "sandbox": {"name": name, "persistent": True},
        "session": {
            "id": "sbx_test123",
            "status": "pending",
            "requestedAt": 1,
        },
        "routes": [],
    }


def _snapshotting_payload(name: str = "my-sandbox") -> dict[str, object]:
    return {
        "sandbox": {"name": name, "persistent": True},
        "session": {
            "id": "sbx_test123",
            "status": "snapshotting",
            "requestedAt": 1,
        },
        "routes": [],
    }


def _running_payload(name: str = "my-sandbox") -> dict[str, object]:
    return {
        "sandbox": {"name": name, "persistent": True},
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


def _failed_payload(name: str = "my-sandbox") -> dict[str, object]:
    return {
        "sandbox": {"name": name, "persistent": True},
        "session": {
            "id": "sbx_test123",
            "status": "failed",
            "requestedAt": 1,
        },
        "routes": [],
    }


def _aborted_payload(name: str = "my-sandbox") -> dict[str, object]:
    return {
        "sandbox": {"name": name, "persistent": True},
        "session": {
            "id": "sbx_test123",
            "status": "aborted",
            "requestedAt": 1,
        },
        "routes": [],
    }


async def test_create_wait_true_zero_polls_when_running(
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
        SandboxCreateParams(runtime="python3.12", name="my-sandbox"),
        wait=True,
    )

    assert sandbox.name == "my-sandbox"
    assert sandbox.current_session is not None
    assert sandbox.current_session.status == SandboxStatus.RUNNING
    assert len(fake_sandbox_api.requests) == 1
    assert fake_sandbox_api.requests[0].method == "POST"


async def test_create_wait_true_pending_to_running(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    fake_sandbox_api.script_response(status_code=201, json=_pending_payload())
    fake_sandbox_api.script_response_for_path("v2/sandboxes/my-sandbox", json=_running_payload())
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
        SandboxCreateParams(runtime="python3.12", name="my-sandbox"),
        wait=True,
    )

    assert sandbox.current_session is not None
    assert sandbox.current_session.status == SandboxStatus.RUNNING
    assert len(fake_sandbox_api.requests) == 2
    assert fake_sandbox_api.requests[0].method == "POST"
    assert fake_sandbox_api.requests[1].method == "GET"
    assert fake_sandbox_api.requests[1].path == "/v2/sandboxes/my-sandbox"


async def test_create_wait_true_snapshotting_to_running(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    fake_sandbox_api.script_response(status_code=201, json=_snapshotting_payload())
    fake_sandbox_api.script_response_for_path("v2/sandboxes/my-sandbox", json=_running_payload())
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
        SandboxCreateParams(runtime="python3.12", name="my-sandbox"),
        wait=True,
    )

    assert sandbox.current_session is not None
    assert sandbox.current_session.status == SandboxStatus.RUNNING
    assert len(fake_sandbox_api.requests) == 2


async def test_create_wait_true_terminal_failed_on_create(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    fake_sandbox_api.script_response(status_code=201, json=_failed_payload())
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

    with pytest.raises(SandboxTerminalStateError) as raised:
        await accessor.create(
            SandboxCreateParams(runtime="python3.12", name="my-sandbox"),
            wait=True,
        )

    assert "failed" in str(raised.value)
    assert len(fake_sandbox_api.requests) == 1


async def test_create_wait_true_terminal_aborted_during_poll(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    fake_sandbox_api.script_response(status_code=201, json=_pending_payload())
    fake_sandbox_api.script_response_for_path("v2/sandboxes/my-sandbox", json=_aborted_payload())
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

    with pytest.raises(SandboxTerminalStateError) as raised:
        await accessor.create(
            SandboxCreateParams(runtime="python3.12", name="my-sandbox"),
            wait=True,
        )

    assert "aborted" in str(raised.value)
    assert len(fake_sandbox_api.requests) == 2


async def test_create_wait_true_timeout_during_poll(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    fake_sandbox_api.script_response(status_code=201, json=_pending_payload())
    fake_sandbox_api.script_response_for_path(
        "v2/sandboxes/my-sandbox",
        json=_pending_payload(),
        delay=2.0,
    )
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
            SandboxCreateParams(runtime="python3.12", name="my-sandbox"),
            wait=True,
            timeout=timedelta(seconds=0.5),
        )

    assert "exceeded timeout" in str(raised.value)


def test_sync_create_wait_true_pending_to_running(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    fake_sandbox_api.script_response(status_code=201, json=_pending_payload())
    fake_sandbox_api.script_response_for_path("v2/sandboxes/my-sandbox", json=_running_payload())
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

    sandbox = accessor.create(
        SandboxCreateParams(runtime="python3.12", name="my-sandbox"),
        wait=True,
    )

    assert sandbox.current_session is not None
    assert sandbox.current_session.status == SandboxStatus.RUNNING
    assert len(fake_sandbox_api.requests) == 2
    assert fake_sandbox_api.requests[0].method == "POST"
    assert fake_sandbox_api.requests[1].method == "GET"


def test_sync_create_wait_true_terminal_failure(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    fake_sandbox_api.script_response(status_code=201, json=_pending_payload())
    fake_sandbox_api.script_response_for_path("v2/sandboxes/my-sandbox", json=_failed_payload())
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

    with pytest.raises(SandboxTerminalStateError) as raised:
        accessor.create(
            SandboxCreateParams(runtime="python3.12", name="my-sandbox"),
            wait=True,
        )

    assert "failed" in str(raised.value)
    assert len(fake_sandbox_api.requests) == 2
