from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta

import pytest

from tests.unstable.fake_sandbox_api import FakeSandboxAPI
from vercel._internal.unstable.sandbox import accessor as accessor_module
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


def _payload(status: SandboxStatus | None, *, name: str = "my-sandbox") -> dict[str, object]:
    session: dict[str, object] = {"id": "sbx_test123", "requestedAt": 1}
    if status is not None:
        session["status"] = status.value
    if status is SandboxStatus.RUNNING:
        session.update(
            {
                "memory": 1024,
                "vcpus": 2,
                "region": "iad1",
                "runtime": "python3.13",
                "timeout": 300000,
                "startedAt": 2,
                "cwd": "/vercel/sandbox",
            }
        )
    return {
        "sandbox": {"name": name, "persistent": True},
        "session": session,
        "routes": [],
    }


async def _no_sleep(delay: float) -> None:
    _ = delay


@pytest.mark.parametrize(
    ("statuses", "terminal"),
    [
        ([SandboxStatus.RUNNING], False),
        ([SandboxStatus.PENDING, SandboxStatus.RUNNING], False),
        ([SandboxStatus.SNAPSHOTTING, SandboxStatus.RUNNING], False),
        ([None, SandboxStatus.PENDING, SandboxStatus.RUNNING], False),
        ([SandboxStatus.FAILED], True),
        ([SandboxStatus.PENDING, SandboxStatus.ABORTED], True),
        ([SandboxStatus.PENDING, SandboxStatus.STOPPED], True),
        ([SandboxStatus.PENDING, SandboxStatus.STOPPING], True),
    ],
)
async def test_create_wait_follows_status_sequence(
    monkeypatch: pytest.MonkeyPatch,
    fake_sandbox_api: FakeSandboxAPI,
    statuses: Sequence[SandboxStatus | None],
    terminal: bool,
) -> None:
    monkeypatch.setattr(accessor_module.anyio, "sleep", _no_sleep)
    fake_sandbox_api.script_response(status_code=201, json=_payload(statuses[0]))
    for status in statuses[1:]:
        fake_sandbox_api.script_response_for_path("v2/sandboxes/my-sandbox", json=_payload(status))
    session = Session()
    fake_sandbox_api.install(session)
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

    if terminal:
        with pytest.raises(SandboxTerminalStateError):
            await accessor.create(
                SandboxCreateParams(runtime="python3.13", name="my-sandbox"),
                wait=True,
            )
    else:
        sandbox = await accessor.create(
            SandboxCreateParams(runtime="python3.13", name="my-sandbox"),
            wait=True,
        )
        assert sandbox.current_session is not None
        assert sandbox.current_session.status == SandboxStatus.RUNNING

    assert len(fake_sandbox_api.requests) == len(statuses)
    assert fake_sandbox_api.requests[0].method == "POST"
    for request in fake_sandbox_api.requests[1:]:
        assert request.method == "GET"
        assert request.path == "/v2/sandboxes/my-sandbox"
        assert request.body is None


def test_sync_create_wait_pending_to_running_smoke(
    monkeypatch: pytest.MonkeyPatch,
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    monkeypatch.setattr(accessor_module.time, "sleep", lambda delay: None)
    fake_sandbox_api.script_response(status_code=201, json=_payload(SandboxStatus.PENDING))
    fake_sandbox_api.script_response_for_path(
        "v2/sandboxes/my-sandbox",
        json=_payload(SandboxStatus.RUNNING),
    )
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

    sandbox = accessor.create(
        SandboxCreateParams(runtime="python3.13", name="my-sandbox"),
        wait=True,
    )

    assert sandbox.current_session is not None
    assert sandbox.current_session.status == SandboxStatus.RUNNING
    assert [request.method for request in fake_sandbox_api.requests] == ["POST", "GET"]
    assert fake_sandbox_api.requests[1].body is None


def test_sync_create_wait_terminal_smoke(
    monkeypatch: pytest.MonkeyPatch,
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    monkeypatch.setattr(accessor_module.time, "sleep", lambda delay: None)
    fake_sandbox_api.script_response(status_code=201, json=_payload(SandboxStatus.PENDING))
    fake_sandbox_api.script_response_for_path(
        "v2/sandboxes/my-sandbox",
        json=_payload(SandboxStatus.FAILED),
    )
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

    with pytest.raises(SandboxTerminalStateError):
        accessor.create(
            SandboxCreateParams(runtime="python3.13", name="my-sandbox"),
            wait=True,
        )

    assert [request.method for request in fake_sandbox_api.requests] == ["POST", "GET"]


def test_sync_create_timeout_bounds_wait(
    monkeypatch: pytest.MonkeyPatch,
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    clock = 0.0

    def monotonic() -> float:
        return clock

    def sleep(delay: float) -> None:
        nonlocal clock
        clock += delay

    monkeypatch.setattr(accessor_module.time, "monotonic", monotonic)
    monkeypatch.setattr(accessor_module.time, "sleep", sleep)
    fake_sandbox_api.script_response(status_code=201, json=_payload(SandboxStatus.PENDING))
    fake_sandbox_api.script_response_for_path(
        "v2/sandboxes/my-sandbox",
        json=_payload(SandboxStatus.RUNNING),
    )
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

    with pytest.raises(SandboxOperationTimeoutError):
        accessor.create(
            SandboxCreateParams(runtime="python3.13", name="my-sandbox"),
            wait=True,
            timeout=timedelta(seconds=0.5),
        )

    assert [request.method for request in fake_sandbox_api.requests] == ["POST"]
