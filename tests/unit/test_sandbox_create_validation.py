import warnings
from typing import Any, cast

import pytest

import vercel.sandbox.sandbox as sandbox_module
from vercel._internal.sandbox.models import (
    GitSource,
    Resources,
    Sandbox,
    SandboxAndRoutesResponse,
    SandboxStatus,
    parse_resources,
    parse_source,
)
from vercel.oidc.types import Credentials
from vercel.sandbox import SandboxValidationError
from vercel.sandbox.sandbox import Sandbox as SyncSandbox


def test_parse_source_accumulates_issues() -> None:
    with pytest.raises(SandboxValidationError) as exc_info:
        parse_source(
            {
                "type": "git",
                "depth": 0,
                "username": "scott",
                "unexpected": True,
            }
        )

    issues = {(issue.path, issue.message) for issue in exc_info.value.issues}
    assert ("source.url", "is required") in issues
    assert ("source.depth", "must be a positive integer") in issues
    assert ("source", "git username and password must be provided together") in issues


def test_parse_source_drops_unknown_keys() -> None:
    source = parse_source(
        {
            "type": "git",
            "url": "https://github.com/vercel/vercel-py",
            "revision": "main",
            "unexpected": True,
        }
    )

    assert source is not None
    assert source.model_dump(by_alias=True, exclude_none=True) == {
        "type": "git",
        "url": "https://github.com/vercel/vercel-py",
        "revision": "main",
    }


def test_parse_source_accepts_camel_case_snapshot_id() -> None:
    source = parse_source({"type": "snapshot", "snapshotId": "snap_123"})
    assert source is not None
    assert source.model_dump(by_alias=True, exclude_none=True) == {
        "type": "snapshot",
        "snapshotId": "snap_123",
    }


def test_parse_source_validates_mapping_field_types() -> None:
    with pytest.raises(SandboxValidationError) as exc_info:
        parse_source(
            {
                "type": "git",
                "url": 123,
                "depth": "1",
            }
        )

    issues = {(issue.path, issue.message) for issue in exc_info.value.issues}
    assert ("source.url", "must be a string") in issues
    assert ("source.depth", "must be an integer") in issues


def test_parse_source_validates_non_git_mapping_field_types() -> None:
    with pytest.raises(SandboxValidationError) as exc_info:
        parse_source({"type": "tarball", "url": 123})

    assert {(issue.path, issue.message) for issue in exc_info.value.issues} == {
        ("source.url", "must be a string")
    }

    with pytest.raises(SandboxValidationError) as exc_info:
        parse_source({"type": "snapshot", "snapshot_id": 123})

    assert {(issue.path, issue.message) for issue in exc_info.value.issues} == {
        ("source.snapshot_id", "must be a string")
    }


def test_parse_resources_accumulates_issues() -> None:
    with pytest.raises(SandboxValidationError) as exc_info:
        parse_resources({"vcpus": 3, "memory": 4096, "extra": "nope"})

    issues = {(issue.path, issue.message) for issue in exc_info.value.issues}
    assert ("resources.vcpus", "must be even") in issues
    assert ("resources.memory", "must equal resources.vcpus * 2048 (6144)") in issues


def test_parse_resources_drops_unknown_keys() -> None:
    resources = parse_resources({"vcpus": 2, "memory": 4096, "extra": "nope"})

    assert resources == Resources(vcpus=2, memory=4096)


class _RecordingSyncSandboxOpsClient:
    def __init__(self, *, team_id: str, token: str) -> None:
        self.team_id = team_id
        self.token = token
        self.calls: list[dict[str, object]] = []

    async def create_sandbox(self, **kwargs: object) -> SandboxAndRoutesResponse:
        self.calls.append(kwargs)
        return SandboxAndRoutesResponse(
            sandbox=Sandbox(
                id="sbx_123",
                memory=4096,
                vcpus=2,
                region="iad1",
                runtime="node22",
                timeout=60_000,
                status=SandboxStatus.PENDING,
                requestedAt=0,
                createdAt=0,
                cwd="/",
                updatedAt=0,
            ),
            routes=[],
        )

    def close(self) -> None:
        return None


def test_sandbox_create_warns_for_mapping_source_and_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _RecordingSyncSandboxOpsClient(team_id="team_123", token="token_123")
    monkeypatch.setattr(
        sandbox_module,
        "get_credentials",
        lambda **_: Credentials(
            token="token_123",
            project_id="project_123",
            team_id="team_123",
        ),
    )
    monkeypatch.setattr(sandbox_module, "SyncSandboxOpsClient", lambda **_: client)

    with pytest.warns(DeprecationWarning) as record:
        sandbox = SyncSandbox.create(
            source=cast(Any, {"type": "git", "url": "https://github.com/vercel/vercel-py"}),
            resources=cast(Any, {"vcpus": 2, "memory": 4096}),
        )

    assert sandbox.sandbox_id == "sbx_123"
    assert [str(w.message) for w in record] == [
        "Passing a raw mapping for Sandbox.create(..., source=...) is deprecated; "
        "pass a typed GitSource model instead.",
        "Passing a raw mapping for Sandbox.create(..., resources=...) is deprecated; "
        "pass a typed Resources model instead.",
    ]
    assert client.calls == [
        {
            "project_id": "project_123",
            "source": GitSource(type="git", url="https://github.com/vercel/vercel-py"),
            "ports": None,
            "timeout": None,
            "resources": Resources(vcpus=2, memory=4096),
            "runtime": None,
            "interactive": False,
            "env": None,
            "network_policy": None,
        }
    ]


def test_sandbox_create_does_not_warn_for_typed_models(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _RecordingSyncSandboxOpsClient(team_id="team_123", token="token_123")
    monkeypatch.setattr(
        sandbox_module,
        "get_credentials",
        lambda **_: Credentials(
            token="token_123",
            project_id="project_123",
            team_id="team_123",
        ),
    )
    monkeypatch.setattr(sandbox_module, "SyncSandboxOpsClient", lambda **_: client)

    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        sandbox = SyncSandbox.create(
            source=GitSource(type="git", url="https://github.com/vercel/vercel-py"),
            resources=Resources(vcpus=2, memory=4096),
        )

    assert sandbox.sandbox_id == "sbx_123"
    assert len(record) == 0
