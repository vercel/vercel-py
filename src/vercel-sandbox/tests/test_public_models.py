"""Validation and serialization contracts for public Sandbox values."""

import dataclasses
import json
from datetime import timedelta
from subprocess import CalledProcessError
from types import SimpleNamespace

import pytest
from pydantic import BaseModel, ValidationError

from vercel import sandbox
from vercel._internal.core.options import ServiceOptions
from vercel.sandbox._internal.models import _serialize_network_policy
from vercel.sandbox._internal.state import SandboxRouteState


@pytest.mark.parametrize("module", [sandbox, sandbox.sync])
def test_public_exports_do_not_expose_dataclasses(module) -> None:
    for name in module.__all__:
        assert not dataclasses.is_dataclass(getattr(module, name)), name


@pytest.mark.parametrize(
    ("model", "data"),
    [
        (sandbox.NetworkPolicyMatcher, {"kind": "exact", "value": "/api"}),
        (sandbox.NetworkPolicyKeyValueMatcher, {"key": {"kind": "exact", "value": "token"}}),
        (sandbox.NetworkPolicyRequestMatcher, {"method": ["GET"]}),
        (sandbox.NetworkPolicyTransform, {"headers": {"X-Token": "value"}}),
        (sandbox.NetworkPolicyResponse, {"status_code": 403}),
        (sandbox.NetworkPolicyRule, {"forward_url": "https://example.com"}),
        (sandbox.NetworkPolicyInjectionRuleSummary, {"domain": "example.com"}),
        (
            sandbox.NetworkPolicyForwardRuleSummary,
            {"domain": "example.com", "forward_url": "https://proxy.example.com"},
        ),
        (sandbox.NetworkPolicyResponseRuleSummary, {"domain": "example.com", "status_code": 403}),
        (sandbox.NetworkPolicySubnets, {"allow": ["10.0.0.0/8"]}),
        (sandbox.NetworkPolicy, {"mode": "custom", "allow": {"example.com": []}}),
        (sandbox.NetworkPolicyReadback, {"mode": "custom", "allowed_domains": ["example.com"]}),
        (sandbox.SnapshotExpiration, {"value": 86400}),
        (sandbox.DirectoryEntry, {"path": "app.py", "kind": "file"}),
        (sandbox.DriveMount, {"drive": "data"}),
        (sandbox.SandboxMount, {"drive": "data", "mode": "snapshot"}),
        (
            sandbox.SandboxCredentials,
            {"token": "token", "team_id": "team", "project_id": "project"},
        ),
        (sandbox.SnapshotRetentionState, {"count": 3, "expiration": timedelta(days=1)}),
        (SandboxRouteState, {"url": "https://example.com", "port": 3000, "subdomain": "app"}),
        (
            sandbox.CompletedProcess,
            {
                "id": "cmd_1",
                "name": "echo",
                "args": ["echo", "hello"],
                "cwd": "/vercel/sandbox",
                "session_id": "sbx_1",
                "started_at": 1,
                "returncode": 0,
                "stdout": "hello\n",
                "stderr": None,
            },
        ),
    ],
)
def test_public_values_are_frozen_models_with_json_roundtrips(
    model: type[BaseModel], data: dict[str, object]
) -> None:
    value = model.model_validate(data)
    assert not dataclasses.is_dataclass(value)
    assert model.model_validate(value.model_dump()) == value
    assert model.model_validate_json(value.model_dump_json()) == value
    assert model.model_json_schema()["type"] == "object"
    field = next(iter(model.model_fields))
    with pytest.raises(ValidationError, match="frozen"):
        setattr(value, field, getattr(value, field))


def test_nested_network_policy_validation_and_serialization() -> None:
    data = {
        "mode": "custom",
        "allow": {
            "api.example.com": [
                {
                    "match": {"path": {"kind": "starts_with", "value": "/api"}},
                    "transform": [{"headers": {"X-Token": "secret"}}],
                }
            ]
        },
    }
    policy = sandbox.NetworkPolicy.model_validate(data)
    assert policy.allow["api.example.com"][0].match == sandbox.NetworkPolicyRequestMatcher(
        path=sandbox.NetworkPolicyMatcher.starts_with("/api")
    )
    assert json.loads(policy.model_dump_json(exclude_none=True))["allow"] == data["allow"]
    assert _serialize_network_policy(policy) == {
        "allow": {
            "api.example.com": [
                {
                    "match": {"path": {"startsWith": "/api"}},
                    "transform": [{"headers": {"X-Token": "secret"}}],
                }
            ]
        }
    }
    with pytest.raises(TypeError):
        policy.allow["other.example.com"] = ()  # type: ignore[index]
    headers = policy.allow["api.example.com"][0].transform[0].headers
    assert headers is not None
    with pytest.raises(TypeError):
        headers["X-Token"] = "changed"  # type: ignore[index]


@pytest.mark.parametrize(
    ("model", "data"),
    [
        (sandbox.NetworkPolicyMatcher, {"kind": "unknown", "value": "test"}),
        (sandbox.NetworkPolicyKeyValueMatcher, {}),
        (sandbox.NetworkPolicyRequestMatcher, {"method": []}),
        (sandbox.NetworkPolicyRequestMatcher, {"method": "GET"}),
        (sandbox.NetworkPolicyTransform, {"headers": {"X-Token": 1}}),
        (sandbox.NetworkPolicyResponse, {"status_code": True}),
        (sandbox.NetworkPolicyResponse, {"status_code": "403"}),
        (sandbox.NetworkPolicyResponse, {"status_code": 403, "headers": {"X-A": "1", "x-a": "2"}}),
        (sandbox.NetworkPolicyRule, {"transform": [], "response": None}),
        (sandbox.NetworkPolicyRule, {"response": {"status_code": 204, "body": "no"}}),
        (sandbox.NetworkPolicyResponseRuleSummary, {"domain": "example.com", "status_code": 600}),
        (sandbox.NetworkPolicySubnets, {"allow": "10.0.0.0/8"}),
        (sandbox.NetworkPolicy, {"mode": "allow-all", "allow": {"example.com": []}}),
        (sandbox.NetworkPolicyReadback, {"mode": "invalid"}),
        (sandbox.SnapshotExpiration, {"value": 1}),
        (sandbox.DirectoryEntry, {"path": "app.py", "kind": "invalid"}),
        (sandbox.DirectoryEntry, {"path": "app.py", "kind": "file", "unexpected": True}),
        (sandbox.DriveMount, {"drive": "data", "mode": "invalid"}),
        (sandbox.SandboxMount, {"drive": "data", "mode": "invalid"}),
        (sandbox.SandboxCredentials, {"token": 1, "team_id": "team", "project_id": "project"}),
    ],
)
def test_public_model_validation_rejects_invalid_values(
    model: type[BaseModel], data: dict[str, object]
) -> None:
    with pytest.raises(ValidationError):
        model.model_validate(data)


@pytest.mark.parametrize(
    "model", [sandbox.SandboxServiceOptions, sandbox.sync.SandboxServiceOptions]
)
def test_service_options_are_models_and_keep_defaults_and_factory_identity(
    model, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERCEL_REGION", "iad1")
    options = model(base_url=None, file_transfer_timeout=None, region=None)
    assert issubclass(type(options), BaseModel)
    assert issubclass(type(options), ServiceOptions)
    assert options.base_url == "https://vercel.com/api"
    assert options.region == "iad1"
    assert options.file_transfer_timeout == timedelta(minutes=5)
    assert callable(options.credentials_factory)
    assert options.model_copy().credentials_factory is options.credentials_factory
    assert model.model_validate(options.model_dump()) == options
    with pytest.raises(ValidationError, match="frozen"):
        options.region = "sfo1"
    with pytest.raises(ValidationError, match="callable"):
        model.model_validate({"credentials_factory": "not callable"})


def test_drive_mount_accepts_handles_and_serializes_the_name() -> None:
    drive = SimpleNamespace(name="data", project_id="project", region="iad1")
    mount = sandbox.DriveMount(drive)
    assert mount.drive is drive
    assert mount.to_api_dict() == {"drive": "data", "mode": "read-write"}
    assert mount.model_dump() == mount.to_api_dict()
    assert json.loads(mount.model_dump_json()) == mount.to_api_dict()


def test_convenience_constructors_and_snapshot_retention_payload() -> None:
    assert sandbox.SandboxCredentials("token", "team", "project").token == "token"
    expiration = sandbox.SnapshotExpiration(timedelta(days=2))
    assert sandbox.SnapshotExpiration(172800) == expiration
    assert sandbox.SnapshotExpiration(0).value == timedelta(0)
    assert sandbox.NetworkPolicyMatcher("exact", "value") == sandbox.NetworkPolicyMatcher.exact(
        "value"
    )
    retention = sandbox.SnapshotRetention(count=3, expiration=expiration)
    assert sandbox.SnapshotRetention.model_validate_json(retention.model_dump_json()) == retention
    assert retention.to_api_dict() == {"count": 3, "expiration": 172800000, "deleteEvicted": True}


def test_completed_process_keeps_check_returncode() -> None:
    result = sandbox.CompletedProcess(
        id="cmd_1",
        name="false",
        args=("false",),
        cwd="/vercel/sandbox",
        session_id="sbx_1",
        started_at=1,
        returncode=1,
        stdout="output",
        stderr="error",
    )
    with pytest.raises(CalledProcessError) as raised:
        result.check_returncode()
    assert raised.value.returncode == 1
    assert raised.value.cmd == ["false"]
    assert raised.value.output == "output"
    assert raised.value.stderr == "error"
    result.model_copy(update={"returncode": 0}).check_returncode()
