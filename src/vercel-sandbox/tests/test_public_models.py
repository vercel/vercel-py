"""Validation and serialization behavior introduced by Sandbox models."""

import json
from datetime import timedelta
from types import SimpleNamespace

from vercel import sandbox


def test_command_types_preserve_process_import_compatibility() -> None:
    from vercel.sandbox import sync

    for module, pairs in (
        (
            sandbox,
            (
                ("Command", "Process"),
                ("CompletedCommand", "CompletedProcess"),
                ("CommandStatus", "ProcessStatus"),
                ("CommandSignal", "ProcessSignal"),
                ("CommandStdin", "ProcessStdin"),
            ),
        ),
        (
            sync,
            (
                ("SyncCommand", "SyncProcess"),
                ("CompletedCommand", "CompletedProcess"),
                ("CommandStatus", "ProcessStatus"),
                ("CommandSignal", "ProcessSignal"),
                ("SyncCommandStdin", "SyncProcessStdin"),
            ),
        ),
    ):
        for canonical, legacy in pairs:
            cls = getattr(module, canonical)
            assert cls.__name__ == canonical
            assert cls is getattr(module, legacy)
            assert canonical in module.__all__
            assert legacy in module.__all__

    result = sandbox.CompletedCommand(
        id="cmd_test",
        name="echo",
        args=("echo", "hello"),
        cwd="/vercel/sandbox",
        session_id="sbx_test",
        started_at=1,
        returncode=0,
        stdout="hello\n",
        stderr="",
    )
    assert isinstance(result, sandbox.CompletedProcess)
    assert sync.CompletedCommand is sandbox.CompletedCommand
    assert sandbox.CompletedProcess.model_validate_json(result.model_dump_json()) == result
    assert sandbox.CompletedCommand.model_json_schema()["title"] == "CompletedCommand"
    assert sandbox.CommandStatus.RUNNING.value == "running"
    assert sandbox.CommandSignal.SIGTERM == 15
    assert sync.CommandStatus is sandbox.CommandStatus
    assert sync.CommandSignal is sandbox.CommandSignal


def test_network_policy_validates_and_roundtrips_nested_data() -> None:
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
    assert json.loads(policy.model_dump_json(exclude_none=True)) == data
    assert sandbox.NetworkPolicy.model_validate_json(policy.model_dump_json()) == policy


def test_drive_mount_serializes_handle_as_name() -> None:
    drive = SimpleNamespace(name="data", project_id="project", region="iad1")
    mount = sandbox.DriveMount(drive)
    expected = {"drive": "data", "mode": "read-write"}
    assert mount.model_dump() == expected
    assert json.loads(mount.model_dump_json()) == expected


def test_snapshot_retention_roundtrip_preserves_api_payload() -> None:
    retention = sandbox.SnapshotRetention(
        count=3, expiration=sandbox.SnapshotExpiration(timedelta(days=2))
    )
    restored = sandbox.SnapshotRetention.model_validate_json(retention.model_dump_json())
    assert restored == retention
    assert restored.to_api_dict() == {
        "count": 3,
        "expiration": 172800000,
        "deleteEvicted": True,
    }
