"""Tests for sandbox metadata network policy parsing."""

from __future__ import annotations

from vercel.sandbox import (
    NetworkPolicyCustom,
    NetworkPolicyRule,
    NetworkTransformer,
    SandboxStatus,
)
from vercel.sandbox.models import Sandbox as SandboxModel


class TestSandboxModelNetworkPolicy:
    def test_model_preserves_network_policy_payload(self) -> None:
        sandbox = SandboxModel.model_validate(
            {
                "id": "sbx_test123456",
                "memory": 512,
                "vcpus": 1,
                "region": "iad1",
                "runtime": "nodejs20.x",
                "timeout": 300,
                "status": "running",
                "requestedAt": 1705320600000,
                "startedAt": 1705320601000,
                "requestedStopAt": None,
                "stoppedAt": None,
                "duration": None,
                "sourceSnapshotId": None,
                "snapshottedAt": None,
                "createdAt": 1705320600000,
                "cwd": "/app",
                "updatedAt": 1705320601000,
                "interactivePort": None,
                "networkPolicy": {
                    "mode": "custom",
                    "allowedDomains": ["example.com"],
                    "injectionRules": [{"domain": "example.com", "headerNames": ["X-Trace"]}],
                },
            }
        )

        assert sandbox.network_policy == NetworkPolicyCustom(
            allow={
                "example.com": [
                    NetworkPolicyRule(
                        transform=[NetworkTransformer(headers={"X-Trace": "<redacted>"})]
                    )
                ]
            }
        )
        assert sandbox.status is SandboxStatus.RUNNING

    def test_model_accepts_missing_network_policy(self) -> None:
        sandbox = SandboxModel.model_validate(
            {
                "id": "sbx_test123456",
                "memory": 512,
                "vcpus": 1,
                "region": "iad1",
                "runtime": "nodejs20.x",
                "timeout": 300,
                "status": "running",
                "requestedAt": 1705320600000,
                "startedAt": 1705320601000,
                "requestedStopAt": None,
                "stoppedAt": None,
                "duration": None,
                "sourceSnapshotId": None,
                "snapshottedAt": None,
                "createdAt": 1705320600000,
                "cwd": "/app",
                "updatedAt": 1705320601000,
                "interactivePort": None,
            }
        )

        assert sandbox.network_policy is None
        assert sandbox.status is SandboxStatus.RUNNING

    def test_model_rejects_unknown_status(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="status"):
            SandboxModel.model_validate(
                {
                    "id": "sbx_test123456",
                    "memory": 512,
                    "vcpus": 1,
                    "region": "iad1",
                    "runtime": "nodejs20.x",
                    "timeout": 300,
                    "status": "unknown",
                    "requestedAt": 1705320600000,
                    "startedAt": 1705320601000,
                    "requestedStopAt": None,
                    "stoppedAt": None,
                    "duration": None,
                    "sourceSnapshotId": None,
                    "snapshottedAt": None,
                    "createdAt": 1705320600000,
                    "cwd": "/app",
                    "updatedAt": 1705320601000,
                    "interactivePort": None,
                }
            )
