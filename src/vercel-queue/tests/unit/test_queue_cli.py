from __future__ import annotations

from typing import Any, ClassVar

import json
import os
from dataclasses import dataclass
from pathlib import Path

import pytest

from vercel.queue._internal import cli

REAL_RESOLVE_CURRENT_PRODUCTION_DEPLOYMENT = cli._resolve_current_production_deployment


@dataclass
class FakeCompletedProcess:
    args: list[str]
    returncode: int
    stdout: str
    stderr: str


class FakeQueueClient:
    instances: ClassVar[list[FakeQueueClient]] = []
    message_id: ClassVar[str | None] = "msg_123"
    error: ClassVar[Exception | None] = None

    def __init__(self, *, region: str | None = None, deployment: str | None = None) -> None:
        self.region = region
        self.deployment = deployment
        self.sent: list[tuple[str, Any]] = []
        self.instances.append(self)

    def send(self, topic: str, payload: Any) -> str | None:
        if self.error is not None:
            raise self.error
        self.sent.append((topic, payload))
        return self.message_id


@pytest.fixture(autouse=True)
def fake_queue_client(monkeypatch: pytest.MonkeyPatch) -> type[FakeQueueClient]:
    FakeQueueClient.instances = []
    FakeQueueClient.message_id = "msg_123"
    FakeQueueClient.error = None
    monkeypatch.setattr(cli, "QueueClient", FakeQueueClient)
    monkeypatch.setattr(cli, "_resolve_current_production_deployment", lambda: "dpl_default")
    return FakeQueueClient


def _main_discard_output(argv: list[str], capsys: pytest.CaptureFixture[str]) -> int:
    code = cli.main(argv)
    capsys.readouterr()
    return code


def test_send_json_payload(capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["send", "--topic", "orders", "--json", '{"ok":true}']) == 0

    client = FakeQueueClient.instances[0]
    assert client.region is None
    assert client.deployment == "dpl_default"
    assert client.sent == [("orders", {"ok": True})]
    assert capsys.readouterr().out == "msg_123\ndeployment: dpl_default\n"


def test_send_text_payload(capsys: pytest.CaptureFixture[str]) -> None:
    assert _main_discard_output(["send", "--topic", "logs", "--text", "hello"], capsys) == 0

    assert FakeQueueClient.instances[0].sent == [("logs", "hello")]


def test_send_binary_payload(capsys: pytest.CaptureFixture[str]) -> None:
    assert _main_discard_output(["send", "--topic", "images", "--binary", "aGVsbG8="], capsys) == 0

    assert FakeQueueClient.instances[0].sent == [("images", b"hello")]


def test_send_json_from_payload(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = tmp_path / "payload.json"
    path.write_text('{"order_id":"ord_123"}', encoding="utf-8")

    assert (
        _main_discard_output(["send", "--topic", "orders", "--json-from", str(path)], capsys) == 0
    )
    assert FakeQueueClient.instances[0].sent == [("orders", {"order_id": "ord_123"})]


def test_send_text_from_payload(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = tmp_path / "payload.txt"
    path.write_text("started\nfinished\n", encoding="utf-8")

    assert _main_discard_output(["send", "--topic", "logs", "--text-from", str(path)], capsys) == 0
    assert FakeQueueClient.instances[0].sent == [("logs", "started\nfinished\n")]


def test_send_binary_from_payload(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = tmp_path / "payload.bin"
    path.write_bytes(b"\x00\x01\x02")

    assert (
        _main_discard_output(["send", "--topic", "files", "--binary-from", str(path)], capsys) == 0
    )
    assert FakeQueueClient.instances[0].sent == [("files", b"\x00\x01\x02")]


def test_send_region(capsys: pytest.CaptureFixture[str]) -> None:
    assert (
        _main_discard_output(
            ["send", "--region", "iad1", "--topic", "orders", "--json", "null"],
            capsys,
        )
        == 0
    )

    assert FakeQueueClient.instances[0].region == "iad1"


def test_send_deployment_bypasses_resolution(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail_resolve() -> str:
        raise AssertionError("deployment should not be resolved")

    monkeypatch.setattr(cli, "_resolve_current_production_deployment", fail_resolve)

    assert (
        _main_discard_output(
            [
                "send",
                "--deployment",
                "dpl_123",
                "--topic",
                "orders",
                "--json",
                "null",
            ],
            capsys,
        )
        == 0
    )

    assert FakeQueueClient.instances[0].deployment == "dpl_123"


def test_send_resolves_current_production_deployment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli,
        "_resolve_current_production_deployment",
        REAL_RESOLVE_CURRENT_PRODUCTION_DEPLOYMENT,
    )
    (tmp_path / ".vercel").mkdir()
    (tmp_path / ".vercel" / "project.json").write_text(
        json.dumps({"projectId": "prj_123", "orgId": "team_123"}),
        encoding="utf-8",
    )
    calls: list[list[str]] = []

    def fake_run(command: list[str], **kwargs: object) -> FakeCompletedProcess:
        calls.append(command)
        if command[1] == "list":
            return FakeCompletedProcess(
                command,
                0,
                stdout=json.dumps({"deployments": [{"url": "prod.vercel.app", "state": "READY"}]}),
                stderr="",
            )
        return FakeCompletedProcess(
            command,
            0,
            stdout=json.dumps({"id": "dpl_prod"}),
            stderr="",
        )

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    assert _main_discard_output(["send", "--topic", "orders", "--json", "null"], capsys) == 0

    client = FakeQueueClient.instances[0]
    assert client.deployment == "dpl_prod"
    assert calls == [
        [
            "vc",
            "list",
            "--environment",
            "production",
            "--status",
            "READY",
            "--format",
            "json",
            "--non-interactive",
            "--scope",
            "team_123",
        ],
        [
            "vc",
            "inspect",
            "prod.vercel.app",
            "--format",
            "json",
            "--non-interactive",
            "--scope",
            "team_123",
        ],
    ]


def test_send_resolves_current_production_deployment_with_team_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli,
        "_resolve_current_production_deployment",
        REAL_RESOLVE_CURRENT_PRODUCTION_DEPLOYMENT,
    )
    (tmp_path / ".vercel").mkdir()
    (tmp_path / ".vercel" / "project.json").write_text(
        json.dumps({"projectId": "prj_123", "teamId": "team_456"}),
        encoding="utf-8",
    )
    calls: list[list[str]] = []

    def fake_run(command: list[str], **kwargs: object) -> FakeCompletedProcess:
        calls.append(command)
        if command[1] == "list":
            stdout = {"deployments": [{"url": "prod.vercel.app", "state": "READY"}]}
            return FakeCompletedProcess(command, 0, stdout=json.dumps(stdout), stderr="")
        return FakeCompletedProcess(command, 0, stdout=json.dumps({"id": "dpl_prod"}), stderr="")

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    assert _main_discard_output(["send", "--topic", "orders", "--json", "null"], capsys) == 0

    assert FakeQueueClient.instances[0].deployment == "dpl_prod"
    assert calls[0][-2:] == ["--scope", "team_456"]
    assert calls[1][-2:] == ["--scope", "team_456"]


def test_send_loads_env_local(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("VERCEL_QUEUE_TOKEN", raising=False)
    (tmp_path / ".env.local").write_text("VERCEL_QUEUE_TOKEN=token-from-file\n", encoding="utf-8")

    assert _main_discard_output(["send", "--topic", "orders", "--json", "null"], capsys) == 0

    assert os.environ["VERCEL_QUEUE_TOKEN"] == "token-from-file"


def test_send_loads_env_local_token_for_deployment_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli,
        "_resolve_current_production_deployment",
        REAL_RESOLVE_CURRENT_PRODUCTION_DEPLOYMENT,
    )
    monkeypatch.delenv("VERCEL_TOKEN", raising=False)
    monkeypatch.delenv("VERCEL_QUEUE_TOKEN", raising=False)
    monkeypatch.delenv("VERCEL_DEPLOYMENT_ID", raising=False)
    (tmp_path / ".env.local").write_text(
        "VERCEL_TOKEN=api-token\nVERCEL_QUEUE_TOKEN=queue-token\nVERCEL_DEPLOYMENT_ID=dpl_wrong\n",
        encoding="utf-8",
    )
    (tmp_path / ".vercel").mkdir()
    (tmp_path / ".vercel" / "project.json").write_text(
        json.dumps({"projectId": "prj_123"}),
        encoding="utf-8",
    )

    def fake_run(command: list[str], **kwargs: object) -> FakeCompletedProcess:
        if command[1] == "inspect":
            return FakeCompletedProcess(
                command,
                0,
                stdout=json.dumps({"id": "dpl_prod"}),
                stderr="",
            )
        stdout = {"deployments": [{"url": "prod.vercel.app", "state": "READY"}]}
        return FakeCompletedProcess(command, 0, stdout=json.dumps(stdout), stderr="")

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    assert _main_discard_output(["send", "--topic", "orders", "--json", "null"], capsys) == 0

    assert os.environ["VERCEL_TOKEN"] == "api-token"
    assert os.environ["VERCEL_QUEUE_TOKEN"] == "queue-token"
    assert "VERCEL_DEPLOYMENT_ID" not in os.environ
    assert FakeQueueClient.instances[0].deployment == "dpl_prod"


def test_send_loads_env_local_oidc_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli,
        "_resolve_current_production_deployment",
        REAL_RESOLVE_CURRENT_PRODUCTION_DEPLOYMENT,
    )
    monkeypatch.delenv("VERCEL_TOKEN", raising=False)
    monkeypatch.delenv("VERCEL_OIDC_TOKEN", raising=False)
    (tmp_path / ".env.local").write_text("VERCEL_OIDC_TOKEN=oidc-token\n", encoding="utf-8")
    (tmp_path / ".vercel").mkdir()
    (tmp_path / ".vercel" / "project.json").write_text(
        json.dumps({"projectId": "prj_123"}),
        encoding="utf-8",
    )

    def fake_run(command: list[str], **kwargs: object) -> FakeCompletedProcess:
        if command[1] == "inspect":
            return FakeCompletedProcess(
                command,
                0,
                stdout=json.dumps({"id": "dpl_prod"}),
                stderr="",
            )
        stdout = {"deployments": [{"url": "prod.vercel.app", "state": "READY"}]}
        return FakeCompletedProcess(command, 0, stdout=json.dumps(stdout), stderr="")

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    assert _main_discard_output(["send", "--topic", "orders", "--json", "null"], capsys) == 0

    assert os.environ["VERCEL_OIDC_TOKEN"] == "oidc-token"
    assert FakeQueueClient.instances[0].deployment == "dpl_prod"


def test_send_env_local_does_not_override_existing_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("VERCEL_QUEUE_TOKEN", "token-from-env")
    (tmp_path / ".env.local").write_text("VERCEL_QUEUE_TOKEN=token-from-file\n", encoding="utf-8")

    assert _main_discard_output(["send", "--topic", "orders", "--json", "null"], capsys) == 0

    assert os.environ["VERCEL_QUEUE_TOKEN"] == "token-from-env"


def test_env_local_parser_supports_export_quotes_and_comments() -> None:
    assert cli._parse_dotenv_line('export VERCEL_QUEUE_TOKEN="token"') == (
        "VERCEL_QUEUE_TOKEN",
        "token",
    )
    assert cli._parse_dotenv_line("VERCEL_REGION=iad1 # local region") == (
        "VERCEL_REGION",
        "iad1",
    )
    assert cli._parse_dotenv_line("# ignored") is None


def test_send_missing_project_metadata_returns_runtime_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli,
        "_resolve_current_production_deployment",
        REAL_RESOLVE_CURRENT_PRODUCTION_DEPLOYMENT,
    )
    monkeypatch.setenv("VERCEL_TOKEN", "api-token")

    assert cli.main(["send", "--topic", "orders", "--json", "null"]) == 1

    assert FakeQueueClient.instances == []
    assert "vercel link" in capsys.readouterr().err


def test_send_missing_vercel_cli_returns_runtime_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli,
        "_resolve_current_production_deployment",
        REAL_RESOLVE_CURRENT_PRODUCTION_DEPLOYMENT,
    )
    (tmp_path / ".vercel").mkdir()
    (tmp_path / ".vercel" / "project.json").write_text(
        json.dumps({"projectId": "prj_123"}),
        encoding="utf-8",
    )

    def fake_run(command: list[str], **kwargs: object) -> FakeCompletedProcess:
        raise FileNotFoundError

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    assert cli.main(["send", "--topic", "orders", "--json", "null"]) == 1

    assert FakeQueueClient.instances == []
    err = capsys.readouterr().err
    assert "install the Vercel CLI" in err
    assert "Vercel CLI" in err
    assert "--deployment" in err


def test_send_failed_vercel_cli_returns_runtime_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli,
        "_resolve_current_production_deployment",
        REAL_RESOLVE_CURRENT_PRODUCTION_DEPLOYMENT,
    )
    (tmp_path / ".vercel").mkdir()
    (tmp_path / ".vercel" / "project.json").write_text(
        json.dumps({"projectId": "prj_123"}),
        encoding="utf-8",
    )

    def fake_run(command: list[str], **kwargs: object) -> FakeCompletedProcess:
        return FakeCompletedProcess(command, 1, stdout="", stderr="not logged in")

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    assert cli.main(["send", "--topic", "orders", "--json", "null"]) == 1

    assert FakeQueueClient.instances == []
    assert "Vercel CLI failed" in capsys.readouterr().err


def test_send_empty_production_deployment_list_returns_runtime_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli,
        "_resolve_current_production_deployment",
        REAL_RESOLVE_CURRENT_PRODUCTION_DEPLOYMENT,
    )
    (tmp_path / ".vercel").mkdir()
    (tmp_path / ".vercel" / "project.json").write_text(
        json.dumps({"projectId": "prj_123"}),
        encoding="utf-8",
    )

    def fake_run(command: list[str], **kwargs: object) -> FakeCompletedProcess:
        return FakeCompletedProcess(
            command,
            0,
            stdout=json.dumps({"deployments": []}),
            stderr="",
        )

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    assert cli.main(["send", "--topic", "orders", "--json", "null"]) == 1

    assert FakeQueueClient.instances == []
    assert "no production deployment" in capsys.readouterr().err


@pytest.mark.parametrize(
    "argv",
    [
        ["send", "--topic", "orders"],
        ["send", "--topic", "orders", "--json", "{}", "--text", "hello"],
    ],
)
def test_payload_usage_errors(argv: list[str], capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(argv) == 2

    assert FakeQueueClient.instances == []
    assert "error:" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (["send", "--topic", "orders", "--json", "{"], "valid JSON"),
        (["send", "--topic", "images", "--binary", "not-base64"], "valid base64"),
    ],
)
def test_payload_validation_errors(
    argv: list[str],
    message: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert cli.main(argv) == 2

    assert FakeQueueClient.instances == []
    assert message in capsys.readouterr().err


def test_text_output_includes_deployment_when_message_id_is_none(
    capsys: pytest.CaptureFixture[str],
) -> None:
    FakeQueueClient.message_id = None

    assert cli.main(["send", "--topic", "orders", "--json", "null"]) == 0

    assert capsys.readouterr().out == "deployment: dpl_default\n"


@pytest.mark.parametrize("message_id", ["msg_123", None])
def test_json_output(message_id: str | None, capsys: pytest.CaptureFixture[str]) -> None:
    FakeQueueClient.message_id = message_id

    assert (
        cli.main([
            "send",
            "--topic",
            "orders",
            "--json",
            "null",
            "--output-format",
            "json",
        ])
        == 0
    )

    assert json.loads(capsys.readouterr().out) == {
        "message_id": message_id,
        "deployment_id": "dpl_default",
    }


def test_send_runtime_error(capsys: pytest.CaptureFixture[str]) -> None:
    FakeQueueClient.error = RuntimeError("send failed")

    assert cli.main(["send", "--topic", "orders", "--json", "null"]) == 1

    assert "send failed" in capsys.readouterr().err
