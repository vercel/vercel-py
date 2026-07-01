from __future__ import annotations

from typing import Any

import argparse
import base64
import binascii
import json
import os
import subprocess  # noqa: S404 - CLI delegates deployment lookup to the Vercel CLI.
import sys
from collections.abc import Sequence
from pathlib import Path

from vercel.queue.sync import QueueClient


def main(argv: Sequence[str] | None = None) -> int:
    """Run the ``python -m vercel.queue`` command."""
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code) if isinstance(exc.code, int) else 2

    if args.command == "send":
        return _send(args, parser)

    parser.error("unknown command")
    return 2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m vercel.queue")
    subparsers = parser.add_subparsers(dest="command", required=True)

    send = subparsers.add_parser("send", help="send a queue message")
    send.add_argument("--topic", required=True, help="queue topic name")
    send.add_argument("--region", help="queue region, such as iad1")
    send.add_argument("--deployment", help="deployment ID, such as dpl_123")
    send.add_argument(
        "--output-format",
        choices=["text", "json"],
        default="text",
        help="output format",
    )

    payload = send.add_mutually_exclusive_group(required=True)
    payload.add_argument("--json", dest="json_value", help="JSON payload")
    payload.add_argument("--json-from", help="path to a UTF-8 JSON payload file")
    payload.add_argument("--text", help="text payload")
    payload.add_argument("--text-from", help="path to a UTF-8 text payload file")
    payload.add_argument("--binary", help="base64-encoded binary payload")
    payload.add_argument("--binary-from", help="path to a raw binary payload file")
    return parser


def _send(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    try:
        payload = _payload_from_args(args)
    except ValueError as exc:
        parser.print_usage(sys.stderr)
        sys.stderr.write(f"{parser.prog}: error: {exc}\n")
        return 2

    try:
        _load_dotenv_local()
        deployment = args.deployment or _resolve_current_production_deployment()
        queue = QueueClient(region=args.region, deployment=deployment)
        message_id = queue.send(args.topic, payload)
    except Exception as exc:  # noqa: BLE001 - CLI boundary converts runtime failures to exit codes.
        sys.stderr.write(f"vercel.queue: {exc}\n")
        return 1

    _write_output(message_id, deployment, args.output_format)
    return 0


def _payload_from_args(args: argparse.Namespace) -> Any:
    if args.json_value is not None:
        return _parse_json(args.json_value, "--json")
    if args.json_from is not None:
        return _parse_json(_read_text(args.json_from, "--json-from"), "--json-from")
    if args.text is not None:
        return args.text
    if args.text_from is not None:
        return _read_text(args.text_from, "--text-from")
    if args.binary is not None:
        return _decode_base64(args.binary)
    if args.binary_from is not None:
        return _read_bytes(args.binary_from, "--binary-from")
    raise ValueError("one payload option is required")


def _parse_json(value: str, option: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{option} must contain valid JSON: {exc.msg}") from exc


def _decode_base64(value: str) -> bytes:
    try:
        return base64.b64decode(value, validate=True)
    except binascii.Error as exc:
        raise ValueError(f"--binary must contain valid base64: {exc}") from exc


def _read_text(path: str, option: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"{option} could not be read: {exc}") from exc
    except UnicodeDecodeError as exc:
        raise ValueError(f"{option} must be UTF-8 text: {exc}") from exc


def _read_bytes(path: str, option: str) -> bytes:
    try:
        return Path(path).read_bytes()
    except OSError as exc:
        raise ValueError(f"{option} could not be read: {exc}") from exc


def _resolve_current_production_deployment() -> str:
    project = _load_linked_project()
    scope = project.get("teamId")
    deployment_url = _find_current_production_deployment_url(scope)
    if deployment_url is None:
        raise RuntimeError(
            "Failed to resolve deployment ID: no production deployment was found for the "
            "linked project. Pass --deployment to choose one explicitly."
        )
    return _inspect_deployment_id(deployment_url, scope)


def _load_linked_project(start: Path | None = None) -> dict[str, str]:
    project_path = _find_project_json(start or Path.cwd())
    if project_path is None:
        raise RuntimeError(
            "Failed to resolve deployment ID: .vercel/project.json was not found. "
            "Run `vercel link` or pass --deployment."
        )
    try:
        data = json.loads(project_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            "Failed to resolve deployment ID: .vercel/project.json is invalid. "
            "Run `vercel link` or pass --deployment."
        ) from exc
    if not isinstance(data, dict):
        raise TypeError(
            "Failed to resolve deployment ID: .vercel/project.json is invalid. "
            "Run `vercel link` or pass --deployment."
        )

    project_id = data.get("projectId")
    team_id = data.get("teamId") or data.get("orgId")
    if not isinstance(project_id, str) or not project_id:
        raise RuntimeError(
            "Failed to resolve deployment ID: .vercel/project.json is missing projectId. "
            "Run `vercel link` or pass --deployment."
        )

    project = {"projectId": project_id}
    if isinstance(team_id, str) and team_id:
        project["teamId"] = team_id
    return project


def _find_project_json(start: Path) -> Path | None:
    current = start.resolve()
    if current.is_file():
        current = current.parent
    while True:
        candidate = current / ".vercel" / "project.json"
        if candidate.is_file():
            return candidate
        if current.parent == current:
            return None
        current = current.parent


def _find_current_production_deployment_url(scope: str | None = None) -> str | None:
    args = [
        "list",
        "--environment",
        "production",
        "--status",
        "READY",
        "--format",
        "json",
        "--non-interactive",
    ]
    if scope is not None:
        args.extend(["--scope", scope])
    data = _run_vercel_json(args)
    if not isinstance(data, dict):
        raise TypeError("Failed to resolve deployment ID: Vercel CLI returned an invalid response.")
    deployments = data.get("deployments")
    if not isinstance(deployments, list):
        raise TypeError("Failed to resolve deployment ID: Vercel CLI returned an invalid response.")
    if not deployments:
        return None
    first = deployments[0]
    url = first.get("url") if isinstance(first, dict) else None
    if not isinstance(url, str):
        raise TypeError("Failed to resolve deployment ID: Vercel CLI returned an invalid response.")
    return url


def _inspect_deployment_id(deployment_url: str, scope: str | None = None) -> str:
    args = ["inspect", deployment_url, "--format", "json", "--non-interactive"]
    if scope is not None:
        args.extend(["--scope", scope])
    data = _run_vercel_json(args)
    deployment_id = data.get("id") if isinstance(data, dict) else None
    if not isinstance(deployment_id, str):
        raise TypeError("Failed to resolve deployment ID: Vercel CLI returned an invalid response.")
    return deployment_id


def _run_vercel_json(args: Sequence[str]) -> object:
    command = ["vc", *args]
    try:
        result = subprocess.run(  # noqa: S603 - command is fixed and args are internal constants.
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "Failed to resolve deployment ID: install the Vercel CLI, log in, or pass --deployment."
        ) from exc
    except subprocess.SubprocessError as exc:
        raise RuntimeError(f"Failed to resolve deployment ID: Vercel CLI failed: {exc}") from exc
    if result.returncode != 0:
        message = (result.stderr or result.stdout).strip()
        detail = f": {message}" if message else ""
        raise RuntimeError(
            "Failed to resolve deployment ID: Vercel CLI failed"
            f"{detail}. Log in with the Vercel CLI or pass --deployment."
        )
    return _parse_vercel_json_output(result.stdout)


def _parse_vercel_json_output(output: str) -> object:
    start = output.find("{")
    if start == -1:
        raise TypeError("Failed to resolve deployment ID: Vercel CLI returned an invalid response.")
    try:
        return json.loads(output[start:])
    except json.JSONDecodeError as exc:
        raise TypeError(
            "Failed to resolve deployment ID: Vercel CLI returned an invalid response."
        ) from exc


def _load_dotenv_local(path: Path | None = None) -> None:
    env_path = path or Path.cwd() / ".env.local"
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise RuntimeError(f"failed to read {env_path}: {exc}") from exc

    for line in lines:
        parsed = _parse_dotenv_line(line)
        if parsed is None:
            continue
        key, value = parsed
        if key == "VERCEL_DEPLOYMENT_ID":
            continue
        os.environ.setdefault(key, value)


def _parse_dotenv_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    if stripped.startswith("export "):
        stripped = stripped.removeprefix("export ").lstrip()
    key, separator, value = stripped.partition("=")
    key = key.strip()
    if separator != "=" or not key:
        return None
    return key, _parse_dotenv_value(value.strip())


def _parse_dotenv_value(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    if " #" in value:
        value = value.split(" #", 1)[0].rstrip()
    return value


def _write_output(message_id: str | None, deployment: str, output_format: str) -> None:
    if output_format == "json":
        sys.stdout.write(
            json.dumps(
                {"message_id": message_id, "deployment_id": deployment},
                separators=(",", ":"),
            )
        )
        sys.stdout.write("\n")
        return
    if message_id is not None:
        sys.stdout.write(f"{message_id}\n")
    sys.stdout.write(f"deployment: {deployment}\n")
