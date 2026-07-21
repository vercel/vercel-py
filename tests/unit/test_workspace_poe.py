from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "poe" / "workspace_poe.py"
SPEC = importlib.util.spec_from_file_location("workspace_poe", SCRIPT)
assert SPEC is not None
assert SPEC.loader is not None
workspace_poe = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = workspace_poe
SPEC.loader.exec_module(workspace_poe)


def test_split_workspace_args_accepts_verbose_after_task() -> None:
    parsed = workspace_poe.split_workspace_args(["tests/unit", "--verbose"])

    assert parsed.scopes == ("tests/unit",)
    assert parsed.passthrough == ()
    assert parsed.verbose is True


def test_split_workspace_args_consumes_internal_verbose_flag() -> None:
    parsed = workspace_poe.split_workspace_args(["--poe-verbose", "true", "vercel-oidc"])

    assert parsed.scopes == ("vercel-oidc",)
    assert parsed.passthrough == ()
    assert parsed.verbose is True


def test_split_workspace_args_leaves_tool_options_as_passthrough() -> None:
    parsed = workspace_poe.split_workspace_args(["tests/unit", "--", "-k", "time"])

    assert parsed.scopes == ("tests/unit",)
    assert parsed.passthrough == ("-k", "time")
    assert parsed.verbose is False


def test_parse_qa_args_reports_verbose() -> None:
    scopes, poe_flags, verbose = workspace_poe.parse_qa_args(["--verbose", "vercel"])

    assert scopes == ("vercel",)
    assert poe_flags == ("-v",)
    assert verbose is True


def test_poe_verbose_enabled_matches_repo_baseline() -> None:
    assert workspace_poe.poe_verbose_enabled({"POE_VERBOSITY": "-1"}) is False
    assert workspace_poe.poe_verbose_enabled({"POE_VERBOSITY": "0"}) is True
    assert workspace_poe.poe_verbose_enabled({"POE_VERBOSITY": "1"}) is True
    assert workspace_poe.poe_verbose_enabled({"POE_VERBOSITY": "nope"}) is False


def test_quiet_command_suppresses_output_by_default() -> None:
    command = workspace_poe.CommandSpec(
        label="lint",
        argv=("ruff", "check"),
        cwd=Path.cwd(),
        env={"POE_VERBOSITY": "-1"},
        quiet=True,
    )

    assert command.suppress_output is True


def test_quiet_command_does_not_suppress_verbose_output() -> None:
    command = workspace_poe.CommandSpec(
        label="lint",
        argv=("ruff", "check"),
        cwd=Path.cwd(),
        env={"POE_VERBOSITY": "0"},
        quiet=True,
    )

    assert command.suppress_output is False


def test_quiet_entry_preserves_failure_detail_without_live_message() -> None:
    entry = workspace_poe._quiet_entry(
        {
            "message": "ruff found a lint error",
            "lograil.status.detail": "ruff found a lint error",
        }
    )

    assert entry["message"] == ""
    assert entry["lograil.status_only"] is True
    assert "lograil.status.detail" not in entry
    assert entry[workspace_poe.QUIET_FAILURE_DETAIL] == "ruff found a lint error"


def test_scope_command_lograil_name_includes_task_and_package() -> None:
    runner = object.__new__(workspace_poe.WorkspaceRunner)
    runner.root = Path.cwd()
    runner.project_root = None
    scope = workspace_poe.Scope("root", Path.cwd())

    command = runner.scope_command("test", scope, (), "test-root")

    assert command.label == "test/root"
    assert command.display_label == "root"
    assert command.subject == "root"


def test_tool_command_lograil_name_includes_tool_and_package(monkeypatch) -> None:
    monkeypatch.setenv("WORKSPACE_POE_PACKAGE", "vercel-celery")

    command = workspace_poe.env_command("mypy", "mypy .", category="typecheck")

    assert command.label == "mypy/vercel-celery"
    assert command.display_label == "mypy"
    assert command.subject == "vercel-celery"


def test_single_whole_workspace_scope_still_uses_lograil(monkeypatch) -> None:
    runner = object.__new__(workspace_poe.WorkspaceRunner)
    runner.root = Path.cwd()
    runner.project_root = None
    scope = workspace_poe.Scope("vercel-queue", Path.cwd())
    recorded = []

    monkeypatch.setattr(runner, "enter_tree", lambda: None)
    monkeypatch.setattr(runner, "resolve_scopes", lambda task, scope_args: (scope,))
    monkeypatch.setattr(workspace_poe, "parallel_enabled", lambda: True)

    def fake_run_group(commands, *, category=None, compact_labels=False):
        recorded.append((tuple(commands), category, compact_labels))
        return 0

    monkeypatch.setattr(workspace_poe, "run_group", fake_run_group)
    monkeypatch.setattr(
        workspace_poe,
        "run_command",
        lambda command: (_ for _ in ()).throw(AssertionError("run_command should not be used")),
    )

    assert runner.run_workspace("test", ["vercel-queue"]) == 0

    commands, category, compact_labels = recorded[0]
    assert category == "test"
    assert compact_labels is False
    assert [command.label for command in commands] == ["test/vercel-queue"]
    assert [command.display_label for command in commands] == ["vercel-queue"]
    assert [command.subject for command in commands] == ["vercel-queue"]


def test_run_sequential_can_stop_after_first_failure(monkeypatch) -> None:
    commands = [
        workspace_poe.CommandSpec("first", ("first",), Path.cwd(), {}),
        workspace_poe.CommandSpec("second", ("second",), Path.cwd(), {}),
    ]
    labels = []

    def fake_run_command(command):
        labels.append(command.label)
        return 1

    monkeypatch.setattr(workspace_poe, "run_command", fake_run_command)

    assert workspace_poe.run_sequential(commands, fail_fast=True) == 1
    assert labels == ["first"]


def test_hook_commands_are_verbose_and_not_suppressed(monkeypatch) -> None:
    runner = object.__new__(workspace_poe.WorkspaceRunner)
    runner.root = Path.cwd()
    runner.project_root = None
    recorded = []
    scopes_by_task = {
        "lint": (workspace_poe.Scope("root", Path.cwd()),),
        "typecheck": (workspace_poe.Scope("vercel-celery", Path.cwd()),),
    }

    monkeypatch.setattr(runner, "enter_tree", lambda: None)
    monkeypatch.setattr(runner, "uv_run_args", lambda args: ["uv", "run", *args])
    monkeypatch.setattr(runner, "resolve_scopes", lambda task, scope_args: scopes_by_task[task])
    monkeypatch.setattr(workspace_poe, "parallel_enabled", lambda: True)

    def fake_run_group(commands, *, category=None, compact_labels=False):
        recorded.append((tuple(commands), category, compact_labels))
        return 0

    monkeypatch.setattr(workspace_poe, "run_group", fake_run_group)

    assert runner.run_hook("pre-commit") == 0

    commands, category, compact_labels = recorded[0]
    assert category is None
    assert compact_labels is True
    assert [command.label for command in commands] == ["lint/root", "typecheck/vercel-celery"]
    assert [command.display_label for command in commands] == ["root", "vercel-celery"]
    assert [command.subject for command in commands] == ["root", "vercel-celery"]


def test_failure_tail_lines_filter_progress_noise() -> None:
    entries = [
        {"message": "[gw0] [ 10%] PASSED tests/unit/test_ok.py::test_ok"},
        {"message": "FAILED tests/unit/test_bad.py::test_bad - AssertionError"},
        {"message": "E       AssertionError: bad value"},
    ]

    assert workspace_poe.failure_tail_lines(entries) == [
        "FAILED tests/unit/test_bad.py::test_bad - AssertionError",
        "E       AssertionError: bad value",
    ]


def test_failure_tail_lines_use_status_detail_for_quiet_entries() -> None:
    entries = [
        {"message": "", workspace_poe.QUIET_FAILURE_DETAIL: "F401 imported but unused"},
    ]

    assert workspace_poe.failure_tail_lines(entries) == ["F401 imported but unused"]


def test_print_failure_summary_uses_category_and_subject(capsys) -> None:
    process = SimpleNamespace(
        success=False,
        spec=SimpleNamespace(category="test", subject="root", name="test/root", process=None),
        tail=({"message": "FAILED tests/unit/test_bad.py::test_bad - AssertionError"},),
        last_message=None,
        exit_code=1,
    )

    workspace_poe.print_failure_summary((process,))

    captured = capsys.readouterr()
    assert "==> test/root" in captured.err
    assert "FAILED tests/unit/test_bad.py::test_bad - AssertionError" in captured.err
