#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
RESOLVER = SCRIPT_DIR / "workspace_poe_resolve.py"
PARALLEL_FALSE = {"0", "false", "no"}
FAILURE_TAIL_LINES = 20
QUIET_FAILURE_DETAIL = "workspace_poe.failure.detail"


@dataclass(frozen=True)
class Scope:
    package: str
    package_path: Path
    paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class CommandSpec:
    label: str
    argv: tuple[str, ...]
    cwd: Path
    env: dict[str, str]
    display_label: str | None = None
    category: str | None = None
    subject: str | None = None
    parser: str | None = None
    quiet: bool = False

    @property
    def suppress_output(self) -> bool:
        return self.quiet and not poe_verbose_enabled(self.env)


@dataclass(frozen=True)
class WorkspaceArgs:
    scopes: tuple[str, ...]
    passthrough: tuple[str, ...]
    verbose: bool = False


def main(argv: Sequence[str] | None = None) -> int:
    if poe_verbose_enabled():
        os.environ.setdefault("LOGRAIL_OUTPUT", "plain")
    args = list(argv if argv is not None else sys.argv[1:])
    if not args or args[0] in {"-h", "--help"}:
        parser = argparse.ArgumentParser(prog="workspace_poe.py")
        parser.add_argument(
            "command",
            choices=("workspace", "qa", "root-task", "tool-group", "hook"),
        )
        parser.print_help()
        return 0 if args else 2
    command = args.pop(0)
    try:
        if command == "workspace" and args:
            return WorkspaceRunner().run_workspace(args[0], args[1:])
        if command == "qa":
            return WorkspaceRunner().run_qa(args)
        if command == "root-task" and args:
            return run_root_task(args[0], args[1:])
        if command == "tool-group" and len(args) == 1:
            if args[0] not in {"fix", "lint", "typecheck"}:
                raise SystemExit(f"unsupported tool group: {args[0]}")
            return run_tool_group(args[0])
        if command == "hook" and len(args) == 1:
            if args[0] not in {"pre-commit", "pre-push"}:
                raise SystemExit(f"unsupported hook: {args[0]}")
            return WorkspaceRunner().run_hook(args[0])
    except KeyboardInterrupt:
        return 130
    raise SystemExit(f"invalid workspace_poe.py invocation: {command} {' '.join(args)}")


class WorkspaceRunner:
    def __init__(self) -> None:
        self.original_root = git_toplevel()
        self.root = self.original_root
        self.project_root: Path | None = None
        self._snapshot: tempfile.TemporaryDirectory[str] | None = None

    def run_workspace(self, task: str, argv: Sequence[str]) -> int:
        self.enter_tree()
        parsed = split_workspace_args(argv)
        if parsed.verbose:
            os.environ.setdefault("LOGRAIL_OUTPUT", "plain")
        scope_args, subcommand_args = parsed.scopes, parsed.passthrough
        scopes = self.resolve_scopes(task, scope_args)
        ordered = ordered_scopes(scopes) if not parallel_enabled() else scopes
        commands = self.commands_for_scopes(
            task,
            ordered,
            subcommand_args,
            verbose_output=parsed.verbose,
        )
        if not parallel_enabled():
            return run_sequential(commands)
        return run_group(commands, category=task)

    def run_qa(self, argv: Sequence[str]) -> int:
        self.enter_tree()
        scope_args, poe_flags, verbose = parse_qa_args(argv)
        if verbose:
            os.environ.setdefault("LOGRAIL_OUTPUT", "plain")
        commands: list[CommandSpec] = []
        for task in ("lint", "typecheck", "test"):
            commands.extend(
                self.workspace_commands(
                    task,
                    scope_args,
                    (),
                    poe_flags=poe_flags,
                    verbose_output=verbose,
                )
            )
        if parallel_enabled():
            return run_group(commands, compact_labels=True)
        return run_sequential(commands, headings=True)

    def workspace_commands(
        self,
        task: str,
        scope_args: Sequence[str],
        subcommand_args: Sequence[str],
        *,
        poe_flags: Sequence[str] = (),
        verbose_output: bool = False,
    ) -> list[CommandSpec]:
        scopes = self.resolve_scopes(task, scope_args)
        ordered = ordered_scopes(scopes) if not parallel_enabled() else scopes
        return self.commands_for_scopes(
            task,
            ordered,
            subcommand_args,
            poe_flags=poe_flags,
            verbose_output=verbose_output,
        )

    def commands_for_scopes(
        self,
        task: str,
        scopes: Sequence[Scope],
        subcommand_args: Sequence[str],
        *,
        poe_flags: Sequence[str] = (),
        verbose_output: bool = False,
    ) -> list[CommandSpec]:
        root_task = f"{task}-root" if task in {"fix", "lint", "test", "typecheck"} else None
        return [
            self.scope_command(
                task,
                scope,
                subcommand_args,
                root_task,
                poe_flags=poe_flags,
                verbose_output=verbose_output,
            )
            for scope in scopes
        ]

    def run_hook(self, name: str) -> int:
        self.enter_tree()
        commands: list[CommandSpec] = []
        tasks: Sequence[str]
        if name == "pre-commit":
            tasks = ("lint", "typecheck")
        else:
            tasks = ("check-news-fragments", "lint", "typecheck", "test")
        for task in tasks:
            if task == "check-news-fragments":
                commands.append(self.top_level_poe_command(task, (), ()))
            else:
                commands.extend(self.workspace_commands(task, (), ()))
        return run_group(commands, compact_labels=True)

    def top_level_poe_command(
        self,
        task: str,
        scopes: Sequence[str],
        poe_flags: Sequence[str],
        *,
        parser: str | None = None,
        quiet: bool = True,
    ) -> CommandSpec:
        return CommandSpec(
            label=task,
            argv=tuple(self.uv_run_args(("poe", "-q", *poe_flags, task, *scopes))),
            cwd=self.root,
            env=self.base_env(),
            category=task,
            subject="workspace",
            parser=parser,
            quiet=quiet,
        )

    def scope_command(
        self,
        task: str,
        scope: Scope,
        subcommand_args: Sequence[str],
        root_task: str | None,
        *,
        poe_flags: Sequence[str] = (),
        verbose_output: bool = False,
    ) -> CommandSpec:
        package_task = root_task if scope.package == "root" and root_task else task
        uv_scope = ("--all-packages",) if scope.package == "root" else ("--package", scope.package)
        env = self.base_env()
        env["WORKSPACE_POE_PACKAGE"] = scope.package
        if task == "test":
            env["WORKSPACE_POE_LOGRAIL_PROGRESS"] = "1"
        if scope.paths:
            env["WORKSPACE_POE_SCOPE_ARGS"] = shlex.join(scope.paths)
        argv = self.uv_run_args(
            (*uv_scope, "poe", "-q", *poe_flags, package_task, *subcommand_args)
        )
        label = lograil_name(task, scope.package)
        return CommandSpec(
            label=label,
            argv=tuple(argv),
            cwd=scope.package_path,
            env=env,
            display_label=scope.package,
            category=task,
            subject=scope.package,
            parser="pytest" if task == "test" else None,
            quiet=task in {"lint", "typecheck"} and not verbose_output,
        )

    def resolve_scopes(self, task: str, argv: Sequence[str]) -> tuple[Scope, ...]:
        env = self.base_env()
        env["WORKSPACE_POE_SCOPE_TASK"] = task
        output = subprocess.check_output(
            (sys.executable, str(RESOLVER), *argv),
            cwd=self.root,
            env=env,
            text=True,
        )
        scopes: list[Scope] = []
        for line in output.splitlines():
            if not line:
                continue
            package, package_path, *paths = line.split("\t")
            scopes.append(Scope(package, Path(package_path), tuple(paths)))
        return tuple(scopes)

    def enter_tree(self) -> None:
        mode = os.environ.get("WORKSPACE_POE_GIT_SCOPE", "tree") or "tree"
        if mode == "tree" or os.environ.get("WORKSPACE_POE_GIT_SCOPE_ACTIVE") == mode:
            return
        if mode not in {"staged", "commit"}:
            raise SystemExit("WORKSPACE_POE_GIT_SCOPE must be 'tree', 'staged', or 'commit'")
        snapshot = tempfile.TemporaryDirectory(prefix=f"vercel-py-{mode}.")
        snapshot_path = Path(snapshot.name)
        if mode == "staged":
            subprocess.check_call(
                (
                    "git",
                    "-C",
                    str(self.original_root),
                    "checkout-index",
                    "--all",
                    "--force",
                    f"--prefix={snapshot_path}/",
                )
            )
        else:
            commit = (
                os.environ.get("WORKSPACE_POE_GIT_COMMIT")
                or subprocess.check_output(
                    ("git", "-C", str(self.original_root), "rev-parse", "HEAD"), text=True
                ).strip()
            )
            archive = subprocess.Popen(
                ("git", "-C", str(self.original_root), "archive", commit),
                stdout=subprocess.PIPE,
            )
            try:
                subprocess.check_call(
                    ("tar", "-x", "-f", "-", "-C", str(snapshot_path)),
                    stdin=archive.stdout,
                )
            finally:
                if archive.stdout is not None:
                    archive.stdout.close()
                archive.wait()
            os.environ["WORKSPACE_POE_GIT_COMMIT"] = commit
        (snapshot_path / ".git").symlink_to(self.original_root / ".git")
        for cache in (".mypy_cache", ".ruff_cache"):
            (self.original_root / cache).mkdir(exist_ok=True)
            (snapshot_path / cache).symlink_to(self.original_root / cache)
        self._snapshot = snapshot
        self.root = snapshot_path
        self.project_root = self.original_root
        os.environ["WORKSPACE_POE_PROJECT_ROOT"] = str(self.original_root)
        os.environ["WORKSPACE_POE_GIT_SCOPE_ACTIVE"] = mode

    def uv_run_args(self, args: Sequence[str]) -> list[str]:
        command = ["uv", "run"]
        if self.project_root is not None:
            command.extend(("--project", str(self.project_root), "--no-sync"))
        command.extend(args)
        return command

    def base_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["POE"] = str(self.root / "scripts" / "poe" / "tasks" / "poe")
        if poe_verbose_enabled(env):
            env.setdefault("LOGRAIL_OUTPUT", "plain")
        if self.project_root is not None:
            env["WORKSPACE_POE_PROJECT_ROOT"] = str(self.project_root)
        if color_supported():
            env.setdefault("FORCE_COLOR", "1")
            env.setdefault("CLICOLOR_FORCE", "1")
            env.setdefault("PY_COLORS", "1")
        return env


def run_tool_group(task: str) -> int:
    if task == "fix":
        return run_sequential(
            [
                env_command("ruff check", os.environ["RUFF_CHECK_FIX"]),
                env_command("ruff format", os.environ["RUFF_FORMAT_FIX"]),
            ]
        )
    if task == "lint":
        commands = [
            env_command("ruff check", os.environ["RUFF_CHECK"], category="lint"),
            env_command("ruff format", os.environ["RUFF_FORMAT"], category="lint"),
        ]
    else:
        commands = [
            env_command("mypy", f"{os.environ['POE']} typecheck-mypy", category="typecheck"),
            env_command("ty", f"{os.environ['POE']} typecheck-ty", category="typecheck"),
        ]
    if parallel_enabled():
        return run_group(commands, category=task)
    return run_sequential(commands)


def run_root_task(task: str, argv: Sequence[str]) -> int:
    if task == "check-news-fragments-root":
        return subprocess.call((sys.executable, "scripts/release.py", "check-news-fragments"))
    if task == "fix-root":
        return run_tool_group("fix")
    if task == "lint-root":
        commands = [
            env_command("ruff check", os.environ["RUFF_CHECK"], category="lint"),
            env_command("ruff format", os.environ["RUFF_FORMAT"], category="lint"),
            env_command("towncrier", f"{os.environ['POE']} lint-towncrier", category="lint"),
        ]
        if not os.environ.get("WORKSPACE_POE_SCOPE_ARGS"):
            commands.append(
                env_command("zizmor", f"{os.environ['POE']} lint-zizmor", category="lint")
            )
        return run_sequential(commands, fail_fast=True)
    if task == "typecheck-root":
        return run_command(
            env_command("mypy", f"{os.environ['POE']} typecheck-mypy", category="typecheck")
        )
    if task == "test-root":
        extra_args = list(argv) or poe_extra_args()
        scope_args = shlex.split(os.environ.get("WORKSPACE_POE_SCOPE_ARGS", ""))
        args = ["--ignore=tests/test_examples.py"]
        if scope_args:
            args.extend(scope_args)
        elif not extra_args:
            args.append("tests")
        args.extend(extra_args)
        return run_command(
            env_command(
                "pytest",
                shlex.join((os.environ["PYTEST"], *args)),
                category="test",
                parser="pytest",
            )
        )
    raise SystemExit(f"unsupported root task: {task}")


def env_command(
    label: str,
    command: str,
    *,
    category: str | None = None,
    parser: str | None = None,
) -> CommandSpec:
    subject = os.environ.get("WORKSPACE_POE_PACKAGE") or Path.cwd().name
    log_name = lograil_name(label, subject)
    return CommandSpec(
        label=log_name,
        argv=tuple(shlex.split(command)),
        cwd=Path.cwd(),
        env=os.environ.copy(),
        display_label=label,
        category=category,
        subject=subject,
        parser=parser,
    )


def lograil_name(task: str, subject: str | None) -> str:
    if not subject:
        return task
    return f"{task}/{subject}"


def run_group(
    commands: Sequence[CommandSpec],
    *,
    category: str | None = None,
    compact_labels: bool = False,
) -> int:
    if not commands:
        return 0
    try:
        from lograil import DEFAULT_REMAPS, ProcessSpec, configure_logging, run_process_group
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "lograil is required; run `uv sync` after adding ../lograil as an editable dependency"
        ) from exc
    configure_logging()
    specs = [
        ProcessSpec(
            command.argv,
            cwd=str(command.cwd),
            env=command.env,
            name=command.label,
            process=command.display_label if compact_labels else None,
            category=command.category or category,
            subject=command.subject if compact_labels else command.label,
            stream="combined",
            parser=command.parser,
            remaps=(*DEFAULT_REMAPS, _quiet_entry) if command.suppress_output else None,
            kind="pytest" if command.parser == "pytest" else None,
        )
        for command in commands
    ]
    result = run_process_group(specs)
    if not result.success:
        print_failure_summary(result.processes)
    return 0 if result.success else 1


def print_failure_summary(processes: Sequence[Any]) -> None:
    failed = [process for process in processes if not process.success]
    if not failed:
        return
    print("\nFailures:", file=sys.stderr)
    for process in failed:
        spec = process.spec
        category = spec.category or "process"
        subject = spec.subject or spec.name or spec.process or "unknown"
        heading = subject if str(subject).startswith(f"{category}/") else f"{category}/{subject}"
        print(f"\n==> {heading}", file=sys.stderr)
        lines = failure_tail_lines(process.tail)
        if lines:
            for line in lines:
                print(line, file=sys.stderr)
        elif process.last_message:
            print(process.last_message, file=sys.stderr)
        else:
            print(f"exited with status {process.exit_code}", file=sys.stderr)


def failure_tail_lines(entries: Sequence[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for entry in entries:
        message = entry.get("message") or entry.get(QUIET_FAILURE_DETAIL)
        if not isinstance(message, str):
            continue
        message = message.rstrip()
        if not message or message in lines:
            continue
        if is_low_signal_failure_tail_line(message):
            continue
        lines.append(message)
    return lines[-FAILURE_TAIL_LINES:]


def is_low_signal_failure_tail_line(message: str) -> bool:
    stripped = message.strip()
    if not stripped:
        return True
    if stripped.startswith("[") and "]" in stripped and "::" in stripped:
        return True
    if stripped.startswith("tests/") and "::" in stripped and "PASSED" in stripped:
        return True
    return False


def _quiet_entry(entry: dict[str, Any]) -> dict[str, Any]:
    message = entry.get("message")
    if isinstance(message, str) and message:
        entry[QUIET_FAILURE_DETAIL] = message
    entry["message"] = ""
    entry.pop("lograil.status.detail", None)
    entry["lograil.status_only"] = True
    return entry


def run_sequential(
    commands: Sequence[CommandSpec], *, headings: bool = False, fail_fast: bool = False
) -> int:
    status = 0
    for command in commands:
        if headings:
            print(f"==> {command.label}", flush=True)
        result = run_command(command)
        if result != 0 and status == 0:
            status = result
            if fail_fast:
                break
    return status


def run_command(command: CommandSpec) -> int:
    return subprocess.call(command.argv, cwd=command.cwd, env=command.env)


def split_workspace_args(argv: Sequence[str]) -> WorkspaceArgs:
    scopes: list[str] = []
    passthrough: list[str] = []
    verbose = False
    in_passthrough = False
    args = list(argv)
    while args:
        arg = args.pop(0)
        if in_passthrough:
            passthrough.append(arg)
        elif arg == "--":
            in_passthrough = True
        elif arg == "--poe-verbose":
            if args and is_poe_bool(args[0]):
                verbose = parse_poe_bool(args.pop(0))
        elif arg in {"-v", "--verbose"}:
            verbose = True
        elif arg.startswith("-"):
            in_passthrough = True
            passthrough.append(arg)
        else:
            scopes.append(arg)
    return WorkspaceArgs(tuple(scopes), tuple(passthrough), verbose)


def parse_qa_args(argv: Sequence[str]) -> tuple[tuple[str, ...], tuple[str, ...], bool]:
    scopes: list[str] = []
    poe_flags: list[str] = []
    verbose = False
    args = list(argv)
    while args:
        arg = args.pop(0)
        if arg in {"-h", "--help"}:
            print("Usage: workspace_poe.py qa [-q|--quiet] [-v|--verbose] [scope ...]")
            raise SystemExit(0)
        if arg == "--poe-quiet":
            if args and is_poe_bool(args[0]) and parse_poe_bool(args.pop(0)):
                poe_flags.append("-q")
            continue
        elif arg == "--poe-verbose":
            if args and is_poe_bool(args[0]) and parse_poe_bool(args.pop(0)):
                poe_flags.append("-v")
                verbose = True
            continue
        if arg in {"-q", "--quiet"}:
            poe_flags.append("-q")
        elif arg in {"-v", "--verbose"}:
            poe_flags.append("-v")
            verbose = True
        elif arg == "--":
            raise SystemExit("qa does not accept tool-specific arguments after --")
        elif arg.startswith("-"):
            raise SystemExit("qa only accepts -q/--quiet and -v/--verbose options")
        else:
            scopes.append(arg)
    return tuple(scopes), tuple(poe_flags), verbose


def parse_poe_bool(value: str) -> bool:
    return value.lower() in {"1", "true", "yes"}


def is_poe_bool(value: str) -> bool:
    return value.lower() in {"0", "1", "false", "true", "no", "yes"}


def poe_verbose_enabled(env: dict[str, str] | None = None) -> bool:
    value = (env or os.environ).get("POE_VERBOSITY")
    if value is None:
        return False
    try:
        return int(value) >= 0
    except ValueError:
        return False


def poe_extra_args() -> list[str]:
    extra = os.environ.get("POE_EXTRA_ARGS", "")
    if not extra:
        return []
    args = shlex.split(extra)
    if args[:1] == ["--"]:
        return args[1:]
    return args


def is_single_whole_scope(scopes: Sequence[Scope]) -> bool:
    return len(scopes) == 1 and not scopes[0].paths


def ordered_scopes(scopes: Sequence[Scope]) -> tuple[Scope, ...]:
    packages = [scope for scope in scopes if scope.package != "root"]
    roots = [scope for scope in scopes if scope.package == "root"]
    return tuple(packages + roots)


def parallel_enabled() -> bool:
    return os.environ.get("WORKSPACE_POE_PARALLEL", "").lower() not in PARALLEL_FALSE


def color_supported() -> bool:
    if os.environ.get("NO_COLOR", "")[:1] not in {"", "0"}:
        return False
    for name in ("FORCE_COLOR", "CLICOLOR_FORCE", "PY_COLORS"):
        if os.environ.get(name, "")[:1] not in {"", "0"}:
            return True
    return sys.stderr.isatty()


def git_toplevel() -> Path:
    return Path(subprocess.check_output(("git", "rev-parse", "--show-toplevel"), text=True).strip())


if __name__ == "__main__":
    raise SystemExit(main())
