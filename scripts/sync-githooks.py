#!/usr/bin/env python3
from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HOOKS = ROOT / "scripts" / "githooks"
MANAGED_BY = "vercel-py"
HOOK_RE = re.compile(r"^(?P<event>[a-z0-9-]+)\.(?P<name>[a-z0-9-]+)\.[^.]+$")


@dataclass(frozen=True)
class Hook:
    event: str
    name: str
    path: str

    @property
    def section(self) -> str:
        return f"{self.event}-{self.name}"

    @property
    def command(self) -> str:
        command = f"echo 'Running {self.event}.{self.name} hook...' && {self.path}"
        if self.event == "pre-commit":
            return f"WORKSPACE_POE_GIT_SCOPE=staged {command}"
        if self.event == "pre-push":
            return f"WORKSPACE_POE_GIT_SCOPE=commit {command}"
        return command


def git_config(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "config", *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=check,
    )


def discover_hooks() -> list[Hook]:
    hooks: list[Hook] = []
    for path in sorted(HOOKS.iterdir()):
        if not path.is_file():
            continue
        match = HOOK_RE.fullmatch(path.name)
        if match is None:
            raise SystemExit(
                f"invalid hook filename {path.relative_to(ROOT)}; expected <event>.<name>.<ext>"
            )
        hooks.append(
            Hook(
                event=match.group("event"),
                name=match.group("name"),
                path=str(path.relative_to(ROOT)),
            )
        )
    return hooks


def configured_sections() -> set[str]:
    result = git_config("--get-regexp", r"^hook\..*\.managed-by$", check=False)
    if result.returncode == 1:
        return set()
    if result.returncode != 0:
        raise SystemExit(result.stderr.strip())

    sections: set[str] = set()
    for line in result.stdout.splitlines():
        key, _, value = line.partition(" ")
        if value != MANAGED_BY:
            continue
        _, section, _ = key.split(".", 2)
        sections.add(section)
    return sections


def config_value(key: str) -> str | None:
    result = git_config("--get", key, check=False)
    if result.returncode == 1:
        return None
    if result.returncode != 0:
        raise SystemExit(result.stderr.strip())
    return result.stdout.rstrip("\n")


def unset_section(section: str) -> None:
    result = git_config("--remove-section", f"hook.{section}", check=False)
    if result.returncode == 0:
        return
    if "no such section" in result.stderr.lower():
        return
    if result.returncode != 1:
        raise SystemExit(result.stderr.strip())


def sync_hooks() -> int:
    desired = {hook.section: hook for hook in discover_hooks()}
    managed = configured_sections()

    conflicts: list[str] = []
    for section in sorted(desired):
        marker = config_value(f"hook.{section}.managed-by")
        if marker not in {None, MANAGED_BY}:
            conflicts.append(
                f"Refusing to manage hook.{section}: managed-by is {marker!r}.",
            )
        if marker is None and config_value(f"hook.{section}.command") is not None:
            conflicts.append(f"Refusing to overwrite unmanaged hook.{section}.")

    if conflicts:
        for conflict in conflicts:
            print(conflict, file=sys.stderr)
        return 1

    for section in sorted(managed - set(desired)):
        unset_section(section)

    for section, hook in sorted(desired.items()):
        unset_section(section)
        git_config("set", f"hook.{section}.managed-by", MANAGED_BY)
        git_config("set", f"hook.{section}.command", hook.command)
        git_config("set", "--append", f"hook.{section}.event", hook.event)
        print(f"Installed hook.{section} for {hook.event}: {hook.command}")

    return 0


if __name__ == "__main__":
    raise SystemExit(sync_hooks())
