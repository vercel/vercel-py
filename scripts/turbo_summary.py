#!/usr/bin/env python3
"""Report a Turborepo run and fail if it did not cover every package.

A cached task is skipped, so a run that passes proves nothing about a package
whose task never entered the task graph. This reads the run summary written by
``turbo run --summarize`` and requires each requested task to have passed, or
to have been restored from the cache, for every workspace package and the
workspace root. It also writes which tasks ran to the GitHub step summary.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tomllib
from pathlib import Path
from typing import Any

try:
    from scripts import workspace
except ImportError:  # pragma: no cover - script execution path
    import workspace  # type: ignore[no-redef]

RUNS_DIR = workspace.ROOT / ".turbo" / "runs"


def latest_summary(runs_dir: Path = RUNS_DIR) -> dict[str, Any]:
    runs = sorted(runs_dir.glob("*.json"), key=lambda path: path.stat().st_mtime)
    if not runs:
        raise SystemExit(f"no run summary in {runs_dir}; pass --summarize to `turbo run`")
    return json.loads(runs[-1].read_text(encoding="utf-8"))


def workspace_package_names() -> list[str]:
    with (workspace.ROOT / "pyproject.toml").open("rb") as fp:
        root = tomllib.load(fp)["tool"]["turbo"]["name"]
    return sorted([*workspace.workspace_list(), root])


def task_status(task: dict[str, Any]) -> str:
    """Return ``cached``, ``passed``, or ``failed`` for one summarized task."""
    if task["cache"]["status"] == "HIT":
        return "cached"
    execution = task.get("execution") or {}
    return "passed" if execution.get("exitCode") == 0 else "failed"


def collect(
    summary: dict[str, Any], packages: list[str], tasks: list[str]
) -> tuple[dict[str, str], list[str]]:
    """Return each expected task's status and the expected tasks that did not run."""
    ran = {task["taskId"]: task_status(task) for task in summary["tasks"]}
    expected = [f"{package}#{task}" for package in packages for task in tasks]
    statuses = {task_id: ran[task_id] for task_id in expected if task_id in ran}
    missing = [task_id for task_id in expected if task_id not in ran]
    return statuses, missing


def render(
    statuses: dict[str, str], missing: list[str], packages: list[str], tasks: list[str]
) -> str:
    lines = [
        "| Package | " + " | ".join(tasks) + " |",
        "| --- | " + " | ".join("---" for _ in tasks) + " |",
    ]
    for package in packages:
        cells = [statuses.get(f"{package}#{task}", "missing") for task in tasks]
        lines.append(f"| {package} | " + " | ".join(cells) + " |")
    counts = {
        status: sum(1 for value in statuses.values() if value == status)
        for status in ("passed", "cached", "failed")
    }
    lines.append("")
    lines.append(
        f"{counts['passed']} ran, {counts['cached']} restored from cache, "
        f"{counts['failed']} failed, {len(missing)} missing."
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("tasks", nargs="+", help="task names passed to `turbo run`")
    args = parser.parse_args(argv)

    packages = workspace_package_names()
    statuses, missing = collect(latest_summary(), packages, args.tasks)
    report = render(statuses, missing, packages, args.tasks)
    print(report)
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as fp:
            fp.write(report)

    if missing:
        print(f"tasks missing from the Turborepo run: {', '.join(missing)}", file=sys.stderr)
        return 1
    return 1 if "failed" in statuses.values() else 0


if __name__ == "__main__":
    sys.exit(main())
