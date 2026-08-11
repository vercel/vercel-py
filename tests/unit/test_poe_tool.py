from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


TOOL = Path(__file__).resolve().parents[2] / "scripts" / "poe" / "tasks" / "pytest"


def _runner(path: Path, name: str) -> None:
    executable = path / name
    executable.write_text('#!/bin/sh\nprintf \'%s\\n\' "$0" "$@"\n')
    executable.chmod(0o755)


def _run(
    tmp_path: Path,
    args: Sequence[str],
    *,
    force_pytest: bool = False,
    lograil_progress: bool = False,
    parallel: bool = True,
) -> list[str]:
    env = os.environ.copy()
    env["PATH"] = os.pathsep.join((str(tmp_path), "/usr/bin", "/bin"))
    env.pop("POE_EXTRA_ARGS", None)
    env.pop("WORKSPACE_POE_TEST_RUNNER", None)
    if lograil_progress:
        env["WORKSPACE_POE_LOGRAIL_PROGRESS"] = "1"
    else:
        env.pop("WORKSPACE_POE_LOGRAIL_PROGRESS", None)
    env.pop("WORKSPACE_POE_SCOPE_ARGS", None)
    if force_pytest:
        env["FORCE_PYTEST"] = "1"
    else:
        env.pop("FORCE_PYTEST", None)
    if parallel:
        env.pop("WORKSPACE_POE_PARALLEL", None)
    else:
        env["WORKSPACE_POE_PARALLEL"] = "0"
    result = subprocess.run(
        (TOOL, *args),
        check=True,
        capture_output=True,
        cwd=tmp_path,
        env=env,
        text=True,
    )
    return result.stdout.splitlines()


def test_pytest_wrapper_prefers_ggt(tmp_path: Path) -> None:
    _runner(tmp_path, "ggt")
    _runner(tmp_path, "pytest")

    output = _run(tmp_path, ("tests",))

    assert output[-2:] == [str(tmp_path / "ggt"), "tests"]


def test_pytest_wrapper_runs_ggt_sequentially_when_parallel_is_disabled(
    tmp_path: Path,
) -> None:
    _runner(tmp_path, "ggt")

    output = _run(tmp_path, ("tests",), parallel=False)

    assert output[-4:] == [str(tmp_path / "ggt"), "-j", "1", "tests"]


def test_pytest_wrapper_uses_structured_ggt_output_for_lograil(
    tmp_path: Path,
) -> None:
    _runner(tmp_path, "ggt")

    output = _run(tmp_path, ("tests",), lograil_progress=True)

    assert output[-4:] == [
        str(tmp_path / "ggt"),
        "--output-format",
        "json",
        "tests",
    ]


def test_pytest_wrapper_falls_back_to_parallel_pytest(tmp_path: Path) -> None:
    _runner(tmp_path, "pytest")

    output = _run(tmp_path, ("tests",))

    assert output[-4:] == [str(tmp_path / "pytest"), "-n", "auto", "tests"]


def test_pytest_wrapper_can_force_pytest(tmp_path: Path) -> None:
    _runner(tmp_path, "ggt")
    _runner(tmp_path, "pytest")

    output = _run(tmp_path, ("tests",), force_pytest=True)

    assert output[-4:] == [str(tmp_path / "pytest"), "-n", "auto", "tests"]
