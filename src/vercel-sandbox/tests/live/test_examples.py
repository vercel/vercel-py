"""Executable Sandbox examples."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from .conftest import requires_sandbox_credentials

_EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"
_EXAMPLE_ARGUMENTS = {
    "sandbox_04_dev_server.py": ("--destroy",),
}


def _discover_examples(directory: Path) -> list[Path]:
    """Return only top-level, standalone Sandbox example programs."""
    if not directory.is_dir():
        return []

    examples = sorted(
        path
        for path in directory.iterdir()
        if path.is_file() and path.suffix == ".py" and path.name.startswith("sandbox_")
    )
    scope_args = shlex.split(os.getenv("WORKSPACE_POE_SCOPE_ARGS", ""))
    if not scope_args:
        return examples

    directory = directory.resolve()
    selected = []
    for scope_arg in scope_args:
        candidate = (Path.cwd() / scope_arg).resolve()
        if candidate == directory:
            return examples
        if candidate.parent == directory and candidate in examples:
            selected.append(candidate)
    return selected or examples


_EXAMPLE_FILES = _discover_examples(_EXAMPLES_DIR)


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.parametrize("script_path", _EXAMPLE_FILES, ids=lambda path: path.name)
def test_example(script_path: Path) -> None:
    """Run one standalone Sandbox example and verify it succeeds."""
    _run_example(script_path)


def _run_example(script_path: Path) -> None:
    command = [sys.executable, str(script_path), *_EXAMPLE_ARGUMENTS.get(script_path.name, ())]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired as error:
        stdout = error.stdout.decode() if isinstance(error.stdout, bytes) else error.stdout or ""
        stderr = error.stderr.decode() if isinstance(error.stderr, bytes) else error.stderr or ""
        max_chars = 10000
        if len(stdout) > max_chars:
            stdout = f"... [{len(stdout) - max_chars} chars truncated] ...\n" + stdout[-max_chars:]
        pytest.fail(
            f"{script_path.name} timed out after {error.timeout}s\n"
            f"STDOUT (tail):\n{stdout}\n"
            f"STDERR:\n{stderr}"
        )

    assert result.returncode == 0, (
        f"{script_path.name} failed with code {result.returncode}\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
