"""Execute package-owned Blob examples with live credentials."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.live

_EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"


def _discover_examples(directory: Path) -> list[Path]:
    examples = sorted(directory.glob("blob_*.py"))
    scope_args = shlex.split(os.getenv("WORKSPACE_POE_SCOPE_ARGS", ""))
    if not scope_args:
        return examples

    directory = directory.resolve()
    selected = []
    for scope_arg in scope_args:
        candidate = (Path.cwd() / scope_arg).resolve()
        if candidate in (Path(__file__).resolve(), directory):
            return examples
        if candidate.parent == directory and candidate in examples:
            selected.append(candidate)
    if not selected and os.getenv("WORKSPACE_POE_SCOPE_TASK") == "test-examples":
        raise pytest.UsageError(
            f"no example scripts under {directory} matched the requested scope: "
            f"{' '.join(scope_args)}"
        )
    return selected or examples


EXAMPLES = _discover_examples(_EXAMPLES_DIR)


def _has_credentials() -> bool:
    return bool(
        os.getenv("BLOB_READ_WRITE_TOKEN")
        or os.getenv("VERCEL_BLOB_READ_WRITE_TOKEN")
        or (os.getenv("VERCEL_OIDC_TOKEN") and os.getenv("BLOB_STORE_ID"))
    )


def _has_sandbox_credentials() -> bool:
    return bool(
        os.getenv("VERCEL_OIDC_TOKEN")
        or (
            os.getenv("VERCEL_TOKEN")
            and os.getenv("VERCEL_TEAM_ID")
            and os.getenv("VERCEL_PROJECT_ID")
        )
    )


@pytest.mark.parametrize("script", EXAMPLES, ids=lambda path: path.name)
def test_example(script: Path) -> None:
    if not _has_credentials():
        pytest.skip("requires live Blob credentials")
    if script.name == "blob_sandbox_streaming.py" and not _has_sandbox_credentials():
        pytest.skip("requires live Sandbox credentials")

    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
