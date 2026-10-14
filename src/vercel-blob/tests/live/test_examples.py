"""Execute package-owned examples using live credentials."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.live
_EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"


def _examples() -> list[Path]:
    examples = sorted(_EXAMPLES_DIR.glob("blob_*.py"))
    scopes = shlex.split(os.getenv("WORKSPACE_POE_SCOPE_ARGS", ""))
    if not scopes:
        return examples
    selected = []
    for scope in scopes:
        path = (Path.cwd() / scope).resolve()
        if path in (_EXAMPLES_DIR, Path(__file__).resolve()):
            return examples
        if path in examples:
            selected.append(path)
    if not selected and os.getenv("WORKSPACE_POE_SCOPE_TASK") == "test-examples":
        raise pytest.UsageError("No Blob example matches the requested scope")
    return selected or examples


@pytest.mark.parametrize("script", _examples(), ids=lambda path: path.name)
@pytest.mark.usefixtures("store")
def test_example(script: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True, timeout=120
    )
    assert result.returncode == 0, f"{script.name} failed with exit code {result.returncode}"
    assert "lifecycle passed; test object deleted." in result.stdout
