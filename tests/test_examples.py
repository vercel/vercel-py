from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

# Required on CI, optional locally
_is_ci = bool(os.getenv("CI"))
_has_credentials = bool(
    os.getenv("BLOB_READ_WRITE_TOKEN")
    and os.getenv("VERCEL_TOKEN")
    and os.getenv("VERCEL_PROJECT_ID")
    and os.getenv("VERCEL_TEAM_ID")
)

_examples_dir = Path(__file__).resolve().parents[1] / "examples"
_example_files = (
    sorted([p for p in _examples_dir.iterdir() if p.is_file() and p.suffix == ".py"])
    if _examples_dir.is_dir()
    else []
)


@pytest.mark.skipif(
    not _is_ci and not _has_credentials,
    reason="Requires BLOB_READ_WRITE_TOKEN, VERCEL_TOKEN, VERCEL_PROJECT_ID, and VERCEL_TEAM_ID",
)
@pytest.mark.parametrize("script_path", _example_files, ids=lambda p: p.name)
def test_example(script_path: Path) -> None:
    """Run a single example script and verify it succeeds."""
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"{script_path.name} failed with code {result.returncode}\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
