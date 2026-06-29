import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

# Required on CI, optional locally
_is_ci = bool(os.getenv("CI"))
_has_oidc_credentials = bool(os.getenv("VERCEL_OIDC_TOKEN"))
_has_vercel_token_credentials = bool(
    os.getenv("VERCEL_TOKEN") and os.getenv("VERCEL_PROJECT_ID") and os.getenv("VERCEL_TEAM_ID")
)
_has_explicit_vercel_credentials = bool(
    (os.getenv("VERCEL_TOKEN") or os.getenv("VERCEL_OIDC_TOKEN"))
    and os.getenv("VERCEL_PROJECT_ID")
    and os.getenv("VERCEL_TEAM_ID")
)
_has_credentials = bool(os.getenv("BLOB_READ_WRITE_TOKEN") and _has_explicit_vercel_credentials)
_has_sandbox_credentials = _has_oidc_credentials or _has_vercel_token_credentials

_examples_dir = Path(__file__).resolve().parents[1] / "examples"
_example_files = (
    sorted([p for p in _examples_dir.iterdir() if p.is_file() and p.suffix == ".py"])
    if _examples_dir.is_dir()
    else []
)
_unstable_examples_dir = _examples_dir / "unstable"
_unstable_example_files = (
    sorted([p for p in _unstable_examples_dir.iterdir() if p.is_file() and p.suffix == ".py"])
    if _unstable_examples_dir.is_dir()
    else []
)


@pytest.mark.skipif(
    not _is_ci and not _has_credentials,
    reason=(
        "Requires BLOB_READ_WRITE_TOKEN, VERCEL_TOKEN or VERCEL_OIDC_TOKEN, "
        "VERCEL_PROJECT_ID, and VERCEL_TEAM_ID"
    ),
)
@pytest.mark.parametrize("script_path", _example_files, ids=lambda p: p.name)
def test_example(script_path: Path) -> None:
    """Run a single example script and verify it succeeds."""
    _run_example(script_path)


@pytest.mark.skipif(
    not _has_sandbox_credentials,
    reason=("Requires VERCEL_OIDC_TOKEN or VERCEL_TOKEN with VERCEL_PROJECT_ID and VERCEL_TEAM_ID"),
)
@pytest.mark.parametrize("script_path", _unstable_example_files, ids=lambda p: p.name)
def test_unstable_example(script_path: Path) -> None:
    """Run a single unstable Sandbox example script and verify it succeeds."""
    _run_example(script_path)


def _run_example(script_path: Path) -> None:
    command = [sys.executable, str(script_path)]
    if script_path == _unstable_examples_dir / "sandbox_04_dev_server.py":
        command.extend(
            [
                "--name",
                f"vercel-py-example-dev-{uuid4().hex[:12]}",
                "--install",
                "true",
                "--destroy",
            ]
        )

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired as e:
        stdout = e.stdout.decode() if e.stdout else ""
        stderr = e.stderr.decode() if e.stderr else ""
        # Tail stdout to avoid overwhelming output
        max_chars = 10000
        if len(stdout) > max_chars:
            stdout = f"... [{len(stdout) - max_chars} chars truncated] ...\n" + stdout[-max_chars:]
        pytest.fail(
            f"{script_path.name} timed out after {e.timeout}s\n"
            f"STDOUT (tail):\n{stdout}\n"
            f"STDERR:\n{stderr}"
        )
    assert result.returncode == 0, (
        f"{script_path.name} failed with code {result.returncode}\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
