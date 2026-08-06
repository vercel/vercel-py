import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

# Optional when credentials are unavailable, including forked pull requests.
_has_explicit_vercel_credentials = bool(
    (os.getenv("VERCEL_TOKEN") or os.getenv("VERCEL_OIDC_TOKEN"))
    and os.getenv("VERCEL_PROJECT_ID")
    and os.getenv("VERCEL_TEAM_ID")
)
_has_credentials = bool(os.getenv("BLOB_READ_WRITE_TOKEN") and _has_explicit_vercel_credentials)
_examples_dir = Path(__file__).resolve().parents[1] / "examples"


def _discover_examples(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    examples = sorted(
        path for path in directory.iterdir() if path.is_file() and path.suffix == ".py"
    )
    scope_args = shlex.split(os.getenv("WORKSPACE_POE_SCOPE_ARGS", ""))
    if not scope_args:
        return examples

    directory = directory.resolve()
    selected = []
    for scope_arg in scope_args:
        candidate = (Path.cwd() / scope_arg).resolve()
        if candidate == Path(__file__).resolve():
            return examples
        if candidate == directory:
            return examples
        if candidate.parent == directory and candidate in examples:
            selected.append(candidate)
    if not selected and os.getenv("WORKSPACE_POE_SCOPE_TASK") == "test-examples":
        raise pytest.UsageError(
            f"no example scripts under {directory} matched the requested scope: "
            f"{' '.join(scope_args)}"
        )
    return selected or examples


_example_files = _discover_examples(_examples_dir)


@pytest.mark.skipif(
    not _has_credentials,
    reason=(
        "Requires BLOB_READ_WRITE_TOKEN, VERCEL_TOKEN or VERCEL_OIDC_TOKEN, "
        "VERCEL_PROJECT_ID, and VERCEL_TEAM_ID"
    ),
)
@pytest.mark.parametrize("script_path", _example_files, ids=lambda p: p.name)
def test_example(script_path: Path) -> None:
    """Run a single example script and verify it succeeds."""
    _run_example(script_path)


def _run_example(script_path: Path) -> None:
    command = [sys.executable, str(script_path)]

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
