"""Execute package-owned Blob examples with live credentials."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.live

EXAMPLES = sorted((Path(__file__).resolve().parents[2] / "examples").glob("blob_*.py"))


def _has_credentials() -> bool:
    return bool(
        os.getenv("BLOB_READ_WRITE_TOKEN")
        or os.getenv("VERCEL_BLOB_READ_WRITE_TOKEN")
        or (os.getenv("VERCEL_OIDC_TOKEN") and os.getenv("BLOB_STORE_ID"))
    )


@pytest.mark.skipif(not _has_credentials(), reason="requires live Blob credentials")
@pytest.mark.parametrize("script", EXAMPLES, ids=lambda path: path.name)
def test_example(script: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
