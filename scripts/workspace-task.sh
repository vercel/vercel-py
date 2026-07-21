#!/usr/bin/env python
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    task = Path(sys.argv[0]).stem
    root = Path(__file__).resolve().parent.parent
    runner = root / "scripts" / "poe" / "workspace_poe.py"
    return subprocess.call(
        (
            "uv",
            "run",
            "--project",
            str(root),
            "python",
            str(runner),
            "workspace",
            task,
            *sys.argv[1:],
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
