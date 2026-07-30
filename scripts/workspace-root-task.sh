#!/usr/bin/env python
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} <poe-task> [args ...]", file=sys.stderr)
        return 2
    root = Path(__file__).resolve().parent.parent
    runner = root / "scripts" / "poe" / "workspace_poe.py"
    return subprocess.call(
        ("uv", "run", "--project", str(root), "python", str(runner), "root-task", *sys.argv[1:])
    )


if __name__ == "__main__":
    raise SystemExit(main())
