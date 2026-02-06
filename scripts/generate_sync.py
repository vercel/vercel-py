#!/usr/bin/env python3
"""Thin wrapper around unasyncd for generating sync code from async sources."""

import subprocess
import sys


def main() -> int:
    cmd = ["unasyncd"]
    if "--check" in sys.argv[1:]:
        cmd.append("--check")
    result = subprocess.run(cmd)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
