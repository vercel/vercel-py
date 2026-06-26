#!/usr/bin/env python3
from __future__ import annotations

import ast
import sys
from pathlib import Path

from workspace_packages import version_files


def read_version(path: Path) -> str:
    module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in module.body:
        value: ast.expr | None
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets
        ):
            value = node.value
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "__version__"
        ):
            value = node.value
        else:
            continue
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return value.value
    raise RuntimeError(f"could not find __version__ in {path}")


package = sys.argv[1] if len(sys.argv) > 1 else "vercel"
versions = version_files()
try:
    version_file = versions[package]
except KeyError as exc:
    packages = ", ".join(versions)
    raise SystemExit(f"unknown package {package!r}; expected one of: {packages}") from exc

print(read_version(version_file))
