#!/usr/bin/env python3
"""Sync the version from package.json into pyproject.toml."""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    new_version = json.loads((ROOT / "package.json").read_text())["version"]
    pyproject_path = ROOT / "pyproject.toml"
    content = pyproject_path.read_text()
    updated, count = re.subn(
        r'^(version\s*=\s*")[^"]*(")',
        rf"\g<1>{new_version}\g<2>",
        content,
        count=1,
        flags=re.MULTILINE,
    )
    if count == 0:
        print("ERROR: Could not find version in pyproject.toml", file=sys.stderr)
        sys.exit(1)
    pyproject_path.write_text(updated)
    print(f"Synced version {new_version} to pyproject.toml")


if __name__ == "__main__":
    main()
