"""Resolve CI's AnyIO boundary from the queue SDK's declared dependency range."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from packaging.requirements import Requirement

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    import tomli as tomllib  # type: ignore[no-redef]

PYPROJECT = Path(__file__).resolve().parent.parent / "src/vercel-queue/pyproject.toml"


def anyio_requirement(pyproject: Path = PYPROJECT) -> str:
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    for dependency in data["tool"]["vercel"]["release"]["dependencies"]["dependencies"]:
        requirement = Requirement(dependency)
        if requirement.name.lower() == "anyio":
            return str(requirement)
    raise ValueError(f"No AnyIO dependency declared in {pyproject}")


def resolve_boundary(boundary: str, pyproject: Path = PYPROJECT) -> str:
    requirement = anyio_requirement(pyproject)
    result = subprocess.run(
        [
            "uv",
            "pip",
            "compile",
            "--no-config",
            "--no-deps",
            "--no-header",
            "--no-annotate",
            "--upgrade",
            "--python",
            sys.executable,
            "--resolution",
            "lowest-direct" if boundary == "lowest" else "highest",
            "-",
        ],
        input=requirement + "\n",
        text=True,
        stdout=subprocess.PIPE,
        check=True,
    )
    return result.stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("boundary", choices=("lowest", "latest"))
    args = parser.parse_args()
    print(resolve_boundary(args.boundary))


if __name__ == "__main__":
    main()
