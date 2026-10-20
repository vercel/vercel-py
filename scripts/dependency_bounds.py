"""Resolve CI's dependency boundaries from every workspace package's declared ranges.

CI installs the result over the locked environment, so the test suite runs
against the lowest (or latest) versions the published packages permit while
the test tooling stays at its locked versions.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

try:
    from scripts import workspace
except ImportError:  # pragma: no cover - script execution path
    import workspace  # type: ignore[no-redef]

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    import tomli as tomllib  # type: ignore[no-redef]

ROOT = Path(__file__).resolve().parent.parent
RESOLUTIONS = {"lowest": "lowest-direct", "latest": "highest"}
LOWER_BOUND_OPERATORS = frozenset({">=", ">", "==", "~=", "==="})


def _load_pyproject(path: Path) -> dict:
    return tomllib.loads((path / "pyproject.toml").read_text(encoding="utf-8"))


def _package_requirements(data: dict) -> list[tuple[str, str]]:
    """Return ``(where, requirement)`` pairs a package publishes, extras included."""
    project = data.get("project", {})
    name = project.get("name", "?")
    release = data.get("tool", {}).get("vercel", {}).get("release", {})
    if "dependencies" in release:
        dependencies = release["dependencies"].get("dependencies", [])
    else:
        dependencies = project.get("dependencies", [])
    pairs = [(name, requirement) for requirement in dependencies]
    for extra, requirements in project.get("optional-dependencies", {}).items():
        pairs.extend((f"{name}[{extra}]", requirement) for requirement in requirements)
    return pairs


def declared_requirements(package_paths: Mapping[str, Path]) -> list[str]:
    """Collect the third-party requirements published by the workspace packages.

    Requirements on workspace siblings are dropped: CI always tests them at
    their workspace versions. Every remaining requirement must declare a lower
    bound, since a lowest resolution of an unbounded requirement picks a
    release nobody has tested in years.
    """
    members = {canonicalize_name(name) for name in package_paths}
    collected: list[str] = []
    unbounded: list[str] = []
    for path in package_paths.values():
        for where, text in _package_requirements(_load_pyproject(path)):
            requirement = Requirement(text)
            if canonicalize_name(requirement.name) in members:
                continue
            if not any(spec.operator in LOWER_BOUND_OPERATORS for spec in requirement.specifier):
                unbounded.append(f"{where}: {text}")
            collected.append(text)
    if unbounded:
        raise ValueError(
            "Every published dependency needs a lower bound; missing on:\n  "
            + "\n  ".join(unbounded)
        )
    return sorted(set(collected))


def resolve(bound: str, requirements: Iterable[str], *, exclude_newer: str | None) -> str:
    """Resolve *requirements* to pins at *bound* for the running interpreter."""
    args = [
        "uv",
        "pip",
        "compile",
        "--no-config",
        "--no-header",
        "--no-annotate",
        "--upgrade",
        "--python",
        sys.executable,
        "--resolution",
        RESOLUTIONS[bound],
    ]
    if exclude_newer:
        # Hold the same release cooldown as the lockfile, which `--no-config`
        # would otherwise drop.
        args.extend(["--exclude-newer", exclude_newer])
    result = subprocess.run(
        [*args, "-"],
        input="".join(f"{requirement}\n" for requirement in requirements),
        text=True,
        stdout=subprocess.PIPE,
        check=True,
    )
    return result.stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bound", choices=sorted(RESOLUTIONS))
    args = parser.parse_args()
    package_paths = {name: package.path for name, package in workspace.packages().items()}
    exclude_newer = _load_pyproject(ROOT).get("tool", {}).get("uv", {}).get("exclude-newer")
    print(resolve(args.bound, declared_requirements(package_paths), exclude_newer=exclude_newer))


if __name__ == "__main__":
    main()
