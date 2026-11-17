#!/usr/bin/env python3
"""Keep the workspace dependency edges in turbo.json in sync with uv.

Turborepo only adds a package-graph edge for a dependency it finds in a
member's static ``[project]`` metadata. Our members declare ``dependencies``
as dynamic and generate them in a Hatch hook, so Turborepo sees no edges
between them. Without edges, a change to ``vercel-oidc`` would not invalidate
the cached tests of the packages that import it.

Each member therefore gets a ``<package>#sources`` task that depends on the
``sources`` task of its workspace dependencies. ``typecheck`` and ``test``
depend on ``sources``, so their hashes cover the importable code of every
transitive workspace dependency. This script generates those entries from the
graph uv resolved.
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path
from typing import Any

try:
    from scripts import workspace
except ImportError:  # pragma: no cover - script execution path
    import workspace  # type: ignore[no-redef]

TURBO_JSON = workspace.ROOT / "turbo.json"
SOURCES_TASK = "sources"
SUFFIX = f"#{SOURCES_TASK}"
MAX_INLINE_WIDTH = 88


def source_tasks(
    packages: dict[str, workspace.Package], base: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    """Return the ``<package>#sources`` entries for members with dependencies.

    A package-specific entry replaces the shared ``sources`` definition instead
    of extending it, so each entry repeats *base*.
    """
    return {
        f"{name}{SUFFIX}": {
            **base,
            "dependsOn": [f"{dependency}{SUFFIX}" for dependency in package.dependencies],
        }
        for name, package in sorted(packages.items())
        if package.dependencies
    }


def render(config: dict[str, Any], packages: dict[str, workspace.Package]) -> dict[str, Any]:
    """Return *config* with its generated entries replaced."""
    tasks = {name: task for name, task in config["tasks"].items() if not name.endswith(SUFFIX)}
    generated = source_tasks(packages, tasks[SOURCES_TASK])
    return {**config, "tasks": {**tasks, **generated}}


def module_owners(packages: dict[str, workspace.Package]) -> dict[str, str]:
    """Map each importable ``vercel`` module to the member that ships it."""
    owners: dict[str, str] = {}
    for name, package in packages.items():
        for path in (package.path / "vercel").rglob("*.py"):
            parts = list(path.relative_to(package.path).with_suffix("").parts)
            if parts[-1] == "__init__":
                parts.pop()
            owners[".".join(parts)] = name
    return owners


def _owner(module: str, owners: dict[str, str]) -> str | None:
    parts = module.split(".")
    while parts:
        owner = owners.get(".".join(parts))
        if owner is not None:
            return owner
        parts.pop()
    return None


def _imported_modules(path: Path) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"), filename=str(path))):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            modules.add(node.module)
            modules.update(f"{node.module}.{alias.name}" for alias in node.names)
    return modules


def _closure(name: str, packages: dict[str, workspace.Package]) -> set[str]:
    seen = {name}
    pending = [name]
    while pending:
        for dependency in packages[pending.pop()].dependencies:
            if dependency not in seen:
                seen.add(dependency)
                pending.append(dependency)
    return seen


def undeclared_imports(packages: dict[str, workspace.Package]) -> dict[str, set[str]]:
    """Return the members each package imports without depending on them.

    Every member is installed in one environment, so such an import works. It
    would not be part of the importing package's task hash, though, and a
    change to the imported member would leave stale results in the cache.
    """
    owners = module_owners(packages)
    undeclared: dict[str, set[str]] = {}
    for name, package in packages.items():
        declared = _closure(name, packages)
        for path in package.path.rglob("*.py"):
            for module in _imported_modules(path):
                owner = _owner(module, owners)
                if owner is not None and owner not in declared:
                    undeclared.setdefault(name, set()).add(owner)
    return undeclared


def dumps(value: Any, indent: int = 0) -> str:
    """Serialize like ``json.dumps(indent=2)``, keeping short lists on one line."""
    text = _dumps(value, indent)
    return text + "\n" if indent == 0 else text


def _dumps(value: Any, indent: int) -> str:
    pad = "  " * (indent + 1)
    end = "  " * indent
    if isinstance(value, dict) and value:
        items = [
            f"{pad}{json.dumps(key)}: {_dumps(item, indent + 1)}" for key, item in value.items()
        ]
        return "{\n" + ",\n".join(items) + f"\n{end}}}"
    if isinstance(value, list) and value:
        inline = json.dumps(value)
        if len(pad) + len(inline) <= MAX_INLINE_WIDTH:
            return inline
        return (
            "[\n" + ",\n".join(f"{pad}{_dumps(item, indent + 1)}" for item in value) + f"\n{end}]"
        )
    return json.dumps(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="fail if turbo.json is out of date")
    args = parser.parse_args(argv)

    current = TURBO_JSON.read_text(encoding="utf-8")
    expected = dumps(render(json.loads(current), workspace.packages()))
    if current == expected:
        return 0
    if args.check:
        print(
            "turbo.json does not match the uv workspace graph; "
            "run `uv run python scripts/turbo_graph.py`",
            file=sys.stderr,
        )
        return 1
    TURBO_JSON.write_text(expected, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
