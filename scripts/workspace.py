#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    import tomli as tomllib  # type: ignore[no-redef]


ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Package:
    name: str
    path: Path
    version_file: Path
    dependencies: tuple[str, ...]


def _run_uv(args: list[str]) -> str:
    return subprocess.check_output(["uv", *args], cwd=ROOT, text=True, stderr=subprocess.STDOUT)


def _parse_json_output(output: str) -> Any:
    start = output.find("{")
    if start < 0:
        raise RuntimeError(f"uv did not emit JSON: {output}")
    return json.loads(output[start:])


def workspace_list(*, paths: bool = False) -> list[str]:
    args = ["workspace", "list"]
    if paths:
        args.append("--paths")
    return [line for line in _run_uv(args).splitlines() if line]


def workspace_metadata(*, locked: bool = True) -> dict[str, Any]:
    args = ["workspace", "metadata"]
    if locked:
        args.append("--locked")
    return _parse_json_output(_run_uv(args))


def _load_pyproject(path: Path) -> dict[str, Any]:
    with (path / "pyproject.toml").open("rb") as fp:
        return tomllib.load(fp)


def _version_path(path: Path, data: dict[str, Any]) -> Path:
    try:
        rel = data["tool"]["hatch"]["version"]["path"]
    except KeyError as exc:
        raise RuntimeError(
            f"{path / 'pyproject.toml'} is missing [tool.hatch.version].path"
        ) from exc
    return path / rel


def _member_dependency_names(
    metadata: dict[str, Any], members: dict[str, Path]
) -> dict[str, set[str]]:
    ids_by_name = {member["name"]: member["id"] for member in metadata["members"]}
    names_by_id = {member_id: name for name, member_id in ids_by_name.items()}
    edges: dict[str, set[str]] = {name: set() for name in members}

    resolution = metadata.get("resolution", {})
    for name, member_id in ids_by_name.items():
        record = resolution.get(member_id, {})
        for dep in record.get("dependencies", []):
            dep_name = names_by_id.get(dep.get("id"))
            if dep_name is not None:
                edges[name].add(dep_name)
    return edges


def _fallback_dependency_names(path: Path, members: dict[str, Path]) -> set[str]:
    data = _load_pyproject(path)
    sources = data.get("tool", {}).get("uv", {}).get("sources", {})
    return {
        name
        for name, source in sources.items()
        if name in members and source.get("workspace") is True
    }


def packages(*, locked: bool = True) -> dict[str, Package]:
    metadata = workspace_metadata(locked=locked)
    members = {member["name"]: Path(member["path"]) for member in metadata["members"]}
    edges = _member_dependency_names(metadata, members)

    result: dict[str, Package] = {}
    for name, path in members.items():
        data = _load_pyproject(path)
        deps = edges.get(name) or _fallback_dependency_names(path, members)
        result[name] = Package(
            name=name,
            path=path,
            version_file=_version_path(path, data),
            dependencies=tuple(sorted(deps)),
        )
    return result


def topological_names(packages_by_name: dict[str, Package]) -> list[str]:
    remaining = {name: set(package.dependencies) for name, package in packages_by_name.items()}
    ordered: list[str] = []
    while remaining:
        ready = sorted(name for name, deps in remaining.items() if not deps.intersection(remaining))
        if not ready:
            cycle = ", ".join(sorted(remaining))
            raise RuntimeError(f"workspace dependency cycle detected among: {cycle}")
        ordered.extend(ready)
        for name in ready:
            del remaining[name]
    return ordered


def reverse_dependencies(packages_by_name: dict[str, Package]) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {name: set() for name in packages_by_name}
    for name, package in packages_by_name.items():
        for dependency in package.dependencies:
            result[dependency].add(name)
    return result


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


def write_version(path: Path, version: str) -> None:
    text = path.read_text(encoding="utf-8")
    module = ast.parse(text, filename=str(path))
    for node in module.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets
        ):
            start, end = node.value.col_offset, node.value.end_col_offset
            line_no = node.value.lineno - 1
            lines = text.splitlines(keepends=True)
            lines[line_no] = lines[line_no][:start] + f'"{version}"' + lines[line_no][end:]
            path.write_text("".join(lines), encoding="utf-8")
            return
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "__version__"
        ):
            start, end = node.value.col_offset, node.value.end_col_offset  # type: ignore[union-attr]
            line_no = node.value.lineno - 1  # type: ignore[union-attr]
            lines = text.splitlines(keepends=True)
            lines[line_no] = lines[line_no][:start] + f'"{version}"' + lines[line_no][end:]
            path.write_text("".join(lines), encoding="utf-8")
            return
    raise RuntimeError(f"could not find __version__ in {path}")


def _cmd_list(args: argparse.Namespace) -> int:
    if args.paths and not args.names:
        values = workspace_list(paths=True)
    else:
        package_map = packages(locked=not args.unlocked)
        names = topological_names(package_map) if args.topological else sorted(package_map)
        if args.paths:
            values = [str(package_map[name].path) for name in names]
        else:
            values = names
    for value in values:
        print(value)
    return 0


def _cmd_version_file(args: argparse.Namespace) -> int:
    package_map = packages(locked=not args.unlocked)
    try:
        package = package_map[args.package]
    except KeyError as exc:
        expected = ", ".join(sorted(package_map))
        raise SystemExit(f"unknown package {args.package!r}; expected one of: {expected}") from exc
    print(package.version_file)
    return 0


def _cmd_version(args: argparse.Namespace) -> int:
    package_map = packages(locked=not args.unlocked)
    try:
        package = package_map[args.package]
    except KeyError as exc:
        expected = ", ".join(sorted(package_map))
        raise SystemExit(f"unknown package {args.package!r}; expected one of: {expected}") from exc
    print(read_version(package.version_file))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list")
    list_parser.add_argument("--names", action="store_true")
    list_parser.add_argument("--paths", action="store_true")
    list_parser.add_argument("--topological", action="store_true")
    list_parser.add_argument("--unlocked", action="store_true")
    list_parser.set_defaults(func=_cmd_list)

    version_file_parser = subparsers.add_parser("version-file")
    version_file_parser.add_argument("package")
    version_file_parser.add_argument("--unlocked", action="store_true")
    version_file_parser.set_defaults(func=_cmd_version_file)

    version_parser = subparsers.add_parser("version")
    version_parser.add_argument("package", nargs="?", default="vercel")
    version_parser.add_argument("--unlocked", action="store_true")
    version_parser.set_defaults(func=_cmd_version)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
