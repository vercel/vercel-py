#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import configparser
import email.parser
import email.policy
import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from collections import defaultdict, deque
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
SHARED_BUNDLE_DISTRIBUTION = "vercel-internal-shared-vendored-deps"
SPLIT_OWNERSHIP_FILES = frozenset(
    {
        "vercel/__init__.py",
        "vercel/py.typed",
    }
)
SPLIT_OWNERSHIP_PREFIXES = (
    "vercel/internal/core/",
    "vercel/sandbox/",
)


class WheelTestError(RuntimeError):
    """A wheel could not satisfy an installed-artifact test invariant."""


class WheelOwnershipError(WheelTestError):
    """More than one distribution owns the same first-party file."""


class _CaseSensitiveConfigParser(configparser.ConfigParser):
    def optionxform(self, optionstr: str) -> str:
        return optionstr


@dataclass(frozen=True)
class WheelArtifact:
    path: Path
    distribution: str
    version: str
    files: frozenset[str]
    requirements: tuple[Requirement, ...]
    console_scripts: tuple[tuple[str, str], ...]

    @classmethod
    def load(cls, path: Path) -> WheelArtifact:
        path = path.resolve()
        if not path.is_file():
            raise WheelTestError(f"wheel does not exist: {path}")

        with zipfile.ZipFile(path) as archive:
            names = [info.filename for info in archive.infolist() if not info.is_dir()]
            duplicates = sorted(name for name, count in _counts(names).items() if count > 1)
            if duplicates:
                rendered = "\n".join(f"  - {name}" for name in duplicates)
                raise WheelTestError(f"wheel contains duplicate members: {path}\n{rendered}")

            metadata_paths = [name for name in names if name.endswith(".dist-info/METADATA")]
            if len(metadata_paths) != 1:
                raise WheelTestError(
                    f"expected one .dist-info/METADATA member in {path}, "
                    f"found {len(metadata_paths)}"
                )
            metadata = email.parser.BytesParser(policy=email.policy.default).parsebytes(
                archive.read(metadata_paths[0])
            )
            distribution = metadata.get("Name")
            version = metadata.get("Version")
            if not distribution or not version:
                raise WheelTestError(f"wheel metadata is missing Name or Version: {path}")

            requirements = tuple(
                Requirement(value) for value in metadata.get_all("Requires-Dist", failobj=[])
            )
            entry_points_paths = [
                name for name in names if name.endswith(".dist-info/entry_points.txt")
            ]
            if len(entry_points_paths) > 1:
                raise WheelTestError(f"wheel contains multiple entry_points.txt files: {path}")
            console_scripts: tuple[tuple[str, str], ...] = ()
            if entry_points_paths:
                parser = _CaseSensitiveConfigParser(interpolation=None)
                parser.read_string(archive.read(entry_points_paths[0]).decode("utf-8"))
                if parser.has_section("console_scripts"):
                    console_scripts = tuple(sorted(parser.items("console_scripts")))

        return cls(
            path=path,
            distribution=str(distribution),
            version=str(version),
            files=frozenset(names),
            requirements=requirements,
            console_scripts=console_scripts,
        )

    @property
    def normalized_distribution(self) -> str:
        return _normalize_name(self.distribution)

    @property
    def first_party_files(self) -> frozenset[str]:
        return frozenset(name for name in self.files if name.startswith("vercel/"))

    def to_dict(self) -> dict[str, object]:
        return {
            "console_scripts": dict(self.console_scripts),
            "distribution": self.distribution,
            "files": sorted(self.files),
            "path": str(self.path),
            "requires_dist": [str(requirement) for requirement in self.requirements],
            "version": self.version,
        }


def _counts(values: Iterable[str]) -> dict[str, int]:
    result: dict[str, int] = defaultdict(int)
    for value in values:
        result[value] += 1
    return result


def _normalize_name(name: str) -> str:
    return str(canonicalize_name(name))


def _load_pyproject(path: Path) -> dict[str, Any]:
    with path.joinpath("pyproject.toml").open("rb") as fp:
        return tomllib.load(fp)


def wheel_include_paths(package: workspace.Package) -> tuple[Path, ...]:
    """Return all import paths owned by a package's ordinary wheel configuration."""
    data = _load_pyproject(package.path)
    wheel = (
        data.get("tool", {}).get("hatch", {}).get("build", {}).get("targets", {}).get("wheel", {})
    )
    includes = wheel.get("only-include")
    if includes is None:
        try:
            includes = [package.version_file.parent.relative_to(package.path).as_posix()]
        except ValueError as exc:
            raise WheelTestError(
                f"version file for {package.name} is outside its package directory"
            ) from exc
    if not isinstance(includes, list) or not includes:
        raise WheelTestError(f"{package.name} wheel only-include must be a non-empty list")

    result = []
    for include in includes:
        if not isinstance(include, str) or not include.strip("/"):
            raise WheelTestError(f"{package.name} has an invalid wheel include: {include!r}")
        result.append(Path(include.strip("/")))
    return tuple(result)


def dependency_closure(
    selected: Sequence[str], packages: dict[str, workspace.Package]
) -> tuple[str, ...]:
    unknown = sorted(set(selected) - packages.keys())
    if unknown:
        raise WheelTestError(f"unknown workspace package(s): {', '.join(unknown)}")

    required = set(selected)
    pending = list(selected)
    while pending:
        package_name = pending.pop()
        for dependency in packages[package_name].dependencies:
            if dependency not in required:
                required.add(dependency)
                pending.append(dependency)
    return tuple(name for name in workspace.topological_names(packages) if name in required)


def build_workspace_wheels(package_names: Sequence[str], *, dist_dir: Path) -> tuple[Path, ...]:
    packages = workspace.packages()
    ordered_names = dependency_closure(package_names, packages)
    dist_dir = dist_dir.resolve()
    dist_dir.mkdir(parents=True, exist_ok=True)

    built = []
    for package_name in ordered_names:
        subprocess.check_call(
            [
                "uv",
                "build",
                "--package",
                package_name,
                "--no-sources",
                "--out-dir",
                str(dist_dir),
            ],
            cwd=ROOT,
        )
        built.append(find_distribution_wheel(dist_dir, package_name).path)
    return tuple(built)


def discover_wheels(dist_dir: Path) -> tuple[WheelArtifact, ...]:
    dist_dir = dist_dir.resolve()
    if not dist_dir.is_dir():
        raise WheelTestError(f"artifact directory does not exist: {dist_dir}")
    return tuple(WheelArtifact.load(path) for path in sorted(dist_dir.glob("*.whl")))


def _artifact_index(artifacts: Iterable[WheelArtifact]) -> dict[str, WheelArtifact]:
    result: dict[str, WheelArtifact] = {}
    for artifact in artifacts:
        name = artifact.normalized_distribution
        previous = result.get(name)
        if previous is not None:
            raise WheelTestError(
                f"multiple wheels for {artifact.distribution}: {previous.path}, {artifact.path}"
            )
        result[name] = artifact
    return result


def find_distribution_wheel(dist_dir: Path, distribution: str) -> WheelArtifact:
    normalized = _normalize_name(distribution)
    artifacts = _artifact_index(discover_wheels(dist_dir))
    try:
        return artifacts[normalized]
    except KeyError:
        raise WheelTestError(
            f"expected one {distribution} wheel in {dist_dir.resolve()}, found none"
        ) from None


def _split_ownership_file(path: str) -> bool:
    return path in SPLIT_OWNERSHIP_FILES or path.startswith(SPLIT_OWNERSHIP_PREFIXES)


def ownership_overlaps(
    artifacts: Iterable[WheelArtifact], *, split_paths_only: bool = False
) -> dict[str, tuple[str, ...]]:
    owners: dict[str, list[str]] = defaultdict(list)
    for artifact in artifacts:
        for path in artifact.first_party_files:
            if split_paths_only and not _split_ownership_file(path):
                continue
            owners[path].append(artifact.distribution)
    return {
        path: tuple(sorted(distributions))
        for path, distributions in sorted(owners.items())
        if len(distributions) > 1
    }


def assert_unique_ownership(
    artifacts: Iterable[WheelArtifact], *, split_paths_only: bool = False
) -> None:
    overlaps = ownership_overlaps(artifacts, split_paths_only=split_paths_only)
    if not overlaps:
        return
    rendered = "\n".join(
        f"  - {path}: {', '.join(distributions)}" for path, distributions in overlaps.items()
    )
    scope = "split-owned" if split_paths_only else "first-party"
    raise WheelOwnershipError(f"overlapping {scope} wheel files:\n{rendered}")


def workspace_distribution_names() -> frozenset[str]:
    names = set(workspace.packages())
    names.update(f"{name}-bundle" for name in tuple(names))
    names.add(SHARED_BUNDLE_DISTRIBUTION)
    return frozenset(_normalize_name(name) for name in names)


def local_dependency_artifacts(
    targets: Sequence[WheelArtifact],
    available: Sequence[WheelArtifact],
    *,
    require_all_workspace_dependencies: bool = True,
) -> tuple[WheelArtifact, ...]:
    index = _artifact_index(available)
    workspace_names = workspace_distribution_names()
    selected = {target.normalized_distribution: target for target in targets}
    pending = deque(targets)

    while pending:
        artifact = pending.popleft()
        for requirement in artifact.requirements:
            if requirement.marker is not None and not requirement.marker.evaluate():
                continue
            name = _normalize_name(requirement.name)
            if name not in workspace_names:
                continue
            dependency = index.get(name)
            if dependency is None:
                if require_all_workspace_dependencies:
                    raise WheelTestError(
                        f"{artifact.distribution} requires workspace distribution "
                        f"{requirement.name}, but its wheel is absent from the artifact directory"
                    )
                continue
            if name not in selected:
                selected[name] = dependency
                pending.append(dependency)
    return tuple(selected[name] for name in sorted(selected))


def _root_dev_requirements() -> tuple[str, ...]:
    dependencies = _load_pyproject(ROOT).get("dependency-groups", {}).get("dev", [])
    if not isinstance(dependencies, list) or not all(
        isinstance(dependency, str) for dependency in dependencies
    ):
        raise WheelTestError("root dependency-groups.dev must be a list of requirements")
    return tuple(dependencies)


def _copy_test_inputs(package: workspace.Package, test_root: Path) -> None:
    tests = package.path / "tests"
    if not tests.is_dir():
        raise WheelTestError(f"package has no package-owned tests: {tests}")
    shutil.copytree(tests, test_root / "tests")
    for name in ("examples",):
        source = package.path / name
        if source.is_dir():
            shutil.copytree(source, test_root / name)
    pyproject = package.path / "pyproject.toml"
    if pyproject.is_file():
        shutil.copy2(pyproject, test_root / "pyproject.toml")


def _import_roots(path: Path) -> set[str]:
    try:
        module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return set()
    result: set[str] = set()
    for node in ast.walk(module):
        if isinstance(node, ast.Import):
            result.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            result.add(node.module.split(".", 1)[0])
    return result


def _selected_test_requirements(test_root: Path) -> tuple[str, ...]:
    root_dev = {
        _normalize_name(Requirement(requirement).name): requirement
        for requirement in _root_dev_requirements()
    }
    required_names = {"pytest", "pytest-asyncio"}
    for path in test_root.joinpath("tests").rglob("*.py"):
        required_names.update(_normalize_name(name) for name in _import_roots(path))
    return tuple(root_dev[name] for name in sorted(required_names) if name in root_dev)


def _write_test_requirements(test_root: Path) -> Path:
    path = test_root / "test-requirements.txt"
    path.write_text("\n".join(_selected_test_requirements(test_root)) + "\n", encoding="utf-8")
    return path


def _write_no_leakage_guard(test_root: Path) -> None:
    guard = f"""from pathlib import Path
import sys


def pytest_sessionstart(session):
    repository = Path({str(ROOT)!r}).resolve()
    leaked = []
    for value in sys.path:
        if not value:
            continue
        path = Path(value).resolve()
        if path == repository or repository in path.parents:
            leaked.append(str(path))
    if leaked:
        message = "repository path leaked into installed-wheel tests: " + ", ".join(leaked)
        raise RuntimeError(message)
"""
    (test_root / "conftest.py").write_text(guard, encoding="utf-8")


def run_installed_tests(
    package_name: str,
    *,
    wheel: Path,
    dist_dir: Path,
    test_paths: Sequence[str] = ("tests",),
    pytest_args: Sequence[str] = (),
    require_all_workspace_dependencies: bool = True,
) -> None:
    packages = workspace.packages()
    try:
        package = packages[package_name]
    except KeyError:
        raise WheelTestError(f"unknown workspace package: {package_name}") from None

    target = WheelArtifact.load(wheel)
    available = discover_wheels(dist_dir)
    installed = local_dependency_artifacts(
        [target],
        available,
        require_all_workspace_dependencies=require_all_workspace_dependencies,
    )
    assert_unique_ownership(installed)
    assert_unique_ownership(installed, split_paths_only=True)

    with tempfile.TemporaryDirectory(prefix="vercel-py-installed-wheel-") as temp_dir:
        test_root = Path(temp_dir).resolve()
        if test_root == ROOT or ROOT in test_root.parents:
            raise WheelTestError(f"installed tests must run outside the repository: {test_root}")
        _copy_test_inputs(package, test_root)
        requirements = _write_test_requirements(test_root)
        _write_no_leakage_guard(test_root)

        command = [
            "uv",
            "run",
            "--no-cache",
            "--isolated",
            "--no-project",
            "--directory",
            str(test_root),
        ]
        for artifact in installed:
            command.extend(("--with", str(artifact.path)))
        command.extend(
            (
                "--with-requirements",
                str(requirements),
                "pytest",
                "-v",
                "--tb=short",
                "-m",
                "not live",
                *pytest_args,
                *test_paths,
            )
        )
        environment = os.environ.copy()
        environment.pop("PYTHONPATH", None)
        subprocess.check_call(command, cwd=test_root, env=environment)


def build_and_test(
    package_names: Sequence[str], *, dist_dir: Path, pytest_args: Sequence[str] = ()
) -> None:
    build_workspace_wheels(package_names, dist_dir=dist_dir)
    artifacts = discover_wheels(dist_dir)
    selected = [find_distribution_wheel(dist_dir, name) for name in package_names]
    combination = local_dependency_artifacts(selected, artifacts)
    assert_unique_ownership(combination)
    assert_unique_ownership(combination, split_paths_only=True)
    for package_name in package_names:
        run_installed_tests(
            package_name,
            wheel=find_distribution_wheel(dist_dir, package_name).path,
            dist_dir=dist_dir,
            pytest_args=pytest_args,
        )


def _selected_artifacts(args: argparse.Namespace) -> tuple[WheelArtifact, ...]:
    explicit_wheels = tuple(WheelArtifact.load(path) for path in args.wheel)
    discovered = discover_wheels(args.dist_dir) if args.package else ()
    indexed = _artifact_index(discovered)
    selected = list(explicit_wheels)
    for package_name in args.package:
        normalized = _normalize_name(package_name)
        try:
            selected.append(indexed[normalized])
        except KeyError:
            raise WheelTestError(
                f"expected one {package_name} wheel in {args.dist_dir.resolve()}, found none"
            ) from None
    if not selected:
        raise WheelTestError("select at least one --package or --wheel")
    return tuple(selected)


def _add_artifact_selection(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--dist-dir", type=Path, required=True)
    parser.add_argument("--package", action="append", default=[])
    parser.add_argument("--wheel", action="append", type=Path, default=[])


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build")
    build_parser.add_argument("--package", action="append", required=True)
    build_parser.add_argument("--dist-dir", type=Path, required=True)

    inspect_parser = subparsers.add_parser("inspect")
    _add_artifact_selection(inspect_parser)

    ownership_parser = subparsers.add_parser("check-ownership")
    _add_artifact_selection(ownership_parser)

    test_parser = subparsers.add_parser("test")
    test_parser.add_argument("--package", required=True)
    test_parser.add_argument("--wheel", type=Path, required=True)
    test_parser.add_argument("--dist-dir", type=Path, required=True)
    test_parser.add_argument("--test-path", action="append", default=[])
    test_parser.add_argument("pytest_args", nargs=argparse.REMAINDER)

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--package", action="append", required=True)
    run_parser.add_argument("--dist-dir", type=Path, required=True)
    run_parser.add_argument("pytest_args", nargs=argparse.REMAINDER)

    args = parser.parse_args(argv)
    if args.command == "build":
        for path in build_workspace_wheels(args.package, dist_dir=args.dist_dir):
            print(path)
        return 0
    if args.command == "inspect":
        print(json.dumps([artifact.to_dict() for artifact in _selected_artifacts(args)], indent=2))
        return 0
    if args.command == "check-ownership":
        artifacts = _selected_artifacts(args)
        assert_unique_ownership(artifacts)
        assert_unique_ownership(artifacts, split_paths_only=True)
        return 0
    if args.command == "test":
        pytest_args = args.pytest_args
        if pytest_args[:1] == ["--"]:
            pytest_args = pytest_args[1:]
        run_installed_tests(
            args.package,
            wheel=args.wheel,
            dist_dir=args.dist_dir,
            test_paths=args.test_path or ("tests",),
            pytest_args=pytest_args,
        )
        return 0
    if args.command == "run":
        pytest_args = args.pytest_args
        if pytest_args[:1] == ["--"]:
            pytest_args = pytest_args[1:]
        build_and_test(args.package, dist_dir=args.dist_dir, pytest_args=pytest_args)
        return 0
    return 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except WheelTestError as exc:
        raise SystemExit(str(exc)) from None
