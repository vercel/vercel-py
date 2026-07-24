#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import configparser
import email.parser
import email.policy
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from collections import defaultdict, deque
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from importlib.machinery import EXTENSION_SUFFIXES
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
BUNDLE_SUFFIX = "-bundle"
SHARED_BUNDLE_DISTRIBUTION = "vercel-internal-shared-vendored-deps"
SHARED_VENDOR_NAMESPACE = "vercel.internal._vendor"
SHARED_TEST_LIBRARIES = ("anyio", "httpx")


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

        try:
            with zipfile.ZipFile(path) as archive:
                members = [info.filename for info in archive.infolist()]
                names = [info.filename for info in archive.infolist() if not info.is_dir()]
                duplicates = sorted(name for name, count in _counts(members).items() if count > 1)
                if duplicates:
                    rendered = "\n".join(f"  - {name}" for name in duplicates)
                    raise WheelTestError(f"wheel contains duplicate members: {path}\n{rendered}")

                nested_first_party = sorted(name for name in members if "/_vendor/vercel/" in name)
                if nested_first_party:
                    rendered = "\n".join(f"  - {name}" for name in nested_first_party)
                    raise WheelTestError(
                        "vercel-* packages must be installed side-by-side as bundle "
                        "dependencies, not copied into another package's vendor tree:\n"
                        f"{rendered}"
                    )

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

                try:
                    requirements = tuple(
                        Requirement(value)
                        for value in metadata.get_all("Requires-Dist", failobj=[])
                    )
                except ValueError as exc:
                    raise WheelTestError(
                        f"wheel metadata contains an invalid requirement: {path}: {exc}"
                    ) from exc

                entry_points_paths = [
                    name for name in names if name.endswith(".dist-info/entry_points.txt")
                ]
                if len(entry_points_paths) > 1:
                    raise WheelTestError(f"wheel contains multiple entry_points.txt files: {path}")
                console_scripts: tuple[tuple[str, str], ...] = ()
                if entry_points_paths:
                    try:
                        parser = _CaseSensitiveConfigParser(interpolation=None)
                        parser.read_string(archive.read(entry_points_paths[0]).decode("utf-8"))
                    except (
                        UnicodeDecodeError,
                        configparser.Error,
                    ) as exc:
                        raise WheelTestError(
                            f"wheel contains invalid entry_points.txt: {path}: {exc}"
                        ) from exc
                    if parser.has_section("console_scripts"):
                        console_scripts = tuple(sorted(parser.items("console_scripts")))
        except (OSError, zipfile.BadZipFile) as exc:
            raise WheelTestError(f"could not read wheel {path}: {exc}") from exc

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

    @property
    def is_bundle(self) -> bool:
        return self.normalized_distribution.endswith(BUNDLE_SUFFIX)

    def has_active_requirement(self, distribution: str) -> bool:
        normalized = _normalize_name(distribution)
        return any(
            _normalize_name(requirement.name) == normalized
            and (requirement.marker is None or requirement.marker.evaluate())
            for requirement in self.requirements
        )

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
    """Return the source paths included by a package's ordinary wheel."""
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


def _artifact_index(
    artifacts: Iterable[WheelArtifact],
) -> dict[str, WheelArtifact]:
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


def ownership_overlaps(
    artifacts: Iterable[WheelArtifact],
) -> dict[str, tuple[str, ...]]:
    owners: dict[str, list[str]] = defaultdict(list)
    for artifact in artifacts:
        for path in artifact.first_party_files:
            owners[path].append(artifact.distribution)
    return {
        path: tuple(sorted(distributions))
        for path, distributions in sorted(owners.items())
        if len(distributions) > 1
    }


def assert_unique_ownership(
    artifacts: Iterable[WheelArtifact],
) -> None:
    overlaps = ownership_overlaps(artifacts)
    if not overlaps:
        return
    rendered = "\n".join(
        f"  - {path}: {', '.join(distributions)}" for path, distributions in overlaps.items()
    )
    raise WheelOwnershipError(f"overlapping first-party wheel files:\n{rendered}")


def workspace_distribution_names() -> frozenset[str]:
    names = set(workspace.packages())
    names.update(f"{name}{BUNDLE_SUFFIX}" for name in tuple(names))
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
                        f"{requirement.name}, but its wheel is absent from the "
                        "artifact directory"
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


def _copy_test_inputs(package: workspace.Package, test_root: Path) -> bool:
    tests = package.path / "tests"
    if not tests.is_dir():
        return False
    shutil.copytree(tests, test_root / "tests")
    examples = package.path / "examples"
    if examples.is_dir():
        shutil.copytree(examples, test_root / "examples")
    pyproject = package.path / "pyproject.toml"
    if pyproject.is_file():
        shutil.copy2(pyproject, test_root / "pyproject.toml")
    return True


def _imported_modules(path: Path) -> set[str]:
    try:
        module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return set()
    result: set[str] = set()
    for node in ast.walk(module):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            result.add(node.module)
    return result


def _import_roots(path: Path) -> set[str]:
    return {imported.split(".", 1)[0] for imported in _imported_modules(path)}


def artifact_import_roots(
    artifacts: Iterable[WheelArtifact],
) -> frozenset[str]:
    roots = set()
    for artifact in artifacts:
        for path in artifact.files:
            if ".dist-info/" in path:
                continue
            root, separator, _ = path.partition("/")
            if separator:
                roots.add(root)
            elif path.endswith(".py"):
                roots.add(Path(path).stem)
            elif any(path.endswith(suffix) for suffix in EXTENSION_SUFFIXES):
                roots.add(path.split(".", 1)[0])
    return frozenset(roots)


def source_import_roots(
    package: workspace.Package,
) -> frozenset[str]:
    roots = set()
    for path in package.path.iterdir():
        if path.name in {"tests", "examples"}:
            continue
        if path.is_file() and path.suffix == ".py":
            roots.add(path.stem)
        elif path.is_dir() and path.joinpath("__init__.py").is_file():
            roots.add(path.name)
    return frozenset(roots)


def source_only_test_files(
    test_root: Path,
    package: workspace.Package,
    installed: Sequence[WheelArtifact],
) -> tuple[Path, ...]:
    tests_root = test_root / "tests"
    if not tests_root.is_dir():
        return ()
    unavailable = source_import_roots(package) - artifact_import_roots(installed)
    return tuple(
        path
        for path in sorted(tests_root.rglob("test_*.py"))
        if unavailable.intersection(_import_roots(path))
    )


def _selected_test_requirements(
    test_root: Path, *, ignored: Iterable[Path] = ()
) -> tuple[str, ...]:
    root_dev = {
        _normalize_name(Requirement(requirement).name): requirement
        for requirement in _root_dev_requirements()
    }
    ignored_set = {path.resolve() for path in ignored}
    required_names = {"pytest", "pytest-asyncio"}
    for path in test_root.joinpath("tests").rglob("*.py"):
        if path.resolve() not in ignored_set:
            required_names.update(_normalize_name(name) for name in _import_roots(path))
    return tuple(root_dev[name] for name in sorted(required_names) if name in root_dev)


def _write_test_requirements(test_root: Path, *, ignored: Iterable[Path] = ()) -> Path:
    path = test_root / "test-requirements.txt"
    requirements = _selected_test_requirements(test_root, ignored=ignored)
    path.write_text("\n".join(requirements) + "\n", encoding="utf-8")
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


def bundle_uses_shared_vendor(artifact: WheelArtifact) -> bool:
    return artifact.is_bundle and artifact.has_active_requirement(SHARED_BUNDLE_DISTRIBUTION)


def artifact_uses_shared_vendored_library(artifact: WheelArtifact, library: str) -> bool:
    vendored_module = f"{SHARED_VENDOR_NAMESPACE}.{library}"
    with zipfile.ZipFile(artifact.path) as archive:
        for name in artifact.files:
            if not name.endswith(".py"):
                continue
            try:
                module = ast.parse(
                    archive.read(name).decode("utf-8", errors="ignore"),
                    filename=name,
                )
            except SyntaxError:
                continue
            for node in ast.walk(module):
                if isinstance(node, ast.Import) and any(
                    alias.name == vendored_module or alias.name.startswith(f"{vendored_module}.")
                    for alias in node.names
                ):
                    return True
                if not isinstance(node, ast.ImportFrom) or not node.module:
                    continue
                if node.module == SHARED_VENDOR_NAMESPACE and any(
                    alias.name == library for alias in node.names
                ):
                    return True
                if node.module == vendored_module or node.module.startswith(f"{vendored_module}."):
                    return True
    return False


def _rewrite_test_imports(text: str) -> str:
    for library in SHARED_TEST_LIBRARIES:
        from_pattern = re.compile(
            rf"^(?P<indent>[ \t]*)from {library}"
            r"(?P<submodule>(?:\.[A-Za-z_]\w*)*) import ",
            re.MULTILINE,
        )
        text = from_pattern.sub(
            rf"\g<indent>from {SHARED_VENDOR_NAMESPACE}.{library}"
            r"\g<submodule> import ",
            text,
        )

        import_pattern = re.compile(
            rf"^(?P<indent>[ \t]*)import {library}"
            r"(?P<submodule>(?:\.[A-Za-z_]\w*)*)"
            r"(?: as (?P<alias>[A-Za-z_]\w*))?"
            r"(?P<comment>[ \t]*(?:#.*)?)$",
            re.MULTILINE,
        )

        def replace_import(match: re.Match[str], library: str = library) -> str:
            indent = match.group("indent")
            submodule = match.group("submodule").removeprefix(".")
            alias = match.group("alias")
            comment = match.group("comment")
            if not submodule:
                rewritten = f"{indent}from {SHARED_VENDOR_NAMESPACE} import {library}"
                if alias:
                    rewritten += f" as {alias}"
                return rewritten + comment

            parent, _, child = submodule.rpartition(".")
            module = f"{SHARED_VENDOR_NAMESPACE}.{library}"
            if parent:
                module = f"{module}.{parent}"
            rewritten = f"{indent}from {module} import {child}"
            if alias:
                return f"{rewritten} as {alias}{comment}"
            return f"{indent}from {SHARED_VENDOR_NAMESPACE} import {library}\n{rewritten}{comment}"

        text = import_pattern.sub(replace_import, text)
    return text


def rewrite_bundle_test_imports(test_root: Path) -> None:
    tests_root = test_root / "tests"
    if not tests_root.is_dir():
        return
    for path in tests_root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        rewritten = _rewrite_test_imports(text)
        if rewritten != text:
            path.write_text(rewritten, encoding="utf-8")


def _respx_import_bindings(module: ast.Module) -> frozenset[str]:
    bindings = set()
    for node in module.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "respx" or alias.name.startswith("respx."):
                    bindings.add(alias.asname or alias.name.split(".", 1)[0])
        elif (
            isinstance(node, ast.ImportFrom)
            and node.module
            and (node.module == "respx" or node.module.startswith("respx."))
        ):
            bindings.update(alias.asname or alias.name for alias in node.names if alias.name != "*")
    return frozenset(bindings)


def _references_respx(nodes: Iterable[ast.AST], bindings: frozenset[str]) -> bool:
    for root in nodes:
        for node in ast.walk(root):
            if isinstance(node, ast.Import) and any(
                alias.name == "respx" or alias.name.startswith("respx.") for alias in node.names
            ):
                return True
            if (
                isinstance(node, ast.ImportFrom)
                and node.module
                and (node.module == "respx" or node.module.startswith("respx."))
            ):
                return True
            if isinstance(node, ast.Name) and (node.id in bindings or node.id.startswith("respx")):
                return True
            if isinstance(node, ast.arg) and (node.arg in bindings or node.arg.startswith("respx")):
                return True
    return False


def pytest_deselections_for_vendored_httpx(
    paths: Iterable[Path], *, test_root: Path
) -> tuple[str, ...]:
    deselected = set()
    for path in paths:
        text = path.read_text(encoding="utf-8")
        try:
            module = ast.parse(text, filename=str(path))
        except SyntaxError:
            continue
        bindings = _respx_import_bindings(module)
        relative = path.relative_to(test_root).as_posix()
        for node in module.body:
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                if node.name.startswith("test") and _references_respx((node,), bindings):
                    deselected.add(f"{relative}::{node.name}")
                continue
            if not isinstance(node, ast.ClassDef) or not node.name.startswith("Test"):
                continue

            class_scope = [
                *node.decorator_list,
                *node.bases,
                *(keyword.value for keyword in node.keywords),
                *(
                    statement
                    for statement in node.body
                    if not isinstance(
                        statement,
                        ast.FunctionDef | ast.AsyncFunctionDef,
                    )
                ),
            ]
            methods = [
                statement
                for statement in node.body
                if isinstance(statement, ast.FunctionDef | ast.AsyncFunctionDef)
            ]
            class_dependent = _references_respx(class_scope, bindings) or any(
                not method.name.startswith("test") and _references_respx((method,), bindings)
                for method in methods
            )
            if class_dependent:
                deselected.add(f"{relative}::{node.name}")
                continue
            for method in methods:
                if method.name.startswith("test") and _references_respx((method,), bindings):
                    deselected.add(f"{relative}::{node.name}::{method.name}")
    return tuple(sorted(deselected))


def installed_test_command(
    *,
    test_root: Path,
    artifacts: Sequence[WheelArtifact],
    requirements: Path,
    ignored: Sequence[Path],
    deselected: Sequence[str],
    test_paths: Sequence[str],
    pytest_args: Sequence[str],
) -> list[str]:
    command = [
        "uv",
        "run",
        "--no-cache",
        "--isolated",
        "--no-project",
        "--directory",
        str(test_root),
    ]
    for artifact in artifacts:
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
        )
    )
    command.extend(f"--deselect={node_id}" for node_id in deselected)
    command.extend(f"--ignore={path}" for path in ignored)
    command.extend(pytest_args)
    command.extend(test_paths)
    return command


def run_installed_tests(
    package_name: str,
    *,
    wheel: Path,
    dist_dir: Path,
    test_paths: Sequence[str] = ("tests",),
    pytest_args: Sequence[str] = (),
    require_all_workspace_dependencies: bool = True,
) -> None:
    target = WheelArtifact.load(wheel)
    normalized_package = _normalize_name(package_name)
    expected_distributions = {
        normalized_package,
        f"{normalized_package}{BUNDLE_SUFFIX}",
    }
    if target.normalized_distribution not in expected_distributions:
        expected = ", ".join(sorted(expected_distributions))
        raise WheelTestError(
            f"wheel distribution {target.distribution} does not match "
            f"package {package_name}; expected one of: {expected}"
        )
    available = discover_wheels(dist_dir)
    installed = local_dependency_artifacts(
        [target],
        available,
        require_all_workspace_dependencies=require_all_workspace_dependencies,
    )
    assert_unique_ownership(installed)

    package = next(
        (
            candidate
            for candidate in workspace.packages().values()
            if _normalize_name(candidate.name) == normalized_package
        ),
        None,
    )
    if package is None or not package.path.joinpath("tests").is_dir():
        return

    with tempfile.TemporaryDirectory(prefix="vercel-py-installed-wheel-") as temp_dir:
        test_root = Path(temp_dir).resolve()
        if test_root == ROOT or ROOT in test_root.parents:
            raise WheelTestError(f"installed tests must run outside the repository: {test_root}")
        if not _copy_test_inputs(package, test_root):
            return

        ignored = source_only_test_files(test_root, package, installed)
        test_files = tuple(
            path
            for path in sorted(test_root.joinpath("tests").rglob("test_*.py"))
            if path not in ignored
        )
        if not test_files and tuple(test_paths) == ("tests",):
            return

        uses_shared_vendor = bundle_uses_shared_vendor(target)
        uses_vendored_httpx = artifact_uses_shared_vendored_library(target, "httpx")
        if uses_shared_vendor:
            rewrite_bundle_test_imports(test_root)
        requirements = _write_test_requirements(test_root, ignored=ignored)
        _write_no_leakage_guard(test_root)
        deselected = (
            pytest_deselections_for_vendored_httpx(
                test_files,
                test_root=test_root,
            )
            if uses_vendored_httpx
            else ()
        )
        command = installed_test_command(
            test_root=test_root,
            artifacts=installed,
            requirements=requirements,
            ignored=ignored,
            deselected=deselected,
            test_paths=test_paths,
            pytest_args=pytest_args,
        )
        environment = os.environ.copy()
        environment.pop("PYTHONPATH", None)
        subprocess.check_call(command, cwd=test_root, env=environment)


def build_and_test(
    package_names: Sequence[str],
    *,
    dist_dir: Path,
    pytest_args: Sequence[str] = (),
) -> None:
    build_workspace_wheels(package_names, dist_dir=dist_dir)
    artifacts = discover_wheels(dist_dir)
    selected = [find_distribution_wheel(dist_dir, name) for name in package_names]
    combination = local_dependency_artifacts(selected, artifacts)
    assert_unique_ownership(combination)
    for package_name in package_names:
        run_installed_tests(
            package_name,
            wheel=find_distribution_wheel(dist_dir, package_name).path,
            dist_dir=dist_dir,
            pytest_args=pytest_args,
        )


def _selected_artifacts(
    args: argparse.Namespace,
) -> tuple[WheelArtifact, ...]:
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
        print(
            json.dumps(
                [artifact.to_dict() for artifact in _selected_artifacts(args)],
                indent=2,
            )
        )
        return 0
    if args.command == "check-ownership":
        assert_unique_ownership(_selected_artifacts(args))
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
        build_and_test(
            args.package,
            dist_dir=args.dist_dir,
            pytest_args=pytest_args,
        )
        return 0
    return 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except WheelTestError as exc:
        raise SystemExit(str(exc)) from None
