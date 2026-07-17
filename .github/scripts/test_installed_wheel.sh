#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
    printf 'usage: %s <package> <wheel>\n' "$0" >&2
    exit 2
fi

package=$1
wheel_path=$2
extra_wheel_args=

if [ ! -f "$wheel_path" ]; then
    printf 'wheel does not exist: %s\n' "$wheel_path" >&2
    exit 1
fi
wheel_path=$(python - "$wheel_path" <<'PY'
import sys
from pathlib import Path

print(Path(sys.argv[1]).resolve())
PY
)

wheel_dir=$(dirname "$wheel_path")
for dependency_wheel in "$wheel_dir"/vercel_*.whl; do
    if [ ! -f "$dependency_wheel" ] || [ "$dependency_wheel" = "$wheel_path" ]; then
        continue
    fi
    dependency_wheel=$(python - "$dependency_wheel" <<'PY'
import sys
from pathlib import Path

print(Path(sys.argv[1]).resolve())
PY
)
    extra_wheel_args="$extra_wheel_args --with $dependency_wheel"
done

package_path=$(python - "$package" <<'PY'
import sys

from scripts import workspace

package = sys.argv[1]
packages = workspace.packages()
if package in packages:
    print(packages[package].path)
PY
)

temp_dir=${RUNNER_TEMP:-${TMPDIR:-/tmp}}
test_root=$(mktemp -d "$temp_dir/vercel-py-installed-package-tests.XXXXXX")
test_root=$(python - "$test_root" <<'PY'
import sys
from pathlib import Path

print(Path(sys.argv[1]).resolve())
PY
)
if [ -n "$package_path" ] && [ -d "$package_path/tests" ]; then
    cp -R "$package_path/tests" "$test_root/tests"
fi
if [ -n "$package_path" ] && [ -f "$package_path/pyproject.toml" ]; then
    cp "$package_path/pyproject.toml" "$test_root/pyproject.toml"
fi
if [ -n "$package_path" ] && [ -d "$package_path/examples" ]; then
    cp -R "$package_path/examples" "$test_root/examples"
fi

python - "$wheel_path" "$test_root" <<'PY'
import sys
import zipfile
from pathlib import Path

wheel = Path(sys.argv[1])
test_root = Path(sys.argv[2])
with zipfile.ZipFile(wheel) as archive:
    names = archive.namelist()
    assert not any("/_vendor/vercel/" in name for name in names), (
        "vercel-* packages must be installed side-by-side as -bundle dependencies, "
        "not copied into another package's vendor tree"
    )
    for member in archive.namelist():
        if member.startswith("vercel/") and not member.endswith("/"):
            archive.extract(member, test_root)
PY

python - "$test_root" <<'PY'
import sys
from pathlib import Path

test_root = Path(sys.argv[1])
uses_vendored_httpx = any(
    "from vercel.internal._vendor import httpx" in path.read_text(encoding="utf-8")
    for path in (test_root / "vercel").rglob("*.py")
)
(test_root / ".uses-vendored-httpx").write_text(
    "1\n" if uses_vendored_httpx else "0\n",
    encoding="utf-8",
)
if not uses_vendored_httpx:
    raise SystemExit

for path in (test_root / "tests").rglob("*.py"):
    text = path.read_text(encoding="utf-8")
    rewritten = text.replace("import httpx\n", "from vercel.internal._vendor import httpx\n")
    if rewritten != text:
        path.write_text(rewritten, encoding="utf-8")
PY

python - "$package" "$package_path" "$test_root" <<'PY'
from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import Any

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    import tomli as tomllib  # type: ignore[no-redef]

from scripts import workspace

ROOT = Path.cwd()
package_name = sys.argv[1]
package_path = Path(sys.argv[2]) if sys.argv[2] else None
test_root = Path(sys.argv[3])
tests_root = test_root / "tests"
requirements_path = test_root / "requirements.txt"
pytest_extra_args_path = test_root / "pytest-extra-args.txt"
pytest_filter_path = test_root / "pytest-filter.txt"
uses_vendored_httpx = test_root.joinpath(".uses-vendored-httpx").read_text(
    encoding="utf-8"
).strip() == "1"


def load_pyproject(path: Path) -> dict[str, Any]:
    with path.joinpath("pyproject.toml").open("rb") as fp:
        return tomllib.load(fp)


def requirement_name(requirement: str) -> str:
    return normalize_name(Requirement(requirement).name)


def normalize_name(name: str) -> str:
    return str(canonicalize_name(name))


def import_to_distribution(name: str) -> str:
    return normalize_name(name)


def wheel_import_roots() -> set[str]:
    return {path.name for path in test_root.iterdir() if path.is_dir() and path.name != "tests"}


def local_import_roots() -> set[str]:
    if package_path is None:
        return set()
    roots = set()
    for path in package_path.iterdir():
        if path.name in {"tests", "examples"}:
            continue
        if path.is_file() and path.suffix == ".py":
            roots.add(path.stem)
        elif path.is_dir() and path.joinpath("__init__.py").exists():
            roots.add(path.name)
    return roots - wheel_import_roots()


def imported_modules(path: Path) -> set[str]:
    try:
        module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return set()
    imports = set()
    for node in ast.walk(module):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imports.add(node.module)
    return imports


def imported_workspace_packages(imports: set[str]) -> set[str]:
    packages = workspace.packages()
    result = {package_name} if package_name in packages else set()
    for name, package in packages.items():
        data = load_pyproject(package.path)
        include = data.get("tool", {}).get("hatch", {}).get("build", {}).get("targets", {})
        wheel = include.get("wheel", {})
        roots = wheel.get("only-include") or []
        if not roots:
            roots = [package.version_file.parent.relative_to(package.path).as_posix()]
        modules = [root.strip("/").replace("/", ".") for root in roots]
        if any(import_name == module or import_name.startswith(f"{module}.") for import_name in imports for module in modules):
            result.add(name)
    return result


def project_dependencies(data: dict[str, Any]) -> list[str]:
    release = data.get("tool", {}).get("vercel", {}).get("release", {})
    dependencies = release.get("dependencies", data.get("project", {}).get("dependencies", []))
    if isinstance(dependencies, dict):
        dependencies = dependencies.get("dependencies", [])
    return dependencies if isinstance(dependencies, list) else []


def root_dev_dependencies() -> dict[str, str]:
    deps = load_pyproject(ROOT).get("dependency-groups", {}).get("dev", [])
    result = {}
    for dependency in deps:
        if isinstance(dependency, str):
            result[requirement_name(dependency)] = dependency
    return result


def add_requirement(requirement: str) -> None:
    requirements[requirement_name(requirement)] = requirement


def selected_test_files() -> list[Path]:
    if not tests_root.exists():
        return []
    unavailable_roots = local_import_roots()
    selected = []
    for path in sorted(tests_root.rglob("test_*.py")):
        imports = imported_modules(path)
        if unavailable_roots.intersection(import_name.split(".", 1)[0] for import_name in imports):
            continue
        selected.append(path)
    return selected


def ignored_test_files() -> list[Path]:
    if not tests_root.exists():
        return []
    unavailable_roots = local_import_roots()
    ignored = []
    for path in sorted(tests_root.rglob("test_*.py")):
        imports = imported_modules(path)
        if unavailable_roots.intersection(import_name.split(".", 1)[0] for import_name in imports):
            ignored.append(path)
    return ignored


def pytest_filter_for_tests(paths: list[Path], texts: str) -> str:
    if not uses_vendored_httpx or "respx_mock" not in texts or "httpx.Response" not in texts:
        return ""

    excluded_names = []
    for path in paths:
        text = path.read_text(encoding="utf-8")
        module = ast.parse(text, filename=str(path))
        for node in module.body:
            if not isinstance(node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            source = ast.get_source_segment(text, node) or ""
            if "respx_mock" in source and "httpx.Response" in source:
                excluded_names.append(node.name)
    return " and ".join(f"not {name}" for name in excluded_names)


stdlib_modules = sys.stdlib_module_names | {"__future__"}
test_files = selected_test_files()
ignored_files = ignored_test_files()
imports = {module for path in test_files for module in imported_modules(path)}
texts = "\n".join(path.read_text(encoding="utf-8") for path in test_files)
requirements: dict[str, str] = {}
root_dev = root_dev_dependencies()
requirements.update(root_dev)

for module in imports:
    root = module.split(".", 1)[0]
    if root in stdlib_modules or root in {"tests", "vercel"}:
        continue
    dependency = root_dev.get(import_to_distribution(root))
    if dependency is not None:
        add_requirement(dependency)

pytest_config = load_pyproject(package_path) if package_path is not None else {}
pytest_options = pytest_config.get("tool", {}).get("pytest", {}).get("ini_options", {})

if "pytest.mark.asyncio" in texts or "asyncio_mode" in pytest_options:
    dependency = root_dev.get("pytest-asyncio")
    if dependency is not None:
        add_requirement(dependency)

packages = workspace.packages()
for name in imported_workspace_packages(imports):
    if name not in packages:
        continue
    data = load_pyproject(packages[name].path)
    for dependency in project_dependencies(data):
        normalized = requirement_name(dependency)
        if normalized.startswith("vercel-") and normalized in packages:
            continue
        add_requirement(root_dev.get(normalized, dependency))

pytest_filter = pytest_filter_for_tests(test_files, texts)

requirements_path.write_text("\n".join(sorted(requirements.values())) + "\n", encoding="utf-8")
pytest_extra_args_path.write_text(
    "\n".join(f"--ignore={path}" for path in ignored_files) + "\n",
    encoding="utf-8",
)
pytest_filter_path.write_text(f"{pytest_filter}\n", encoding="utf-8")
PY

pytest_extra_args=$(cat "$test_root/pytest-extra-args.txt")
pytest_filter=$(cat "$test_root/pytest-filter.txt")
set --
if [ -n "$pytest_filter" ]; then
    set -- -k "$pytest_filter"
fi

if [ -d "$test_root/tests" ]; then
    # shellcheck disable=SC2086
    uv run \
        --no-cache \
        --isolated \
        --no-project \
        --directory "$test_root" \
        --with "$wheel_path" \
        --with pytest \
        --with-requirements "$test_root/requirements.txt" \
        $extra_wheel_args \
        pytest \
        -v \
        --tb=short \
        "$@" \
        $pytest_extra_args
fi
