#!/usr/bin/env python3
from __future__ import annotations

import argparse
import email.parser
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from packaging.requirements import Requirement
from packaging.version import Version

try:
    from scripts import workspace
except ImportError:  # pragma: no cover - script execution path
    import workspace  # type: ignore[no-redef]

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    import tomli as tomllib  # type: ignore[no-redef]


ROOT = Path(__file__).resolve().parent.parent
VENDORED_SUFFIX = "-bundle"
WORKSPACE_REQUIREMENT_PREFIX = "@workspace:"
SHARED_VENDORED_PACKAGE = "vercel-internal-shared-vendored-deps"
SHARED_VENDOR_NAMESPACE = "vercel.internal._vendor"
SHARED_VERSION_ENV = "VERCEL_INTERNAL_SHARED_VENDORED_DEPS_VERSION"
SHARED_DEPS_METADATA = "_shared_deps.json"
LICENSE_FILE_RE = re.compile(
    r"(?:^|[-_.])(license|licence|copying|notice|copyright|authors?)(?:[-_.]|$)",
    re.IGNORECASE,
)
SHARED_VENDORED_LIBS = {
    "anyio": "anyio",
    "certifi": "certifi",
    "h11": "h11",
    "h2": "h2",
    "hpack": "hpack",
    "httpcore": "httpcore",
    "httpx": "httpx",
    "hyperframe": "hyperframe",
    "idna": "idna",
    "typing_extensions": "typing-extensions",
}
SHARED_VENDORED_REQUIREMENTS = tuple(SHARED_VENDORED_LIBS.values())
SHARED_VENDORED_CONSUMERS = {
    "vercel-cache",
    "vercel-celery",
    "vercel-dramatiq",
    "vercel-internal-telemetry",
    "vercel-oidc",
    "vercel-queue",
}
PEER_DEPENDENCIES = {
    "vercel-celery": {"celery"},
    "vercel-dramatiq": {"dramatiq"},
    # Starlette classes are part of vercel.proxy's public interoperability
    # contract. Vendoring them would make application Response objects fail
    # identity checks against a second, private Starlette installation.
    "vercel-proxy": {"starlette"},
}
COMMON_DROP_TRANSFORMATIONS = (
    "*.so",
    "*/tests/",
    "*/__pycache__/",
)
ANYIO_FROM_THREAD_SUBSTITUTION = (
    r"import anyio\.from_thread",
    "from anyio import from_thread",
)


@dataclass(frozen=True)
class VendoringConfig:
    destination: Path
    requirements: Path
    namespace: str
    protected_files: tuple[str, ...]


@dataclass(frozen=True)
class VendoredPlan:
    package: workspace.Package
    variant_name: str
    config: VendoringConfig
    vendored_requirements: tuple[str, ...]
    external_dependencies: tuple[str, ...]


@dataclass(frozen=True)
class VendoringTransformations:
    substitutions: tuple[tuple[str, str], ...] = ()
    drops: tuple[str, ...] = COMMON_DROP_TRANSFORMATIONS


def is_vendored_eligible(package: workspace.Package) -> bool:
    data = _load_pyproject(package.path)
    return _vendoring_config_for_package(package, data) is not None


def load_plan(package_name: str) -> VendoredPlan:
    if package_name == SHARED_VENDORED_PACKAGE:
        return _shared_vendored_plan()

    packages = workspace.packages()
    try:
        package = packages[package_name]
    except KeyError:
        raise SystemExit(f"unknown package: {package_name}") from None
    if not is_vendored_eligible(package):
        raise SystemExit(f"package is not vendored-eligible: {package_name}")

    data = _load_pyproject(package.path)
    config = _vendoring_config_for_package(package, data)
    if config is None:
        raise SystemExit(f"package is not vendored-eligible: {package_name}")
    requirements = _derive_vendor_requirements(package.name, data)
    return VendoredPlan(
        package=package,
        variant_name=_variant_name(package.name),
        config=config,
        vendored_requirements=requirements,
        external_dependencies=_external_dependencies(package.name, data, requirements),
    )


def _variant_name(package_name: str) -> str:
    if package_name == SHARED_VENDORED_PACKAGE:
        return package_name
    return f"{package_name}{VENDORED_SUFFIX}"


def _shared_vendored_plan() -> VendoredPlan:
    package = workspace.Package(
        SHARED_VENDORED_PACKAGE,
        Path("<generated>") / SHARED_VENDORED_PACKAGE,
        Path("<generated>") / SHARED_VENDORED_PACKAGE / "vercel/internal/_vendor/version.py",
        (),
    )
    data = _shared_pyproject_data()
    return VendoredPlan(
        package=package,
        variant_name=SHARED_VENDORED_PACKAGE,
        config=VendoringConfig(
            destination=Path("vercel/internal/_vendor"),
            requirements=Path("vercel/internal/_vendor/vendor.txt"),
            namespace=SHARED_VENDOR_NAMESPACE,
            protected_files=(
                "__init__.py",
                "py.typed",
                "vendor.txt",
                "version.py",
                SHARED_DEPS_METADATA,
            ),
        ),
        vendored_requirements=_derive_vendor_requirements(SHARED_VENDORED_PACKAGE, data),
        external_dependencies=(),
    )


def _shared_pyproject_data() -> dict[str, Any]:
    return {"tool": {"vercel": {"release": {"dependencies": []}}}}


def _load_pyproject(path: Path) -> dict[str, Any]:
    with (path / "pyproject.toml").open("rb") as fp:
        return tomllib.load(fp)


def _vendoring_config_for_package(
    package: workspace.Package, data: dict[str, Any]
) -> VendoringConfig | None:
    package_root = _vendoring_package_root_from_wheel_include(data)
    if package_root is None:
        package_root = _vendoring_package_root_from_version_file(package)
    if package_root is None:
        return None
    destination = package_root / "_vendor"
    return VendoringConfig(
        destination=destination,
        requirements=destination / "vendor.txt",
        namespace=".".join(package_root.parts + ("_vendor",)),
        protected_files=("__init__.py", "vendor.txt"),
    )


def _vendoring_package_root_from_wheel_include(data: dict[str, Any]) -> Path | None:
    wheel = (
        data.get("tool", {}).get("hatch", {}).get("build", {}).get("targets", {}).get("wheel", {})
    )
    includes = wheel.get("only-include", [])
    if not isinstance(includes, list) or len(includes) != 1:
        return None
    include = includes[0]
    if not isinstance(include, str):
        return None
    package_root = Path(include.lstrip("/"))
    if package_root.parts[:1] != ("vercel",):
        return None
    return package_root


def _vendoring_package_root_from_version_file(package: workspace.Package) -> Path | None:
    try:
        relative = package.version_file.relative_to(package.path)
    except ValueError:
        return None
    if relative.parts[:1] != ("vercel",):
        return None
    return relative.parent


def _derive_vendor_requirements(package_name: str, data: dict[str, Any]) -> tuple[str, ...]:
    lock_versions = _lock_versions()
    if package_name == SHARED_VENDORED_PACKAGE:
        return _pin_requirements(SHARED_VENDORED_REQUIREMENTS, lock_versions)

    vendored_names = []
    peers = PEER_DEPENDENCIES.get(package_name, set())
    for dependency in _release_dependencies(data):
        parsed = Requirement(dependency)
        normalized = _normalize_name(parsed.name)
        if normalized in SHARED_VENDORED_LIBS.values():
            continue
        if normalized.startswith("vercel-"):
            continue
        if normalized in peers:
            continue
        vendored_names.append(normalized)
    return _pin_requirements(tuple(vendored_names), lock_versions)


def _pin_requirements(names: tuple[str, ...], lock_versions: dict[str, str]) -> tuple[str, ...]:
    pinned = []
    for name in names:
        normalized = _normalize_name(name)
        try:
            version = lock_versions[normalized]
        except KeyError:
            raise SystemExit(f"missing {normalized} in uv.lock") from None
        pinned.append(f"{normalized}=={version}")
    return tuple(pinned)


def _lock_versions() -> dict[str, str]:
    data = tomllib.loads((ROOT / "uv.lock").read_text(encoding="utf-8"))
    versions = {}
    for package in data.get("package", []):
        if not isinstance(package, dict):
            continue
        name = package.get("name")
        version = package.get("version")
        if isinstance(name, str) and isinstance(version, str):
            versions[_normalize_name(name)] = version
    return versions


def _release_dependencies(data: dict[str, Any]) -> list[str]:
    release = data.get("tool", {}).get("vercel", {}).get("release", {})
    if "dependencies" in release:
        dependency_table = release.get("dependencies", {})
        if isinstance(dependency_table, list):
            return dependency_table
        dependencies = dependency_table.get("dependencies", [])
        if isinstance(dependencies, list):
            return dependencies
    dependencies = data.get("project", {}).get("dependencies", [])
    if isinstance(dependencies, list):
        return dependencies
    return []


def _external_dependencies(
    package_name: str, data: dict[str, Any], vendored_requirements: tuple[str, ...]
) -> tuple[str, ...]:
    packages = workspace.packages()
    vendored_names = {_requirement_name(requirement) for requirement in vendored_requirements}
    peers = PEER_DEPENDENCIES.get(package_name, set())
    external = []
    for dependency in _release_dependencies(data):
        parsed = Requirement(dependency)
        normalized = _normalize_name(parsed.name)
        if normalized in SHARED_VENDORED_LIBS.values():
            continue
        if normalized.startswith("vercel-") and normalized in packages:
            external.append(_vendored_dependency(parsed))
        elif normalized in peers or normalized not in vendored_names:
            external.append(dependency)
    if package_name != SHARED_VENDORED_PACKAGE and _uses_shared_vendored_deps(package_name, data):
        external.append(_vendored_dependency(Requirement(SHARED_VENDORED_PACKAGE)))
    return tuple(external)


def _uses_shared_vendored_deps(package_name: str, data: dict[str, Any]) -> bool:
    if package_name in SHARED_VENDORED_CONSUMERS:
        return True
    for dependency in _release_dependencies(data):
        parsed = Requirement(dependency)
        if _normalize_name(parsed.name) in SHARED_VENDORED_LIBS.values():
            return True
    return False


def _vendored_dependency(requirement: Requirement) -> str:
    extras = f"[{','.join(sorted(requirement.extras))}]" if requirement.extras else ""
    normalized = _normalize_name(requirement.name)
    if normalized == SHARED_VENDORED_PACKAGE:
        version = shared_vendored_version()
    else:
        packages = workspace.packages()
        package = packages[normalized]
        version = workspace.read_version(package.version_file)
    specifiers = [
        str(specifier) for specifier in requirement.specifier if specifier.operator != ">="
    ]
    specifier = ",".join([f">={version}", *specifiers])
    marker = f" ; {requirement.marker}" if requirement.marker else ""
    name = requirement.name
    if _normalize_name(name) != SHARED_VENDORED_PACKAGE:
        name = f"{name}{VENDORED_SUFFIX}"
    return f"{name}{extras}{specifier}{marker}"


def _requirement_name(requirement: str) -> str:
    if requirement.startswith(WORKSPACE_REQUIREMENT_PREFIX):
        return _normalize_name(requirement.removeprefix(WORKSPACE_REQUIREMENT_PREFIX))
    return _normalize_name(Requirement(requirement).name)


def _normalize_name(name: str) -> str:
    return name.lower().replace("_", "-")


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def print_plan(plan: VendoredPlan) -> None:
    print(f"package: {plan.package.name}")
    print(f"bundle-package: {plan.variant_name}")
    print(f"path: {_display_path(plan.package.path)}")
    print(f"destination: {plan.config.destination}")
    print(f"namespace: {plan.config.namespace}")
    print("vendored-requirements:")
    for requirement in plan.vendored_requirements:
        print(f"  - {requirement}")
    print("external-dependencies:")
    if plan.external_dependencies:
        for dependency in plan.external_dependencies:
            print(f"  - {dependency}")
    else:
        print("  - <none>")


def shared_vendored_version() -> str:
    override = os.environ.get(SHARED_VERSION_ENV)
    if override:
        return override
    previous = _latest_pypi_release(SHARED_VENDORED_PACKAGE)
    if previous is None:
        return "0.1.0"
    if _pypi_shared_deps_fingerprint(previous) == _shared_deps_fingerprint():
        return previous
    return _bump_patch(previous)


def shared_vendored_needs_publish() -> bool:
    previous = _latest_pypi_release(SHARED_VENDORED_PACKAGE)
    if previous is None:
        return True
    return _pypi_shared_deps_fingerprint(previous) != _shared_deps_fingerprint()


def _latest_pypi_release(package_name: str) -> str | None:
    try:
        data = _read_json_url(f"https://pypi.org/pypi/{package_name}/json")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    releases = data.get("releases", {})
    versions = [Version(version) for version, files in releases.items() if files]
    if not versions:
        return None
    return str(max(versions))


def _pypi_shared_deps_fingerprint(version: str) -> str | None:
    try:
        data = _read_json_url(f"https://pypi.org/pypi/{SHARED_VENDORED_PACKAGE}/{version}/json")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    urls = data.get("urls", [])
    for file in urls:
        if file.get("packagetype") != "bdist_wheel":
            continue
        url = file.get("url")
        if isinstance(url, str):
            return _wheel_shared_deps_fingerprint(url)
    return None


def _wheel_shared_deps_fingerprint(url: str) -> str | None:
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        wheel_bytes = response.read()
    with zipfile.ZipFile(io.BytesIO(wheel_bytes)) as archive:
        try:
            content = archive.read(f"vercel/internal/_vendor/{SHARED_DEPS_METADATA}")
        except KeyError:
            return None
    data = json.loads(content.decode("utf-8"))
    value = data.get("fingerprint")
    return value if isinstance(value, str) else None


def _read_json_url(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        data = json.loads(response.read().decode("utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"unexpected JSON from {url}")
    return data


def _shared_deps_fingerprint() -> str:
    payload = "\n".join(_derive_vendor_requirements(SHARED_VENDORED_PACKAGE, {})) + "\n"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _bump_patch(version: str) -> str:
    parsed = Version(version)
    release = list(parsed.release)
    while len(release) < 3:
        release.append(0)
    release[2] += 1
    return f"{release[0]}.{release[1]}.{release[2]}"


def build_bundle_package(package_name: str, *, out_dir: Path, work_dir: Path) -> Path:
    plan = load_plan(package_name)
    generated = work_dir / plan.package.name
    if generated.exists():
        shutil.rmtree(generated)
    if plan.package.name == SHARED_VENDORED_PACKAGE:
        _generate_shared_package(generated)
    else:
        shutil.copytree(
            plan.package.path,
            generated,
            ignore=shutil.ignore_patterns(
                "__pycache__", ".mypy_cache", ".pytest_cache", ".ruff_cache", ".venv*"
            ),
        )
        shutil.copy2(ROOT / "scripts" / "hatch_build.py", generated / "_vercel_hatch_build.py")

    wheel_dir = work_dir / "workspace-wheels" / plan.package.name
    _rewrite_vendor_requirements(plan, generated=generated, wheel_dir=wheel_dir)
    _rewrite_readme(plan, generated)
    _write_vendoring_config(plan, generated)
    _run(["uvx", "--with=pip", "vendoring", "sync"], cwd=generated)
    _preserve_vendored_licenses(plan, generated=generated)
    _rewrite_pyproject(plan, generated)
    _rewrite_nested_vendor_namespace(plan, generated)
    _rewrite_source_imports(plan, generated)
    out_dir.mkdir(parents=True, exist_ok=True)
    _run(["uv", "build", str(generated), "--out-dir", str(out_dir)], cwd=ROOT)
    wheel = _single_wheel(out_dir, plan.variant_name)
    print(wheel)
    return wheel


def _generate_shared_package(path: Path) -> None:
    vendor_path = path / "vercel/internal/_vendor"
    vendor_path.mkdir(parents=True)
    version = shared_vendored_version()
    _write_shared_pyproject(path, version=version)
    (path / "README.md").write_text(
        "# vercel-internal-shared-vendored-deps\n\n"
        "Shared vendored third-party dependencies for Vercel Python `-bundle` packages.\n",
        encoding="utf-8",
    )
    shutil.copy2(ROOT / "LICENSE", path / "LICENSE")
    (vendor_path / "__init__.py").write_text(
        '"""Shared vendored third-party dependencies for Vercel Python packages."""\n',
        encoding="utf-8",
    )
    (vendor_path / "py.typed").write_text("", encoding="utf-8")
    (vendor_path / "version.py").write_text(f'__version__ = "{version}"\n', encoding="utf-8")
    _write_shared_deps_metadata(vendor_path)


def _rewrite_readme(plan: VendoredPlan, generated: Path) -> None:
    if plan.package.name == SHARED_VENDORED_PACKAGE:
        return
    path = generated / "README.md"
    if not path.exists():
        return
    original_name = plan.package.name
    bundle_name = plan.variant_name
    preface = (
        f"# {bundle_name}\n\n"
        f"This is a version of `{original_name}` with third-party dependencies bundled. "
        f"For normal use, install the unbundled `{original_name}` package instead: "
        f"https://pypi.org/project/{original_name}/\n\n"
    )
    path.write_text(preface + path.read_text(encoding="utf-8"), encoding="utf-8")


def _write_shared_pyproject(path: Path, *, version: str) -> None:
    path.joinpath("pyproject.toml").write_text(
        f"""
[build-system]
requires = ["hatchling>=1.27.0,<2"]
build-backend = "hatchling.build"

[project]
name = "{SHARED_VENDORED_PACKAGE}"
dynamic = ["version"]
description = "Shared vendored dependencies for Vercel Python packages"
readme = "README.md"
requires-python = ">=3.10"
dependencies = []
license = "MIT"
license-files = ["LICENSE", "LICENSE.*"]

[tool.vercel.release.dependencies]
dependencies = []

[tool.hatch.version]
path = "vercel/internal/_vendor/version.py"

[tool.hatch.build.targets.sdist]
include = [
    "/vercel/internal/_vendor/**/*.py",
    "/vercel/internal/_vendor/{SHARED_DEPS_METADATA}",
    "/vercel/internal/_vendor/py.typed",
    "/README.md",
    "/pyproject.toml",
    "/LICENSE",
]
exclude = [
    "/**/__pycache__",
]

[tool.hatch.build.targets.wheel]
dev-mode-dirs = ["."]
only-include = [
    "/vercel/internal/_vendor",
]
exclude = [
    "/**/__pycache__",
]
""".lstrip(),
        encoding="utf-8",
    )


def _shared_h2_transformations() -> str:
    lines = []
    for module in ("config", "connection", "events", "exceptions", "settings"):
        lines.append(
            "    { "
            f"match = '''import h2\\.{module}''', "
            "replace = '''from vercel.internal._vendor import h2\n"
            f"from vercel.internal._vendor.h2 import {module}''' "
            "},"
        )
    return "\n".join(lines)


def _write_vendoring_config(plan: VendoredPlan, generated: Path) -> None:
    path = generated / "pyproject.toml"
    text = path.read_text(encoding="utf-8").rstrip()
    text = _strip_vendoring_config(text)
    text = f"{text}\n\n{_render_vendoring_config(plan)}"
    path.write_text(text, encoding="utf-8")


def _strip_vendoring_config(text: str) -> str:
    return re.sub(
        r"(?ms)^\[tool\.vendoring\].*?(?=^\[[^\n]+\]|\Z)",
        "",
        text,
    ).rstrip()


def _render_vendoring_config(plan: VendoredPlan) -> str:
    transformations = _vendoring_transformations(plan)
    lines = [
        "[tool.vendoring]",
        f'destination = "{_toml_path(plan.config.destination)}/"',
        f'requirements = "{_toml_path(plan.config.requirements)}"',
        f'namespace = "{plan.config.namespace}"',
        f"protected-files = {_format_toml_string_array(plan.config.protected_files)}",
        "",
        "[tool.vendoring.transformations]",
    ]
    if transformations.substitutions:
        lines.append("substitute = [")
        lines.extend(
            _format_substitution(match, replace) for match, replace in transformations.substitutions
        )
        lines.append("]")
    lines.append(f"drop = {_format_toml_string_array(transformations.drops)}")
    return "\n".join(lines) + "\n"


def _vendoring_transformations(plan: VendoredPlan) -> VendoringTransformations:
    if plan.package.name == SHARED_VENDORED_PACKAGE:
        return VendoringTransformations(
            substitutions=tuple(_shared_h2_substitution_pairs()),
        )
    if any(_requirement_name(requirement) == "anyio" for requirement in plan.vendored_requirements):
        return VendoringTransformations(substitutions=(ANYIO_FROM_THREAD_SUBSTITUTION,))
    return VendoringTransformations()


def _shared_h2_substitution_pairs() -> tuple[tuple[str, str], ...]:
    pairs = []
    for module in ("config", "connection", "events", "exceptions", "settings"):
        pairs.append(
            (
                rf"import h2\.{module}",
                "from vercel.internal._vendor import h2\n"
                f"from vercel.internal._vendor.h2 import {module}",
            )
        )
    return tuple(pairs)


def _format_substitution(match: str, replace: str) -> str:
    return f"    {{ match = {json.dumps(match)}, replace = {json.dumps(replace)} }},"


def _toml_path(path: Path) -> str:
    return path.as_posix().rstrip("/")


def _write_shared_deps_metadata(vendor_path: Path) -> None:
    metadata = {
        "fingerprint": _shared_deps_fingerprint(),
        "requirements": list(_derive_vendor_requirements(SHARED_VENDORED_PACKAGE, {})),
    }
    (vendor_path / SHARED_DEPS_METADATA).write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _rewrite_vendor_requirements(plan: VendoredPlan, *, generated: Path, wheel_dir: Path) -> None:
    workspace_wheels: dict[str, Path] = {}
    rewritten = []
    for requirement in plan.vendored_requirements:
        if not requirement.startswith(WORKSPACE_REQUIREMENT_PREFIX):
            rewritten.append(requirement)
            continue
        package_name = requirement.removeprefix(WORKSPACE_REQUIREMENT_PREFIX)
        wheel = workspace_wheels.get(package_name)
        if wheel is None:
            wheel = _build_workspace_wheel(package_name, wheel_dir)
            workspace_wheels[package_name] = wheel
        rewritten.append(str(wheel))
    path = generated / plan.config.requirements
    path.parent.mkdir(parents=True, exist_ok=True)
    _ensure_vendor_package_marker(plan, generated=generated)
    path.write_text("\n".join(rewritten) + "\n", encoding="utf-8")


def _ensure_vendor_package_marker(plan: VendoredPlan, *, generated: Path) -> None:
    marker = generated / plan.config.destination / "__init__.py"
    if marker.exists():
        return
    marker.write_text(
        f'"""Generated vendored dependencies for {plan.variant_name}."""\n',
        encoding="utf-8",
    )


def _build_workspace_wheel(package_name: str, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    before = set(out_dir.glob("*.whl"))
    _run(
        [
            "uv",
            "build",
            "--package",
            package_name,
            "--wheel",
            "--no-sources",
            "--out-dir",
            str(out_dir),
        ],
        cwd=ROOT,
    )
    wheels = sorted(set(out_dir.glob(f"{package_name.replace('-', '_')}-*.whl")) - before)
    if not wheels:
        wheels = sorted(out_dir.glob(f"{package_name.replace('-', '_')}-*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"expected one built wheel for {package_name}, found {len(wheels)}")
    return wheels[0]


def _rewrite_pyproject(plan: VendoredPlan, generated: Path) -> None:
    path = generated / "pyproject.toml"
    text = path.read_text(encoding="utf-8")
    text = re.sub(
        rf'(?m)^(name\s*=\s*)"{re.escape(plan.package.name)}"',
        rf'\1"{plan.variant_name}"',
        text,
        count=1,
    )
    deps = _format_toml_string_array(plan.external_dependencies)
    if "[tool.vercel.release.dependencies]" in text:
        text = re.sub(
            r"(?ms)^\[tool\.vercel\.release\.dependencies\]\ndependencies = \[.*?\]\n",
            f"[tool.vercel.release.dependencies]\ndependencies = {deps}\n",
            text,
            count=1,
        )
    else:
        text = re.sub(
            r"(?ms)(^\[project\]\n.*?^dependencies = )\[.*?\]",
            rf"\1{deps}",
            text,
            count=1,
        )
    text = re.sub(
        r'force-include = \{ "\.\./\.\./scripts/hatch_build\.py" = "/_vercel_hatch_build\.py" \}',
        'force-include = { "_vercel_hatch_build.py" = "/_vercel_hatch_build.py" }',
        text,
    )
    text = _rewrite_license_files(plan, text, generated=generated)
    text = _ensure_sdist_license_include(plan, text, generated=generated)
    path.write_text(text, encoding="utf-8")


def _preserve_vendored_licenses(plan: VendoredPlan, *, generated: Path) -> None:
    requirements = _third_party_vendored_requirements(plan)
    if not requirements:
        return

    with tempfile.TemporaryDirectory(prefix="vendored-licenses-") as temp_dir:
        temp = Path(temp_dir)
        requirements_path = temp / "requirements.txt"
        requirements_path.write_text("\n".join(requirements) + "\n", encoding="utf-8")
        site_packages = temp / "site"
        _run(
            [
                "uv",
                "pip",
                "install",
                "--target",
                str(site_packages),
                "--no-deps",
                "--requirement",
                str(requirements_path),
            ],
            cwd=ROOT,
        )
        _copy_vendored_license_files(
            plan,
            site_packages,
            generated=generated,
            package_names=tuple(_requirement_name(requirement) for requirement in requirements),
        )


def _third_party_vendored_requirements(plan: VendoredPlan) -> tuple[str, ...]:
    requirements = []
    for requirement in plan.vendored_requirements:
        if requirement.startswith(WORKSPACE_REQUIREMENT_PREFIX):
            continue
        parsed = Requirement(requirement)
        if parsed.marker is not None and not parsed.marker.evaluate():
            continue
        name = _normalize_name(parsed.name)
        if name.startswith("vercel-"):
            continue
        requirements.append(requirement)
    return tuple(requirements)


def _copy_vendored_license_files(
    plan: VendoredPlan, site_packages: Path, *, generated: Path, package_names: tuple[str, ...]
) -> None:
    destination = generated / _vendored_license_dir(plan)
    required = {_normalize_name(name) for name in package_names}
    copied = dict.fromkeys(required, 0)
    dist_infos = sorted(site_packages.glob("*.dist-info"))
    for dist_info in dist_infos:
        name = _dist_info_name(dist_info)
        if name not in required:
            continue
        for source, relative in _dist_info_license_files(dist_info):
            target = destination / _vendored_license_filename(name, relative)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
            copied[name] += 1

    missing = sorted(name for name, count in copied.items() if count == 0)
    if missing:
        packages = ", ".join(missing)
        raise SystemExit(f"missing vendored license files for: {packages}")


def _dist_info_name(dist_info: Path) -> str:
    metadata = dist_info / "METADATA"
    if metadata.exists():
        parsed = email.parser.Parser().parsestr(metadata.read_text(encoding="utf-8"))
        name = parsed.get("Name")
        if name:
            return _normalize_name(name)
    return _normalize_name(dist_info.name.rsplit("-", 2)[0])


def _dist_info_license_files(dist_info: Path) -> list[tuple[Path, Path]]:
    candidates: dict[Path, Path] = {}
    metadata = dist_info / "METADATA"
    if metadata.exists():
        parsed = email.parser.Parser().parsestr(metadata.read_text(encoding="utf-8"))
        for value in parsed.get_all("License-File", []):
            relative = Path(value)
            for source in (dist_info / "licenses" / relative, dist_info / relative):
                if source.is_file():
                    candidates[source] = _license_target_path(relative)

    for source in sorted(path for path in dist_info.rglob("*") if path.is_file()):
        relative = source.relative_to(dist_info)
        if "licenses" in {part.lower() for part in relative.parts} or LICENSE_FILE_RE.search(
            source.name
        ):
            candidates[source] = _license_target_path(relative)
    return sorted(candidates.items(), key=lambda item: item[1].as_posix())


def _license_target_path(relative: Path) -> Path:
    parts = relative.parts
    if parts[:1] and parts[0].lower() == "licenses":
        parts = parts[1:]
    if not parts:
        return Path("LICENSE")
    return Path(*parts)


def _vendored_license_filename(package_name: str, relative: Path) -> Path:
    name = relative.name
    stem = Path(name).stem or name
    suffix = Path(name).suffix
    return Path(f"{stem}.{package_name}{suffix}")


def _rewrite_license_files(plan: VendoredPlan, text: str, *, generated: Path) -> str:
    patterns = ["LICENSE", "LICENSE.*"]
    if _has_vendored_license_files(plan, generated):
        patterns.append(_vendored_license_glob(plan))
    value = _format_toml_string_array(tuple(patterns))
    if re.search(r"(?m)^license-files\s*=", text):
        return re.sub(
            r"(?ms)^license-files\s*=\s*\[.*?\]\n",
            f"license-files = {value}\n",
            text,
            count=1,
        )
    return re.sub(r"(?m)^(license\s*=.*\n)", rf"\1license-files = {value}\n", text, count=1)


def _ensure_sdist_license_include(plan: VendoredPlan, text: str, *, generated: Path) -> str:
    if not _has_vendored_license_files(plan, generated):
        return text
    include = f'    "/{_vendored_license_glob(plan)}",'
    if include.strip().strip(",") in text:
        return text
    return re.sub(
        r"(?ms)(^\[tool\.hatch\.build\.targets\.sdist\]\n.*?^include = \[\n)(.*?)(^\])",
        rf"\1\2{include}\n\3",
        text,
        count=1,
    )


def _has_vendored_license_files(plan: VendoredPlan, generated: Path) -> bool:
    path = generated / _vendored_license_dir(plan)
    return path.is_dir() and any(child.is_file() for child in path.rglob("*"))


def _vendored_license_dir(plan: VendoredPlan) -> Path:
    return plan.config.destination


def _vendored_license_glob(plan: VendoredPlan) -> str:
    return f"{_toml_path(_vendored_license_dir(plan))}/LICEN[CS]E*"


def _format_toml_string_array(values: tuple[str, ...]) -> str:
    if not values:
        return "[]"
    lines = ["["]
    lines.extend(f'    "{value}",' for value in values)
    lines.append("]")
    return "\n".join(lines)


def _rewrite_source_imports(plan: VendoredPlan, generated: Path) -> None:
    package_root = generated / _package_root_from_destination(plan.config.destination)
    files = []
    for path in sorted(package_root.rglob("*.py")):
        if plan.config.destination in path.relative_to(generated).parents:
            continue
        files.append(path)
    if not files:
        return

    script = """
import json
import sys
from pathlib import Path

from vendoring.tasks.vendor import rewrite_file_imports

namespace = sys.argv[1]
vendored_libs = json.loads(sys.argv[2])
substitutions = json.loads(sys.argv[3])
for filename in sys.argv[4:]:
    rewrite_file_imports(Path(filename), namespace, vendored_libs, substitutions)
""".strip()
    _run(
        [
            "uvx",
            "--with=pip",
            "--with=vendoring",
            "python",
            "-c",
            script,
            plan.config.namespace,
            json.dumps(_source_rewrite_libs(plan)),
            json.dumps(_source_rewrite_substitutions(plan)),
            *(str(path) for path in files),
        ],
        cwd=generated,
    )


def _rewrite_nested_vendor_namespace(plan: VendoredPlan, generated: Path) -> None:
    source = f"{plan.config.namespace}.{plan.config.namespace}"
    package_root = generated / _package_root_from_destination(plan.config.destination)
    for path in sorted(package_root.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        rewritten = text.replace(source, plan.config.namespace)
        if rewritten != text:
            path.write_text(rewritten, encoding="utf-8")


def _package_root_from_destination(destination: Path) -> Path:
    parts = destination.parts
    if parts[-1:] == ("_vendor",):
        return Path(*parts[:-1])
    raise SystemExit(f"vendoring destination must end with _vendor: {destination}")


def _source_rewrite_libs(plan: VendoredPlan) -> tuple[str, ...]:
    result: list[str] = []
    for requirement in plan.vendored_requirements:
        name = _requirement_name(requirement)
        if name.startswith("vercel-"):
            module = "vercel." + name.removeprefix("vercel-").replace("-", ".")
        elif name == "python-multipart":
            module = "python_multipart"
        elif name == "typing-extensions":
            module = "typing_extensions"
        else:
            module = name.replace("-", "_")
        result.append(module)
    return tuple(sorted(result, key=len, reverse=True))


def _source_rewrite_substitutions(plan: VendoredPlan) -> tuple[dict[str, str], ...]:
    substitutions = []
    if plan.package.name != SHARED_VENDORED_PACKAGE:
        for lib in sorted(SHARED_VENDORED_LIBS, key=len, reverse=True):
            escaped = re.escape(lib)
            substitutions.extend(
                [
                    {
                        "match": rf"from {escaped}(\.|\s)",
                        "replace": rf"from {SHARED_VENDOR_NAMESPACE}.{lib}\1",
                    },
                    {
                        "match": rf"import {escaped} as ([A-Za-z_]\w*)",
                        "replace": rf"import {SHARED_VENDOR_NAMESPACE}.{lib} as \1",
                    },
                    {
                        "match": rf"import {escaped}\.([A-Za-z_]\w*) as ([A-Za-z_]\w*)",
                        "replace": rf"import {SHARED_VENDOR_NAMESPACE}.{lib}.\1 as \2",
                    },
                    {
                        "match": rf"import {escaped}\.([A-Za-z_]\w*)",
                        "replace": rf"from {SHARED_VENDOR_NAMESPACE}.{lib} import \1",
                    },
                    {
                        "match": rf"import {escaped}(\s|$)",
                        "replace": rf"from {SHARED_VENDOR_NAMESPACE} import {lib}\1",
                    },
                ]
            )
    for lib in _source_rewrite_libs(plan):
        if "." not in lib:
            continue
        parent, _, child = lib.rpartition(".")
        substitutions.append(
            {
                "match": rf"import {re.escape(lib)} as ([A-Za-z_]\w*)",
                "replace": rf"from {plan.config.namespace}.{parent} import {child} as \1",
            }
        )
        substitutions.append(
            {
                "match": rf"import {re.escape(lib)}\.([A-Za-z_]\w*) as ([A-Za-z_]\w*)",
                "replace": rf"from {plan.config.namespace}.{lib} import \1 as \2",
            }
        )
    if "anyio" in _source_rewrite_libs(plan):
        substitutions.append(
            {
                "match": r"import anyio\.from_thread",
                "replace": "from anyio import from_thread",
            }
        )
    return tuple(substitutions)


def test_wheel(package_name: str, *, dist_dir: Path) -> None:
    plan = load_plan(package_name)
    wheel = _single_wheel(dist_dir, plan.variant_name)
    script = ROOT / ".github" / "scripts" / "test_installed_wheel.sh"
    _run(["sh", str(script), package_name, str(wheel.resolve())], cwd=ROOT)


def shared_github_release_body() -> str:
    derived_requirements = _derive_vendor_requirements(SHARED_VENDORED_PACKAGE, {})
    requirements = "\n".join(f"- `{requirement}`" for requirement in derived_requirements)
    return (
        f"## {shared_vendored_version()} - generated\n\n"
        "### Internal\n\n"
        "- Update shared vendored dependency set.\n\n"
        f"{requirements}\n"
    )


def _single_wheel(dist_dir: Path, package_name: str) -> Path:
    prefix = package_name.replace("-", "_")
    wheels = sorted(
        path for path in dist_dir.glob(f"{prefix}-*.whl") if not path.name.endswith(".metadata")
    )
    if len(wheels) != 1:
        message = f"expected exactly one {package_name} wheel in {dist_dir}, found {len(wheels)}"
        raise SystemExit(message)
    return wheels[0]


def _run(cmd: list[str], *, cwd: Path) -> None:
    subprocess.check_call(cmd, cwd=cwd)


def _default_work_dir() -> Path:
    return Path(tempfile.mkdtemp(prefix="bundle-release-"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser("plan")
    plan_parser.add_argument("--package", required=True)

    build_parser = subparsers.add_parser("build")
    build_parser.add_argument("--package", required=True)
    build_parser.add_argument("--out-dir", type=Path, required=True)
    build_parser.add_argument("--work-dir", type=Path)
    build_parser.add_argument("--dry-run", action="store_true")

    test_parser = subparsers.add_parser("test-wheel")
    test_parser.add_argument("--package", required=True)
    test_parser.add_argument("--dist-dir", type=Path, required=True)

    shared_version_parser = subparsers.add_parser("shared-version")
    shared_version_parser.add_argument("--needs-publish", action="store_true")

    subparsers.add_parser("shared-github-release-body")

    args = parser.parse_args(argv)
    if args.command == "plan":
        print_plan(load_plan(args.package))
        return 0
    if args.command == "build":
        plan = load_plan(args.package)
        if args.dry_run:
            print_plan(plan)
            print(f"out-dir: {args.out_dir}")
            work_dir = args.work_dir or Path(tempfile.gettempdir()) / "bundle-release-<temp>"
            print(f"work-dir: {work_dir}")
            return 0
        build_bundle_package(
            args.package,
            out_dir=args.out_dir,
            work_dir=args.work_dir or _default_work_dir(),
        )
        return 0
    if args.command == "test-wheel":
        test_wheel(args.package, dist_dir=args.dist_dir)
        return 0
    if args.command == "shared-version":
        if args.needs_publish and not shared_vendored_needs_publish():
            return 1
        print(shared_vendored_version())
        return 0
    if args.command == "shared-github-release-body":
        sys.stdout.write(shared_github_release_body())
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
