#!/usr/bin/env python3
"""Exercise split-distribution upgrades and uninstalls in clean environments."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    from scripts import wheel_test
except ImportError:  # pragma: no cover - script execution path
    import wheel_test  # type: ignore[no-redef]


ROOT = Path(__file__).resolve().parent.parent
# Fixed final pre-split aggregate release: it still owns the legacy Sandbox paths
# whose transition this upgrade exercises. Do not advance it to track the latest release.
DEFAULT_PRE_SPLIT_AGGREGATE = "vercel==0.7.2"


def _python(environment: Path) -> Path:
    return environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def _launcher(environment: Path, name: str) -> Path:
    suffix = ".exe" if os.name == "nt" else ""
    directory = "Scripts" if os.name == "nt" else "bin"
    return environment / directory / f"{name}{suffix}"


def _clean_environment(root: Path, name: str) -> Path:
    environment = root / name
    subprocess.check_call(["uv", "venv", "--python", sys.executable, str(environment)], cwd=root)
    return environment


def _run_python(environment: Path, code: str) -> None:
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    subprocess.check_call([str(_python(environment)), "-c", code], cwd=environment.parent, env=env)


def _install(
    environment: Path,
    *requirements: str | Path,
    find_links: Path | None = None,
    upgrade: bool = False,
) -> None:
    command = [
        "uv",
        "pip",
        "install",
        "--python",
        str(_python(environment)),
        "--no-config",
    ]
    if find_links is not None:
        command.extend(("--find-links", str(find_links.resolve())))
    if upgrade:
        command.append("--upgrade")
    command.extend(str(requirement) for requirement in requirements)
    subprocess.check_call(command, cwd=environment.parent)


def _uninstall(environment: Path, *distributions: str) -> None:
    subprocess.check_call(
        ["uv", "pip", "uninstall", "--python", str(_python(environment)), *distributions],
        cwd=environment.parent,
    )


def _assert_upgraded(environment: Path) -> None:
    _run_python(
        environment,
        """
import importlib.util
from importlib.metadata import distribution

import vercel
import vercel.blob
import vercel.projects
import vercel.sandbox
from vercel.sandbox._internal.api_client import USER_AGENT, VERSION

assert vercel.session.__module__ == "vercel.internal.core.session"
assert vercel.sandbox.__file__
assert callable(vercel.sandbox.create_sandbox)
assert callable(vercel.sandbox.sync.create_sandbox)
assert VERSION == distribution("vercel-sandbox").version
assert USER_AGENT.startswith(f"vercel-sandbox/{VERSION} ")
assert any(
    str(path).replace("\\\\", "/").endswith("vercel/sandbox/__init__.py")
    for path in distribution("vercel-sandbox").files or ()
)

def module_exists(name):
    try:
        return importlib.util.find_spec(name) is not None
    except ModuleNotFoundError:
        return False

for removed_name in (
    "vercel.sandbox.aio",
    "vercel.sandbox.command",
    "vercel.sandbox.models",
    "vercel.sandbox.pty",
    "vercel.sandbox.sandbox",
    "vercel.sandbox.snapshot",
    "vercel._internal.sandbox",
    "vercel.unstable",
    "vercel.unstable.sandbox",
    "vercel._internal.unstable",
    "vercel._internal.unstable.sandbox",
):
    assert not module_exists(removed_name), removed_name

aggregate_scripts = {
    entry_point.name: entry_point.value
    for entry_point in distribution("vercel").entry_points
    if entry_point.group == "console_scripts"
}
sandbox_scripts = {
    entry_point.name: entry_point.value
    for entry_point in distribution("vercel-sandbox").entry_points
    if entry_point.group == "console_scripts"
}
assert aggregate_scripts == {"vercel": "vercel.__main__:main"}
assert sandbox_scripts == {
    "sandbox": "vercel.sandbox.__main__:main",
    "vercel-sandbox": "vercel.sandbox.__main__:main",
}
""",
    )
    for launcher_name in ("vercel", "sandbox", "vercel-sandbox"):
        launcher = _launcher(environment, launcher_name)
        assert launcher.is_file(), launcher


def _assert_sandbox_and_core(environment: Path, *, aggregate_absent: bool = False) -> None:
    aggregate_check = (
        """
from importlib.metadata import PackageNotFoundError, distribution
try:
    distribution("vercel")
except PackageNotFoundError:
    pass
else:
    raise AssertionError("Sandbox-only install pulled in aggregate vercel")
"""
        if aggregate_absent
        else ""
    )
    _run_python(
        environment,
        f"""
from vercel import sandbox, session
from importlib.metadata import distribution
assert sandbox.__name__ == "vercel.sandbox"
assert callable(session)
assert distribution("vercel-sandbox")
assert distribution("vercel-internal-core")
{aggregate_check}
""",
    )


def _assert_cache_and_core_without_aggregate(environment: Path) -> None:
    _run_python(
        environment,
        """
from importlib.metadata import PackageNotFoundError, distribution

import vercel
import vercel.cache
from vercel import session

assert callable(session)
assert distribution("vercel-internal-core")
assert distribution("vercel-cache")

for absent_distribution in ("vercel", "vercel-sandbox"):
    try:
        distribution(absent_distribution)
    except PackageNotFoundError:
        pass
    else:
        raise AssertionError(f"{absent_distribution} was unexpectedly installed")
""",
    )


def _assert_owned_files_survive(environment: Path, survivor: str) -> None:
    _run_python(
        environment,
        f"""
from importlib.metadata import distribution
dist = distribution({survivor!r})
files = [dist.locate_file(path) for path in dist.files or () if str(path).startswith("vercel/")]
assert files
assert all(path.exists() for path in files)
""",
    )


def run_lifecycle(*, dist_dir: Path, old_aggregate: str, mode: str = "all") -> None:
    dist_dir = dist_dir.resolve()
    wheel_test.build_workspace_wheels(["vercel"], dist_dir=dist_dir)
    artifacts = wheel_test.discover_wheels(dist_dir)
    aggregate = wheel_test.find_distribution_wheel(dist_dir, "vercel").path
    sandbox = wheel_test.find_distribution_wheel(dist_dir, "vercel-sandbox").path
    core = wheel_test.find_distribution_wheel(dist_dir, "vercel-internal-core")
    cache = wheel_test.find_distribution_wheel(dist_dir, "vercel-cache")
    cache_and_core_artifacts = wheel_test.local_dependency_artifacts(
        (core, cache),
        artifacts,
    )
    cache_and_core_distributions = {
        artifact.normalized_distribution for artifact in cache_and_core_artifacts
    }
    assert "vercel" not in cache_and_core_distributions
    assert "vercel-sandbox" not in cache_and_core_distributions

    wheel_test.assert_unique_ownership(artifacts)
    wheel_test.assert_unique_ownership(artifacts, split_paths_only=True)

    with tempfile.TemporaryDirectory(prefix="vercel-py-lifecycle-") as temporary:
        root = Path(temporary).resolve()

        if mode in ("all", "upgrade"):
            upgrade = _clean_environment(root, "upgrade")
            _install(upgrade, old_aggregate)
            _run_python(upgrade, "import vercel.sandbox")
            _install(upgrade, aggregate, find_links=dist_dir, upgrade=True)
            _assert_upgraded(upgrade)

        if mode in ("all", "local"):
            cache_and_core = _clean_environment(root, "cache-and-core")
            _install(
                cache_and_core,
                *(artifact.path for artifact in cache_and_core_artifacts),
                find_links=dist_dir,
            )
            _assert_cache_and_core_without_aggregate(cache_and_core)

            sandbox_only = _clean_environment(root, "sandbox-only")
            _install(sandbox_only, sandbox, find_links=dist_dir)
            _assert_sandbox_and_core(sandbox_only, aggregate_absent=True)

            retain_dependencies = _clean_environment(root, "retain-dependencies")
            _install(retain_dependencies, sandbox, find_links=dist_dir)
            _install(retain_dependencies, aggregate, find_links=dist_dir)
            _uninstall(retain_dependencies, "vercel")
            _assert_sandbox_and_core(retain_dependencies, aggregate_absent=True)
            _uninstall(retain_dependencies, "vercel-sandbox")
            _assert_owned_files_survive(retain_dependencies, "vercel-internal-core")

            preserve_service = _clean_environment(root, "preserve-service")
            _install(preserve_service, sandbox, find_links=dist_dir)
            _uninstall(preserve_service, "vercel-internal-core")
            _assert_owned_files_survive(preserve_service, "vercel-sandbox")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dist-dir", type=Path, required=True)
    parser.add_argument("--old-aggregate", default=DEFAULT_PRE_SPLIT_AGGREGATE)
    parser.add_argument("--mode", choices=("all", "local", "upgrade"), default="all")
    args = parser.parse_args()
    run_lifecycle(
        dist_dir=args.dist_dir,
        old_aggregate=args.old_aggregate,
        mode=args.mode,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
