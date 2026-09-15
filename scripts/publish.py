#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections import deque
from dataclasses import dataclass
from graphlib import TopologicalSorter
from html import escape
from pathlib import Path
from uuid import uuid4

try:
    from scripts import bundle_release, release, workspace
except ImportError:  # pragma: no cover - script execution path
    import bundle_release  # type: ignore[no-redef]
    import release  # type: ignore[no-redef]
    import workspace  # type: ignore[no-redef]

ROOT = Path(__file__).resolve().parent.parent
SHARED = bundle_release.SHARED_VENDORED_PACKAGE
PLAN_FILENAME = "plan.json"


@dataclass(frozen=True)
class PackagePlan:
    name: str
    version: str
    bundle: str | None
    dependencies: tuple[str, ...]
    release_body: str

    def to_json(self) -> dict[str, object]:
        return {
            "name": self.name,
            "version": self.version,
            "bundle": self.bundle,
            "dependencies": list(self.dependencies),
            "release-body": self.release_body,
        }

    @classmethod
    def from_json(cls, value: object) -> PackagePlan:
        if not isinstance(value, dict):
            raise ValueError("each release-plan package must be an object")
        name = value.get("name")
        version = value.get("version")
        bundle = value.get("bundle")
        dependencies = value.get("dependencies")
        release_body = value.get("release-body")
        if not isinstance(name, str) or not isinstance(version, str):
            raise ValueError("release-plan package name and version must be strings")
        if bundle is not None and not isinstance(bundle, str):
            raise ValueError("release-plan package bundle must be a string or null")
        if not isinstance(dependencies, list) or not all(
            isinstance(dependency, str) for dependency in dependencies
        ):
            raise ValueError("release-plan package dependencies must be an array of strings")
        if not isinstance(release_body, str):
            raise ValueError("release-plan package release-body must be a string")
        return cls(name, version, bundle, tuple(dependencies), release_body)


@dataclass(frozen=True)
class ReleasePlan:
    packages: tuple[PackagePlan, ...]
    shared_version: str
    publish_shared: bool

    def to_json(self) -> dict[str, object]:
        return {
            "packages": [package.to_json() for package in self.packages],
            "shared-version": self.shared_version,
            "publish-shared": self.publish_shared,
        }

    @classmethod
    def from_json(cls, value: object) -> ReleasePlan:
        if not isinstance(value, dict):
            raise ValueError("release plan must be an object")
        packages = value.get("packages")
        shared_version = value.get("shared-version")
        publish_shared = value.get("publish-shared")
        if not isinstance(packages, list):
            raise ValueError("release-plan packages must be an array")
        if not isinstance(shared_version, str):
            raise ValueError("release-plan shared-version must be a string")
        if not isinstance(publish_shared, bool):
            raise ValueError("release-plan publish-shared must be a boolean")
        result = cls(
            tuple(PackagePlan.from_json(package) for package in packages),
            shared_version,
            publish_shared,
        )
        names = [package.name for package in result.packages]
        if len(names) != len(set(names)):
            raise ValueError("release-plan package names must be unique")
        available: set[str] = set()
        for package in result.packages:
            missing = set(package.dependencies) - available
            if missing:
                raise ValueError(
                    f"release-plan package {package.name} appears before dependencies: "
                    f"{', '.join(sorted(missing))}"
                )
            available.add(package.name)
        return result


def read_plan(path: Path) -> ReleasePlan:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError(f"could not read release plan {path}: {error}") from error
    return ReleasePlan.from_json(value)


def write_plan(plan: ReleasePlan, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(plan.to_json(), indent=2) + "\n", encoding="utf-8")


def dependency_graph(packages: dict[str, workspace.Package]) -> dict[str, set[str]]:
    graph = {name: set(package.dependencies) for name, package in packages.items()}
    graph[SHARED] = set()
    for name, package in packages.items():
        if bundle_release.is_vendored_eligible(
            package
        ) and bundle_release._uses_shared_vendored_deps(
            name, bundle_release._load_pyproject(package.path)
        ):
            graph[name].add(SHARED)
    return graph


def ancestors(graph: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    unknown = set().union(*graph.values()) - graph.keys() if graph else set()
    if unknown:
        raise ValueError(f"Unknown publish dependencies: {', '.join(sorted(unknown))}")
    result: dict[str, tuple[str, ...]] = {}
    ordered = TopologicalSorter({name: sorted(deps) for name, deps in sorted(graph.items())})
    for name in ordered.static_order():
        dependencies = set(graph[name])
        for dependency in graph[name]:
            dependencies.update(result[dependency])
        result[name] = tuple(sorted(dependencies))
    return result


def _shared_release_body(version: str) -> str:
    old_version = os.environ.get(bundle_release.SHARED_VERSION_ENV)
    os.environ[bundle_release.SHARED_VERSION_ENV] = version
    try:
        return bundle_release.shared_github_release_body()
    finally:
        if old_version is None:
            os.environ.pop(bundle_release.SHARED_VERSION_ENV, None)
        else:
            os.environ[bundle_release.SHARED_VERSION_ENV] = old_version


def create_plan(*, base: str, force: bool) -> ReleasePlan:
    names = (
        release.publishable_packages()
        if force
        else release.changed_packages(base="HEAD^" if base == "0" * 40 else base, head="HEAD")
    )
    try:
        shared_version, needs_shared_publish = bundle_release.shared_vendored_release()
    except Exception as error:
        raise RuntimeError(
            f"Shared dependency lookup failed: {type(error).__name__}: {error}"
        ) from error

    if names or needs_shared_publish or force:
        names = [SHARED, *names]
    selected = set(names)
    packages_by_name = workspace.packages()
    ordered = ancestors(dependency_graph(packages_by_name))
    unknown = selected - ordered.keys()
    if unknown:
        raise ValueError(f"Unknown publish packages: {', '.join(sorted(unknown))}")

    packages: list[PackagePlan] = []
    for name, dependencies in ordered.items():
        if name not in selected:
            continue
        if name == SHARED:
            version = shared_version
            bundle = None
            release_body = _shared_release_body(version)
        else:
            package = packages_by_name[name]
            version = workspace.read_version(package.version_file)
            bundle = (
                bundle_release.variant_name(name)
                if bundle_release.is_vendored_eligible(package)
                else None
            )
            release_body = release.github_release_body(name)
        packages.append(
            PackagePlan(
                name=name,
                version=version,
                bundle=bundle,
                dependencies=tuple(
                    dependency for dependency in dependencies if dependency in selected
                ),
                release_body=release_body,
            )
        )
    return ReleasePlan(tuple(packages), shared_version, needs_shared_publish or force)


def run_command_streaming(
    command: list[str],
    *,
    env: dict[str, str] | None = None,
    group_title: str,
) -> subprocess.CompletedProcess[str]:
    tail: deque[str] = deque(maxlen=40)
    print(f"::group::{group_title}", flush=True)
    try:
        with subprocess.Popen(
            command,
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        ) as process:
            assert process.stdout is not None
            try:
                for line in process.stdout:
                    print(line, end="", flush=True)
                    tail.append(line[-1000:])
                returncode = process.wait()
            except BaseException:
                process.kill()
                process.wait()
                raise
        return subprocess.CompletedProcess(command, returncode, stdout="".join(tail)[-8000:])
    except OSError as error:
        return subprocess.CompletedProcess(command, 1, stdout=f"{' '.join(command)}: {error}")
    finally:
        print("::endgroup::", flush=True)


def build_package(
    package: PackagePlan, *, directory: Path, shared_version: str
) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, bundle_release.SHARED_VERSION_ENV: shared_version}
    commands: list[tuple[str, list[str]]] = []
    if package.name != SHARED:
        commands.extend(
            [
                (
                    "build distribution",
                    [
                        "uv",
                        "build",
                        "--package",
                        package.name,
                        "--no-sources",
                        "--out-dir",
                        str(directory),
                    ],
                ),
                (
                    "verify distribution",
                    [
                        sys.executable,
                        str(ROOT / "scripts/verify_dist.py"),
                        "--dist-dir",
                        str(directory),
                        "--package",
                        package.name,
                    ],
                ),
            ]
        )
    if package.name == SHARED or package.bundle is not None:
        commands.extend(
            [
                (
                    "build bundle",
                    [
                        sys.executable,
                        str(ROOT / "scripts/bundle_release.py"),
                        "build",
                        "--package",
                        package.name,
                        "--out-dir",
                        str(directory),
                    ],
                ),
                (
                    "verify bundle",
                    [
                        sys.executable,
                        str(ROOT / "scripts/bundle_release.py"),
                        "test-wheel",
                        "--package",
                        package.name,
                        "--dist-dir",
                        str(directory),
                    ],
                ),
            ]
        )
    for step, command in commands:
        result = run_command_streaming(
            command,
            env=env,
            group_title=f"Build and verify {package.name}: {step}",
        )
        if result.returncode:
            return result
    return subprocess.CompletedProcess(["build", package.name], 0, stdout="")


def _format_error_annotation(title: str, message: str, log: str = "") -> str:
    body = f"{message}\n{log}" if log else message
    encoded = body.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    return f"::error title={title}::{encoded}"


def _write_step_summary(summary: str) -> None:
    print(summary, flush=True)
    if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary_path).open("a", encoding="utf-8") as output:
            output.write(summary)


def build_packages(plan: ReleasePlan, *, build_root: Path) -> int:
    build_root.mkdir(parents=True, exist_ok=True)
    existing = [package.name for package in plan.packages if (build_root / package.name).exists()]
    if existing:
        raise ValueError("build directory already contains package output: " + ", ".join(existing))
    (build_root / PLAN_FILENAME).unlink(missing_ok=True)
    build_dirs: dict[str, Path] = {}
    for package in plan.packages:
        build_dir = build_root / package.name
        build_dir.mkdir(parents=True)
        for dependency in package.dependencies:
            for wheel in build_dirs[dependency].glob("*.whl"):
                shutil.copy2(wheel, build_dir)
        result = build_package(package, directory=build_dir, shared_version=plan.shared_version)
        if result.returncode != 0:
            message = result.stdout.strip() or "Build or verification failed."
            print(
                _format_error_annotation(
                    f"Build and verification {package.name} failed",
                    f"{package.name} failed (exit {result.returncode})",
                    message,
                ),
                flush=True,
            )
            _write_step_summary(
                "## Build results\n\n"
                f"{package.name}: failed (exit {result.returncode})\n\n"
                f"<pre>{escape(message)}</pre>\n"
            )
            return 1
        build_dirs[package.name] = build_dir

    write_plan(plan, build_root / PLAN_FILENAME)
    _write_step_summary(f"## Build results\n\n{len(plan.packages)} succeeded, 0 failed.\n")
    return 0


def _registry_status(distribution: str, version: str) -> int:
    url = f"https://pypi.org/pypi/{distribution}/{version}/json"
    result = subprocess.run(
        ["curl", "--silent", "--output", os.devnull, "--write-out", "%{http_code}", url],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or f"PyPI request failed for {distribution}")
    try:
        return int(result.stdout)
    except ValueError as error:
        raise RuntimeError(
            f"Unexpected PyPI response for {distribution}: {result.stdout!r}"
        ) from error


def _artifacts(build_dir: Path, distribution: str, version: str) -> list[Path]:
    normalized = distribution.replace("-", "_")
    pattern = re.compile(rf"^{re.escape(normalized)}-{re.escape(version)}(?:-.+\.whl|\.tar\.gz)$")
    try:
        matching = sorted(
            path.resolve() for path in build_dir.iterdir() if pattern.match(path.name)
        )
    except OSError as error:
        raise RuntimeError(f"Could not read built artifacts in {build_dir}: {error}") from error
    if not matching:
        raise RuntimeError(f"No built artifacts found for {distribution} {version} in {build_dir}")
    return matching


@dataclass(frozen=True)
class PublishedPackage:
    tags: tuple[str, ...]
    body: str


def publish_package(
    package: PackagePlan,
    *,
    build_dir: Path,
    publish_shared: bool,
    dry_run: bool,
) -> PublishedPackage:
    if package.name == SHARED and not publish_shared:
        print(f"{SHARED} dependency set already matches latest PyPI release, skipping publish")
        return PublishedPackage((), "")
    if package.name != SHARED and package.version == "0.0.0":
        print(f"{package.name} is at the unpublished sentinel version, skipping publish")
        return PublishedPackage((), "")

    distributions = [name for name in (package.bundle, package.name) if name is not None]
    missing: list[tuple[str, list[Path]]] = []
    for distribution in distributions:
        status = _registry_status(distribution, package.version)
        if status == 200:
            print(f"{distribution} {package.version} already exists on PyPI, skipping publish")
        elif status == 404:
            missing.append((distribution, _artifacts(build_dir, distribution, package.version)))
        else:
            raise RuntimeError(
                f"Unexpected PyPI response for {distribution} {package.version}: {status}"
            )

    for distribution, artifacts in missing:
        if dry_run:
            print("Dry run: would publish " + " ".join(str(path) for path in artifacts))
            continue
        result = run_command_streaming(
            ["uv", "publish", *(str(path) for path in artifacts)],
            group_title=f"Publish {distribution}",
        )
        if result.returncode:
            raise RuntimeError(result.stdout.strip() or f"uv publish failed for {distribution}")

    tags = tuple(f"{distribution}-v{package.version}" for distribution in distributions)
    return PublishedPackage(tags, package.release_body)


def publish_packages(plan: ReleasePlan, *, build_root: Path, directory: Path, dry_run: bool) -> int:
    failed: set[str] = set()
    blocked: set[str] = set()
    tags: list[str] = []
    rows = ["| Package | Result | Details |", "| --- | --- | --- |"]
    failure_logs: list[str] = []
    release_bodies = directory / "release-bodies"
    release_bodies.mkdir(parents=True, exist_ok=True)

    for package in plan.packages:
        unavailable = set(package.dependencies) & (failed | blocked)
        if unavailable:
            blocked.add(package.name)
            reason = f"Blocked by {', '.join(sorted(unavailable))}"
            rows.append(f"| {package.name} | blocked | {reason} |")
            print(f"{package.name}: {reason}", flush=True)
            continue
        try:
            published = publish_package(
                package,
                build_dir=build_root / package.name,
                publish_shared=plan.publish_shared,
                dry_run=dry_run,
            )
            for tag in published.tags:
                (release_bodies / f"{tag}.md").write_text(published.body, encoding="utf-8")
            tags.extend(published.tags)
        except (OSError, UnicodeError, RuntimeError) as error:
            failed.add(package.name)
            message = f"{package.name}: failed"
            log = str(error) or "No output captured."
            reason = escape(log.splitlines()[-1][:240]).replace("|", "&#124;")
            rows.append(f"| {package.name} | failed | <code>{reason}</code> |")
            failure_logs.append(
                f"<details><summary>{message}</summary>\n\n<pre>{escape(log)}</pre>\n\n</details>\n"
            )
            print(_format_error_annotation(f"Publish {package.name}", message, log), flush=True)
        else:
            rows.append(f"| {package.name} | succeeded | |")
            print(f"{package.name}: succeeded", flush=True)

    summary = "\n".join(
        [
            "## Publish results",
            "",
            f"{len(plan.packages) - len(failed) - len(blocked)} succeeded, "
            f"{len(failed)} failed, {len(blocked)} blocked.",
            "",
            *rows,
            "",
        ]
    )
    summary += "\n".join(failure_logs)
    write_outputs({"tags": "\n".join(tags), "release-bodies": str(release_bodies)})
    _write_step_summary(summary)
    return int(bool(failed or blocked))


def write_outputs(values: dict[str, str]) -> None:
    delimiter = f"publish_{uuid4().hex}"
    text = "".join(
        f"{key}<<{delimiter}\n{value}\n{delimiter}\n"
        if "\n" in value or "\r" in value
        else f"{key}={value}\n"
        for key, value in values.items()
    )
    if output_path := os.environ.get("GITHUB_OUTPUT"):
        with Path(output_path).open("a", encoding="utf-8") as output:
            output.write(text)
    else:
        print(text, end="")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build and publish release packages.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    detect_parser = subparsers.add_parser("detect", help="write a frozen release plan")
    detect_parser.add_argument("--base", required=True)
    detect_parser.add_argument("--output", type=Path, required=True)
    detect_parser.add_argument("--force", action="store_true")

    build_parser = subparsers.add_parser("build", help="build and verify a release plan")
    build_parser.add_argument("--plan", type=Path, required=True)
    build_parser.add_argument("--build-dir", type=Path, required=True)

    publish_parser = subparsers.add_parser("publish", help="publish previously built artifacts")
    publish_parser.add_argument("--plan", type=Path, required=True)
    publish_parser.add_argument("--build-dir", type=Path, required=True)
    publish_parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args(argv)
    if args.command == "detect":
        plan = create_plan(base=args.base, force=args.force)
        write_plan(plan, args.output)
        write_outputs({"packages": json.dumps([package.name for package in plan.packages])})
        return 0
    if args.command == "build":
        plan = read_plan(args.plan)
        return build_packages(plan, build_root=args.build_dir)
    plan = read_plan(args.plan)
    directory = Path(tempfile.mkdtemp(prefix="vercel-publish-", dir=os.getenv("RUNNER_TEMP")))
    return publish_packages(
        plan, build_root=args.build_dir, directory=directory, dry_run=args.dry_run
    )


if __name__ == "__main__":
    raise SystemExit(main())
