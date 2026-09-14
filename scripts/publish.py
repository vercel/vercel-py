#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import tempfile
from collections import deque
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
    unknown = set().union(*graph.values()) - graph.keys()
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


def run_command_streaming(
    command: list[str],
    *,
    env: dict[str, str],
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


def build_package(name: str, *, directory: Path) -> subprocess.CompletedProcess[str]:
    command = ["bash", str(ROOT / "scripts/build-publish-package.sh")]
    return run_command_streaming(
        command,
        env={**os.environ, "PACKAGE": name, "BUILD_DIR": str(directory)},
        group_title=f"Build and verify {name}",
    )


def run_package(
    name: str,
    *,
    directory: Path,
    build_dir: Path,
) -> subprocess.CompletedProcess[str]:
    command = ["bash", str(ROOT / "scripts/publish-package.sh")]
    env = {
        **os.environ,
        "PACKAGE": name,
        "PUBLISH_DIR": str(directory),
        "BUILD_DIR": str(build_dir),
    }
    return run_command_streaming(
        command,
        env=env,
        group_title=f"Publish {name}",
    )


def _format_error_annotation(title: str, message: str, log: str = "") -> str:
    body = f"{message}\n{log}" if log else message
    encoded = body.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    return f"::error title={title}::{encoded}"


def _write_step_summary(summary: str) -> None:
    print(summary, flush=True)
    if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary_path).open("a", encoding="utf-8") as output:
            output.write(summary)


def run_packages(names: list[str], *, directory: Path, shared_error: str = "") -> int:
    graph = dependency_graph(workspace.packages())
    selected = set(names)
    unknown = selected - graph.keys()
    if unknown:
        raise ValueError(f"Unknown publish packages: {', '.join(sorted(unknown))}")
    ordered = ancestors(graph)
    if not selected:
        release_bodies = directory / "release-bodies"
        release_bodies.mkdir(parents=True)
        write_outputs({"tags": "", "release-bodies": str(release_bodies)})
        _write_step_summary("## Publish results\n\n0 succeeded, 0 failed, 0 blocked.\n")
        return 0
    if shared_error:
        print(_format_error_annotation("Shared dependency lookup failed", shared_error), flush=True)
        _write_step_summary(
            "## Publish results\n\nShared dependency lookup failed; publishing was not started.\n\n"
            f"<pre>{escape(shared_error)}</pre>\n"
        )
        return 1

    build_root = directory / "build"
    build_dirs: dict[str, Path] = {}
    for name, dependencies in ordered.items():
        if name not in selected:
            continue
        build_dir = build_root / name
        build_dir.mkdir(parents=True)
        for dependency in dependencies:
            if dependency not in selected:
                continue
            for wheel in build_dirs[dependency].glob("*.whl"):
                shutil.copy2(wheel, build_dir)
        build_result = build_package(name, directory=build_dir)
        if build_result.returncode != 0:
            err_msg = build_result.stdout.strip() or "Build or verification failed."
            print(
                _format_error_annotation(
                    f"Build and verification {name} failed",
                    f"{name} failed (exit {build_result.returncode})",
                    err_msg,
                ),
                flush=True,
            )
            summary = (
                "## Publish results\n\n"
                "Build or verification failed; publishing was not started.\n\n"
                f"{name}: exit {build_result.returncode}\n\n<pre>{escape(err_msg)}</pre>\n"
            )
            _write_step_summary(summary)
            return 1
        build_dirs[name] = build_dir

    failed: set[str] = set()
    blocked: set[str] = set()
    tags: list[str] = []
    rows = ["| Package | Result | Details |", "| --- | --- | --- |"]
    failure_logs: list[str] = []
    release_bodies = directory / "release-bodies"
    release_bodies.mkdir(parents=True)

    for name, dependencies in ordered.items():
        if name not in selected:
            continue
        unavailable = set(dependencies) & (failed | blocked)
        if unavailable:
            blocked.add(name)
            reason = f"Blocked by {', '.join(sorted(unavailable))}"
            rows.append(f"| {name} | blocked | {reason} |")
            print(f"{name}: {reason}", flush=True)
            continue
        package_dir = directory / name
        try:
            package_dir.mkdir()
            result = run_package(name, directory=package_dir, build_dir=build_dirs[name])
            if result.returncode == 0:
                package_tags = (package_dir / "tags.txt").read_text(encoding="utf-8").splitlines()
                for tag in package_tags:
                    shutil.copy2(package_dir / "release-bodies" / f"{tag}.md", release_bodies)
                tags.extend(package_tags)
        except (OSError, UnicodeError) as error:
            result = subprocess.CompletedProcess(["prepare/collect"], 1, stdout=str(error))
        if result.returncode:
            failed.add(name)
            message = f"{name}: failed (exit {result.returncode})"
            log = result.stdout.strip() or "No output captured."
            reason = escape(log.splitlines()[-1][:240]).replace("|", "&#124;")
            rows.append(f"| {name} | failed, exit {result.returncode} | <code>{reason}</code> |")
            failure_logs.append(
                f"<details><summary>{message}</summary>\n\n<pre>{escape(log)}</pre>\n\n</details>\n"
            )
            annotation = _format_error_annotation(f"Publish {name}", message, log)
            print(annotation, flush=True)
        else:
            rows.append(f"| {name} | succeeded | |")
            print(f"{name}: succeeded", flush=True)

    summary = "\n".join(
        [
            "## Publish results",
            "",
            f"{len(selected) - len(failed) - len(blocked)} succeeded, "
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


def detect(*, base: str, force: bool) -> dict[str, str]:
    if force:
        packages = release.publishable_packages()
    else:
        if base == "0" * 40:
            base = "HEAD^"
        packages = release.changed_packages(base=base, head="HEAD")
    try:
        version, needs_publish = bundle_release.shared_vendored_release()
    except Exception as error:
        return {
            "packages": json.dumps([SHARED, *packages]),
            "shared-version": "",
            "publish-shared": "false",
            "shared-error": f"Shared dependency lookup failed: {type(error).__name__}: {error}",
        }
    if packages or needs_publish or force:
        packages = [SHARED, *packages]
    return {
        "packages": json.dumps(packages),
        "shared-version": version,
        "publish-shared": str(needs_publish or force).lower(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish packages without failing fast.")
    parser.add_argument("command", choices=("detect", "run"))
    args = parser.parse_args(argv)
    if args.command == "detect":
        write_outputs(
            detect(base=os.environ.get("BASE_REF", "HEAD^"), force=os.getenv("FORCE") == "true")
        )
        return 0
    names = json.loads(os.environ["PACKAGES_JSON"])
    if not isinstance(names, list) or not all(isinstance(name, str) for name in names):
        raise ValueError("PACKAGES_JSON must be an array of package names")
    directory = Path(tempfile.mkdtemp(prefix="vercel-publish-", dir=os.getenv("RUNNER_TEMP")))
    return run_packages(names, directory=directory, shared_error=os.getenv("SHARED_ERROR", ""))


if __name__ == "__main__":
    raise SystemExit(main())
