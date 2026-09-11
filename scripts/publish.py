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


def run_package(name: str, *, directory: Path) -> subprocess.CompletedProcess[str]:
    command = ["bash", str(ROOT / "scripts/publish-package.sh")]
    tail: deque[str] = deque(maxlen=40)
    print(f"::group::Publish {name}", flush=True)
    try:
        with subprocess.Popen(
            command,
            cwd=ROOT,
            env={**os.environ, "PACKAGE": name, "PUBLISH_DIR": str(directory)},
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


def run_packages(names: list[str], *, directory: Path, shared_error: str = "") -> int:
    graph = dependency_graph(workspace.packages())
    selected = set(names)
    unknown = selected - graph.keys()
    if unknown:
        raise ValueError(f"Unknown publish packages: {', '.join(sorted(unknown))}")
    ordered = ancestors(graph)
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
            for dependency in dependencies:
                for kind in ("standard", "bundle"):
                    destination = package_dir / "dependencies" / kind
                    destination.mkdir(parents=True, exist_ok=True)
                    for wheel in (directory / dependency / "artifacts" / kind).glob("*.whl"):
                        shutil.copy2(wheel, destination)
            if name == SHARED and shared_error:
                result = subprocess.CompletedProcess(["detect"], 1, stdout=shared_error)
            else:
                result = run_package(name, directory=package_dir)
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
            annotation = (
                f"{message}\n{log}".replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
            )
            print(f"::error title=Publish {name}::{annotation}", flush=True)
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
    print(summary, flush=True)
    summary += "\n".join(failure_logs)
    write_outputs({"tags": "\n".join(tags), "release-bodies": str(release_bodies)})
    if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary_path).open("a", encoding="utf-8") as output:
            output.write(summary)
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
