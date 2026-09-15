from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import bundle_release, publish, workspace


@pytest.fixture
def pipeline(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    for name in ("build-publish-package.sh", "publish-package.sh"):
        shutil.copy2(publish.ROOT / "scripts" / name, scripts / name)

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    driver = fake_bin / "driver"
    driver.write_text(
        f"#!{sys.executable}\n"
        + """import json
import os
import sys
from pathlib import Path

args = sys.argv[1:]
program = Path(sys.argv[0]).name
root = Path(os.environ["PIPELINE_ROOT"])
shared = "vercel-internal-shared-vendored-deps"


def event(kind, name="", paths=()):
    with (root / "events.jsonl").open("a") as output:
        output.write(json.dumps([kind, name, list(paths)]) + "\\n")
    if os.getenv("FAIL_EVENT") == f"{kind}:{name}":
        sys.exit(42)


def option(name):
    return args[args.index(name) + 1]


def variant(name):
    return name if name == shared else name + "-bundle"


def write_artifacts(directory, name):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    prefix = name.replace("-", "_")
    for suffix in ("-py3-none-any.whl", ".tar.gz"):
        (directory / f"{prefix}-1.0.0{suffix}").write_text(f"built:{name}")
    (directory / f"{prefix}-1.0.00-py3-none-any.whl").write_text("wrong version")


if program == "curl":
    status = os.getenv("PYPI_STATUS", "404")
    print(os.getenv("BUNDLE_PYPI_STATUS", status) if "-bundle/" in args[-1] else status)
elif program == "python":
    if args[0] == "scripts/get-version.py":
        print("1.0.0")
    elif args[0] == "scripts/release.py":
        print("Release notes")
    elif args[0] == "-":
        print("false" if args[1] == "independent" else "true")
    elif args[:2] == ["scripts/verify_dist.py", "--dist-dir"]:
        name = option("--package")
        build_dir = Path(option("--dist-dir"))
        wheel_names = {path.name for path in build_dir.glob("*.whl")}
        prefix = name.replace("-", "_") + "-1.0.0-"
        assert any(filename.startswith(prefix) for filename in wheel_names)
        assert name == "independent" or not any(
            filename.startswith("independent-") for filename in wheel_names
        )
        event("verify", name, sorted(wheel_names))
    elif args[:2] == ["scripts/bundle_release.py", "shared-version"]:
        print(os.environ["VERCEL_INTERNAL_SHARED_VENDORED_DEPS_VERSION"])
    elif args[:2] == ["scripts/bundle_release.py", "shared-github-release-body"]:
        print("Shared release notes")
    elif args[:2] == ["scripts/bundle_release.py", "plan"]:
        name = option("--package")
        event("plan", name)
        print("bundle-package: " + variant(name))
    elif args[:2] == ["scripts/bundle_release.py", "build"]:
        name = option("--package")
        build_dir = Path(option("--out-dir"))
        if name == "app":
            assert (build_dir / "base_bundle-1.0.0-py3-none-any.whl").is_file()
            assert not any(build_dir.glob("independent-*.whl"))
        event("bundle", name, sorted(path.name for path in build_dir.glob("*.whl")))
        write_artifacts(build_dir, variant(name))
    elif args[:2] == ["scripts/bundle_release.py", "test-wheel"]:
        name = option("--package")
        build_dir = Path(option("--dist-dir"))
        if name == "app":
            assert (build_dir / "base_bundle-1.0.0-py3-none-any.whl").is_file()
            assert not any(build_dir.glob("independent-*.whl"))
        event("test-wheel", name, sorted(path.name for path in build_dir.glob("*.whl")))
    else:
        sys.exit(f"Unexpected python invocation: {args}")
elif program == "uv":
    if args[0] == "build":
        name = option("--package")
        build_dir = Path(option("--out-dir"))
        if name == "app":
            assert (build_dir / "base-1.0.0-py3-none-any.whl").is_file()
            assert not any(build_dir.glob("independent-*.whl"))
        event("build", name, sorted(path.name for path in build_dir.glob("*.whl")))
        write_artifacts(build_dir, name)
    elif args[0] == "publish":
        paths = [Path(value) for value in args[1:]]
        assert paths
        assert all(path.read_text().startswith("built:") for path in paths)
        name = paths[0].name.rsplit("-1.0.0", 1)[0]
        event("publish", name, [str(path) for path in paths])
    else:
        sys.exit(f"Unexpected uv invocation: {args}")
else:
    sys.exit(f"Unexpected program: {program}")
""",
        encoding="utf-8",
    )
    driver.chmod(0o755)
    for name in ("uv", "python", "curl"):
        (fake_bin / name).symlink_to(driver)
    (fake_bin / "python3").symlink_to(sys.executable)

    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("PIPELINE_ROOT", str(tmp_path))
    monkeypatch.setenv("DRY_RUN", "false")
    monkeypatch.setenv("PUBLISH_SHARED", "true")
    monkeypatch.setenv(bundle_release.SHARED_VERSION_ENV, "1.0.0")
    monkeypatch.setattr(publish, "ROOT", tmp_path)
    monkeypatch.setattr(workspace, "packages", lambda: {})
    monkeypatch.setattr(
        publish,
        "dependency_graph",
        lambda _: {
            publish.SHARED: set(),
            "base": {publish.SHARED},
            "app": {"base"},
            "independent": set(),
        },
    )
    return tmp_path


def events(root: Path) -> list[list[object]]:
    path = root / "events.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def event_names(root: Path) -> list[tuple[str, str]]:
    return [(str(kind), str(name)) for kind, name, _ in events(root)]


@pytest.mark.skipif(sys.platform == "win32", reason="publishing runs on POSIX runners")
def test_built_artifacts_can_be_published_in_a_separate_run(pipeline: Path) -> None:
    build_root = pipeline / "artifacts"
    assert publish.build_packages(["base"], build_root=build_root) == 0
    assert (
        publish.publish_packages(["base"], build_root=build_root, directory=pipeline / "run") == 0
    )
    assert event_names(pipeline) == [
        ("build", "base"),
        ("verify", "base"),
        ("plan", "base"),
        ("bundle", "base"),
        ("test-wheel", "base"),
        ("publish", "base_bundle"),
        ("publish", "base"),
    ]
    build_events = events(pipeline)[:5]
    assert all(publish.SHARED.replace("-", "_") not in str(paths) for _, _, paths in build_events)


@pytest.mark.skipif(sys.platform == "win32", reason="publishing runs on POSIX runners")
def test_build_order_and_selected_ancestor_handoff(pipeline: Path) -> None:
    assert (
        publish.run_packages(
            [publish.SHARED, "base", "app", "independent"], directory=pipeline / "run"
        )
        == 0
    )
    recorded = event_names(pipeline)
    assert recorded.index(("bundle", publish.SHARED)) < recorded.index(("build", "base"))
    assert recorded.index(("build", "base")) < recorded.index(("build", "app"))
    first_upload = next(index for index, (kind, _) in enumerate(recorded) if kind == "publish")
    assert all(kind == "publish" for kind, _ in recorded[first_upload:])
    assert recorded.count(("build", "base")) == 1
    assert recorded.count(("bundle", "base")) == 1
    assert recorded.count(("build", "app")) == 1
    assert recorded.count(("bundle", "app")) == 1


@pytest.mark.skipif(sys.platform == "win32", reason="publishing runs on POSIX runners")
@pytest.mark.parametrize(
    "failure",
    ["build:base", "verify:base", "plan:base", "bundle:base", "test-wheel:base"],
)
def test_every_build_or_verification_failure_prevents_all_uploads(
    pipeline: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    monkeypatch.setenv("FAIL_EVENT", failure)
    assert publish.run_packages(["base", "independent"], directory=pipeline / "run") == 1
    assert all(kind != "publish" for kind, _ in event_names(pipeline))


@pytest.mark.skipif(sys.platform == "win32", reason="publishing runs on POSIX runners")
def test_empty_selection_does_not_build_or_upload(pipeline: Path) -> None:
    assert publish.run_packages([], directory=pipeline / "run") == 0
    assert events(pipeline) == []


@pytest.mark.skipif(sys.platform == "win32", reason="publishing runs on POSIX runners")
@pytest.mark.parametrize(("dry_run", "status"), [("true", "404"), ("false", "200")])
def test_dry_run_and_existing_versions_still_verify_without_uploads(
    pipeline: Path, monkeypatch: pytest.MonkeyPatch, dry_run: str, status: str
) -> None:
    monkeypatch.setenv("DRY_RUN", dry_run)
    monkeypatch.setenv("PYPI_STATUS", status)
    assert publish.run_packages(["base"], directory=pipeline / "run") == 0
    assert all(kind != "publish" for kind, _ in event_names(pipeline))
    assert ("test-wheel", "base") in event_names(pipeline)


@pytest.mark.skipif(sys.platform == "win32", reason="publishing runs on POSIX runners")
def test_bundle_upload_failure_blocks_dependents_but_not_independent_packages(
    pipeline: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAIL_EVENT", "publish:base_bundle")
    assert publish.run_packages(["base", "app", "independent"], directory=pipeline / "run") == 1
    uploaded = [name for kind, name in event_names(pipeline) if kind == "publish"]
    assert uploaded == ["independent", "base_bundle"]


@pytest.mark.skipif(sys.platform == "win32", reason="publishing runs on POSIX runners")
@pytest.mark.parametrize("distribution", ["base", "base-bundle", publish.SHARED])
def test_dry_run_validates_upload_artifact_selection(
    pipeline: Path, monkeypatch: pytest.MonkeyPatch, distribution: str
) -> None:
    monkeypatch.setenv("DRY_RUN", "true")
    original_build = publish.build_package

    def remove_artifacts(name: str, *, directory: Path) -> subprocess.CompletedProcess[str]:
        result = original_build(name, directory=directory)
        for path in directory.glob(f"{distribution.replace('-', '_')}-1.0.0[.-]*"):
            path.unlink()
        return result

    monkeypatch.setattr(publish, "build_package", remove_artifacts)
    selected = publish.SHARED if distribution == publish.SHARED else "base"
    assert publish.run_packages([selected], directory=pipeline / "run") == 1
    assert all(kind != "publish" for kind, _ in event_names(pipeline))


@pytest.mark.skipif(sys.platform == "win32", reason="publishing runs on POSIX runners")
@pytest.mark.parametrize("status", ["403", "500"])
def test_dry_run_does_not_ignore_bundle_registry_errors(
    pipeline: Path, monkeypatch: pytest.MonkeyPatch, status: str
) -> None:
    monkeypatch.setenv("DRY_RUN", "true")
    monkeypatch.setenv("BUNDLE_PYPI_STATUS", status)
    assert publish.run_packages(["base"], directory=pipeline / "run") == 1
    assert all(kind != "publish" for kind, _ in event_names(pipeline))
