from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import bundle_release, publish, release, workspace


def package(
    name: str,
    *,
    dependencies: tuple[str, ...] = (),
    bundle: str | None = None,
) -> publish.PackagePlan:
    return publish.PackagePlan(name, "1.0.0", bundle, dependencies, f"Notes for {name}\n")


def plan(*packages: publish.PackagePlan) -> publish.ReleasePlan:
    return publish.ReleasePlan(packages, "1.0.0", True)


def test_detection_freezes_the_complete_native_release_plan(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    package_root = tmp_path / "env"
    package_root.mkdir()
    version_file = package_root / "version.py"
    version_file.write_text('__version__ = "1.2.3"\n', encoding="utf-8")
    env_package = workspace.Package("vercel-env", package_root, version_file, ())
    monkeypatch.setattr(release, "changed_packages", lambda **_: ["vercel-env"])
    monkeypatch.setattr(release, "github_release_body", lambda _: "Release notes\n")
    snapshots: list[bool] = []

    def shared_release() -> tuple[str, bool]:
        snapshots.append(True)
        return "0.8.2", False

    monkeypatch.setattr(bundle_release, "shared_vendored_release", shared_release)
    monkeypatch.setattr(bundle_release, "shared_github_release_body", lambda: "Shared notes\n")
    monkeypatch.setattr(workspace, "packages", lambda: {"vercel-env": env_package})
    monkeypatch.setattr(
        publish,
        "dependency_graph",
        lambda _: {publish.SHARED: set(), "vercel-env": {publish.SHARED}},
    )
    monkeypatch.setattr(bundle_release, "is_vendored_eligible", lambda _: False)

    result = publish.create_plan(base="HEAD^", force=False)

    assert result == publish.ReleasePlan(
        (
            publish.PackagePlan(publish.SHARED, "0.8.2", None, (), "Shared notes\n"),
            publish.PackagePlan("vercel-env", "1.2.3", None, (publish.SHARED,), "Release notes\n"),
        ),
        "0.8.2",
        False,
    )
    encoded = result.to_json()
    assert isinstance(encoded["packages"], list)
    assert encoded["publish-shared"] is False
    assert publish.ReleasePlan.from_json(encoded) == result
    assert snapshots == [True]


def test_detection_fails_when_the_shared_registry_snapshot_cannot_be_frozen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(release, "changed_packages", lambda **_: ["vercel-env"])

    def unavailable() -> tuple[str, bool]:
        raise OSError("PyPI unavailable")

    monkeypatch.setattr(bundle_release, "shared_vendored_release", unavailable)
    with pytest.raises(RuntimeError, match="Shared dependency lookup failed.*PyPI unavailable"):
        publish.create_plan(base="HEAD^", force=False)


def test_builds_receive_only_selected_ancestor_wheels_and_stop_on_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    release_plan = plan(
        package("base"),
        package("independent"),
        package("app", dependencies=("base",)),
    )
    seen: dict[str, set[str]] = {}

    def build(
        item: publish.PackagePlan, *, directory: Path, shared_version: str
    ) -> subprocess.CompletedProcess[str]:
        seen[item.name] = {path.name for path in directory.glob("*.whl")}
        (directory / f"{item.name}-1.0.0-py3-none-any.whl").write_text("wheel")
        return subprocess.CompletedProcess(["build"], 0, stdout="")

    monkeypatch.setattr(publish, "build_package", build)
    build_root = tmp_path / "build"
    assert publish.build_packages(release_plan, build_root=build_root) == 0
    assert seen == {"base": set(), "independent": set(), "app": {"base-1.0.0-py3-none-any.whl"}}
    assert publish.read_plan(build_root / publish.PLAN_FILENAME) == release_plan

    called: list[str] = []

    def fail_first(
        item: publish.PackagePlan, *, directory: Path, shared_version: str
    ) -> subprocess.CompletedProcess[str]:
        called.append(item.name)
        return subprocess.CompletedProcess(["build"], 2, stdout="verification failed")

    monkeypatch.setattr(publish, "build_package", fail_first)
    assert publish.build_packages(release_plan, build_root=tmp_path / "failed") == 1
    assert called == ["base"]


def test_bundle_build_and_verification_must_both_finish_before_plan_completion(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    release_plan = plan(package("base", bundle="base-bundle"))
    bundle_commands: list[str] = []

    def run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        is_bundle_command = len(command) > 2 and Path(command[1]).name == "bundle_release.py"
        if is_bundle_command:
            bundle_commands.append(command[2])
        return subprocess.CompletedProcess(
            command,
            1 if is_bundle_command and command[2] == "test-wheel" else 0,
            stdout="bundle verification failed",
        )

    monkeypatch.setattr(publish, "run_command_streaming", run)
    build_root = tmp_path / "build"
    assert publish.build_packages(release_plan, build_root=build_root) == 1
    assert bundle_commands == ["build", "test-wheel"]
    assert not (build_root / publish.PLAN_FILENAME).exists()


def test_publish_failure_blocks_dependents_but_allows_independent_packages(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    release_plan = plan(
        package("base"),
        package("app", dependencies=("base",)),
        package("independent"),
    )
    calls: list[str] = []

    def run(item: publish.PackagePlan, **_: object) -> publish.PublishedPackage:
        calls.append(item.name)
        if item.name == "base":
            raise RuntimeError("upload rejected")
        return publish.PublishedPackage((f"{item.name}-v1.0.0",), item.release_body)

    monkeypatch.setattr(publish, "publish_package", run)
    output = tmp_path / "output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    assert (
        publish.publish_packages(
            release_plan, build_root=tmp_path / "build", directory=tmp_path / "state", dry_run=False
        )
        == 1
    )
    assert calls == ["base", "independent"]
    text = output.read_text(encoding="utf-8")
    assert "independent-v1.0.0" in text
    assert "base-v1.0.0" not in text
    assert "app-v1.0.0" not in text


def test_partial_bundle_upload_retry_is_idempotent_and_only_then_emits_metadata(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    item = package("base", bundle="base-bundle")
    release_plan = plan(item)
    build_dir = tmp_path / "build" / "base"
    build_dir.mkdir(parents=True)
    for distribution in ("base", "base_bundle"):
        (build_dir / f"{distribution}-1.0.0-py3-none-any.whl").write_text("wheel")
        (build_dir / f"{distribution}-1.0.0.tar.gz").write_text("sdist")

    registry = {"base-bundle": 404, "base": 404}
    monkeypatch.setattr(publish, "_registry_status", lambda name, _: registry[name])
    uploads: list[str] = []
    fail_standard = {"enabled": True}

    def upload(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        distribution = Path(command[2]).name.rsplit("-1.0.0", 1)[0].replace("_", "-")
        uploads.append(distribution)
        if distribution == "base" and fail_standard["enabled"]:
            return subprocess.CompletedProcess(command, 1, stdout="upload rejected")
        registry[distribution] = 200
        return subprocess.CompletedProcess(command, 0, stdout="")

    monkeypatch.setattr(publish, "run_command_streaming", upload)
    first_output = tmp_path / "first-output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(first_output))
    assert (
        publish.publish_packages(
            release_plan,
            build_root=tmp_path / "build",
            directory=tmp_path / "first-state",
            dry_run=False,
        )
        == 1
    )
    assert uploads == ["base-bundle", "base"]
    first_output_text = first_output.read_text(encoding="utf-8")
    assert "base-bundle-v1.0.0" not in first_output_text
    assert "base-v1.0.0" not in first_output_text

    uploads.clear()
    fail_standard["enabled"] = False
    second_output = tmp_path / "second-output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(second_output))
    assert (
        publish.publish_packages(
            release_plan,
            build_root=tmp_path / "build",
            directory=tmp_path / "second-state",
            dry_run=False,
        )
        == 0
    )
    assert uploads == ["base"]
    output_text = second_output.read_text(encoding="utf-8")
    assert "base-bundle-v1.0.0" in output_text
    assert "base-v1.0.0" in output_text


@pytest.mark.skipif(sys.platform == "win32", reason="publishing runs on POSIX runners")
def test_cli_publishes_a_relocated_build_using_only_its_frozen_plan(
    tmp_path: Path,
) -> None:
    release_plan = plan(package("example"))
    source_plan = tmp_path / "source-plan.json"
    publish.write_plan(release_plan, source_plan)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    driver = fake_bin / "uv"
    driver.write_text(
        f"#!{sys.executable}\n"
        "import os, sys\n"
        "from pathlib import Path\n"
        "args = sys.argv[1:]\n"
        "if args[0] == 'build':\n"
        "    name = args[args.index('--package') + 1].replace('-', '_')\n"
        "    out = Path(args[args.index('--out-dir') + 1])\n"
        "    (out / f'{name}-1.0.0-py3-none-any.whl').write_text('wheel')\n"
        "    (out / f'{name}-1.0.0.tar.gz').write_text('sdist')\n"
        "elif args[0] == 'venv':\n"
        "    Path(args[-1]).mkdir(parents=True, exist_ok=True)\n"
        "elif args[:2] == ['pip', 'install']:\n"
        "    raise SystemExit(9 if os.getenv('FAIL_INSTALL') else 0)\n"
        "else:\n"
        "    raise SystemExit(f'unexpected uv call: {args}')\n",
        encoding="utf-8",
    )
    driver.chmod(0o755)
    curl = fake_bin / "curl"
    curl.write_text(f"#!{sys.executable}\nprint('404', end='')\n", encoding="utf-8")
    curl.chmod(0o755)
    env = {**os.environ, "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}"}
    failed = tmp_path / "failed-build"
    failed_build = subprocess.run(
        [
            sys.executable,
            str(Path(publish.__file__)),
            "build",
            "--plan",
            str(source_plan),
            "--build-dir",
            str(failed),
        ],
        cwd=publish.ROOT,
        env={**env, "FAIL_INSTALL": "1"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert failed_build.returncode == 1
    assert not (failed / publish.PLAN_FILENAME).exists()

    built = tmp_path / "built"
    build = subprocess.run(
        [
            sys.executable,
            str(Path(publish.__file__)),
            "build",
            "--plan",
            str(source_plan),
            "--build-dir",
            str(built),
        ],
        cwd=publish.ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert build.returncode == 0, build.stdout + build.stderr

    relocated = tmp_path / "downloaded-artifact"
    shutil.copytree(built, relocated)
    shutil.rmtree(built)
    source_plan.unlink()
    output = tmp_path / "github-output"
    result = subprocess.run(
        [
            sys.executable,
            str(Path(publish.__file__)),
            "publish",
            "--plan",
            str(relocated / publish.PLAN_FILENAME),
            "--build-dir",
            str(relocated),
            "--dry-run",
        ],
        cwd=tmp_path,
        env={**env, "GITHUB_OUTPUT": str(output)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert str(relocated / "example") in result.stdout
    output_values = dict(
        line.split("=", 1) for line in output.read_text(encoding="utf-8").splitlines()
    )
    assert output_values["tags"] == "example-v1.0.0"
    release_body = Path(output_values["release-bodies"]) / "example-v1.0.0.md"
    assert release_body.read_text(encoding="utf-8") == "Notes for example\n"


def test_dry_run_rejects_missing_artifacts_and_unexpected_registry_responses(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    item = package("example")
    monkeypatch.setattr(publish, "_registry_status", lambda *_: 404)
    with pytest.raises(RuntimeError, match="No built artifacts found"):
        publish.publish_package(item, build_dir=tmp_path, publish_shared=True, dry_run=True)

    monkeypatch.setattr(publish, "_registry_status", lambda *_: 503)
    with pytest.raises(RuntimeError, match="Unexpected PyPI response.*503"):
        publish.publish_package(item, build_dir=tmp_path, publish_shared=True, dry_run=True)


def test_shared_dependency_edges_match_release_runtime_dependencies() -> None:
    graph = publish.dependency_graph(workspace.packages())
    dependencies = publish.ancestors(graph)
    assert publish.SHARED in dependencies["vercel-internal-core"]
    assert publish.SHARED in dependencies["vercel-oidc"]
    assert "vercel-oidc" in dependencies["vercel-connect"]
    assert "vercel-queue" not in dependencies["vercel-connect"]
    assert "vercel-queue" in dependencies["vercel-workflow"]
