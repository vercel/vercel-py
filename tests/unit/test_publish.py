from __future__ import annotations

import json
import subprocess
import sys
from graphlib import CycleError
from pathlib import Path

import pytest

from scripts import bundle_release, publish, release, workspace


@pytest.mark.parametrize(
    ("selected", "failures", "expected_calls", "expected_result"),
    [
        (["app", "right", "left", "base"], set(), ["base", "left", "right", "app"], 0),
        (["app", "right", "left", "base"], {"left"}, ["base", "left", "right"], 1),
        (["app", "right", "left", "base"], {"base"}, ["base", "right"], 1),
        (["app", "right", "base"], {"base"}, ["base", "right"], 1),
        (["app", "right"], set(), ["right", "app"], 0),
        (["base"], {"base"}, ["base"], 1),
        ([], set(), [], 0),
    ],
)
def test_publish_continues_independent_packages_and_blocks_failed_ancestors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    selected: list[str],
    failures: set[str],
    expected_calls: list[str],
    expected_result: int,
    capsys: pytest.CaptureFixture[str],
) -> None:
    graph = {"base": set(), "left": {"base"}, "right": {"other"}, "other": set(), "app": {"left"}}
    monkeypatch.setattr(workspace, "packages", lambda: {})
    monkeypatch.setattr(publish, "dependency_graph", lambda _: graph)
    output = tmp_path / "outputs"
    summary = tmp_path / "summary"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    monkeypatch.setenv("DRY_RUN", "true")
    monkeypatch.setenv("PUBLISH_SHARED", "false")
    monkeypatch.setenv(bundle_release.SHARED_VERSION_ENV, "0.8.2")
    calls = []

    def publish_package(name: str, *, directory: Path) -> subprocess.CompletedProcess[str]:
        calls.append(name)
        expected_dependencies = set(publish.ancestors(graph)[name]) & set(calls) - failures
        actual_wheels = {path.name for path in (directory / "dependencies").rglob("*.whl")}
        assert actual_wheels == {
            wheel
            for dependency in expected_dependencies
            for wheel in (f"{dependency}.whl", f"{dependency}-bundle.whl")
        }
        for kind, wheel in (("standard", name), ("bundle", f"{name}-bundle")):
            artifacts = directory / "artifacts" / kind
            artifacts.mkdir(parents=True)
            (artifacts / f"{wheel}.whl").write_text("wheel", encoding="utf-8")
        (directory / "tags.txt").write_text(f"{name}-v1.0.0\n", encoding="utf-8")
        bodies = directory / "release-bodies"
        bodies.mkdir()
        (bodies / f"{name}-v1.0.0.md").write_text(name, encoding="utf-8")
        return subprocess.CompletedProcess(
            ["bash"],
            7 if name in failures else 0,
            stdout="validation failed\n<bad> 50% | unavailable\n",
        )

    monkeypatch.setattr(publish, "run_package", publish_package)
    directory = tmp_path / "run"
    assert publish.run_packages(selected, directory=directory) == expected_result
    assert calls == expected_calls
    successful = set(calls) - failures
    assert {path.stem for path in (directory / "release-bodies").glob("*.md")} == {
        f"{name}-v1.0.0" for name in successful
    }
    outputs = output.read_text(encoding="utf-8")
    for name in successful:
        assert f"{name}-v1.0.0" in outputs
    for name in failures:
        assert f"{name}-v1.0.0" not in outputs
    summary_text = summary.read_text(encoding="utf-8")
    console = capsys.readouterr().out
    for name in failures:
        assert f"{name}: failed (exit 7)" in summary_text
        assert "<pre>validation failed\n&lt;bad&gt; 50% | unavailable</pre>" in summary_text
        assert "<code>&lt;bad&gt; 50% &#124; unavailable</code>" in summary_text
        assert f"::error title=Publish {name}::" in console
        assert "%0A&lt;bad&gt;" not in console
        assert "%0A<bad> 50%25 | unavailable" in console
    if "app" in selected and failures:
        assert "| app | blocked | Blocked by" in summary_text


@pytest.mark.skipif(sys.platform == "win32", reason="publishing runs on POSIX runners")
def test_package_logs_stream_live_with_a_bounded_failure_tail(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "publish-package.sh").write_text(
        "set -eu\n"
        'printf "env=%s,%s,%s,%s,%s\\n" "$PACKAGE" "$PUBLISH_DIR" "$DRY_RUN" '
        '"$PUBLISH_SHARED" "$VERCEL_INTERNAL_SHARED_VENDORED_DEPS_VERSION"\n'
        'for i in {0..60}; do printf "line %s\\n" "$i"; done\n'
        'printf "error: rejected upload\\n" >&2\n'
        "exit 7\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(publish, "ROOT", tmp_path)
    monkeypatch.setenv("DRY_RUN", "true")
    monkeypatch.setenv("PUBLISH_SHARED", "false")
    monkeypatch.setenv(bundle_release.SHARED_VERSION_ENV, "0.8.2")
    result = publish.run_package("pkg", directory=tmp_path)
    console = capsys.readouterr().out
    assert result.returncode == 7
    assert f"env=pkg,{tmp_path},true,false,0.8.2\n" in console
    assert "line 0\n" in console
    assert "line 0\n" not in result.stdout
    assert len(result.stdout.splitlines()) == 40
    assert len(result.stdout) <= 8000
    assert result.stdout.endswith("error: rejected upload\n")
    assert console.startswith("::group::Publish pkg\n")
    assert console.endswith("error: rejected upload\n::endgroup::\n")


def test_process_start_failure_is_reported_in_publish_results(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(workspace, "packages", lambda: {})
    monkeypatch.setattr(publish, "dependency_graph", lambda _: {"pkg": set()})
    summary = tmp_path / "summary"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))

    def fail_to_start(*args: object, **kwargs: object) -> None:
        raise FileNotFoundError("bash is unavailable")

    monkeypatch.setattr(publish.subprocess, "Popen", fail_to_start)
    assert publish.run_packages(["pkg"], directory=tmp_path / "run") == 1

    summary_text = summary.read_text(encoding="utf-8")
    assert "| pkg | failed, exit 1 |" in summary_text
    assert "<summary>pkg: failed (exit 1)</summary>" in summary_text
    assert "bash is unavailable" in summary_text
    console = capsys.readouterr().out
    assert "::error title=Publish pkg::pkg: failed (exit 1)" in console
    assert console.index("::endgroup::") < console.index("::error title=Publish pkg::")


@pytest.mark.parametrize("packages_json", ["{}", "null", '"pkg"', "[1]"])
def test_run_rejects_malformed_package_selection(
    monkeypatch: pytest.MonkeyPatch, packages_json: str
) -> None:
    monkeypatch.setenv("PACKAGES_JSON", packages_json)
    with pytest.raises(ValueError, match="PACKAGES_JSON must be an array"):
        publish.main(["run"])


def test_publish_rejects_unknown_packages_before_running(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(workspace, "packages", lambda: {})
    with pytest.raises(ValueError, match="Unknown publish packages: typo"):
        publish.run_packages(["typo"], directory=tmp_path)


def test_publish_does_not_continue_after_interruption(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(workspace, "packages", lambda: {})
    monkeypatch.setattr(publish, "dependency_graph", lambda _: {"first": set(), "second": set()})
    calls = []

    def interrupted(*args: object, **kwargs: object) -> None:
        calls.append(True)
        raise KeyboardInterrupt

    monkeypatch.setattr(publish, "run_package", interrupted)
    with pytest.raises(KeyboardInterrupt):
        publish.run_packages(["first", "second"], directory=tmp_path)
    assert len(calls) == 1


def test_ancestors_keep_independent_branches_separate() -> None:
    graph = {"base": set(), "left": {"base"}, "right": {"base"}, "app": {"left"}}
    assert publish.ancestors(graph) == {
        "base": (),
        "left": ("base",),
        "right": ("base",),
        "app": ("base", "left"),
    }
    assert publish.ancestors({}) == {}


def test_ancestors_reject_cycles_and_unknown_dependencies() -> None:
    with pytest.raises(CycleError):
        publish.ancestors({"a": {"b"}, "b": {"a"}})
    with pytest.raises(ValueError, match="Unknown publish dependencies: missing"):
        publish.ancestors({"app": {"missing"}})


def test_shared_dependency_only_gates_its_consumers() -> None:
    graph = publish.dependency_graph(workspace.packages())
    shared = publish.SHARED
    assert not graph[shared]
    assert not graph["vercel-env"]
    assert not graph["vercel-headers"]
    assert shared in graph["vercel-internal-core"]
    assert shared in graph["vercel-oidc"]
    assert "vercel-oidc" in graph["vercel-connect"]
    dependencies = publish.ancestors(graph)
    for package in ("vercel-sandbox", "vercel-connect"):
        assert "vercel-queue" not in dependencies[package]
    for package in ("vercel-workflow", "vercel-celery", "vercel"):
        assert "vercel-queue" in dependencies[package]


@pytest.mark.parametrize(
    ("selected", "shared_changed", "force", "expected", "publish_shared"),
    [
        ([], False, False, [], "false"),
        ([], True, False, [publish.SHARED], "true"),
        (["vercel-env"], False, False, [publish.SHARED, "vercel-env"], "false"),
        (["vercel-env"], True, False, [publish.SHARED, "vercel-env"], "true"),
        (["vercel-env"], False, True, [publish.SHARED, "vercel-env"], "true"),
    ],
)
def test_detection_freezes_one_shared_release_decision(
    monkeypatch: pytest.MonkeyPatch,
    selected: list[str],
    shared_changed: bool,
    force: bool,
    expected: list[str],
    publish_shared: str,
) -> None:
    lookups = []
    bases = []

    def shared_release() -> tuple[str, bool]:
        lookups.append(True)
        return "0.8.2", shared_changed

    def changed_packages(*, base: str, head: str) -> list[str]:
        bases.append((base, head))
        return selected

    monkeypatch.setattr(bundle_release, "shared_vendored_release", shared_release)
    monkeypatch.setattr(release, "changed_packages", changed_packages)
    monkeypatch.setattr(release, "publishable_packages", lambda: selected)
    result = publish.detect(base="0" * 40, force=force)
    assert json.loads(result["packages"]) == expected
    assert result["shared-version"] == "0.8.2"
    assert result["publish-shared"] == publish_shared
    assert len(lookups) == 1
    assert bases == ([] if force else [("HEAD^", "HEAD")])


def test_shared_lookup_failure_preserves_independent_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(release, "changed_packages", lambda **_: ["vercel-env"])

    def unavailable() -> tuple[str, bool]:
        raise OSError("PyPI unavailable")

    monkeypatch.setattr(bundle_release, "shared_vendored_release", unavailable)
    result = publish.detect(base="HEAD^", force=False)
    assert json.loads(result["packages"]) == [publish.SHARED, "vercel-env"]
    assert result["shared-version"] == ""
    assert result["publish-shared"] == "false"
    assert "PyPI unavailable" in result["shared-error"]


def test_shared_lookup_failure_blocks_consumers_not_independent_packages(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    graph = {publish.SHARED: set(), "consumer": {publish.SHARED}, "independent": set()}
    monkeypatch.setattr(workspace, "packages", lambda: {})
    monkeypatch.setattr(publish, "dependency_graph", lambda _: graph)
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "outputs"))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "summary"))
    calls = []

    def run_package(name: str, *, directory: Path) -> subprocess.CompletedProcess[str]:
        calls.append(name)
        (directory / "tags.txt").write_text("", encoding="utf-8")
        return subprocess.CompletedProcess(["bash"], 0, stdout="")

    monkeypatch.setattr(publish, "run_package", run_package)
    assert (
        publish.run_packages(
            list(graph), directory=tmp_path / "run", shared_error="PyPI unavailable"
        )
        == 1
    )
    assert calls == ["independent"]
    summary = (tmp_path / "summary").read_text(encoding="utf-8")
    assert "PyPI unavailable" in summary
    assert f"| consumer | blocked | Blocked by {publish.SHARED}" in summary


def test_missing_package_outputs_do_not_abort_other_packages(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    graph: dict[str, set[str]] = {"a-good": set(), "b-bad": set(), "c-good": set()}
    monkeypatch.setattr(workspace, "packages", lambda: {})
    monkeypatch.setattr(publish, "dependency_graph", lambda _: graph)
    output = tmp_path / "outputs"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "summary"))
    calls = []

    def run_package(name: str, *, directory: Path) -> subprocess.CompletedProcess[str]:
        calls.append(name)
        if name != "b-bad":
            (directory / "tags.txt").write_text(f"{name}-v1.0.0\n", encoding="utf-8")
            bodies = directory / "release-bodies"
            bodies.mkdir()
            (bodies / f"{name}-v1.0.0.md").write_text(name, encoding="utf-8")
        return subprocess.CompletedProcess(["bash"], 0, stdout="")

    monkeypatch.setattr(publish, "run_package", run_package)
    assert publish.run_packages(list(graph), directory=tmp_path / "run") == 1
    assert calls == list(graph)
    outputs = output.read_text(encoding="utf-8")
    assert "a-good-v1.0.0" in outputs
    assert "c-good-v1.0.0" in outputs
    assert "b-bad-v" not in outputs
    assert "tags.txt" in (tmp_path / "summary").read_text(encoding="utf-8")


def test_detect_writes_github_outputs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    output = tmp_path / "outputs"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setenv("FORCE", "true")
    monkeypatch.setattr(release, "publishable_packages", lambda: ["vercel-env"])
    monkeypatch.setattr(bundle_release, "shared_vendored_release", lambda: ("0.8.2", False))
    assert publish.main(["detect"]) == 0
    assert output.read_text(encoding="utf-8").splitlines() == [
        f'packages=["{publish.SHARED}", "vercel-env"]',
        "shared-version=0.8.2",
        "publish-shared=true",
    ]


@pytest.mark.parametrize(
    ("previous", "fingerprint", "expected"),
    [
        (None, None, ("0.1.0", True)),
        ("0.8.1", "same", ("0.8.1", False)),
        ("0.8.1", "old", ("0.8.2", True)),
    ],
)
def test_shared_release_uses_one_registry_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    previous: str | None,
    fingerprint: str | None,
    expected: tuple[str, bool],
) -> None:
    lookups = []

    def latest(package: str) -> str | None:
        lookups.append(package)
        return previous

    monkeypatch.setattr(bundle_release, "_latest_pypi_release", latest)
    monkeypatch.setattr(bundle_release, "_pypi_shared_deps_fingerprint", lambda _: fingerprint)
    monkeypatch.setattr(bundle_release, "_shared_deps_fingerprint", lambda: "same")
    assert bundle_release.shared_vendored_release() == expected
    assert lookups == [publish.SHARED]
