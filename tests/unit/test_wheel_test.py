from __future__ import annotations

import subprocess
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import bundle_release, wheel_test, workspace


def _write_wheel(
    directory: Path,
    distribution: str,
    *,
    files: tuple[str, ...] = (),
    file_contents: dict[str, str] | None = None,
    requirements: tuple[str, ...] = (),
    scripts: tuple[tuple[str, str], ...] = (),
    version: str = "1.0.0",
) -> Path:
    normalized = distribution.replace("-", "_")
    path = directory / f"{normalized}-{version}-py3-none-any.whl"
    dist_info = f"{normalized}-{version}.dist-info"
    metadata = [
        "Metadata-Version: 2.4",
        f"Name: {distribution}",
        f"Version: {version}",
        *(f"Requires-Dist: {requirement}" for requirement in requirements),
        "",
    ]
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(f"{dist_info}/METADATA", "\n".join(metadata))
        archive.writestr(f"{dist_info}/RECORD", "")
        contents = file_contents or {}
        for name in files:
            archive.writestr(name, contents.get(name, ""))
        for name, content in contents.items():
            if name not in files:
                archive.writestr(name, content)
        if scripts:
            entry_points = [
                "[console_scripts]",
                *(f"{name} = {target}" for name, target in scripts),
                "",
            ]
            archive.writestr(f"{dist_info}/entry_points.txt", "\n".join(entry_points))
    return path


def _package(
    tmp_path: Path,
    name: str,
    *,
    dependencies: tuple[str, ...] = (),
) -> workspace.Package:
    path = tmp_path / name
    return workspace.Package(
        name,
        path,
        path / "version.py",
        dependencies,
    )


def _package_with_tests(
    tmp_path: Path,
    name: str,
    test_source: str = "def test_smoke(): pass\n",
) -> workspace.Package:
    package = _package(tmp_path, name)
    package.path.joinpath("tests").mkdir(parents=True)
    package.path.joinpath("tests/test_smoke.py").write_text(
        test_source,
        encoding="utf-8",
    )
    return package


def test_wheel_artifact_loads_metadata_and_console_scripts(
    tmp_path: Path,
) -> None:
    path = _write_wheel(
        tmp_path,
        "vercel-demo",
        files=("vercel/demo/__init__.py",),
        requirements=("httpx>=0.27",),
        scripts=(("vercel-demo", "vercel.demo:main"),),
    )

    artifact = wheel_test.WheelArtifact.load(path)

    assert artifact.distribution == "vercel-demo"
    assert artifact.version == "1.0.0"
    assert artifact.normalized_distribution == "vercel-demo"
    assert artifact.first_party_files == frozenset({"vercel/demo/__init__.py"})
    assert tuple(map(str, artifact.requirements)) == ("httpx>=0.27",)
    assert dict(artifact.console_scripts) == {"vercel-demo": "vercel.demo:main"}


def test_wheel_artifact_rejects_unreadable_archive(tmp_path: Path) -> None:
    path = tmp_path / "broken.whl"
    path.write_text("not a zip file", encoding="utf-8")

    with pytest.raises(wheel_test.WheelTestError, match="could not read wheel"):
        wheel_test.WheelArtifact.load(path)


def test_wheel_artifact_rejects_missing_metadata(tmp_path: Path) -> None:
    path = tmp_path / "missing-metadata.whl"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("vercel/demo.py", "")

    with pytest.raises(
        wheel_test.WheelTestError,
        match=r"expected one \.dist-info/METADATA member.*found 0",
    ):
        wheel_test.WheelArtifact.load(path)


def test_wheel_artifact_rejects_invalid_entry_points(
    tmp_path: Path,
) -> None:
    path = _write_wheel(tmp_path, "vercel-demo")
    with zipfile.ZipFile(path, "a") as archive:
        archive.writestr(
            "vercel_demo-1.0.0.dist-info/entry_points.txt",
            b"\xff",
        )

    with pytest.raises(
        wheel_test.WheelTestError,
        match=r"invalid entry_points\.txt",
    ):
        wheel_test.WheelArtifact.load(path)


def test_wheel_artifact_rejects_duplicate_members(tmp_path: Path) -> None:
    with pytest.warns(UserWarning, match="Duplicate name"):
        path = _write_wheel(
            tmp_path,
            "vercel-demo",
            files=("vercel/demo.py", "vercel/demo.py"),
        )

    with pytest.raises(wheel_test.WheelTestError, match="duplicate members") as error:
        wheel_test.WheelArtifact.load(path)
    assert "vercel/demo.py" in str(error.value)


@pytest.mark.parametrize(
    "member",
    (
        "vercel/queue/_vendor/vercel/",
        "vercel/queue/_vendor/vercel/cache/__init__.py",
    ),
)
def test_wheel_artifact_rejects_nested_vendored_first_party_member(
    tmp_path: Path,
    member: str,
) -> None:
    path = _write_wheel(
        tmp_path,
        "vercel-queue-bundle",
        files=(member,),
    )

    with pytest.raises(
        wheel_test.WheelTestError,
        match="installed side-by-side as bundle dependencies",
    ):
        wheel_test.WheelArtifact.load(path)


def test_wheel_ownership_rejects_overlapping_first_party_files(
    tmp_path: Path,
) -> None:
    overlap = "vercel/internal/core/session.py"
    artifacts = [
        wheel_test.WheelArtifact.load(_write_wheel(tmp_path, distribution, files=(overlap,)))
        for distribution in ("vercel-first", "vercel-second")
    ]

    with pytest.raises(
        wheel_test.WheelOwnershipError,
        match=overlap.replace(".", r"\."),
    ):
        wheel_test.assert_unique_ownership(artifacts)


def test_namespace_package_portions_compose_without_overlap(
    tmp_path: Path,
) -> None:
    artifacts = [
        wheel_test.WheelArtifact.load(
            _write_wheel(
                tmp_path,
                "vercel-internal-core",
                files=(
                    "vercel/api/__init__.py",
                    "vercel/internal/core/__init__.py",
                ),
            )
        ),
        wheel_test.WheelArtifact.load(
            _write_wheel(
                tmp_path,
                "vercel-sandbox",
                files=("vercel/sandbox/__init__.py",),
            )
        ),
    ]

    wheel_test.assert_unique_ownership(artifacts)


def test_local_dependency_artifacts_uses_metadata_closure_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = wheel_test.WheelArtifact.load(
        _write_wheel(
            tmp_path,
            "vercel-target-bundle",
            requirements=("vercel-dependency-bundle>=1",),
        )
    )
    dependency = wheel_test.WheelArtifact.load(
        _write_wheel(
            tmp_path,
            "vercel-dependency-bundle",
            requirements=("vercel-internal-shared-vendored-deps>=1",),
        )
    )
    shared = wheel_test.WheelArtifact.load(
        _write_wheel(
            tmp_path,
            wheel_test.SHARED_BUNDLE_DISTRIBUTION,
        )
    )
    unrelated = wheel_test.WheelArtifact.load(_write_wheel(tmp_path, "vercel-unrelated-bundle"))
    packages = {
        name: _package(tmp_path, name)
        for name in (
            "vercel-target",
            "vercel-dependency",
            "vercel-unrelated",
        )
    }
    monkeypatch.setattr(wheel_test.workspace, "packages", lambda: packages)

    selected = wheel_test.local_dependency_artifacts(
        [target],
        [target, dependency, shared, unrelated],
    )

    assert {artifact.distribution for artifact in selected} == {
        "vercel-target-bundle",
        "vercel-dependency-bundle",
        wheel_test.SHARED_BUNDLE_DISTRIBUTION,
    }


def test_local_dependency_artifacts_rejects_missing_workspace_wheel(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = wheel_test.WheelArtifact.load(
        _write_wheel(
            tmp_path,
            "vercel-target",
            requirements=("vercel-dependency>=1",),
        )
    )
    packages = {name: _package(tmp_path, name) for name in ("vercel-target", "vercel-dependency")}
    monkeypatch.setattr(wheel_test.workspace, "packages", lambda: packages)

    with pytest.raises(
        wheel_test.WheelTestError,
        match="vercel-dependency.*absent from the artifact directory",
    ):
        wheel_test.local_dependency_artifacts([target], [target])
    assert wheel_test.local_dependency_artifacts(
        [target],
        [target],
        require_all_workspace_dependencies=False,
    ) == (target,)


def test_local_dependency_artifacts_ignores_inactive_requirement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = wheel_test.WheelArtifact.load(
        _write_wheel(
            tmp_path,
            "vercel-target",
            requirements=('vercel-dependency>=1; python_version < "2"',),
        )
    )
    packages = {name: _package(tmp_path, name) for name in ("vercel-target", "vercel-dependency")}
    monkeypatch.setattr(wheel_test.workspace, "packages", lambda: packages)

    assert wheel_test.local_dependency_artifacts([target], [target]) == (target,)


def test_rewrite_bundle_test_imports_aligns_shared_libraries(
    tmp_path: Path,
) -> None:
    tests = tmp_path / "tests"
    tests.mkdir()
    path = tests / "test_imports.py"
    path.write_text(
        "import anyio\n"
        "import anyio.lowlevel\n"
        "import httpx as client\n"
        "from anyio import to_thread\n"
        "from httpx._types import HeaderTypes\n"
        "import respx\n",
        encoding="utf-8",
    )

    wheel_test.rewrite_bundle_test_imports(tmp_path)

    rewritten = path.read_text(encoding="utf-8")
    assert "from vercel.internal._vendor import anyio\n" in rewritten
    assert "from vercel.internal._vendor.anyio import lowlevel\n" in rewritten
    assert "from vercel.internal._vendor import httpx as client\n" in rewritten
    assert "from vercel.internal._vendor.anyio import to_thread\n" in rewritten
    assert "from vercel.internal._vendor.httpx._types import HeaderTypes\n" in rewritten
    assert "import respx\n" in rewritten


def test_vendored_library_detection_requires_an_import(
    tmp_path: Path,
) -> None:
    comment_only = wheel_test.WheelArtifact.load(
        _write_wheel(
            tmp_path,
            "vercel-target-bundle",
            files=("vercel/target/client.py",),
            file_contents={
                "vercel/target/client.py": (
                    '"""from vercel.internal._vendor import httpx"""\n'
                    "# vercel.internal._vendor.httpx\n"
                )
            },
        )
    )

    assert not wheel_test.artifact_uses_shared_vendored_library(
        comment_only,
        "httpx",
    )

    imported = wheel_test.WheelArtifact.load(
        _write_wheel(
            tmp_path,
            "vercel-target-bundle",
            version="2.0.0",
            files=("vercel/target/client.py",),
            file_contents={
                "vercel/target/client.py": ("from vercel.internal._vendor import httpx\n")
            },
        )
    )

    assert wheel_test.artifact_uses_shared_vendored_library(
        imported,
        "httpx",
    )


def test_pytest_deselections_target_exact_respx_nodes(
    tmp_path: Path,
) -> None:
    tests = tmp_path / "tests"
    tests.mkdir()
    dependent = tests / "test_dependent.py"
    dependent.write_text(
        "import respx\n\n"
        "def test_respx_case():\n"
        "    with respx.mock:\n"
        "        pass\n\n"
        "class TestRouter:\n"
        "    @respx.mock\n"
        "    def test_route(self):\n"
        "        pass\n\n"
        "    def test_safe(self):\n"
        '        """A string mentioning respx is harmless."""\n'
        "        pass\n\n"
        "def test_duplicate_name():\n"
        "    with respx.mock:\n"
        "        pass\n\n"
        "def test_string_only():\n"
        '    assert "respx" == "respx"\n',
        encoding="utf-8",
    )
    safe = tests / "test_safe.py"
    safe.write_text(
        "def test_duplicate_name():\n    # A comment mentioning respx is harmless.\n    pass\n",
        encoding="utf-8",
    )

    deselected = wheel_test.pytest_deselections_for_vendored_httpx(
        [dependent, safe],
        test_root=tmp_path,
    )

    assert deselected == (
        "tests/test_dependent.py::TestRouter::test_route",
        "tests/test_dependent.py::test_duplicate_name",
        "tests/test_dependent.py::test_respx_case",
    )


def test_source_only_test_files_are_excluded(
    tmp_path: Path,
) -> None:
    package = _package(tmp_path, "vercel-demo")
    package.path.joinpath("tests").mkdir(parents=True)
    package.path.joinpath("tests/test_shipped_support.py").write_text(
        "import shipped_support\n\ndef test_shipped_support(): pass\n",
        encoding="utf-8",
    )
    package.path.joinpath("tests/test_source_support.py").write_text(
        "import source_support\n\ndef test_source_support(): pass\n",
        encoding="utf-8",
    )
    package.path.joinpath("shipped_support.py").write_text(
        "",
        encoding="utf-8",
    )
    package.path.joinpath("source_support.py").write_text(
        "",
        encoding="utf-8",
    )
    artifact = wheel_test.WheelArtifact.load(
        _write_wheel(
            tmp_path,
            "vercel-demo",
            files=(
                "shipped_support.py",
                "vercel/demo/__init__.py",
            ),
        )
    )
    copied = tmp_path / "copied"
    copied.mkdir()
    assert wheel_test._copy_test_inputs(package, copied)  # noqa: SLF001

    ignored = wheel_test.source_only_test_files(
        copied,
        package,
        [artifact],
    )

    assert tuple(path.name for path in ignored) == ("test_source_support.py",)


def test_artifact_only_distribution_is_validated_without_pytest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    wheel = _write_wheel(
        tmp_path,
        wheel_test.SHARED_BUNDLE_DISTRIBUTION,
        files=("vercel/internal/_vendor/httpx/__init__.py",),
    )
    monkeypatch.setattr(wheel_test.workspace, "packages", lambda: {})

    def unexpected_call(*_args: object, **_kwargs: object) -> None:
        pytest.fail("artifact-only distributions must not invoke pytest")

    monkeypatch.setattr(wheel_test.subprocess, "check_call", unexpected_call)

    wheel_test.run_installed_tests(
        wheel_test.SHARED_BUNDLE_DISTRIBUTION,
        wheel=wheel,
        dist_dir=tmp_path,
    )


def test_installed_tests_reject_mismatched_package_and_wheel(
    tmp_path: Path,
) -> None:
    wheel = _write_wheel(
        tmp_path,
        "vercel-actual",
        files=("vercel/actual/__init__.py",),
    )

    with pytest.raises(
        wheel_test.WheelTestError,
        match=("wheel distribution vercel-actual does not match package vercel-typo"),
    ):
        wheel_test.run_installed_tests(
            "vercel-typo",
            wheel=wheel,
            dist_dir=tmp_path,
        )


def test_installed_tests_resolve_workspace_package_by_normalized_name(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    package = _package_with_tests(tmp_path, "vercel-internal-core")
    wheel = _write_wheel(
        tmp_path,
        package.name,
        files=("vercel/internal/core/__init__.py",),
    )
    calls: list[list[str]] = []

    monkeypatch.setattr(
        wheel_test.workspace,
        "packages",
        lambda: {package.name: package},
    )
    monkeypatch.setattr(
        wheel_test.subprocess,
        "check_call",
        lambda command, **_kwargs: calls.append(command),
    )

    wheel_test.run_installed_tests(
        "Vercel_Internal_Core",
        wheel=wheel,
        dist_dir=tmp_path,
    )

    assert len(calls) == 1


def test_workspace_distribution_without_tests_skips_pytest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    package = _package(tmp_path, "vercel-headers")
    package.path.mkdir()
    wheel = _write_wheel(
        tmp_path,
        package.name,
        files=("vercel/headers/__init__.py",),
    )
    monkeypatch.setattr(
        wheel_test.workspace,
        "packages",
        lambda: {package.name: package},
    )

    def unexpected_call(*_args: object, **_kwargs: object) -> None:
        pytest.fail("packages without tests must not invoke pytest")

    monkeypatch.setattr(wheel_test.subprocess, "check_call", unexpected_call)

    wheel_test.run_installed_tests(
        package.name,
        wheel=wheel,
        dist_dir=tmp_path,
    )


def test_installed_test_command_uses_dependency_closure_and_no_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    package = _package_with_tests(tmp_path, "vercel-target")
    dependency_package = _package(tmp_path, "vercel-dependency")
    unrelated_package = _package(tmp_path, "vercel-unrelated")
    target = _write_wheel(
        tmp_path,
        package.name,
        files=("vercel/target/__init__.py",),
        requirements=("vercel-dependency>=1",),
    )
    dependency = _write_wheel(
        tmp_path,
        dependency_package.name,
        files=("vercel/dependency/__init__.py",),
    )
    unrelated = _write_wheel(
        tmp_path,
        unrelated_package.name,
        files=("vercel/unrelated/__init__.py",),
    )
    packages = {item.name: item for item in (package, dependency_package, unrelated_package)}
    calls: list[tuple[list[str], Path, dict[str, str]]] = []

    def fake_check_call(
        command: list[str],
        *,
        cwd: Path,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[bytes]:
        assert not cwd.joinpath("vercel").exists()
        assert cwd.joinpath("conftest.py").is_file()
        calls.append((command, cwd, env))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(wheel_test.workspace, "packages", lambda: packages)
    monkeypatch.setattr(
        wheel_test,
        "_root_dev_requirements",
        lambda: ("pytest>=7,<10", "pytest-asyncio<2"),
    )
    monkeypatch.setattr(
        wheel_test.subprocess,
        "check_call",
        fake_check_call,
    )
    monkeypatch.setenv("PYTHONPATH", str(wheel_test.ROOT / "src"))

    wheel_test.run_installed_tests(
        package.name,
        wheel=target,
        dist_dir=tmp_path,
    )

    assert len(calls) == 1
    command, cwd, environment = calls[0]
    assert cwd != wheel_test.ROOT
    assert wheel_test.ROOT not in cwd.parents
    assert command[:7] == [
        "uv",
        "run",
        "--no-cache",
        "--isolated",
        "--no-project",
        "--directory",
        str(cwd),
    ]
    assert str(target.resolve()) in command
    assert str(dependency.resolve()) in command
    assert str(unrelated.resolve()) not in command
    assert "PYTHONPATH" not in environment


def test_bundle_installed_tests_rewrite_imports_and_filter_respx(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    package = _package_with_tests(
        tmp_path,
        "vercel-target",
        "import anyio.lowlevel\n"
        "from httpx import Response\n"
        "import respx\n\n"
        "def test_respx_case():\n"
        "    with respx.mock:\n"
        "        Response(200)\n\n"
        "def test_safe_case():\n"
        "    assert anyio.lowlevel is not None\n",
    )
    target = _write_wheel(
        tmp_path,
        "vercel-target-bundle",
        files=(
            "vercel/target/__init__.py",
            "vercel/target/client.py",
        ),
        file_contents={"vercel/target/client.py": ("from vercel.internal._vendor import httpx\n")},
        requirements=(f"{wheel_test.SHARED_BUNDLE_DISTRIBUTION}>=1",),
    )
    shared = _write_wheel(
        tmp_path,
        wheel_test.SHARED_BUNDLE_DISTRIBUTION,
        files=("vercel/internal/_vendor/httpx/__init__.py",),
    )
    captured: dict[str, object] = {}

    def fake_check_call(
        command: list[str],
        *,
        cwd: Path,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[bytes]:
        captured["command"] = command
        captured["test"] = cwd.joinpath("tests/test_smoke.py").read_text(encoding="utf-8")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(
        wheel_test.workspace,
        "packages",
        lambda: {package.name: package},
    )
    monkeypatch.setattr(
        wheel_test,
        "_root_dev_requirements",
        lambda: (
            "pytest>=7,<10",
            "pytest-asyncio<2",
            "respx>=0.21,<1",
        ),
    )
    monkeypatch.setattr(
        wheel_test.subprocess,
        "check_call",
        fake_check_call,
    )

    wheel_test.run_installed_tests(
        package.name,
        wheel=target,
        dist_dir=tmp_path,
    )

    command = captured["command"]
    assert isinstance(command, list)
    rewritten = captured["test"]
    assert isinstance(rewritten, str)
    assert "from vercel.internal._vendor import anyio\n" in rewritten
    assert "from vercel.internal._vendor.anyio import lowlevel\n" in rewritten
    assert "from vercel.internal._vendor.httpx import Response\n" in rewritten
    assert str(target.resolve()) in command
    assert str(shared.resolve()) in command
    assert "--deselect=tests/test_smoke.py::test_respx_case" in command
    assert "-k" not in command


def test_ordinary_installed_tests_do_not_rewrite_imports_or_filter(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = (
        "import httpx\n"
        "import respx\n\n"
        "def test_case():\n"
        "    with respx.mock:\n"
        "        assert httpx.Response(200)\n"
    )
    package = _package_with_tests(
        tmp_path,
        "vercel-target",
        source,
    )
    target = _write_wheel(
        tmp_path,
        package.name,
        files=("vercel/target/__init__.py",),
    )
    captured: dict[str, object] = {}

    def fake_check_call(
        command: list[str],
        *,
        cwd: Path,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[bytes]:
        captured["command"] = command
        captured["test"] = cwd.joinpath("tests/test_smoke.py").read_text(encoding="utf-8")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(
        wheel_test.workspace,
        "packages",
        lambda: {package.name: package},
    )
    monkeypatch.setattr(
        wheel_test,
        "_root_dev_requirements",
        lambda: (
            "pytest>=7,<10",
            "pytest-asyncio<2",
            "respx>=0.21,<1",
        ),
    )
    monkeypatch.setattr(
        wheel_test.subprocess,
        "check_call",
        fake_check_call,
    )

    wheel_test.run_installed_tests(
        package.name,
        wheel=target,
        dist_dir=tmp_path,
    )

    assert captured["test"] == source
    command = captured["command"]
    assert isinstance(command, list)
    assert "-k" not in command
    assert not any(argument.startswith("--deselect=") for argument in command)


def test_test_cli_forwards_pytest_arguments(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    wheel = tmp_path / "target.whl"
    captured: dict[str, object] = {}

    def fake_run_installed_tests(
        package_name: str,
        **kwargs: object,
    ) -> None:
        captured["package"] = package_name
        captured.update(kwargs)

    monkeypatch.setattr(
        wheel_test,
        "run_installed_tests",
        fake_run_installed_tests,
    )

    result = wheel_test.main(
        [
            "test",
            "--package",
            "vercel-target",
            "--wheel",
            str(wheel),
            "--dist-dir",
            str(tmp_path),
            "--test-path",
            "tests/unit",
            "--",
            "-q",
        ]
    )

    assert result == 0
    assert captured == {
        "package": "vercel-target",
        "wheel": wheel,
        "dist_dir": tmp_path,
        "test_paths": ["tests/unit"],
        "pytest_args": ["-q"],
    }


def test_run_cli_builds_and_tests_selected_ordinary_packages(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    def fake_build_and_test(
        package_names: list[str],
        *,
        dist_dir: Path,
        pytest_args: list[str],
    ) -> None:
        captured.update(
            packages=package_names,
            dist_dir=dist_dir,
            pytest_args=pytest_args,
        )

    monkeypatch.setattr(
        wheel_test,
        "build_and_test",
        fake_build_and_test,
    )

    result = wheel_test.main(
        [
            "run",
            "--package",
            "vercel-internal-core",
            "--package",
            "vercel-sandbox",
            "--dist-dir",
            str(tmp_path),
            "--",
            "-q",
        ]
    )

    assert result == 0
    assert captured == {
        "packages": ["vercel-internal-core", "vercel-sandbox"],
        "dist_dir": tmp_path,
        "pytest_args": ["-q"],
    }


def test_bundle_release_test_wheel_uses_python_runner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    wheel = tmp_path / "vercel_target_bundle.whl"
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        bundle_release,
        "load_plan",
        lambda _package: SimpleNamespace(variant_name="vercel-target-bundle"),
    )
    monkeypatch.setattr(
        bundle_release,
        "_single_wheel",
        lambda _dist_dir, _name: wheel,
    )

    def fake_run_installed_tests(
        package_name: str,
        **kwargs: object,
    ) -> None:
        captured["package"] = package_name
        captured.update(kwargs)

    monkeypatch.setattr(
        bundle_release.wheel_test,
        "run_installed_tests",
        fake_run_installed_tests,
    )

    bundle_release.test_wheel("vercel-target", dist_dir=tmp_path)

    assert captured == {
        "package": "vercel-target",
        "wheel": wheel,
        "dist_dir": tmp_path,
        "require_all_workspace_dependencies": False,
    }
