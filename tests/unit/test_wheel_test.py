from __future__ import annotations

import subprocess
import zipfile
from pathlib import Path

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    import tomli as tomllib

from scripts import bundle_release, wheel_test, workspace


def _write_wheel(
    directory: Path,
    distribution: str,
    *,
    files: tuple[str, ...] = (),
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
        for name in files:
            archive.writestr(name, "")
        if scripts:
            entry_points = [
                "[console_scripts]",
                *(f"{name} = {target}" for name, target in scripts),
                "",
            ]
            archive.writestr(f"{dist_info}/entry_points.txt", "\n".join(entry_points))
    return path


def _write_wheel_from_generated_package(package_root: Path, out_dir: Path) -> Path:
    with package_root.joinpath("pyproject.toml").open("rb") as fp:
        data = tomllib.load(fp)
    project = data["project"]
    hatch = data["tool"]["hatch"]
    version_path = package_root / hatch["version"]["path"]
    version = workspace.read_version(version_path)
    include_paths = hatch["build"]["targets"]["wheel"]["only-include"]
    files: list[str] = []
    for include in include_paths:
        source = package_root / include.strip("/")
        candidates = (source,) if source.is_file() else source.rglob("*")
        files.extend(
            path.relative_to(package_root).as_posix()
            for path in candidates
            if path.is_file() and "__pycache__" not in path.parts
        )
    return _write_wheel(
        out_dir,
        project["name"],
        files=tuple(sorted(files)),
        requirements=tuple(data["tool"]["vercel"]["release"]["dependencies"]["dependencies"]),
        scripts=tuple(sorted(project.get("scripts", {}).items())),
        version=version,
    )


def _build_generated_bundle_artifacts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> dict[str, wheel_test.WheelArtifact]:
    out_dir = tmp_path / "dist"
    work_dir = tmp_path / "build"
    monkeypatch.setattr(bundle_release, "shared_vendored_version", lambda: "1.0.0")
    monkeypatch.setattr(
        bundle_release,
        "_preserve_vendored_licenses",
        lambda *_args, **_kwargs: None,
    )

    def fake_run(command: list[str], *, cwd: Path) -> None:
        if command[:2] == ["uv", "build"] and "--out-dir" in command:
            _write_wheel_from_generated_package(
                Path(command[2]), Path(command[command.index("--out-dir") + 1])
            )

    monkeypatch.setattr(bundle_release, "_run", fake_run)
    packages = {
        "vercel-headers": workspace.Package(
            "vercel-headers",
            bundle_release.ROOT / "src/vercel-headers",
            bundle_release.ROOT / "src/vercel-headers/vercel/headers/version.py",
            (),
        ),
        "vercel-internal-core": workspace.Package(
            "vercel-internal-core",
            bundle_release.ROOT / "src/vercel-internal-core",
            bundle_release.ROOT / "src/vercel-internal-core/vercel/internal/core/version.py",
            (),
        ),
        "vercel-oidc": workspace.Package(
            "vercel-oidc",
            bundle_release.ROOT / "src/vercel-oidc",
            bundle_release.ROOT / "src/vercel-oidc/vercel/oidc/version.py",
            ("vercel-headers",),
        ),
        "vercel-sandbox": workspace.Package(
            "vercel-sandbox",
            bundle_release.ROOT / "src/vercel-sandbox",
            bundle_release.ROOT / "src/vercel-sandbox/vercel/sandbox/version.py",
            ("vercel-internal-core", "vercel-oidc"),
        ),
    }
    monkeypatch.setattr(workspace, "packages", lambda: packages)
    bundle_paths = [
        bundle_release.build_bundle_package(name, out_dir=out_dir, work_dir=work_dir)
        for name in wheel_test.dependency_closure(["vercel-sandbox"], packages)
    ]
    shared_path = _write_wheel(
        out_dir,
        bundle_release.SHARED_VENDORED_PACKAGE,
        files=("vercel/internal/_vendor/__init__.py",),
    )
    artifacts = [wheel_test.WheelArtifact.load(path) for path in [*bundle_paths, shared_path]]
    return {artifact.normalized_distribution: artifact for artifact in artifacts}


def _package(tmp_path: Path, name: str, dependencies: tuple[str, ...] = ()) -> workspace.Package:
    path = tmp_path / name
    return workspace.Package(name, path, path / "version.py", dependencies)


def _bundle_workspace_packages(tmp_path: Path) -> dict[str, workspace.Package]:
    return {
        name: _package(tmp_path, name)
        for name in (
            "vercel-headers",
            "vercel-internal-core",
            "vercel-oidc",
            "vercel-sandbox",
        )
    }


def test_core_and_sandbox_bundles_compose_without_overlap(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    generated = _build_generated_bundle_artifacts(monkeypatch, tmp_path)
    core = generated["vercel-internal-core-bundle"]
    sandbox = generated["vercel-sandbox-bundle"]

    artifacts = list(generated.values())
    wheel_test.assert_unique_ownership(artifacts)
    wheel_test.assert_unique_ownership(artifacts, split_paths_only=True)
    assert core.distribution == "vercel-internal-core-bundle"
    assert {"vercel/__init__.py", "vercel/py.typed"} <= core.first_party_files
    assert "vercel/internal/core/session.py" in core.first_party_files
    assert sandbox.distribution == "vercel-sandbox-bundle"
    assert "vercel/sandbox/_internal/service.py" in sandbox.first_party_files
    assert "vercel/headers/__init__.py" in generated["vercel-headers-bundle"].first_party_files
    assert "vercel/oidc/__init__.py" in generated["vercel-oidc-bundle"].first_party_files
    pydantic = [
        requirement for requirement in sandbox.requirements if requirement.name == "pydantic"
    ]
    assert len(pydantic) == 1
    assert any(spec.operator in {"<", "<="} for spec in pydantic[0].specifier)
    assert any(spec.operator in {">", ">="} for spec in pydantic[0].specifier)
    assert {
        artifact.distribution
        for artifact in wheel_test.local_dependency_artifacts(
            [sandbox],
            artifacts,
        )
    } == {
        "vercel-internal-core-bundle",
        "vercel-internal-shared-vendored-deps",
        "vercel-headers-bundle",
        "vercel-oidc-bundle",
        "vercel-sandbox-bundle",
    }
    assert dict(sandbox.console_scripts) == {
        "sandbox": "vercel.sandbox.__main__:main",
        "vercel-sandbox": "vercel.sandbox.__main__:main",
    }


@pytest.mark.parametrize(
    ("overlap", "split_paths_only"),
    [
        ("vercel/internal/core/session.py", True),
        ("vercel/queue/models.py", False),
    ],
)
def test_wheel_ownership_rejects_overlapping_first_party_files(
    tmp_path: Path, overlap: str, split_paths_only: bool
) -> None:
    artifacts = [
        wheel_test.WheelArtifact.load(_write_wheel(tmp_path, distribution, files=(overlap,)))
        for distribution in ("vercel-first", "vercel-second")
    ]

    with pytest.raises(wheel_test.WheelOwnershipError, match=overlap.replace(".", r"\.")):
        wheel_test.assert_unique_ownership(
            artifacts,
            split_paths_only=split_paths_only,
        )


def test_sandbox_bundle_runtime_smoke_uses_declared_artifact_dependencies(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    target = _write_wheel(
        tmp_path,
        "vercel-sandbox-bundle",
        files=("vercel/sandbox/__init__.py",),
        requirements=(
            "vercel-internal-core-bundle>=0.1.0,<0.2.0",
            "vercel-oidc-bundle>=0.7.0",
            "pydantic>=2.7.0,<3",
            "vercel-internal-shared-vendored-deps>=1.0.0",
        ),
    )
    core = _write_wheel(
        tmp_path,
        "vercel-internal-core-bundle",
        requirements=("vercel-internal-shared-vendored-deps>=1.0.0",),
    )
    oidc = _write_wheel(
        tmp_path,
        "vercel-oidc-bundle",
        requirements=(
            "vercel-headers-bundle>=0.7.0",
            "vercel-internal-shared-vendored-deps>=1.0.0",
        ),
    )
    headers = _write_wheel(
        tmp_path,
        "vercel-headers-bundle",
        requirements=("vercel-internal-shared-vendored-deps>=1.0.0",),
    )
    shared = _write_wheel(tmp_path, "vercel-internal-shared-vendored-deps")
    calls: list[tuple[list[str], Path]] = []

    def fake_run(command: list[str], *, cwd: Path) -> None:
        calls.append((command, cwd))

    monkeypatch.setattr(bundle_release, "_run", fake_run)
    monkeypatch.setattr(
        wheel_test.workspace,
        "packages",
        lambda: _bundle_workspace_packages(tmp_path),
    )

    bundle_release._test_sandbox_bundle_runtime(target, dist_dir=tmp_path)  # noqa: SLF001

    assert len(calls) == 1
    command, cwd = calls[0]
    assert cwd != bundle_release.ROOT
    assert bundle_release.ROOT not in cwd.parents
    assert command[:5] == [
        "uv",
        "run",
        "--no-cache",
        "--isolated",
        "--no-project",
    ]
    assert {str(path.resolve()) for path in (target, core, oidc, headers, shared)} <= set(command)
    assert command[-4:-2] == ["python", "-I"]
    smoke = command[-1]
    assert 'version("vercel-sandbox-bundle")' in smoke
    assert "import pydantic" in smoke
    assert "from vercel.sandbox import sync" in smoke


@pytest.mark.parametrize(
    ("requirements", "files", "message"),
    [
        ((), (), "declare exactly one active Pydantic dependency"),
        (("pydantic>=2.7.0",), (), "declare bounded Pydantic metadata"),
        (
            ("pydantic>=2.7.0,<3",),
            ("vercel/sandbox/_vendor/pydantic/__init__.py",),
            "must not vendor Pydantic",
        ),
    ],
)
def test_sandbox_bundle_runtime_smoke_rejects_incomplete_artifacts(
    tmp_path: Path,
    requirements: tuple[str, ...],
    files: tuple[str, ...],
    message: str,
) -> None:
    target = _write_wheel(
        tmp_path,
        "vercel-sandbox-bundle",
        files=files,
        requirements=requirements,
    )

    with pytest.raises(SystemExit, match=message):
        bundle_release._test_sandbox_bundle_runtime(target, dist_dir=tmp_path)  # noqa: SLF001


def test_installed_test_command_uses_only_artifact_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    package = _package(tmp_path, "vercel-sandbox")
    package.path.mkdir()
    package.path.joinpath("tests").mkdir()
    package.path.joinpath("tests/test_smoke.py").write_text(
        "def test_smoke(): pass\n", encoding="utf-8"
    )
    package.path.joinpath("pyproject.toml").write_text("", encoding="utf-8")
    target_path = _write_wheel(tmp_path, "vercel-sandbox")
    calls: list[tuple[list[str], Path, dict[str, str]]] = []

    def fake_check_call(
        command: list[str], *, cwd: Path, env: dict[str, str]
    ) -> subprocess.CompletedProcess[bytes]:
        calls.append((command, cwd, env))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(wheel_test.workspace, "packages", lambda: {package.name: package})
    monkeypatch.setattr(wheel_test, "workspace_distribution_names", lambda: frozenset())
    monkeypatch.setattr(wheel_test, "_root_dev_requirements", lambda: ("pytest",))
    monkeypatch.setattr(wheel_test.subprocess, "check_call", fake_check_call)
    monkeypatch.setenv("PYTHONPATH", str(wheel_test.ROOT / "src"))

    wheel_test.run_installed_tests(
        package.name,
        wheel=target_path,
        dist_dir=tmp_path,
    )

    assert len(calls) == 1
    command, cwd, environment = calls[0]
    assert cwd != wheel_test.ROOT
    assert wheel_test.ROOT not in cwd.parents
    assert "--no-project" in command
    assert str(target_path.resolve()) in command
    assert "PYTHONPATH" not in environment
    assert not (cwd / "vercel").exists()
