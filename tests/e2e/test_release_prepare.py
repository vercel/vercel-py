from __future__ import annotations

import os
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    import tomli as tomllib  # type: ignore[no-redef]


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests/fixtures/release-workspace"


def run(
    *args: str, cwd: Path, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=True,
    )


def test_prepare_release_in_offline_workspace(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    shutil.copytree(FIXTURE, workspace_root)
    shutil.copytree(ROOT / "scripts", workspace_root / "scripts")

    offline_env = {**os.environ, "UV_OFFLINE": "1"}
    run("uv", "lock", "--offline", cwd=workspace_root, env=offline_env)
    run("git", "init", "--quiet", cwd=workspace_root)
    run("git", "add", ".", cwd=workspace_root)
    run(
        "git",
        "-c",
        "user.name=Release Test",
        "-c",
        "user.email=release-test@example.com",
        "commit",
        "--quiet",
        "-m",
        "Add fixture release fragment (#336)",
        cwd=workspace_root,
    )

    result = run(
        sys.executable,
        "scripts/release.py",
        "prepare",
        cwd=workspace_root,
        env=offline_env,
    )

    core_root = workspace_root / "packages/core"
    app_root = workspace_root / "packages/app"
    new_package_root = workspace_root / "packages/new-package"
    unpublished_dependent_root = workspace_root / "packages/unpublished-dependent"
    assert (core_root / "core_version.py").read_text(encoding="utf-8") == (
        '__version__ = "0.2.0"\n'
    )
    assert (app_root / "app_version.py").read_text(encoding="utf-8") == ('__version__ = "1.0.1"\n')
    assert (new_package_root / "new_package_version.py").read_text(encoding="utf-8") == (
        '__version__ = "0.1.0"\n'
    )
    assert (unpublished_dependent_root / "unpublished_dependent_version.py").read_text(
        encoding="utf-8"
    ) == ('__version__ = "0.0.0"\n')

    with (app_root / "pyproject.toml").open("rb") as file:
        app_pyproject = tomllib.load(file)
    assert app_pyproject["tool"]["vercel"]["release"]["dependencies"]["dependencies"] == [
        'core>=0.1.0,<0.3.0 ; python_version < "3.14"',
        "httpx>=0.27,<1",
    ]

    today = date.today().isoformat()
    core_changelog = (core_root / "CHANGELOG.md").read_text(encoding="utf-8")
    app_changelog = (app_root / "CHANGELOG.md").read_text(encoding="utf-8")
    new_package_changelog = (new_package_root / "CHANGELOG.md").read_text(encoding="utf-8")
    assert f"## 0.2.0 - {today}" in core_changelog
    assert (
        "- Advance core across its compatibility boundary\n"
        "  without corrupting environment markers. (#336)\n"
        "\n"
        "  ```python\n"
        "  # marker quotes remain valid\n"
        '  require("core")\n'
        "  ```"
    ) in core_changelog
    assert (
        "- ```python\n  run()\n  ```\n\n  Call `run()` directly instead of through the shim. (#336)"
    ) in core_changelog
    assert (
        "- Keep the existing bullet. (#336)\n"
        "  - Keep this nested detail.\n"
        "\n"
        "  ## Not a release heading\n"
        "\n"
        "  It remains in the same entry."
    ) in core_changelog
    assert core_changelog.count("#336") == 3
    assert "It remains in the same entry.\n\n## 0.1.9" in core_changelog
    assert f"## 1.0.1 - {today}\n\n- Update dependencies." in app_changelog
    assert "- Update dependencies.\n\n## 1.0.0" in app_changelog
    assert "#336" not in app_changelog.split("## 1.0.0", 1)[0]
    assert f"## 0.1.0 - {today}" in new_package_changelog
    assert "### Internal" in new_package_changelog
    assert "- Prepare the unpublished package for its first release. (#336)" in (
        new_package_changelog
    )
    assert (unpublished_dependent_root / "CHANGELOG.md").read_text(encoding="utf-8") == (
        "# Changelog\n"
    )
    assert not (workspace_root / "changes/core").exists()
    assert not (workspace_root / "changes/new-package").exists()
    assert "core: 0.1.9 -> 0.2.0 (minor)" in result.stdout
    assert "app: 1.0.0 -> 1.0.1 (patch dependency-only)" in result.stdout
    assert "new-package: 0.0.0 -> 0.1.0 (minor)" in result.stdout
    assert "unpublished-dependent" not in result.stdout

    release_body = run(
        sys.executable,
        "scripts/release.py",
        "github-release-body",
        "core",
        cwd=workspace_root,
        env=offline_env,
    ).stdout
    assert "## Not a release heading" in release_body
    assert "Previous core release" not in release_body

    for pyproject_path in workspace_root.glob("**/pyproject.toml"):
        with pyproject_path.open("rb") as file:
            tomllib.load(file)
    run("uv", "lock", "--check", "--offline", cwd=workspace_root, env=offline_env)
