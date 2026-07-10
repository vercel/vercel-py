from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import pytest

from scripts import hatch_build, release, workspace


def test_topological_names_orders_dependencies_before_dependents(tmp_path: Path) -> None:
    packages = {
        "app": workspace.Package("app", tmp_path / "app", tmp_path / "app/version.py", ("lib",)),
        "lib": workspace.Package("lib", tmp_path / "lib", tmp_path / "lib/version.py", ()),
    }

    assert workspace.topological_names(packages) == ["lib", "app"]


def test_compute_releases_cascades_dependency_only_patch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    changes = tmp_path / "changes"
    fragment_dir = changes / "lib"
    fragment_dir.mkdir(parents=True)
    (fragment_dir / "123.feature.md").write_text("Add library feature.\n", encoding="utf-8")

    lib_version = tmp_path / "lib/version.py"
    app_version = tmp_path / "app/version.py"
    lib_version.parent.mkdir()
    app_version.parent.mkdir()
    lib_version.write_text('__version__ = "0.6.0"\n', encoding="utf-8")
    app_version.write_text('__version__ = "1.2.3"\n', encoding="utf-8")

    packages = {
        "lib": workspace.Package("lib", tmp_path / "lib", lib_version, ()),
        "app": workspace.Package("app", tmp_path / "app", app_version, ("lib",)),
    }
    monkeypatch.setattr(release, "CHANGES", changes)
    monkeypatch.setattr(workspace, "packages", lambda: packages)
    monkeypatch.setattr(workspace, "topological_names", lambda _packages: ["lib", "app"])

    releases = release.compute_releases()

    assert [(item.package, item.new_version, item.dependency_only) for item in releases] == [
        ("lib", "0.7.0", False),
        ("app", "1.2.4", True),
    ]


def test_workspace_dependency_hook_rewrites_lower_bound(tmp_path: Path) -> None:
    workspace_root = tmp_path
    package_root = workspace_root / "src/app"
    dependency_root = workspace_root / "src/lib"
    package_root.mkdir(parents=True)
    dependency_root.mkdir(parents=True)

    (workspace_root / "pyproject.toml").write_text(
        "[tool.uv.workspace]\nmembers = ['src/*']\n",
        encoding="utf-8",
    )
    (package_root / "pyproject.toml").write_text(
        """
[project]
name = "app"

[tool.uv.sources]
lib = { workspace = true }

[tool.vercel.release.dependencies]
dependencies = ["lib>=0.1.0,<2 ; python_version >= '3.10'", "httpx>=0.27,<1"]
""".strip(),
        encoding="utf-8",
    )
    (dependency_root / "pyproject.toml").write_text(
        """
[project]
name = "lib"

[tool.hatch.version]
path = "version.py"
""".strip(),
        encoding="utf-8",
    )
    (dependency_root / "version.py").write_text('__version__ = "0.8.0"\n', encoding="utf-8")

    hook = hatch_build.WorkspaceDependenciesMetadataHook(str(package_root), {})
    metadata: dict[str, object] = {}

    hook.update(metadata)

    assert metadata["dependencies"] == [
        'lib>=0.8.0,<2 ; python_version >= "3.10"',
        "httpx>=0.27,<1",
    ]


def test_write_version_uses_double_quotes(tmp_path: Path) -> None:
    version_file = tmp_path / "version.py"
    version_file.write_text('__version__ = "0.6.0"\n', encoding="utf-8")

    workspace.write_version(version_file, "0.7.0")

    assert version_file.read_text(encoding="utf-8") == '__version__ = "0.7.0"\n'


def test_parse_fragments_ignores_keep_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    changes = tmp_path / "changes"
    fragment_dir = changes / "lib"
    fragment_dir.mkdir(parents=True)
    (fragment_dir / ".gitkeep").write_text("", encoding="utf-8")
    (fragment_dir / "123.bugfix.md").write_text("Fix it.\n", encoding="utf-8")
    monkeypatch.setattr(release, "CHANGES", changes)

    fragments = release.parse_fragments({"lib"})

    assert [fragment.path.name for fragment in fragments] == ["123.bugfix.md"]


def test_render_changelog_entry_appends_pr_number(tmp_path: Path) -> None:
    fragment = release.Fragment(
        package="pkg",
        path=tmp_path / "123.bugfix.md",
        kind="bugfix",
        text="Fix cache cleanup.",
    )
    item = release.Release(
        package="pkg",
        old_version="0.6.0",
        new_version="0.6.1",
        bump="patch",
        fragments=(fragment,),
    )

    entry = release._render_changelog_entry(item, pr_numbers={fragment.path: 42})

    assert "- Fix cache cleanup. (#42)" in entry


def test_render_changelog_entry_does_not_duplicate_pr_number(tmp_path: Path) -> None:
    fragment = release.Fragment(
        package="pkg",
        path=tmp_path / "123.bugfix.md",
        kind="bugfix",
        text="- Fix cache cleanup. (#42)",
    )
    item = release.Release(
        package="pkg",
        old_version="0.6.0",
        new_version="0.6.1",
        bump="patch",
        fragments=(fragment,),
    )

    entry = release._render_changelog_entry(item, pr_numbers={fragment.path: 42})

    assert entry.count("#42") == 1


def test_dependency_only_changelog_omits_pr_number() -> None:
    item = release.Release(
        package="pkg",
        old_version="0.6.0",
        new_version="0.6.1",
        bump="patch",
        fragments=(),
        dependency_only=True,
    )

    entry = release._render_changelog_entry(item, pr_numbers={})

    assert "- Update dependencies." in entry
    assert "#42" not in entry


def test_write_changelog_separates_prepended_entry(tmp_path: Path) -> None:
    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text(
        "# Changelog\n\n## 0.6.0 - 2026-01-01\n\n- Previous release.\n",
        encoding="utf-8",
    )
    item = release.Release(
        package="pkg",
        old_version="0.6.0",
        new_version="0.6.1",
        bump="patch",
        fragments=(
            release.Fragment(
                package="pkg",
                path=tmp_path / "123.bugfix.md",
                kind="bugfix",
                text="Fix cache cleanup.",
            ),
        ),
    )

    release.write_changelog(tmp_path, item)

    content = changelog.read_text(encoding="utf-8")
    assert "- Fix cache cleanup.\n## 0.6.0" not in content
    assert "- Fix cache cleanup.\n\n## 0.6.0" in content


def test_release_commit_body_uses_latest_changelog_entries(tmp_path: Path) -> None:
    package_path = tmp_path / "pkg"
    package_path.mkdir()
    (package_path / "CHANGELOG.md").write_text(
        """
# Changelog

## 0.7.0 - 2026-07-10

### Features

- Add feature.

## 0.6.0 - 2026-01-01

- Previous release.
""".lstrip(),
        encoding="utf-8",
    )
    item = release.Release(
        package="pkg",
        old_version="0.6.0",
        new_version="0.7.0",
        bump="minor",
        fragments=(),
    )
    package = workspace.Package("pkg", package_path, package_path / "version.py", ())

    body = release._release_commit_body([item], packages_by_name={"pkg": package})

    assert (
        body
        == """
## pkg

### 0.7.0 - 2026-07-10

#### Features

- Add feature.
""".lstrip()
    )
    assert "Previous release" not in body


def test_release_stages_commits_pushes_and_opens_pr(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    root = tmp_path
    package_path = root / "pkg"
    package_path.mkdir()
    (package_path / "CHANGELOG.md").write_text(
        "# Changelog\n\n## 0.7.0 - 2026-07-10\n\n- Add feature.\n",
        encoding="utf-8",
    )
    item = release.Release(
        package="pkg",
        old_version="0.6.0",
        new_version="0.7.0",
        bump="minor",
        fragments=(),
    )
    package = workspace.Package("pkg", package_path, package_path / "version.py", ())
    calls: list[list[str]] = []
    fixed_now = datetime(2026, 7, 10, 12, 34, 56, tzinfo=timezone.utc)

    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz: object = None) -> FakeDatetime:  # noqa: ANN401
            assert tz is timezone.utc
            return cast("FakeDatetime", fixed_now)

    monkeypatch.setattr(release, "ROOT", root)
    monkeypatch.setattr(release, "datetime", FakeDatetime)
    monkeypatch.setattr(release, "prepare_release_files", lambda: ([item], {}))
    monkeypatch.setattr(workspace, "packages", lambda: {"pkg": package})

    def fake_check_call(cmd: list[str], *, cwd: Path) -> None:
        calls.append(cmd)
        if cmd[:3] == ["git", "commit", "-v"]:
            template = Path(cmd[cmd.index("--template") + 1])
            message = template.read_text(encoding="utf-8")
            assert message.startswith("Release Packages\n\n## pkg\n")
            assert "### 0.7.0 - 2026-07-10" in message
        if cmd[:3] == ["gh", "pr", "create"]:
            body = Path(cmd[cmd.index("--body-file") + 1]).read_text(encoding="utf-8")
            assert body.startswith("## pkg\n")

    def fake_check_output(cmd: list[str], *, cwd: Path, text: bool) -> str:
        assert cwd == root
        assert text is True
        if cmd == ["git", "status", "--porcelain"]:
            return ""
        if cmd == ["gh", "api", "user", "--jq", ".login"]:
            return "octocat\n"
        raise AssertionError(cmd)

    monkeypatch.setattr(release.subprocess, "check_call", fake_check_call)
    monkeypatch.setattr(release.subprocess, "check_output", fake_check_output)

    assert release.release() == 0
    assert calls[0] == ["git", "switch", "-c", "octocat/release-20260710123456"]
    assert calls[1] == ["git", "add", "-A"]
    assert calls[2][:4] == ["git", "commit", "-v", "--template"]
    assert calls[3] == ["git", "push", "--set-upstream", "origin", "HEAD"]
    assert calls[4][:3] == ["gh", "pr", "create"]
    assert "--title" in calls[4]
    assert release.RELEASE_COMMIT_TITLE in calls[4]
    assert "--head" in calls[4]
    assert "octocat/release-20260710123456" in calls[4]


def test_release_requires_clean_tree(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(release, "ROOT", tmp_path)
    monkeypatch.setattr(
        release.subprocess,
        "check_output",
        lambda cmd, *, cwd, text: " M scripts/release.py\n",
    )

    with pytest.raises(SystemExit, match="clean Git working tree"):
        release.release()


def test_fragment_pr_number_uses_commit_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fragment = tmp_path / "changes/pkg/123.bugfix.md"
    monkeypatch.setattr(release, "ROOT", tmp_path)

    def fake_check_output(cmd: list[str], *, cwd: Path, text: bool) -> str:
        assert cmd == [
            "git",
            "log",
            "--full-history",
            "--format=%s",
            "--",
            "changes/pkg/123.bugfix.md",
        ]
        return "Fix cache cleanup (#42)\n\n"

    monkeypatch.setattr(release.subprocess, "check_output", fake_check_output)

    assert release._fragment_pr_number(fragment) == 42


def test_fragment_pr_number_accepts_merge_commit_subject(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fragment = tmp_path / "changes/pkg/123.bugfix.md"
    monkeypatch.setattr(release, "ROOT", tmp_path)

    def fake_check_output(cmd: list[str], *, cwd: Path, text: bool) -> str:
        return "Merge pull request #162 from user/branch\n"

    monkeypatch.setattr(release.subprocess, "check_output", fake_check_output)

    assert release._fragment_pr_number(fragment) == 162


def test_fragment_pr_number_reads_subjects_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fragment = tmp_path / "changes/pkg/123.bugfix.md"
    monkeypatch.setattr(release, "ROOT", tmp_path)
    seen: list[str] = []

    def fake_check_output(cmd: list[str], *, cwd: Path, text: bool) -> str:
        seen.extend(cmd)
        return "Add receive_batch (#180)\n"

    monkeypatch.setattr(release.subprocess, "check_output", fake_check_output)

    assert release._fragment_pr_number(fragment) == 180
    assert "--format=%s" in seen
    assert all("%b" not in part for part in seen)


def test_check_fragments_requires_changed_package_fragment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    version_file = tmp_path / "pkg/version.py"
    version_file.parent.mkdir(parents=True)
    package = workspace.Package("pkg", tmp_path / "pkg", version_file, ())
    monkeypatch.setattr(workspace, "packages", lambda: {"pkg": package})
    monkeypatch.setattr(release, "parse_fragments", lambda _packages: [])
    monkeypatch.setattr(
        release,
        "_changed_packages",
        lambda _packages, *, base, head, code_only: {"pkg"},
    )
    monkeypatch.setattr(
        release,
        "_release_prepped_packages",
        lambda _packages, *, base, head: set(),
    )

    assert release.check_fragments(base="origin/main") == 1


def test_check_fragments_exempts_release_prep_version_bumps(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    version_file = tmp_path / "pkg/version.py"
    version_file.parent.mkdir(parents=True)
    package = workspace.Package("pkg", tmp_path / "pkg", version_file, ())
    monkeypatch.setattr(workspace, "packages", lambda: {"pkg": package})
    monkeypatch.setattr(release, "parse_fragments", lambda _packages: [])
    monkeypatch.setattr(
        release,
        "_changed_packages",
        lambda _packages, *, base, head, code_only: {"pkg"},
    )
    monkeypatch.setattr(
        release,
        "_release_prepped_packages",
        lambda _packages, *, base, head: {"pkg"},
    )

    assert release.check_fragments(base="origin/main") == 0


def test_changed_paths_uses_index_diff_when_head_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[str] = []

    def fake_check_output(cmd: list[str], *, cwd: Path, text: bool) -> str:
        seen.extend(cmd)
        return "src/pkg/module.py\n"

    monkeypatch.setattr(release.subprocess, "check_output", fake_check_output)

    assert release._changed_paths(base="origin/main", head=None) == {
        release.ROOT / "src/pkg/module.py"
    }
    assert seen == ["git", "diff", "--name-only", "origin/main"]


def test_changed_packages_accepts_explicit_range(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    package = workspace.Package("pkg", tmp_path / "pkg", tmp_path / "pkg/version.py", ())
    monkeypatch.setattr(workspace, "packages", lambda: {"pkg": package})
    monkeypatch.setattr(workspace, "topological_names", lambda _packages: ["pkg"])

    seen: dict[str, str | bool] = {}

    def fake_changed(
        _packages: dict[str, workspace.Package],
        *,
        base: str,
        head: str,
        code_only: bool,
    ) -> set[str]:
        seen.update(base=base, head=head, code_only=code_only)
        return {"pkg"}

    monkeypatch.setattr(release, "_changed_packages", fake_changed)

    assert release.changed_packages(base="abc", head="def") == ["pkg"]
    assert seen == {"base": "abc", "head": "def", "code_only": False}


def test_package_hatch_build_loader_uses_shared_hook() -> None:
    spec = importlib.util.spec_from_file_location(
        "test_package_hatch_build", Path("src/vercel-headers/hatch_build.py")
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.get_metadata_hook().__name__ == "WorkspaceDependenciesMetadataHook"
