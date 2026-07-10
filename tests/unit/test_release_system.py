from __future__ import annotations

import importlib.util
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import pytest

from scripts import bundle_release, clogedit, hatch_build, release, workspace


def _derived_vendoring_config(include: str) -> bundle_release.VendoringConfig:
    package = workspace.Package("pkg", Path("/tmp/pkg"), Path("/tmp/pkg/version.py"), ())
    config = bundle_release._vendoring_config_for_package(  # noqa: SLF001
        package,
        {"tool": {"hatch": {"build": {"targets": {"wheel": {"only-include": [include]}}}}}},
    )
    assert config is not None
    return config


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


def test_compute_releases_force_bumps_all_packages(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    changes = tmp_path / "changes"
    changes.mkdir()

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

    releases = release.compute_releases(force_bump="minor")

    assert [
        (item.package, item.new_version, item.bump, item.dependency_only, item.forced)
        for item in releases
    ] == [
        ("lib", "0.7.0", "minor", False, True),
        ("app", "1.3.0", "minor", False, True),
    ]


def test_compute_releases_force_accepts_patch_bump(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    changes = tmp_path / "changes"
    changes.mkdir()

    version_file = tmp_path / "pkg/version.py"
    version_file.parent.mkdir()
    version_file.write_text('__version__ = "1.2.3"\n', encoding="utf-8")

    packages = {"pkg": workspace.Package("pkg", tmp_path / "pkg", version_file, ())}
    monkeypatch.setattr(release, "CHANGES", changes)
    monkeypatch.setattr(workspace, "packages", lambda: packages)
    monkeypatch.setattr(workspace, "topological_names", lambda _packages: ["pkg"])

    releases = release.compute_releases(force_bump="patch")

    assert [(item.package, item.new_version, item.bump, item.forced) for item in releases] == [
        ("pkg", "1.2.4", "patch", True)
    ]


def test_compute_releases_force_preserves_larger_fragment_bump(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    changes = tmp_path / "changes"
    fragment_dir = changes / "pkg"
    fragment_dir.mkdir(parents=True)
    (fragment_dir / "123.breaking.md").write_text("Break API.\n", encoding="utf-8")

    version_file = tmp_path / "pkg/version.py"
    version_file.parent.mkdir()
    version_file.write_text('__version__ = "1.2.3"\n', encoding="utf-8")

    packages = {"pkg": workspace.Package("pkg", tmp_path / "pkg", version_file, ())}
    monkeypatch.setattr(release, "CHANGES", changes)
    monkeypatch.setattr(workspace, "packages", lambda: packages)
    monkeypatch.setattr(workspace, "topological_names", lambda _packages: ["pkg"])

    releases = release.compute_releases(force_bump="minor")

    assert [(item.package, item.new_version, item.bump, item.forced) for item in releases] == [
        ("pkg", "2.0.0", "major", False)
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


def test_render_changelog_entry_treats_paragraphs_as_items(tmp_path: Path) -> None:
    fragment = release.Fragment(
        package="pkg",
        path=tmp_path / "123.bugfix.md",
        kind="bugfix",
        text=(
            "Remember the last live Runtime Cache client process-wide and fall back to it\n"
            "on threads that have no request context, and make strict=True raise\n"
            "RuntimeCacheError when no cache is available.\n"
            "\n"
            "Prime the cache while request context is still visible.\n"
        ),
    )
    item = release.Release(
        package="pkg",
        old_version="0.6.0",
        new_version="0.6.1",
        bump="patch",
        fragments=(fragment,),
    )

    entry = release._render_changelog_entry(item, pr_numbers={fragment.path: 178})

    assert (
        "- Remember the last live Runtime Cache client process-wide and fall back to it "
        "on threads that have no request context, and make strict=True raise "
        "RuntimeCacheError when no cache is available. (#178)"
    ) in entry
    assert "- Prime the cache while request context is still visible. (#178)" in entry
    assert entry.count("#178") == 2


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


def test_forced_changelog_entry_uses_no_changes() -> None:
    item = release.Release(
        package="pkg",
        old_version="0.6.0",
        new_version="0.7.0",
        bump="minor",
        fragments=(),
        forced=True,
    )

    entry = release._render_changelog_entry(item, pr_numbers={})

    assert "- No changes." in entry
    assert "- Update dependencies." not in entry


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
pkg
---

0.7.0 - 2026-07-10
------------------

Features
--------

- Add feature.
""".lstrip()
    )
    assert "Previous release" not in body


def test_github_release_body_uses_package_latest_changelog_entry(tmp_path: Path) -> None:
    package_path = tmp_path / "pkg"
    other_package_path = tmp_path / "other"
    package_path.mkdir()
    other_package_path.mkdir()
    (package_path / "CHANGELOG.md").write_text(
        """
# Changelog

## 0.7.0 - 2026-07-10

### Features

- Add package feature.

## 0.6.0 - 2026-01-01

- Previous package release.
""".lstrip(),
        encoding="utf-8",
    )
    (other_package_path / "CHANGELOG.md").write_text(
        """
# Changelog

## 1.0.0 - 2026-07-10

- Add other package feature.
""".lstrip(),
        encoding="utf-8",
    )
    packages = {
        "pkg": workspace.Package("pkg", package_path, package_path / "version.py", ()),
        "other": workspace.Package(
            "other", other_package_path, other_package_path / "version.py", ()
        ),
    }

    body = release._github_release_body("pkg", packages_by_name=packages)

    assert (
        body
        == """
## 0.7.0 - 2026-07-10

### Features

- Add package feature.
""".lstrip()
    )
    assert "Previous package release" not in body
    assert "other package" not in body


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
    monkeypatch.setattr(release, "prepare_release_files", lambda *, force_bump=None: ([item], {}))
    monkeypatch.setattr(workspace, "packages", lambda: {"pkg": package})

    def fake_check_call(cmd: list[str], *, cwd: Path) -> None:
        calls.append(cmd)
        if cmd[:3] == ["git", "commit", "-v"]:
            message_file = Path(cmd[cmd.index("--file") + 1])
            message = message_file.read_text(encoding="utf-8")
            assert message.startswith("Release Packages\n\npkg\n---\n")
            assert "0.7.0 - 2026-07-10\n------------------" in message
            assert "#" not in message
        if cmd[:3] == ["gh", "pr", "create"]:
            body = Path(cmd[cmd.index("--body-file") + 1]).read_text(encoding="utf-8")
            assert body.startswith("pkg\n---\n")
            assert "#" not in body

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
    assert calls[2][:4] == ["git", "commit", "-v", "--file"]
    assert calls[2][-1] == "--edit"
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
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
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
    output = capsys.readouterr().out
    assert "Missing news fragments for changed packages: pkg" in output
    assert "Run `poe changelog`" in output


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


def test_collect_changelog_diff_paths_staged_uses_cached_diff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[list[str]] = []

    def fake_git_name_paths(args: list[str]) -> set[Path]:
        seen.append(args)
        return {release.ROOT / "src/pkg/module.py"}

    monkeypatch.setattr(release, "_git_name_paths", fake_git_name_paths)

    assert release.collect_changelog_diff_paths("staged") == {release.ROOT / "src/pkg/module.py"}
    assert seen == [["diff", "--cached", "--name-only"]]


def test_collect_changelog_diff_paths_tracked_uses_head_diff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[list[str]] = []

    def fake_git_name_paths(args: list[str]) -> set[Path]:
        seen.append(args)
        return {release.ROOT / "src/pkg/module.py"}

    monkeypatch.setattr(release, "_git_name_paths", fake_git_name_paths)

    assert release.collect_changelog_diff_paths("tracked") == {release.ROOT / "src/pkg/module.py"}
    assert seen == [["diff", "HEAD", "--name-only"]]


def test_collect_changelog_diff_paths_all_includes_untracked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values: dict[tuple[str, ...], set[Path]] = {
        ("diff", "HEAD", "--name-only"): {release.ROOT / "src/pkg/module.py"},
        ("ls-files", "--others", "--exclude-standard"): {release.ROOT / "src/pkg/new.py"},
    }

    def fake_git_name_paths(args: list[str]) -> set[Path]:
        return values[tuple(args)]

    monkeypatch.setattr(release, "_git_name_paths", fake_git_name_paths)

    assert release.collect_changelog_diff_paths("all") == {
        release.ROOT / "src/pkg/module.py",
        release.ROOT / "src/pkg/new.py",
    }


def test_collect_changelog_diff_paths_base_uses_lower_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[list[str]] = []
    monkeypatch.setattr(release, "changelog_base_lower_bound", lambda: "abc123")

    def fake_git_name_paths(args: list[str]) -> set[Path]:
        seen.append(args)
        return {release.ROOT / "src/pkg/module.py"}

    monkeypatch.setattr(release, "_git_name_paths", fake_git_name_paths)

    assert release.collect_changelog_diff_paths("base") == {release.ROOT / "src/pkg/module.py"}
    assert seen == [["diff", "abc123..HEAD", "--name-only"]]


def test_changelog_base_lower_bound_uses_newest_news_fragment_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release.changelog_base_lower_bound.cache_clear()
    monkeypatch.setattr(release, "_default_base_ref", lambda: "origin/main")

    def fake_git_lines(args: list[str]) -> list[str]:
        assert args == ["log", "-m", "--format=%x00%H", "--name-only", "origin/main..HEAD"]
        return [
            "\0new",
            "src/pkg/module.py",
            "\0old",
            "changes/pkg/123.feature.md",
        ]

    monkeypatch.setattr(release, "_git_lines", fake_git_lines)

    assert release.changelog_base_lower_bound() == "old"


def test_changelog_base_lower_bound_defaults_to_base_ref(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release.changelog_base_lower_bound.cache_clear()
    monkeypatch.setattr(release, "_default_base_ref", lambda: "origin/main")
    monkeypatch.setattr(release, "_git_lines", lambda _args: [])

    assert release.changelog_base_lower_bound() == "origin/main"


def test_changelog_base_lower_bound_requires_base_ref(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release.changelog_base_lower_bound.cache_clear()
    monkeypatch.setattr(release, "_default_base_ref", lambda: None)

    with pytest.raises(SystemExit, match="could not find origin/main"):
        release.changelog_base_lower_bound()


def test_initial_changelog_selection_marks_changed_uncovered_packages(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    packages = {
        "lib": workspace.Package("lib", tmp_path / "lib", tmp_path / "lib/version.py", ()),
        "app": workspace.Package("app", tmp_path / "app", tmp_path / "app/version.py", ("lib",)),
    }
    monkeypatch.setattr(workspace, "topological_names", lambda _packages: ["lib", "app"])

    selection = clogedit.initial_changelog_selection(packages, {"lib", "app"}, {"app"})

    assert list(selection.packages) == ["lib", "app"]
    assert selection.packages["lib"].selected is True
    assert selection.packages["app"].selected is False
    assert selection.packages["app"].covered is True


def test_packages_for_paths_detects_package_code(tmp_path: Path) -> None:
    package = workspace.Package("pkg", tmp_path / "pkg", tmp_path / "pkg/version.py", ())

    assert release.packages_for_paths(
        {"pkg": package}, {tmp_path / "pkg/module.py"}, code_only=True
    ) == {"pkg"}
    assert (
        release.packages_for_paths(
            {"pkg": package}, {tmp_path / "pkg/tests/test_module.py"}, code_only=True
        )
        == set()
    )


def test_clean_news_fragment_text_strips_blank_and_comment_lines() -> None:
    assert release.clean_news_fragment_text(
        "# template\n\nAdd thing.  \n  # ignored\n- Fix thing.\n"
    ) == ("Add thing.\n- Fix thing.")


def test_clean_news_fragment_text_ignores_cutoff_section() -> None:
    assert (
        release.clean_news_fragment_text(
            "Add thing.\n"
            f"{release.CUTOFF_MARKER}\n"
            "# Do not modify or remove the line above.\n"
            "diff --git a/pkg/module.py b/pkg/module.py\n"
            "+This is not news.\n"
        )
        == "Add thing."
    )


def test_validate_fragment_kind_rejects_unknown_type() -> None:
    with pytest.raises(ValueError, match="invalid news fragment type"):
        release.validate_fragment_kind("security")


def test_write_news_fragment_uses_utc_timestamp_and_collision_suffix(tmp_path: Path) -> None:
    timestamp = datetime(2026, 7, 10, 12, 34, 56, tzinfo=timezone.utc)
    package_dir = tmp_path / "changes/pkg"
    package_dir.mkdir(parents=True)
    (package_dir / "20260710123456.bugfix.md").write_text("Existing.\n", encoding="utf-8")

    path = release.write_news_fragment(
        release.NewsFragmentDraft("pkg", "bugfix", "Fix cleanup."),
        timestamp=timestamp,
        changes=tmp_path / "changes",
    )

    assert path == package_dir / "20260710123456-1.bugfix.md"
    assert path.read_text(encoding="utf-8") == "Fix cleanup.\n"


def test_edit_news_fragment_runs_editor_and_requires_content(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("VISUAL", "test-editor --flag")
    seen: list[str] = []
    templates: list[str] = []

    def fake_runner(cmd: Sequence[str]) -> int:
        seen.extend(cmd[:2])
        assert Path(cmd[-1]).name == "COMMIT_EDITMSG"
        templates.append(Path(cmd[-1]).read_text(encoding="utf-8"))
        Path(cmd[-1]).write_text("# comment\n\nAdd feature.\n", encoding="utf-8")
        return 0

    assert release.edit_news_fragment("pkg", "feature", editor_runner=fake_runner) == "Add feature."
    assert seen == ["test-editor", "--flag"]
    assert templates == [release.FRAGMENT_GUIDANCE.format(package="pkg", package_diff_section="")]
    assert templates[0].startswith("\n\n# Write a concise news fragment for pkg.")


def test_edit_news_fragment_includes_package_diffstat_and_diff(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("VISUAL", "test-editor")
    package_path = release.ROOT / "src/pkg"
    seen_args: list[list[str]] = []
    templates: list[str] = []

    def fake_git_output(args: list[str], *, check: bool = True) -> str:
        seen_args.append(args)
        if args == ["diff", "HEAD", "--stat", "--", "src/pkg"]:
            return " src/pkg/module.py | 2 ++\n 1 file changed, 2 insertions(+)\n"
        if args == ["diff", "HEAD", "--", "src/pkg"]:
            return "diff --git a/src/pkg/module.py b/src/pkg/module.py\n+new line\n"
        raise AssertionError(args)

    def fake_runner(cmd: Sequence[str]) -> int:
        assert Path(cmd[-1]).name == "COMMIT_EDITMSG"
        templates.append(Path(cmd[-1]).read_text(encoding="utf-8"))
        Path(cmd[-1]).write_text("Add feature.\n", encoding="utf-8")
        return 0

    monkeypatch.setattr(release, "_git_output", fake_git_output)

    assert (
        release.edit_news_fragment(
            "pkg",
            "feature",
            package_path=package_path,
            editor_runner=fake_runner,
        )
        == "Add feature."
    )
    assert seen_args == [
        ["diff", "HEAD", "--stat", "--", "src/pkg"],
        ["diff", "HEAD", "--", "src/pkg"],
    ]
    assert templates[0].startswith("\n\n# Write a concise news fragment for pkg.")
    assert release.CUTOFF_MARKER in templates[0]
    assert "# Everything below it will be ignored." in templates[0]
    assert "src/pkg/module.py | 2 ++" in templates[0]
    assert "diff --git a/src/pkg/module.py b/src/pkg/module.py" in templates[0]


def test_edit_news_fragment_rejects_empty_content(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VISUAL", raising=False)
    monkeypatch.delenv("EDITOR", raising=False)

    def fake_runner(cmd: Sequence[str]) -> int:
        Path(cmd[-1]).write_text("# only comments\n\n", encoding="utf-8")
        return 0

    with pytest.raises(SystemExit, match="empty news fragment"):
        release.edit_news_fragment("pkg", "docs", editor_runner=fake_runner)


def test_package_hatch_build_loader_uses_shared_hook() -> None:
    spec = importlib.util.spec_from_file_location(
        "test_package_hatch_build", Path("src/vercel-headers/hatch_build.py")
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.get_metadata_hook().__name__ == "WorkspaceDependenciesMetadataHook"


def test_vendored_eligibility_is_derived_from_package_layout(tmp_path: Path) -> None:
    queue = workspace.Package(
        "vercel-queue",
        tmp_path / "src/vercel-queue",
        tmp_path / "src/vercel-queue/vercel/queue/version.py",
        (),
    )
    integration = workspace.Package(
        "vercel-celery",
        tmp_path / "integrations/vercel-celery",
        tmp_path / "integrations/vercel-celery/vercel/integrations/celery/version.py",
        (),
    )
    dramatiq = workspace.Package(
        "vercel-dramatiq",
        tmp_path / "integrations/vercel-dramatiq",
        tmp_path / "integrations/vercel-dramatiq/vercel/integrations/dramatiq/version.py",
        (),
    )
    headers = workspace.Package(
        "vercel-headers",
        tmp_path / "src/vercel-headers",
        tmp_path / "src/vercel-headers/vercel/headers/version.py",
        (),
    )
    umbrella = workspace.Package(
        "vercel", tmp_path / "src/vercel", tmp_path / "src/vercel/version.py", ()
    )
    for package in (queue, integration, dramatiq, headers):
        package.path.mkdir(parents=True)
        include = {
            "vercel-queue": "/vercel/queue",
            "vercel-celery": "/vercel/integrations/celery",
            "vercel-dramatiq": "/vercel/integrations/dramatiq",
            "vercel-headers": "/vercel/headers",
        }[package.name]
        (package.path / "pyproject.toml").write_text(
            f"""
[project]
name = "{package.name}"

[tool.hatch.build.targets.wheel]
only-include = ["{include}"]
""".lstrip(),
            encoding="utf-8",
        )
    umbrella.path.mkdir(parents=True)
    (umbrella.path / "pyproject.toml").write_text(
        """
[project]
name = "vercel"

[tool.hatch.build.targets.wheel]
only-include = ["/_internal", "/blob"]
""".lstrip(),
        encoding="utf-8",
    )

    assert bundle_release.is_vendored_eligible(queue)
    assert bundle_release.is_vendored_eligible(integration)
    assert bundle_release.is_vendored_eligible(dramatiq)
    assert bundle_release.is_vendored_eligible(headers)
    assert not bundle_release.is_vendored_eligible(umbrella)

    fallback_path = tmp_path / "src/vercel-fallback"
    fallback = workspace.Package(
        "vercel-fallback",
        fallback_path,
        fallback_path / "vercel/fallback/version.py",
        (),
    )
    fallback.path.mkdir(parents=True)
    (fallback.path / "pyproject.toml").write_text(
        """
[project]
name = "vercel-fallback"
""".lstrip(),
        encoding="utf-8",
    )
    assert bundle_release.is_vendored_eligible(fallback)


def test_vendored_external_dependencies_keep_peers_and_side_by_side_vendored_deps(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(bundle_release, "shared_vendored_version", lambda: "0.7.0")
    cache_version = tmp_path / "vercel-cache/version.py"
    queue_version = tmp_path / "vercel-queue/version.py"
    cache_version.parent.mkdir()
    queue_version.parent.mkdir()
    cache_version.write_text('__version__ = "0.7.0"\n', encoding="utf-8")
    queue_version.write_text('__version__ = "0.7.0"\n', encoding="utf-8")
    packages = {
        "vercel-cache": workspace.Package(
            "vercel-cache", tmp_path / "vercel-cache", cache_version, ()
        ),
        "vercel-queue": workspace.Package(
            "vercel-queue", tmp_path / "vercel-queue", queue_version, ()
        ),
    }
    monkeypatch.setattr(workspace, "packages", lambda: packages)
    data = {
        "tool": {
            "vercel": {
                "release": {
                    "dependencies": [
                        "httpx>=0.27,<1",
                        "celery>=5.3,<6",
                        "vercel-cache>=0.6.0",
                        "vercel-queue>=0.6.0",
                    ]
                }
            }
        }
    }

    assert bundle_release._external_dependencies(  # noqa: SLF001
        "vercel-celery",
        data,
        (),
    ) == (
        "celery>=5.3,<6",
        "vercel-cache-bundle>=0.7.0",
        "vercel-queue-bundle>=0.7.0",
        "vercel-internal-shared-vendored-deps>=0.7.0",
    )


def test_dramatiq_bundle_keeps_dramatiq_peer_dependency(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(bundle_release, "shared_vendored_version", lambda: "0.7.0")
    cache_version = tmp_path / "vercel-cache/version.py"
    queue_version = tmp_path / "vercel-queue/version.py"
    cache_version.parent.mkdir()
    queue_version.parent.mkdir()
    cache_version.write_text('__version__ = "0.7.0"\n', encoding="utf-8")
    queue_version.write_text('__version__ = "0.7.0"\n', encoding="utf-8")
    monkeypatch.setattr(
        workspace,
        "packages",
        lambda: {
            "vercel-cache": workspace.Package(
                "vercel-cache", tmp_path / "vercel-cache", cache_version, ()
            ),
            "vercel-queue": workspace.Package(
                "vercel-queue", tmp_path / "vercel-queue", queue_version, ()
            ),
        },
    )
    data = {
        "project": {
            "dependencies": [
                "dramatiq>=2.2,<3",
                "vercel-cache>=0.6.0",
                "vercel-queue>=0.6.0",
            ]
        }
    }

    assert bundle_release._external_dependencies(  # noqa: SLF001
        "vercel-dramatiq",
        data,
        (),
    ) == (
        "dramatiq>=2.2,<3",
        "vercel-cache-bundle>=0.7.0",
        "vercel-queue-bundle>=0.7.0",
        "vercel-internal-shared-vendored-deps>=0.7.0",
    )


def test_vendored_requirements_are_derived_from_release_deps_and_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        bundle_release,
        "_lock_versions",
        lambda: {
            "anyio": "4.13.0",
            "certifi": "2026.4.22",
            "h11": "0.16.0",
            "h2": "4.3.0",
            "hpack": "4.2.0",
            "httpcore": "1.0.9",
            "httpx": "0.28.1",
            "hyperframe": "6.1.0",
            "idna": "3.13",
            "python-multipart": "0.0.32",
            "typing-extensions": "4.15.0",
        },
    )
    monkeypatch.setattr(
        workspace,
        "packages",
        lambda: {
            "vercel-headers": workspace.Package(
                "vercel-headers", Path("/tmp/headers"), Path("/tmp/headers/version.py"), ()
            ),
            "vercel-oidc": workspace.Package(
                "vercel-oidc", Path("/tmp/oidc"), Path("/tmp/oidc/version.py"), ()
            ),
            "vercel-queue": workspace.Package(
                "vercel-queue", Path("/tmp/queue"), Path("/tmp/queue/version.py"), ()
            ),
        },
    )

    queue_data = {
        "tool": {
            "vercel": {
                "release": {
                    "dependencies": [
                        "anyio>=4.0.0",
                        "httpx[http2]>=0.27.0",
                        "python-multipart>=0.0.20",
                        "typing_extensions>=4.0.0",
                        "vercel-headers>=0.6.0",
                        "vercel-oidc>=0.6.0",
                    ]
                }
            }
        }
    }
    celery_data = {
        "tool": {
            "vercel": {
                "release": {
                    "dependencies": [
                        "celery>=5.3,<6",
                        "vercel-cache>=0.6.0",
                        "vercel-queue>=0.6.0",
                    ]
                }
            }
        }
    }

    assert bundle_release._derive_vendor_requirements(  # noqa: SLF001
        bundle_release.SHARED_VENDORED_PACKAGE, {}
    ) == (
        "anyio==4.13.0",
        "certifi==2026.4.22",
        "h11==0.16.0",
        "h2==4.3.0",
        "hpack==4.2.0",
        "httpcore==1.0.9",
        "httpx==0.28.1",
        "hyperframe==6.1.0",
        "idna==3.13",
        "typing-extensions==4.15.0",
    )
    assert bundle_release._derive_vendor_requirements(  # noqa: SLF001
        "vercel-queue", queue_data
    ) == ("python-multipart==0.0.32",)
    assert (
        bundle_release._derive_vendor_requirements(  # noqa: SLF001
            "vercel-celery", celery_data
        )
        == ()
    )


def test_shared_bundle_package_is_generated(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(bundle_release, "shared_vendored_version", lambda: "0.8.1")
    monkeypatch.setattr(bundle_release, "_shared_deps_fingerprint", lambda: "abc123")
    monkeypatch.setattr(
        bundle_release,
        "_derive_vendor_requirements",
        lambda package, _data: (
            ("httpx==0.28.1",) if package == bundle_release.SHARED_VENDORED_PACKAGE else ()
        ),
    )

    bundle_release._generate_shared_package(tmp_path)  # noqa: SLF001

    pyproject = (tmp_path / "pyproject.toml").read_text(encoding="utf-8")
    metadata = (tmp_path / "vercel/internal/_vendor/_shared_deps.json").read_text(encoding="utf-8")
    assert 'name = "vercel-internal-shared-vendored-deps"' in pyproject
    assert "[tool.vendoring]" not in pyproject
    assert (tmp_path / "vercel/internal/_vendor/version.py").read_text(
        encoding="utf-8"
    ) == '__version__ = "0.8.1"\n'
    assert '"fingerprint": "abc123"' in metadata
    assert '"httpx==0.28.1"' in metadata


def test_vendoring_config_is_generated_into_pyproject(tmp_path: Path) -> None:
    package_path = tmp_path / "pkg"
    package_path.mkdir()
    (package_path / "pyproject.toml").write_text(
        """
[project]
name = "vercel-queue"

[tool.hatch.version]
path = "vercel/queue/version.py"
""".lstrip(),
        encoding="utf-8",
    )
    plan = bundle_release.VendoredPlan(
        package=workspace.Package(
            "vercel-queue", package_path, package_path / "vercel/queue/version.py", ()
        ),
        variant_name="vercel-queue-bundle",
        config=_derived_vendoring_config("/vercel/queue"),
        vendored_requirements=("python-multipart==0.0.32",),
        external_dependencies=(),
    )

    bundle_release._write_vendoring_config(plan, package_path)  # noqa: SLF001

    pyproject = (package_path / "pyproject.toml").read_text(encoding="utf-8")
    assert 'destination = "vercel/queue/_vendor/"' in pyproject
    assert 'requirements = "vercel/queue/_vendor/vendor.txt"' in pyproject
    assert 'namespace = "vercel.queue._vendor"' in pyproject
    assert "[tool.vendoring.transformations]" in pyproject
    assert "import anyio\\\\.from_thread" not in pyproject


def test_vendoring_config_includes_anyio_from_thread_transform(tmp_path: Path) -> None:
    package_path = tmp_path / "pkg"
    package_path.mkdir()
    (package_path / "pyproject.toml").write_text(
        """
[project]
name = "pkg"

[tool.hatch.version]
path = "vercel/pkg/version.py"
""".lstrip(),
        encoding="utf-8",
    )
    plan = bundle_release.VendoredPlan(
        package=workspace.Package("pkg", package_path, package_path / "vercel/pkg/version.py", ()),
        variant_name="pkg-bundle",
        config=_derived_vendoring_config("/vercel/pkg"),
        vendored_requirements=("anyio==4.13.0",),
        external_dependencies=(),
    )

    bundle_release._write_vendoring_config(plan, package_path)  # noqa: SLF001

    pyproject = (package_path / "pyproject.toml").read_text(encoding="utf-8")
    assert "import anyio\\\\.from_thread" in pyproject


def test_vendored_license_files_are_copied_from_dist_info(tmp_path: Path) -> None:
    plan = bundle_release.VendoredPlan(
        package=workspace.Package(
            "vercel-queue", tmp_path / "pkg", tmp_path / "pkg/vercel/queue/version.py", ()
        ),
        variant_name="vercel-queue-bundle",
        config=_derived_vendoring_config("/vercel/queue"),
        vendored_requirements=("python-multipart==0.0.32",),
        external_dependencies=(),
    )
    site_packages = tmp_path / "site"
    dist_info = site_packages / "python_multipart-0.0.32.dist-info"
    license_dir = dist_info / "licenses"
    license_dir.mkdir(parents=True)
    (dist_info / "METADATA").write_text(
        "Metadata-Version: 2.4\n"
        "Name: python-multipart\n"
        "Version: 0.0.32\n"
        "License-File: LICENSE.txt\n",
        encoding="utf-8",
    )
    (license_dir / "LICENSE.txt").write_text("third-party license\n", encoding="utf-8")

    bundle_release._copy_vendored_license_files(  # noqa: SLF001
        plan,
        site_packages,
        generated=tmp_path / "generated",
        package_names=("python-multipart",),
    )

    copied = tmp_path / "generated/vercel/queue/_vendor/LICENSE.python-multipart.txt"
    assert copied.read_text(encoding="utf-8") == "third-party license\n"


def test_preserving_shared_vendored_licenses_keeps_existing_vendor_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    plan = bundle_release._shared_vendored_plan()  # noqa: SLF001
    vendor_path = tmp_path / "vercel/internal/_vendor"
    vendor_path.mkdir(parents=True)
    version_file = vendor_path / "version.py"
    version_file.write_text('__version__ = "0.7.0"\n', encoding="utf-8")
    (vendor_path / "anyio").mkdir()
    (vendor_path / "anyio/LICENSE").write_text("license\n", encoding="utf-8")

    monkeypatch.setattr(
        bundle_release,
        "_third_party_vendored_requirements",
        lambda _plan: (),
    )

    bundle_release._preserve_vendored_licenses(plan, generated=tmp_path)  # noqa: SLF001

    assert version_file.read_text(encoding="utf-8") == '__version__ = "0.7.0"\n'
    assert (vendor_path / "anyio/LICENSE").read_text(encoding="utf-8") == "license\n"


def test_bundle_pyproject_exposes_vendored_license_files(tmp_path: Path) -> None:
    package_path = tmp_path / "pkg"
    package_path.mkdir()
    (package_path / "pyproject.toml").write_text(
        """
[project]
name = "vercel-queue"
license = "MIT"
license-files = ["LICENSE", "LICENSE.*"]

[tool.vercel.release.dependencies]
dependencies = []

[tool.hatch.build.targets.sdist]
include = [
    "/vercel/queue/**/*.py",
    "/LICENSE",
]

[tool.hatch.build.targets.wheel]
only-include = ["/vercel/queue"]
""".lstrip(),
        encoding="utf-8",
    )
    (package_path / "vercel/queue/_vendor").mkdir(parents=True)
    (package_path / "vercel/queue/_vendor/LICENSE.python-multipart.txt").write_text(
        "third-party license\n",
        encoding="utf-8",
    )
    plan = bundle_release.VendoredPlan(
        package=workspace.Package(
            "vercel-queue", package_path, package_path / "vercel/queue/version.py", ()
        ),
        variant_name="vercel-queue-bundle",
        config=_derived_vendoring_config("/vercel/queue"),
        vendored_requirements=("python-multipart==0.0.32",),
        external_dependencies=(),
    )

    bundle_release._rewrite_pyproject(plan, package_path)  # noqa: SLF001

    pyproject = (package_path / "pyproject.toml").read_text(encoding="utf-8")
    assert 'name = "vercel-queue-bundle"' in pyproject
    assert '"vercel/queue/_vendor/LICEN[CS]E*"' in pyproject
    assert '"/vercel/queue/_vendor/LICEN[CS]E*"' in pyproject


def test_bundle_pyproject_rewrites_static_project_dependencies(tmp_path: Path) -> None:
    package_path = tmp_path / "pkg"
    package_path.mkdir()
    (package_path / "pyproject.toml").write_text(
        """
[project]
name = "vercel-dramatiq"
license = "MIT"
license-files = ["LICENSE", "LICENSE.*"]
dependencies = [
    "dramatiq>=2.2,<3",
    "vercel-cache>=0.6.0",
    "vercel-queue>=0.6.0",
]

[tool.hatch.build.targets.sdist]
include = [
    "/vercel/integrations/dramatiq/**/*.py",
    "/LICENSE",
]
""".lstrip(),
        encoding="utf-8",
    )
    plan = bundle_release.VendoredPlan(
        package=workspace.Package(
            "vercel-dramatiq",
            package_path,
            package_path / "vercel/integrations/dramatiq/version.py",
            (),
        ),
        variant_name="vercel-dramatiq-bundle",
        config=_derived_vendoring_config("/vercel/integrations/dramatiq"),
        vendored_requirements=(),
        external_dependencies=(
            "dramatiq>=2.2,<3",
            "vercel-cache-bundle>=0.7.1",
            "vercel-queue-bundle>=0.7.1",
            "vercel-internal-shared-vendored-deps>=0.1.0",
        ),
    )

    bundle_release._rewrite_pyproject(plan, package_path)  # noqa: SLF001

    pyproject = (package_path / "pyproject.toml").read_text(encoding="utf-8")
    assert 'name = "vercel-dramatiq-bundle"' in pyproject
    assert '"dramatiq>=2.2,<3"' in pyproject
    assert '"vercel-cache-bundle>=0.7.1"' in pyproject
    assert '"vercel-queue-bundle>=0.7.1"' in pyproject
    assert '"vercel-cache>=0.6.0"' not in pyproject
    assert '"vercel-queue>=0.6.0"' not in pyproject


def test_generated_vendoring_substitutions_escape_newlines() -> None:
    rendered = bundle_release._format_substitution(  # noqa: SLF001
        r"import h2\.config",
        "from vercel.internal._vendor import h2\nfrom vercel.internal._vendor.h2 import config",
    )

    assert "\\n" in rendered
    assert "\nfrom vercel" not in rendered


def test_bundle_readme_recommends_unbundled_package(tmp_path: Path) -> None:
    package_path = tmp_path / "pkg"
    package_path.mkdir()
    (package_path / "README.md").write_text("# Original\n\nDetails.\n", encoding="utf-8")
    plan = bundle_release.VendoredPlan(
        package=workspace.Package(
            "vercel-queue", package_path, package_path / "vercel/queue/version.py", ()
        ),
        variant_name="vercel-queue-bundle",
        config=_derived_vendoring_config("/vercel/queue"),
        vendored_requirements=("python-multipart==0.0.32",),
        external_dependencies=(),
    )

    bundle_release._rewrite_readme(plan, package_path)  # noqa: SLF001

    readme = (package_path / "README.md").read_text(encoding="utf-8")
    assert readme.startswith("# vercel-queue-bundle\n\n")
    assert "with third-party dependencies bundled" in readme
    assert "install the unbundled `vercel-queue` package instead" in readme
    assert "https://pypi.org/project/vercel-queue/" in readme


def test_shared_vendored_version_bumps_when_fingerprint_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(bundle_release.SHARED_VERSION_ENV, raising=False)
    monkeypatch.setattr(bundle_release, "_latest_pypi_release", lambda _package: "0.8.1")
    monkeypatch.setattr(bundle_release, "_pypi_shared_deps_fingerprint", lambda _version: "old")
    monkeypatch.setattr(bundle_release, "_shared_deps_fingerprint", lambda: "new")

    assert bundle_release.shared_vendored_version() == "0.8.2"
    assert bundle_release.shared_vendored_needs_publish()


def test_shared_vendored_version_reuses_previous_when_fingerprint_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(bundle_release.SHARED_VERSION_ENV, raising=False)
    monkeypatch.setattr(bundle_release, "_latest_pypi_release", lambda _package: "0.8.1")
    monkeypatch.setattr(bundle_release, "_pypi_shared_deps_fingerprint", lambda _version: "same")
    monkeypatch.setattr(bundle_release, "_shared_deps_fingerprint", lambda: "same")

    assert bundle_release.shared_vendored_version() == "0.8.1"
    assert not bundle_release.shared_vendored_needs_publish()


def test_vendored_source_import_rewrite_handles_workspace_modules(tmp_path: Path) -> None:
    from vendoring.tasks.vendor import rewrite_file_imports

    plan = bundle_release.VendoredPlan(
        package=workspace.Package("vercel-celery", Path("/tmp/pkg"), Path("/tmp/version.py"), ()),
        variant_name="vercel-celery-bundle",
        config=bundle_release.VendoringConfig(
            destination=Path("vercel/integrations/celery/_vendor"),
            requirements=Path("vercel/integrations/celery/_vendor/vendor.txt"),
            namespace="vercel.integrations.celery._vendor",
            protected_files=("__init__.py", "vendor.txt"),
        ),
        vendored_requirements=("anyio==4.13.0",),
        external_dependencies=(
            "celery>=5.3,<6",
            "vercel-cache-bundle>=0.7.0",
            "vercel-queue-bundle>=0.7.0",
            "vercel-internal-shared-vendored-deps>=0.7.0",
        ),
    )
    path = tmp_path / "module.py"
    path.write_text(
        "from vercel.cache import RuntimeCache\n"
        "from vercel.headers import get_headers\n"
        "from vercel.oidc.utils import find_project_info\n"
        "from vercel.queue import sanitize_name\n"
        "import httpx\n"
        "import anyio.from_thread\n"
        "from typing_extensions import override\n"
        "import vercel.queue as vqs\n"
        "import vercel.queue.sync as vqs_sync\n"
        "from celery import Celery\n",
        encoding="utf-8",
    )

    rewrite_file_imports(
        path,
        plan.config.namespace,
        list(bundle_release._source_rewrite_libs(plan)),  # noqa: SLF001
        list(bundle_release._source_rewrite_substitutions(plan)),  # noqa: SLF001
    )
    rewritten = path.read_text(encoding="utf-8")

    assert "from vercel.cache import RuntimeCache" in rewritten
    assert "from vercel.headers import get_headers" in rewritten
    assert "from vercel.oidc.utils import find_project_info" in rewritten
    assert "from vercel.queue import sanitize_name" in rewritten
    assert "from vercel.internal._vendor import httpx" in rewritten
    assert "from vercel.internal._vendor.anyio import from_thread" in rewritten
    assert "from vercel.internal._vendor.typing_extensions import override" in rewritten
    assert "import vercel.queue as vqs" in rewritten
    assert "import vercel.queue.sync as vqs_sync" in rewritten
    assert "from celery import Celery" in rewritten


def test_shared_bundle_package_keeps_distribution_name() -> None:
    assert (
        bundle_release._variant_name(  # noqa: SLF001
            "vercel-internal-shared-vendored-deps"
        )
        == "vercel-internal-shared-vendored-deps"
    )
    assert bundle_release._variant_name("vercel-queue") == "vercel-queue-bundle"  # noqa: SLF001


def test_vendored_nested_namespace_rewrite_deduplicates_vendor_prefix(tmp_path: Path) -> None:
    plan = bundle_release.VendoredPlan(
        package=workspace.Package("vercel-queue", Path("/tmp/pkg"), Path("/tmp/version.py"), ()),
        variant_name="vercel-queue-bundle",
        config=bundle_release.VendoringConfig(
            destination=Path("vercel/queue/_vendor"),
            requirements=Path("vercel/queue/_vendor/vendor.txt"),
            namespace="vercel.queue._vendor",
            protected_files=("__init__.py", "vendor.txt"),
        ),
        vendored_requirements=("httpcore==1.0.9",),
        external_dependencies=(),
    )
    path = tmp_path / "pkg/vercel/queue/_vendor/httpx/_transports/default.py"
    path.parent.mkdir(parents=True)
    path.write_text(
        "from vercel.queue._vendor.vercel.queue._vendor import httpcore\n",
        encoding="utf-8",
    )

    bundle_release._rewrite_nested_vendor_namespace(plan, tmp_path / "pkg")  # noqa: SLF001

    assert path.read_text(encoding="utf-8") == "from vercel.queue._vendor import httpcore\n"
