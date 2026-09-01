#!/usr/bin/env python3
from __future__ import annotations

import argparse
import functools
import os
import re
import shlex
import subprocess
import sys
import tempfile
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

try:
    from scripts import workspace
except ImportError:  # pragma: no cover - script execution path
    import workspace  # type: ignore[no-redef]


ROOT = Path(__file__).resolve().parent.parent
CHANGES = ROOT / "changes"
IGNORED_FRAGMENT_FILES = {".gitignore", ".gitkeep", ".keep"}
BUMP_ORDER = {"patch": 0, "minor": 1, "major": 2}
BUMP_NAMES = {value: key for key, value in BUMP_ORDER.items()}
UNPUBLISHED_VERSION = "0.0.0"
FRAGMENT_TYPES = {
    "breaking": "Breaking Changes",
    "feature": "Features",
    "bugfix": "Bug Fixes",
    "docs": "Documentation",
    "internal": "Internal",
}
TYPE_BUMPS = {
    "breaking": "major",
    "feature": "minor",
    "bugfix": "patch",
    "docs": "patch",
    "internal": "patch",
}
FRAGMENT_TYPE_ALTERNATION = "|".join(FRAGMENT_TYPES)
FRAGMENT_FILE_RE = re.compile(rf".+\.({FRAGMENT_TYPE_ALTERNATION})\.md")
RELEASE_COMMIT_TITLE = "Release Packages"
CHANGELOG_DIFF_MODES = ("staged", "tracked", "all", "base")
CUTOFF_MARKER = "# ------------------------ >8 ------------------------"
FRAGMENT_GUIDANCE = """

# Write a concise news fragment for {package}.
#
# The whole fragment becomes one changelog entry: a summary, then any detail
# paragraphs or code blocks. Blank lines and indentation are preserved.
# The release script adds '- ' and appends the pull request number.
# Comment lines outside code blocks are ignored.
{package_diff_section}
"""
# A list marker the fragment already opens with, so its entry is not given a
# second one.
LIST_MARKER_RE = re.compile(r"[-*+]\s")
# Up to three leading spaces then a run of at least three backticks or tildes,
# per CommonMark fenced code blocks.
FENCE_RE = re.compile(r" {0,3}(?P<marker>`{3,}|~{3,})")
# Two spaces put a continuation line inside the Markdown list item it follows.
CONTINUATION_INDENT = "  "


@dataclass(frozen=True)
class NewsFragmentDraft:
    package: str
    kind: str
    text: str


@dataclass(frozen=True)
class Fragment:
    package: str
    path: Path
    kind: str
    text: str


@dataclass(frozen=True)
class Release:
    package: str
    old_version: str
    new_version: str
    bump: str
    fragments: tuple[Fragment, ...]
    dependency_only: bool = False
    forced: bool = False


def parse_fragments(packages: set[str]) -> list[Fragment]:
    fragments: list[Fragment] = []
    if not CHANGES.exists():
        return fragments
    ignored = _git_ignored_untracked_paths(CHANGES)
    for package_dir in sorted(path for path in CHANGES.iterdir() if path.is_dir()):
        if package_dir in ignored:
            continue
        package = package_dir.name
        if package not in packages:
            raise SystemExit(f"unknown package changes directory: {package_dir.relative_to(ROOT)}")
        for fragment_path in sorted(package_dir.iterdir()):
            if not fragment_path.is_file():
                continue
            if fragment_path.name in IGNORED_FRAGMENT_FILES or fragment_path in ignored:
                continue
            match = FRAGMENT_FILE_RE.fullmatch(fragment_path.name)
            if match is None:
                expected = f"<id>.({FRAGMENT_TYPE_ALTERNATION}).md"
                raise SystemExit(
                    "invalid news fragment name "
                    f"{fragment_path.relative_to(ROOT)}; expected {expected}"
                )
            text = fragment_path.read_text(encoding="utf-8").strip()
            if not text:
                raise SystemExit(f"empty news fragment: {fragment_path.relative_to(ROOT)}")
            fragments.append(Fragment(package, fragment_path, match.group(1), text))
    return fragments


def _bump_for_fragment(kind: str, version: str) -> str:
    if version == UNPUBLISHED_VERSION:
        return "minor"
    bump = TYPE_BUMPS[kind]
    major = int(version.split(".", 1)[0])
    if major == 0 and bump == "major":
        return "minor"
    return bump


def _larger(left: str, right: str) -> str:
    return left if BUMP_ORDER[left] >= BUMP_ORDER[right] else right


def bump_version(version: str, bump: str) -> str:
    major, minor, patch = (int(part) for part in version.split(".")[:3])
    if bump == "major":
        return f"{major + 1}.0.0"
    if bump == "minor":
        return f"{major}.{minor + 1}.0"
    return f"{major}.{minor}.{patch + 1}"


def compute_releases(*, force_bump: str | None = None) -> list[Release]:
    packages_by_name = workspace.packages()
    versions = {
        name: workspace.read_version(package.version_file)
        for name, package in packages_by_name.items()
    }
    fragments = parse_fragments(set(packages_by_name))
    fragments_by_package: dict[str, list[Fragment]] = {name: [] for name in packages_by_name}
    bumps: dict[str, str] = {}
    for fragment in fragments:
        fragments_by_package[fragment.package].append(fragment)
        bump = _bump_for_fragment(fragment.kind, versions[fragment.package])
        bumps[fragment.package] = _larger(bumps.get(fragment.package, "patch"), bump)

    if force_bump:
        for name in packages_by_name:
            if versions[name] == UNPUBLISHED_VERSION:
                continue
            bumps[name] = _larger(bumps.get(name, "patch"), force_bump)

    reverse_edges = workspace.reverse_dependencies(packages_by_name)
    queue = list(bumps)
    while queue:
        package = queue.pop(0)
        for dependent in sorted(reverse_edges[package]):
            if dependent not in bumps and versions[dependent] != UNPUBLISHED_VERSION:
                bumps[dependent] = "patch"
                queue.append(dependent)

    release_set = set(bumps)
    ordered = [
        name for name in workspace.topological_names(packages_by_name) if name in release_set
    ]
    return [
        Release(
            package=name,
            old_version=versions[name],
            new_version=bump_version(versions[name], bumps[name]),
            bump=bumps[name],
            fragments=tuple(fragments_by_package[name]),
            dependency_only=not force_bump and not fragments_by_package[name],
            forced=force_bump is not None and not fragments_by_package[name],
        )
        for name in ordered
    ]


def _render_changelog_entry(release: Release, *, pr_numbers: dict[Path, int] | None = None) -> str:
    today = date.today().isoformat()
    lines = [f"## {release.new_version} - {today}", ""]
    if release.forced:
        lines.extend(["- No changes.", ""])
        return "\n".join(lines)
    if release.dependency_only:
        lines.extend(["- Update dependencies.", ""])
        return "\n".join(lines)

    for kind, title in FRAGMENT_TYPES.items():
        fragments = [fragment for fragment in release.fragments if fragment.kind == kind]
        if not fragments:
            continue
        lines.extend([f"### {title}", ""])
        bullets: list[list[str]] = []
        for fragment in fragments:
            pr_number = pr_numbers.get(fragment.path) if pr_numbers is not None else None
            bullets.append(_render_changelog_bullet(fragment.text, pr_number=pr_number))
        lines.extend(_join_changelog_bullets(bullets))
        lines.append("")
    return "\n".join(lines)


def _fence_states(lines: Sequence[str]) -> list[bool]:
    """Return, per line, whether it belongs to a fenced code block.

    The delimiters count as part of the block, so a fence line is never treated
    as prose that could take the pull request reference or a comment marker.
    """
    states: list[bool] = []
    fence: str | None = None
    for line in lines:
        match = FENCE_RE.match(line)
        marker = None if match is None else match.group("marker")
        if fence is None:
            states.append(marker is not None)
            fence = marker
            continue
        states.append(True)
        if marker is not None and marker[0] == fence[0] and len(marker) >= len(fence):
            fence = None
    return states


def _trim_blank_lines(lines: Sequence[str]) -> list[str]:
    result = list(lines)
    while result and not result[0]:
        result.pop(0)
    while result and not result[-1]:
        result.pop()
    return result


def _render_changelog_bullet(text: str, *, pr_number: int | None) -> list[str]:
    """Render one news fragment as the lines of a single Markdown list item.

    A fragment is one changelog entry, however many lines it spans: a summary
    that may be hard wrapped, followed by detail paragraphs, nested lists, and
    code blocks. All of it belongs to that one entry, so the line structure is
    kept and the continuation lines are indented to stay inside the bullet.
    """
    lines = _trim_blank_lines([line.rstrip() for line in text.splitlines()])
    if not lines:
        return []
    if pr_number is not None and not _mentions_pr(text, pr_number):
        index = _pr_reference_index(lines)
        lines[index] = f"{lines[index]} (#{pr_number})"
    first, *rest = lines
    bullet = first if LIST_MARKER_RE.match(first) else f"- {first}"
    return [bullet, *(f"{CONTINUATION_INDENT}{line}" if line else "" for line in rest)]


def _pr_reference_index(lines: Sequence[str]) -> int:
    """Index of the line that should carry the ``(#123)`` reference.

    The reference belongs at the end of the entry's summary, which may be hard
    wrapped over several lines, rather than mid-sentence on the first one or
    trailing the detail paragraphs. It never lands inside a fenced code block,
    where Markdown would render it literally.
    """
    summary_end: int | None = None
    for index, (line, in_fence) in enumerate(zip(lines, _fence_states(lines), strict=True)):
        if in_fence:
            continue
        if not line:
            if summary_end is not None:
                return summary_end
            continue
        summary_end = index
    return len(lines) - 1 if summary_end is None else summary_end


def _join_changelog_bullets(bullets: Sequence[Sequence[str]]) -> list[str]:
    """Concatenate rendered bullets, keeping multi-line ones visually separate.

    A bullet that starts immediately after an indented detail paragraph is hard
    to pick out, so multi-line bullets get a blank line on either side. Runs of
    one-line bullets stay tight, leaving simple changelog sections unchanged.
    """
    lines: list[str] = []
    for index, bullet in enumerate(bullets):
        needs_gap = len(bullet) > 1 or (index > 0 and len(bullets[index - 1]) > 1)
        if lines and needs_gap:
            lines.append("")
        lines.extend(bullet)
    return lines


def _mentions_pr(text: str, pr_number: int) -> bool:
    return re.search(rf"(?<!\d)#\s*{pr_number}(?!\d)", text) is not None


def write_changelog(
    package_path: Path, release: Release, *, pr_numbers: dict[Path, int] | None = None
) -> None:
    path = package_path / "CHANGELOG.md"
    entry = _render_changelog_entry(release, pr_numbers=pr_numbers)
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        if existing.startswith("# Changelog"):
            _, _, rest = existing.partition("\n")
            content = f"# Changelog\n\n{entry}\n{rest.lstrip()}"
        else:
            content = f"# Changelog\n\n{entry}\n{existing.lstrip()}"
    else:
        content = f"# Changelog\n\n{entry}"
    path.write_text(content, encoding="utf-8")


def prepare_release_files(
    *, force_bump: str | None = None
) -> tuple[list[Release], dict[Path, int]]:
    releases = compute_releases(force_bump=force_bump)
    if not releases:
        print("No news fragments found.")
        return [], {}

    packages_by_name = workspace.packages()
    pr_numbers = _release_pr_numbers(releases)
    for release in releases:
        package = packages_by_name[release.package]
        workspace.write_version(package.version_file, release.new_version)
        write_changelog(package.path, release, pr_numbers=pr_numbers)
        for fragment in release.fragments:
            fragment.path.unlink()
            if fragment.path.parent.exists() and not any(fragment.path.parent.iterdir()):
                fragment.path.parent.rmdir()

    subprocess.check_call(["uv", "lock"], cwd=ROOT)
    print_release_summary(releases)
    return releases, pr_numbers


def prepare() -> int:
    prepare_release_files()
    return 0


def release(*, force_bump: str | None = None) -> int:
    _ensure_clean_tree()
    branch = _create_release_branch()
    releases, _pr_numbers = prepare_release_files(force_bump=force_bump)
    if not releases:
        return 0

    body = _release_commit_body(releases, packages_by_name=workspace.packages())
    _stage_all()
    _commit_release(body)
    _push_current_branch()
    _create_pull_request(body, branch=branch)
    return 0


def _ensure_clean_tree() -> None:
    output = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)
    if output.strip():
        raise SystemExit("release requires a clean Git working tree")


def _create_release_branch() -> str:
    username = _gh_username()
    branch = f"{username}/release-{fragment_timestamp()}"
    subprocess.check_call(["git", "switch", "-c", branch], cwd=ROOT)
    return branch


def _gh_username() -> str:
    username = subprocess.check_output(
        ["gh", "api", "user", "--jq", ".login"], cwd=ROOT, text=True
    ).strip()
    if not username:
        raise SystemExit("could not determine GitHub username")
    return _branch_component(username)


def _branch_component(value: str) -> str:
    result = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-.")
    if not result:
        raise SystemExit("GitHub username cannot be used in a branch name")
    return result


def _stage_all() -> None:
    subprocess.check_call(["git", "add", "-A"], cwd=ROOT)


def _commit_release(body: str) -> None:
    message_path = _write_temp_text(_release_commit_message(body), prefix="release-commit-")
    try:
        subprocess.check_call(
            ["git", "commit", "-v", "--file", str(message_path), "--edit"], cwd=ROOT
        )
    finally:
        message_path.unlink(missing_ok=True)


def _push_current_branch() -> None:
    subprocess.check_call(["git", "push", "--set-upstream", "origin", "HEAD"], cwd=ROOT)


def _create_pull_request(body: str, *, branch: str) -> None:
    body_path = _write_temp_text(body, prefix="release-pr-body-")
    try:
        subprocess.check_call(
            [
                "gh",
                "pr",
                "create",
                "--title",
                RELEASE_COMMIT_TITLE,
                "--body-file",
                str(body_path),
                "--head",
                branch,
            ],
            cwd=ROOT,
        )
    finally:
        body_path.unlink(missing_ok=True)


def _write_temp_text(text: str, *, prefix: str) -> Path:
    handle, name = tempfile.mkstemp(prefix=prefix, suffix=".md")
    path = Path(name)
    with os.fdopen(handle, "w", encoding="utf-8") as file:
        file.write(text)
    return path


def _release_commit_message(body: str) -> str:
    return f"{RELEASE_COMMIT_TITLE}\n\n{body.rstrip()}\n"


def _release_commit_body(
    releases: list[Release], *, packages_by_name: dict[str, workspace.Package]
) -> str:
    lines: list[str] = []
    for item in releases:
        package = packages_by_name[item.package]
        lines.extend([item.package, "-" * len(item.package), ""])
        lines.extend(_format_commit_markdown(_latest_changelog_entry(package.path)).splitlines())
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _github_release_body(
    package_name: str, *, packages_by_name: dict[str, workspace.Package]
) -> str:
    try:
        package = packages_by_name[package_name]
    except KeyError:
        raise SystemExit(f"unknown package: {package_name}") from None
    return _latest_changelog_entry(package.path).rstrip() + "\n"


def print_github_release_body(args: argparse.Namespace) -> int:
    sys.stdout.write(_github_release_body(args.package, packages_by_name=workspace.packages()))
    return 0


def _latest_changelog_entry(package_path: Path) -> str:
    path = package_path / "CHANGELOG.md"
    lines = path.read_text(encoding="utf-8").splitlines()
    start = next((index for index, line in enumerate(lines) if line.startswith("## ")), None)
    if start is None:
        raise SystemExit(f"missing changelog entry in {path.relative_to(ROOT)}")
    end = next(
        (index for index in range(start + 1, len(lines)) if lines[index].startswith("## ")),
        len(lines),
    )
    return "\n".join(lines[start:end]).rstrip()


def _format_commit_markdown(text: str) -> str:
    lines = []
    for line in text.splitlines():
        if line.startswith("#"):
            heading = line.lstrip("#").strip()
            lines.extend([heading, "-" * len(heading)])
        else:
            lines.append(line)
    return "\n".join(lines)


def _release_pr_numbers(releases: list[Release]) -> dict[Path, int]:
    result: dict[Path, int] = {}
    for release in releases:
        for fragment in release.fragments:
            pr_number = _fragment_pr_number(fragment.path)
            if pr_number is not None:
                result[fragment.path] = pr_number
    return result


def _fragment_pr_number(path: Path) -> int | None:
    try:
        log = subprocess.check_output(
            [
                "git",
                "log",
                "--full-history",
                "--format=%s",
                "--",
                path.relative_to(ROOT).as_posix(),
            ],
            cwd=ROOT,
            text=True,
        )
    except subprocess.CalledProcessError:
        return None
    for subject in log.splitlines():
        pr_number = _subject_pr_number(subject)
        if pr_number is not None:
            return pr_number
    return None


def _subject_pr_number(subject: str) -> int | None:
    for pattern in (r"\(#(\d+)\)", r"^Merge pull request #(\d+)\b"):
        match = re.search(pattern, subject)
        if match is not None:
            return int(match.group(1))
    return None


def print_release_summary(releases: list[Release]) -> None:
    if not releases:
        print("No pending releases.")
        return
    for release in releases:
        suffix = " dependency-only" if release.dependency_only else ""
        version_change = f"{release.old_version} -> {release.new_version}"
        print(f"{release.package}: {version_change} ({release.bump}{suffix})")


def status() -> int:
    releases = compute_releases()
    print_release_summary(releases)
    return 0


def lint_towncrier() -> int:
    parse_fragments(set(workspace.packages()))
    return 0


def changelog(diff: str = "tracked") -> int:
    if diff not in CHANGELOG_DIFF_MODES:
        expected = ", ".join(CHANGELOG_DIFF_MODES)
        raise SystemExit(f"invalid diff mode {diff!r}; expected one of: {expected}")

    # Imported lazily: the TUI needs textual, which is not installed in the
    # environments that run the non-interactive subcommands (e.g. the publish
    # workflow runs `release.py changed` with a bare system Python).
    try:
        from scripts import clogedit
    except ImportError:  # pragma: no cover - script execution path
        import clogedit  # type: ignore[no-redef]
    return clogedit.changelog(diff)


def collect_changelog_diff_paths(diff: str) -> set[Path]:
    paths = _git_name_paths([*changelog_diff_args(diff), "--name-only"])
    if diff == "all":
        paths |= _git_name_paths(["ls-files", "--others", "--exclude-standard"])
    return paths


def changelog_diff_args(diff: str) -> list[str]:
    if diff == "staged":
        return ["diff", "--cached"]
    if diff in ("tracked", "all"):
        return ["diff", "HEAD"]
    if diff == "base":
        return ["diff", f"{changelog_base_lower_bound()}..HEAD"]
    expected = ", ".join(CHANGELOG_DIFF_MODES)
    raise ValueError(f"invalid diff mode {diff!r}; expected one of: {expected}")


@functools.cache
def changelog_base_lower_bound() -> str:
    base = _default_base_ref()
    if base is None:
        raise SystemExit("could not find origin/main or origin/master for `--diff base`")
    # `git log --name-only` lists each commit (newest first) followed by the
    # paths it touched; the NUL prefix cannot appear in a path, so it safely
    # marks the commit lines.  -m makes merge commits list paths too.
    commit: str | None = None
    for line in _git_lines(["log", "-m", "--format=%x00%H", "--name-only", f"{base}..HEAD"]):
        if line.startswith("\0"):
            commit = line[1:]
        elif commit is not None and _is_valid_news_fragment_path(Path(line)):
            return commit
    return base


def _is_valid_news_fragment_path(path: Path) -> bool:
    parts = path.parts
    if len(parts) != 3 or parts[0] != "changes":
        return False
    return FRAGMENT_FILE_RE.fullmatch(parts[2]) is not None


def _git_ignored_untracked_paths(directory: Path) -> set[Path]:
    """Return untracked paths under *directory* excluded by Git ignore rules."""
    try:
        return _git_name_paths(
            [
                "ls-files",
                "--others",
                "--ignored",
                "--exclude-standard",
                "--directory",
                "-z",
                "--",
                str(directory),
            ],
            zero_terminated=True,
        )
    except (subprocess.CalledProcessError, OSError):
        # No git, or not a work tree. Nothing is known to be ignored, which
        # leaves the name check below exactly as strict as it was.
        return set()


def _git_name_paths(args: list[str], *, zero_terminated: bool = False) -> set[Path]:
    return {ROOT / line for line in _git_lines(args, zero_terminated=zero_terminated) if line}


def _git_lines(args: list[str], *, zero_terminated: bool = False) -> list[str]:
    # quotepath=off keeps non-ASCII path names literal instead of C-quoted.
    output = subprocess.check_output(
        ["git", "-c", "core.quotepath=off", *args], cwd=ROOT, text=True
    )
    if zero_terminated:
        return [line for line in output.split("\0") if line]
    return [line for line in output.splitlines() if line]


def packages_for_paths(
    packages_by_name: dict[str, workspace.Package], paths: Iterable[Path], *, code_only: bool
) -> set[str]:
    changed_paths = set(paths)
    result: set[str] = set()
    for name, package in packages_by_name.items():
        if code_only:
            if any(_is_package_code_path(path, package.path) for path in changed_paths):
                result.add(name)
        elif (
            package.version_file in changed_paths or package.path / "CHANGELOG.md" in changed_paths
        ):
            result.add(name)
    return result


def edit_news_fragment(
    package: str,
    kind: str,
    *,
    package_path: Path | None = None,
    diff: str = "tracked",
    editor_runner: Callable[[Sequence[str]], int] | None = None,
) -> str:
    validate_fragment_kind(kind)
    with tempfile.TemporaryDirectory(prefix=f"{package}-{kind}-") as tmpdir:
        # Vim detects this basename as gitcommit for commit-message highlighting.
        tmp_path = Path(tmpdir) / "COMMIT_EDITMSG"
        tmp_path.write_text(
            FRAGMENT_GUIDANCE.format(
                package=package,
                package_diff_section=package_diff_section(package_path, diff=diff),
            ),
            encoding="utf-8",
        )
        editor = _select_editor()
        runner = subprocess.call if editor_runner is None else editor_runner
        try:
            result = runner([*editor, str(tmp_path)])
        except OSError as exc:
            raise SystemExit(f"could not run editor {editor[0]!r}: {exc}") from exc
        if result != 0:
            raise SystemExit(f"editor exited with status {result}")
        text = clean_news_fragment_text(tmp_path.read_text(encoding="utf-8"))
    if not text:
        raise SystemExit(f"empty news fragment for {package}")
    return text


def package_diff_section(package_path: Path | None, *, diff: str) -> str:
    if package_path is None:
        return ""
    diffstat = package_diffstat(package_path, diff=diff).rstrip()
    diff_text = package_diff(package_path, diff=diff).rstrip()
    diff_parts = [part for part in (diffstat, diff_text) if part]
    if not diff_parts:
        return ""
    diff_body = "\n\n".join(diff_parts)
    return (
        "\n"
        f"{CUTOFF_MARKER}\n"
        "# Do not modify or remove the line above.\n"
        "# Everything below it will be ignored.\n"
        f"{diff_body}\n"
    )


def package_diffstat(package_path: Path, *, diff: str) -> str:
    return _package_diff_output(package_path, diff=diff, stat=True)


def package_diff(package_path: Path, *, diff: str) -> str:
    return _package_diff_output(package_path, diff=diff, stat=False)


def _package_diff_output(package_path: Path, *, diff: str, stat: bool) -> str:
    relative_package = package_path.relative_to(ROOT).as_posix()
    stat_args = ["--stat"] if stat else []
    tracked = _git_output([*changelog_diff_args(diff), *stat_args, "--", relative_package])
    if diff != "all":
        return tracked
    untracked = "\n".join(
        _git_untracked_output(path, stat=stat)
        for path in sorted(_git_name_paths(["ls-files", "--others", "--exclude-standard"]))
        if path.is_relative_to(package_path)
    )
    return "\n".join(part for part in (tracked.rstrip(), untracked.rstrip()) if part)


def _git_untracked_output(path: Path, *, stat: bool) -> str:
    relative_path = path.relative_to(ROOT).as_posix()
    stat_args = ["--stat"] if stat else []
    return _git_output(
        ["diff", "--no-index", *stat_args, "--", "/dev/null", relative_path],
        check=False,
    )


def _git_output(args: list[str], *, check: bool = True) -> str:
    return subprocess.run(
        ["git", "-c", "core.quotepath=off", *args],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        check=check,
    ).stdout


def _select_editor() -> list[str]:
    for name in ("VISUAL", "EDITOR"):
        value = os.environ.get(name)
        if value:
            return shlex.split(value)
    return ["vi"]


def clean_news_fragment_text(text: str) -> str:
    """Turn editor buffer contents into the text stored in a news fragment.

    Comment lines and everything below the cutoff marker are dropped. Blank
    lines between paragraphs survive, because a fragment is one changelog entry
    and those breaks are part of it; runs of them collapse to one. Code blocks
    are kept verbatim, so a ``#`` comment in a sample is not read as a comment
    on the fragment.
    """
    body = text.split(CUTOFF_MARKER, 1)[0]
    lines = [line.rstrip() for line in body.splitlines()]
    kept: list[str] = []
    for line, in_fence in zip(lines, _fence_states(lines), strict=True):
        if in_fence:
            kept.append(line)
            continue
        if line.lstrip().startswith("#"):
            continue
        if not line and (not kept or not kept[-1]):
            continue
        kept.append(line)
    return "\n".join(kept).strip()


def validate_fragment_kind(kind: str) -> None:
    if kind not in FRAGMENT_TYPES:
        expected = ", ".join(FRAGMENT_TYPES)
        raise ValueError(f"invalid news fragment type {kind!r}; expected one of: {expected}")


def write_news_fragment(
    draft: NewsFragmentDraft, *, timestamp: datetime | None = None, changes: Path | None = None
) -> Path:
    validate_fragment_kind(draft.kind)
    root = CHANGES if changes is None else changes
    package_dir = root / draft.package
    package_dir.mkdir(parents=True, exist_ok=True)
    stem = fragment_timestamp(timestamp)
    path = package_dir / f"{stem}.{draft.kind}.md"
    suffix = 1
    while path.exists():
        path = package_dir / f"{stem}-{suffix}.{draft.kind}.md"
        suffix += 1
    path.write_text(f"{draft.text.rstrip()}\n", encoding="utf-8")
    return path


def fragment_timestamp(value: datetime | None = None) -> str:
    if value is None:
        value = datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")


def check_fragments(base: str | None = None) -> int:
    packages_by_name = workspace.packages()
    fragments = parse_fragments(set(packages_by_name))
    head = os.environ.get("WORKSPACE_POE_GIT_COMMIT")
    if base is None:
        base = os.environ.get("WORKSPACE_POE_GIT_BASE") or _default_base_ref()
    if base is None:
        print("Could not detect a base branch for news fragment enforcement.")
        return 1

    changed_paths = _changed_paths(base=base, head=head)
    changed = packages_for_paths(packages_by_name, changed_paths, code_only=True)
    changed -= {
        name for name, package in packages_by_name.items() if package.version_file in changed_paths
    }
    changed = set(publishable_packages(changed, packages_by_name=packages_by_name))
    packages_with_fragments = {
        fragment.package for fragment in fragments if fragment.path in changed_paths
    }
    missing = sorted(changed - packages_with_fragments)
    if missing:
        packages = ", ".join(missing)
        print(f"Missing news fragments for changed packages: {packages}")
        print(
            "Run `poe changelog`, add changes/<package>/<id>.<type>.md, "
            "or adjust the changed files."
        )
        return 1
    return 0


def check_new_package_versions(base: str | None = None) -> int:
    packages_by_name = workspace.packages()
    head = os.environ.get("WORKSPACE_POE_GIT_COMMIT")
    if base is None:
        base = os.environ.get("WORKSPACE_POE_GIT_BASE") or _default_base_ref()
    if base is None:
        print("Could not detect a base branch for new package version enforcement.")
        return 1

    added_paths = _added_paths(base=base, head=head)
    invalid: list[tuple[str, str]] = []
    for name, package in sorted(packages_by_name.items()):
        if package.path / "pyproject.toml" not in added_paths:
            continue
        version = workspace.read_version(package.version_file)
        if version != UNPUBLISHED_VERSION:
            invalid.append((name, version))
    if invalid:
        packages = ", ".join(f"{name} ({version})" for name, version in invalid)
        print(f"New packages must start at {UNPUBLISHED_VERSION}: {packages}")
        print("The release workflow assigns the first publishable version.")
        return 1
    return 0


def changed_packages(base: str = "HEAD^", head: str = "HEAD") -> list[str]:
    packages_by_name = workspace.packages()
    changed = _changed_packages(packages_by_name, base=base, head=head, code_only=False)
    return publishable_packages(changed, packages_by_name=packages_by_name)


def publishable_packages(
    names: Iterable[str] | None = None,
    *,
    packages_by_name: dict[str, workspace.Package] | None = None,
) -> list[str]:
    if packages_by_name is None:
        packages_by_name = workspace.packages()
    selected = set(packages_by_name) if names is None else set(names)
    return [
        name
        for name in workspace.topological_names(packages_by_name)
        if name in selected
        and workspace.read_version(packages_by_name[name].version_file) != UNPUBLISHED_VERSION
    ]


def print_changed_packages(args: argparse.Namespace) -> int:
    for name in changed_packages(base=args.base, head=args.head):
        print(name)
    return 0


def print_publishable_packages(_args: argparse.Namespace) -> int:
    for name in publishable_packages():
        print(name)
    return 0


def _changed_packages(
    packages_by_name: dict[str, workspace.Package],
    *,
    base: str,
    head: str | None,
    code_only: bool,
) -> set[str]:
    return packages_for_paths(
        packages_by_name,
        _changed_paths(base=base, head=head),
        code_only=code_only,
    )


def _changed_paths(*, base: str, head: str | None) -> set[Path]:
    diff_range = [f"{base}..{head}"] if head is not None else [base]
    output = subprocess.check_output(
        ["git", "diff", "--name-only", *diff_range], cwd=ROOT, text=True
    )
    return {ROOT / line for line in output.splitlines() if line}


def _added_paths(*, base: str, head: str | None) -> set[Path]:
    diff_range = [f"{base}..{head}"] if head is not None else [base]
    output = subprocess.check_output(
        ["git", "diff", "--diff-filter=A", "--name-only", *diff_range], cwd=ROOT, text=True
    )
    return {ROOT / line for line in output.splitlines() if line}


def _is_package_code_path(path: Path, package_path: Path) -> bool:
    try:
        rel = path.relative_to(package_path)
    except ValueError:
        return False
    if rel.parts[:1] in {("tests",), ("examples",)}:
        return False
    return path.suffix == ".py" or path.name == "pyproject.toml"


def _default_base_ref() -> str | None:
    for ref in ("origin/main", "origin/master"):
        result = subprocess.run(
            ["git", "rev-parse", "--verify", ref],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode == 0:
            return ref
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("status").set_defaults(func=lambda _args: status())
    subparsers.add_parser("prepare").set_defaults(func=lambda _args: prepare())
    release_parser = subparsers.add_parser("release")
    release_parser.add_argument(
        "--force-bump",
        choices=tuple(BUMP_ORDER),
        help="bump every package by this amount and write No changes changelog entries",
    )
    release_parser.set_defaults(func=lambda args: release(force_bump=args.force_bump))
    changed_parser = subparsers.add_parser("changed")
    changed_parser.add_argument("--base", default="HEAD^")
    changed_parser.add_argument("--head", default="HEAD")
    changed_parser.set_defaults(func=print_changed_packages)
    subparsers.add_parser("publishable").set_defaults(func=print_publishable_packages)

    github_release_body_parser = subparsers.add_parser("github-release-body")
    github_release_body_parser.add_argument("package")
    github_release_body_parser.set_defaults(func=print_github_release_body)

    changelog_parser = subparsers.add_parser("changelog")
    changelog_parser.add_argument(
        "--diff",
        choices=CHANGELOG_DIFF_MODES,
        default="tracked",
        help="changed-path source used to preselect packages",
    )
    changelog_parser.set_defaults(func=lambda args: changelog(args.diff))

    check_parser = subparsers.add_parser("check-news-fragments")
    check_parser.add_argument("--base")
    check_parser.set_defaults(func=lambda args: check_fragments(args.base))

    package_versions_parser = subparsers.add_parser("check-new-package-versions")
    package_versions_parser.add_argument("--base")
    package_versions_parser.set_defaults(func=lambda args: check_new_package_versions(args.base))

    subparsers.add_parser("lint-towncrier").set_defaults(func=lambda _args: lint_towncrier())
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
