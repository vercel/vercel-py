#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
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
RELEASE_COMMIT_TITLE = "Release Packages"


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


def parse_fragments(packages: set[str]) -> list[Fragment]:
    fragments: list[Fragment] = []
    if not CHANGES.exists():
        return fragments
    for package_dir in sorted(path for path in CHANGES.iterdir() if path.is_dir()):
        package = package_dir.name
        if package not in packages:
            raise SystemExit(f"unknown package changes directory: {package_dir.relative_to(ROOT)}")
        for fragment_path in sorted(package_dir.iterdir()):
            if not fragment_path.is_file():
                continue
            if fragment_path.name in IGNORED_FRAGMENT_FILES:
                continue
            match = re.fullmatch(
                r".+\.(breaking|feature|bugfix|docs|internal)\.md", fragment_path.name
            )
            if match is None:
                expected = "<id>.(breaking|feature|bugfix|docs|internal).md"
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


def compute_releases() -> list[Release]:
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

    reverse_edges = workspace.reverse_dependencies(packages_by_name)
    queue = list(bumps)
    while queue:
        package = queue.pop(0)
        for dependent in sorted(reverse_edges[package]):
            if dependent not in bumps:
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
            dependency_only=not fragments_by_package[name],
        )
        for name in ordered
    ]


def _render_changelog_entry(release: Release, *, pr_numbers: dict[Path, int] | None = None) -> str:
    today = date.today().isoformat()
    lines = [f"## {release.new_version} - {today}", ""]
    if release.dependency_only:
        lines.extend(["- Update dependencies.", ""])
        return "\n".join(lines)

    for kind, title in FRAGMENT_TYPES.items():
        fragments = [fragment for fragment in release.fragments if fragment.kind == kind]
        if not fragments:
            continue
        lines.extend([f"### {title}", ""])
        for fragment in fragments:
            pr_number = pr_numbers.get(fragment.path) if pr_numbers is not None else None
            for item in fragment.text.splitlines():
                item = item.strip()
                if item:
                    lines.append(_render_changelog_bullet(item, pr_number=pr_number))
        lines.append("")
    return "\n".join(lines)


def _render_changelog_bullet(item: str, *, pr_number: int | None) -> str:
    bullet = item if item.startswith("- ") else f"- {item}"
    if pr_number is None or _mentions_pr(bullet, pr_number):
        return bullet
    return f"{bullet} (#{pr_number})"


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


def prepare_release_files() -> tuple[list[Release], dict[Path, int]]:
    releases = compute_releases()
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


def release() -> int:
    _ensure_clean_tree()
    branch = _create_release_branch()
    releases, _pr_numbers = prepare_release_files()
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
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    branch = f"{username}/release-{timestamp}"
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
        subprocess.check_call(["git", "commit", "-v", "--template", str(message_path)], cwd=ROOT)
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
            ["git", "log", "--full-history", "--format=%s", "--", str(path.relative_to(ROOT))],
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


def check_fragments(base: str | None = None) -> int:
    packages_by_name = workspace.packages()
    fragments = parse_fragments(set(packages_by_name))
    head = os.environ.get("WORKSPACE_POE_GIT_COMMIT")
    if base is None:
        base = _default_base_ref()
    if base is None:
        print("Could not detect a base branch for news fragment enforcement.")
        return 1

    changed = _changed_packages(packages_by_name, base=base, head=head, code_only=True)
    changed -= _release_prepped_packages(packages_by_name, base=base, head=head)
    packages_with_fragments = {fragment.package for fragment in fragments}
    missing = sorted(changed - packages_with_fragments)
    if missing:
        packages = ", ".join(missing)
        print(f"Missing news fragments for changed packages: {packages}")
        print("Add changes/<package>/<id>.<type>.md or adjust the changed files.")
        return 1
    return 0


def changed_packages(base: str = "HEAD^", head: str = "HEAD") -> list[str]:
    packages_by_name = workspace.packages()
    changed = _changed_packages(packages_by_name, base=base, head=head, code_only=False)
    return [name for name in workspace.topological_names(packages_by_name) if name in changed]


def print_changed_packages(args: argparse.Namespace) -> int:
    for name in changed_packages(base=args.base, head=args.head):
        print(name)
    return 0


def _changed_packages(
    packages_by_name: dict[str, workspace.Package],
    *,
    base: str,
    head: str | None,
    code_only: bool,
) -> set[str]:
    changed_paths = _changed_paths(base=base, head=head)
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


def _release_prepped_packages(
    packages_by_name: dict[str, workspace.Package], *, base: str, head: str | None
) -> set[str]:
    changed_paths = _changed_paths(base=base, head=head)
    return {
        name for name, package in packages_by_name.items() if package.version_file in changed_paths
    }


def _changed_paths(*, base: str, head: str | None) -> set[Path]:
    diff_range = [f"{base}..{head}"] if head is not None else [base]
    output = subprocess.check_output(
        ["git", "diff", "--name-only", *diff_range], cwd=ROOT, text=True
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
    subparsers.add_parser("release").set_defaults(func=lambda _args: release())
    changed_parser = subparsers.add_parser("changed")
    changed_parser.add_argument("--base", default="HEAD^")
    changed_parser.add_argument("--head", default="HEAD")
    changed_parser.set_defaults(func=print_changed_packages)

    github_release_body_parser = subparsers.add_parser("github-release-body")
    github_release_body_parser.add_argument("package")
    github_release_body_parser.set_defaults(func=print_github_release_body)

    check_parser = subparsers.add_parser("check-news-fragments")
    check_parser.add_argument("--base")
    check_parser.set_defaults(func=lambda args: check_fragments(args.base))

    subparsers.add_parser("lint-towncrier").set_defaults(func=lambda _args: lint_towncrier())
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
