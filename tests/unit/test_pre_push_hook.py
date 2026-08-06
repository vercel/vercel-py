from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path

HELPER = Path(__file__).resolve().parents[2] / "scripts/githooks/helpers/pre-push-commit.sh"
ZERO_SHA = "0" * 40


def git(repo: Path, *args: str) -> str:
    return subprocess.check_output(("git", *args), cwd=repo, text=True).strip()


def init_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.check_call(("git", "init", "-q", "-b", "feature"), cwd=repo)
    subprocess.check_call(("git", "config", "user.name", "Test User"), cwd=repo)
    subprocess.check_call(("git", "config", "user.email", "test@example.com"), cwd=repo)
    subprocess.check_call(("git", "config", "commit.gpgsign", "false"), cwd=repo)
    (repo / "file.txt").write_text("content\n", encoding="utf-8")
    subprocess.check_call(("git", "add", "file.txt"), cwd=repo)
    subprocess.check_call(("git", "commit", "-q", "-m", "Initial"), cwd=repo)
    return repo, git(repo, "rev-parse", "HEAD")


def run_helper(repo: Path, stdin: str) -> subprocess.CompletedProcess[str]:
    script = f"""
WORKSPACE_POE_GIT_SCOPE=commit
unset WORKSPACE_POE_GIT_COMMIT WORKSPACE_POE_GIT_BASE
. {shlex.quote(str(HELPER))}
printf '%s\\n' "$WORKSPACE_POE_GIT_COMMIT" "${{WORKSPACE_POE_GIT_BASE:-}}"
"""
    return subprocess.run(
        ("sh", "-c", script),
        cwd=repo,
        env=os.environ.copy(),
        input=stdin,
        text=True,
        capture_output=True,
        check=True,
    )


def test_pre_push_uses_configured_github_merge_base(tmp_path: Path) -> None:
    repo, head = init_repo(tmp_path)
    subprocess.check_call(("git", "update-ref", "refs/remotes/origin/parent", head), cwd=repo)
    subprocess.check_call(("git", "config", "branch.feature.gh-merge-base", "parent"), cwd=repo)

    result = run_helper(repo, f"refs/heads/feature {head} refs/heads/feature {ZERO_SHA}\n")

    assert result.stdout.splitlines() == [head, "origin/parent"]
    assert result.stderr == ""


def test_pre_push_falls_back_when_configured_base_is_unavailable(tmp_path: Path) -> None:
    repo, head = init_repo(tmp_path)
    subprocess.check_call(("git", "config", "branch.feature.gh-merge-base", "missing"), cwd=repo)

    result = run_helper(repo, f"refs/heads/feature {head} refs/heads/feature {ZERO_SHA}\n")

    assert result.stdout.splitlines() == [head, ""]
    assert "falling back to the default base" in result.stderr


def test_pre_push_ignores_branch_deletions(tmp_path: Path) -> None:
    repo, head = init_repo(tmp_path)

    result = run_helper(repo, f"(delete) {ZERO_SHA} refs/heads/feature {head}\n")

    assert result.stdout.splitlines() == [head, ""]
