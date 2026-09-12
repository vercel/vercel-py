from __future__ import annotations

import pytest

from vercel.env import Env, get_env


def test_get_env_from_os_environ(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.setenv("VERCEL_URL", "my-app.vercel.app")
    monkeypatch.setenv("VERCEL_REGION", "iad1")

    env = get_env()

    assert isinstance(env, Env)
    assert env.VERCEL == "1"
    assert env.VERCEL_ENV == "production"
    assert env.VERCEL_URL == "my-app.vercel.app"
    assert env.VERCEL_REGION == "iad1"


def test_get_env_from_custom_mapping() -> None:
    custom_env = {
        "VERCEL": "1",
        "VERCEL_ENV": "preview",
        "VERCEL_DEPLOYMENT_ID": "dpl_test123",
    }

    env = get_env(custom_env)

    assert env.VERCEL == "1"
    assert env.VERCEL_ENV == "preview"
    assert env.VERCEL_DEPLOYMENT_ID == "dpl_test123"


def test_get_env_normalizes_empty_strings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_URL", "")

    env = get_env()

    assert env.VERCEL == "1"
    assert env.VERCEL_URL is None


def test_env_to_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("CI", "true")

    env = get_env()
    env_dict = env.to_dict()

    assert isinstance(env_dict, dict)
    assert env_dict["VERCEL"] == "1"
    assert env_dict["CI"] == "true"


def test_env_getitem(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL_ENV", "development")

    env = get_env()

    assert env["VERCEL_ENV"] == "development"

    with pytest.raises(KeyError):
        _ = env["NONEXISTENT_KEY"]


def test_env_get_with_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL", "1")

    env = get_env()

    assert env.get("VERCEL") == "1"
    assert env.get("NONEXISTENT", "default") == "default"
    assert env.get("VERCEL_URL") is None


def test_env_git_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL_GIT_PROVIDER", "github")
    monkeypatch.setenv("VERCEL_GIT_REPO_SLUG", "my-repo")
    monkeypatch.setenv("VERCEL_GIT_REPO_OWNER", "my-org")
    monkeypatch.setenv("VERCEL_GIT_COMMIT_REF", "main")
    monkeypatch.setenv("VERCEL_GIT_COMMIT_SHA", "abc123")

    env = get_env()

    assert env.VERCEL_GIT_PROVIDER == "github"
    assert env.VERCEL_GIT_REPO_SLUG == "my-repo"
    assert env.VERCEL_GIT_REPO_OWNER == "my-org"
    assert env.VERCEL_GIT_COMMIT_REF == "main"
    assert env.VERCEL_GIT_COMMIT_SHA == "abc123"
