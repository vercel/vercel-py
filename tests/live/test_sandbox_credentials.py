"""Credential gating tests for sandbox live tests."""

from tests.live.conftest import has_sandbox_credentials


def test_sandbox_credentials_accept_oidc_without_team_or_project(
    mock_env_clear,
    monkeypatch,
):
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", "oidc-token")

    assert has_sandbox_credentials()


def test_sandbox_credentials_require_team_and_project_for_vercel_token(
    mock_env_clear,
    monkeypatch,
):
    monkeypatch.setenv("VERCEL_TOKEN", "vercel-token")

    assert not has_sandbox_credentials()

    monkeypatch.setenv("VERCEL_TEAM_ID", "team_123")
    assert not has_sandbox_credentials()

    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_123")
    assert has_sandbox_credentials()
