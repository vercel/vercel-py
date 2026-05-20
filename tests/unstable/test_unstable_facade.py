from __future__ import annotations


def test_unstable_facade_imports_without_credentials_or_runtime(mock_env_clear: None) -> None:
    _ = mock_env_clear

    from vercel import unstable as vercel

    assert vercel.Session is not None
    assert vercel.SyncSession is not None
    assert vercel.SessionOptions(client_pool_size=5) is not None
    assert vercel.sandbox is vercel.sandbox


def test_unstable_facade_curates_top_level_exports() -> None:
    from vercel import unstable as vercel

    assert set(vercel.__all__) == {
        "Session",
        "SessionOptions",
        "SyncSession",
        "VercelError",
        "get_default_session",
        "sandbox",
        "setup_default_session",
        "use_session",
    }
    assert "reset_default_session" not in vercel.__all__
    assert "SandboxCreateParams" not in vercel.__all__


def test_unstable_domain_and_auth_exports_are_importable() -> None:
    from vercel.unstable.auth import (
        AccessTokenCredentials,
        OIDCCredentials,
        StaticCredentialProvider,
    )
    from vercel.unstable.sandbox import SandboxCreateParams, SandboxOptions, SandboxStatus

    access_token = AccessTokenCredentials(token="token", team_id="team_123")
    oidc = OIDCCredentials(token="oidc")

    assert StaticCredentialProvider(access_token).credentials is access_token
    assert StaticCredentialProvider(oidc).credentials is oidc
    assert SandboxCreateParams(runtime="python3.12").runtime == "python3.12"
    assert SandboxOptions(team_id="team_123").team_id == "team_123"
    assert SandboxStatus.RUNNING.value == "running"


def test_default_bound_sandbox_with_options_is_side_effect_free() -> None:
    from vercel import unstable as vercel
    from vercel.unstable.sandbox import SandboxOptions

    configured = vercel.sandbox.with_options(SandboxOptions(team_id="team_123"))

    assert configured is not vercel.sandbox
    assert configured.options == SandboxOptions(team_id="team_123")
