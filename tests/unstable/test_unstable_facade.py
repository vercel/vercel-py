from __future__ import annotations


def test_unstable_domain_and_auth_types_construct_without_credentials() -> None:
    from vercel.unstable.auth import (
        AccessTokenCredentials,
        OIDCCredentials,
        StaticCredentialProvider,
    )
    from vercel.unstable.sandbox import SandboxCreateParams, SandboxOptions, SandboxStatus

    access_token = AccessTokenCredentials(
        token="token",
        project_id="prj_123",
        team_id="team_123",
    )
    oidc = OIDCCredentials(
        token="oidc",
        project_id="prj_123",
        team_id="team_123",
    )

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
