from __future__ import annotations

from dataclasses import fields
from datetime import timedelta
from typing import Any

from hypothesis import given, strategies as st

from vercel._internal.unstable.sandbox.accessor import merge_sandbox_options
from vercel.unstable import Session, SyncSession
from vercel.unstable.auth import AccessTokenCredentials, StaticCredentialProvider
from vercel.unstable.sandbox import SandboxCreateParams, SandboxOptions

_PROVIDERS = (
    StaticCredentialProvider(
        AccessTokenCredentials(token="token_a", project_id="project_a", team_id="team_a")
    ),
    StaticCredentialProvider(
        AccessTokenCredentials(token="token_b", project_id="project_b", team_id="team_b")
    ),
)


@st.composite
def sandbox_options(draw: Any) -> SandboxOptions:
    return SandboxOptions(
        api_url=draw(
            st.one_of(st.none(), st.sampled_from(["https://api.a.test", "https://api.b.test"]))
        ),
        team_id=draw(st.one_of(st.none(), st.sampled_from(["team_a", "team_b"]))),
        project_id=draw(st.one_of(st.none(), st.sampled_from(["project_a", "project_b"]))),
        request_timeout=draw(
            st.one_of(
                st.none(),
                st.timedeltas(min_value=timedelta(seconds=1), max_value=timedelta(days=1)),
            )
        ),
        retry_attempts=draw(st.one_of(st.none(), st.integers(min_value=0, max_value=5))),
        credential_provider=draw(st.one_of(st.none(), st.sampled_from(_PROVIDERS))),
    )


def _options_values(options: SandboxOptions | None) -> dict[str, Any]:
    source = options or SandboxOptions()
    return {field.name: getattr(source, field.name) for field in fields(SandboxOptions)}


def test_session_sandbox_accessor_is_cached() -> None:
    session = Session()
    sync_session = SyncSession()

    assert session.sandbox is session.sandbox
    assert sync_session.sandbox is sync_session.sandbox


def test_distinct_sessions_receive_distinct_sandbox_accessors() -> None:
    assert Session().sandbox is not Session().sandbox
    assert SyncSession().sandbox is not SyncSession().sandbox


def test_default_bound_proxy_with_options_merges_without_session_resolution() -> None:
    from vercel import unstable as vercel

    configured = vercel.sandbox.with_options(SandboxOptions(team_id="team_1"))
    clone = configured.with_options(SandboxOptions(project_id="project_1"))

    assert configured is not vercel.sandbox
    assert clone is not configured
    assert clone.options == SandboxOptions(team_id="team_1", project_id="project_1")


@given(st.one_of(st.none(), sandbox_options()))
def test_merge_sandbox_options_none_override_returns_base(
    base: SandboxOptions | None,
) -> None:
    assert merge_sandbox_options(base, None) is base


@given(st.one_of(st.none(), sandbox_options()), sandbox_options())
def test_merge_sandbox_options_non_none_override_fields_win(
    base: SandboxOptions | None,
    override: SandboxOptions,
) -> None:
    merged = merge_sandbox_options(base, override)

    expected = _options_values(base)
    for field in fields(SandboxOptions):
        override_value = getattr(override, field.name)
        if override_value is not None:
            expected[field.name] = override_value

    assert merged == SandboxOptions(**expected)


def test_sandbox_create_params_remains_payload_only() -> None:
    params = SandboxCreateParams(runtime="python3.13")

    assert params.runtime == "python3.13"
    assert not hasattr(params, "request_timeout")
