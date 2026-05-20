from __future__ import annotations

import pytest

from vercel._internal.unstable.errors import SessionClosedError, VercelError
from vercel._internal.unstable.sandbox.accessor import (
    SandboxAccessor,
    SyncSandboxAccessor,
)
from vercel.unstable import Session, SyncSession
from vercel.unstable.auth import AccessTokenCredentials, StaticCredentialProvider
from vercel.unstable.sandbox import SandboxCreateParams, SandboxOptions


def test_async_session_sandbox_accessor_identity_is_lazy() -> None:
    session = Session()

    first = session.sandbox
    second = session.sandbox

    assert first is second
    assert isinstance(first, SandboxAccessor)
    assert first._session is session
    assert session._initialized is False
    assert session._settings is None


def test_sync_session_sandbox_accessor_identity_is_lazy() -> None:
    session = SyncSession()

    first = session.sandbox
    second = session.sandbox

    assert first is second
    assert isinstance(first, SyncSandboxAccessor)
    assert first._session is session
    assert session._initialized is False
    assert session._settings is None


def test_distinct_sessions_receive_distinct_sandbox_accessors() -> None:
    assert Session().sandbox is not Session().sandbox
    assert SyncSession().sandbox is not SyncSession().sandbox


def test_async_accessor_with_options_returns_distinct_clone() -> None:
    accessor = Session().sandbox

    clone = accessor.with_options()

    assert clone is not accessor
    assert clone._session is accessor._session
    assert clone.options is accessor.options


def test_sync_accessor_with_options_returns_distinct_clone() -> None:
    accessor = SyncSession().sandbox

    clone = accessor.with_options()

    assert clone is not accessor
    assert clone._session is accessor._session
    assert clone.options is accessor.options


def test_accessor_option_merge_uses_explicit_values_over_parent() -> None:
    parent = Session().sandbox.with_options(
        SandboxOptions(
            api_url="https://api.example.test",
            team_id="team_1",
            project_id="project_1",
            retry_attempts=2,
        )
    )

    clone = parent.with_options(
        SandboxOptions(
            team_id="team_2",
            request_timeout=None,
        )
    )

    assert clone.options == SandboxOptions(
        api_url="https://api.example.test",
        team_id="team_2",
        project_id="project_1",
        retry_attempts=2,
    )


def test_accessor_option_merge_keeps_credential_provider_unresolved() -> None:
    credentials = AccessTokenCredentials(
        token="token",
        project_id="project_1",
        team_id="team_1",
    )
    provider = StaticCredentialProvider(credentials)
    parent = Session().sandbox.with_options(SandboxOptions(team_id="team_1"))

    clone = parent.with_options(SandboxOptions(credential_provider=provider))

    assert clone.options == SandboxOptions(
        team_id="team_1",
        credential_provider=provider,
    )


def test_accessor_with_session_rebinds_to_explicit_async_or_sync_session() -> None:
    options = SandboxOptions(team_id="team_1")
    source = Session().sandbox.with_options(options)
    async_target = Session()
    sync_target = SyncSession()

    async_bound = source.with_session(async_target)
    sync_bound = source.with_session(sync_target)

    assert isinstance(async_bound, SandboxAccessor)
    assert async_bound._session is async_target
    assert async_bound.options == options
    assert isinstance(sync_bound, SyncSandboxAccessor)
    assert sync_bound._session is sync_target
    assert sync_bound.options == options


def test_sync_accessor_with_session_rebinds_to_explicit_async_or_sync_session() -> None:
    options = SandboxOptions(team_id="team_1")
    source = SyncSession().sandbox.with_options(options)
    async_target = Session()
    sync_target = SyncSession()

    async_bound = source.with_session(async_target)
    sync_bound = source.with_session(sync_target)

    assert isinstance(async_bound, SandboxAccessor)
    assert async_bound._session is async_target
    assert async_bound.options == options
    assert isinstance(sync_bound, SyncSandboxAccessor)
    assert sync_bound._session is sync_target
    assert sync_bound.options == options


def test_default_bound_proxy_with_options_merges_without_session_resolution() -> None:
    from vercel import unstable as vercel

    configured = vercel.sandbox.with_options(SandboxOptions(team_id="team_1"))
    clone = configured.with_options(SandboxOptions(project_id="project_1"))

    assert configured is not vercel.sandbox
    assert clone is not configured
    assert clone.options == SandboxOptions(team_id="team_1", project_id="project_1")


def test_default_bound_proxy_with_session_returns_explicit_accessor() -> None:
    from vercel import unstable as vercel

    async_session = Session()
    sync_session = SyncSession()
    proxy = vercel.sandbox.with_options(SandboxOptions(team_id="team_1"))

    async_bound = proxy.with_session(async_session)
    sync_bound = proxy.with_session(sync_session)

    assert isinstance(async_bound, SandboxAccessor)
    assert async_bound._session is async_session
    assert async_bound.options == SandboxOptions(team_id="team_1")
    assert isinstance(sync_bound, SyncSandboxAccessor)
    assert sync_bound._session is sync_session
    assert sync_bound.options == SandboxOptions(team_id="team_1")


async def test_async_accessor_prepare_rejects_closed_session() -> None:
    session = Session()
    accessor = session.sandbox

    await session.aclose()

    with pytest.raises(SessionClosedError) as raised:
        accessor._prepare()

    assert isinstance(raised.value, VercelError)


def test_sync_accessor_prepare_rejects_closed_session() -> None:
    session = SyncSession()
    accessor = session.sandbox

    session.close()

    with pytest.raises(SessionClosedError) as raised:
        accessor._prepare()

    assert isinstance(raised.value, VercelError)


def test_sandbox_create_params_remains_payload_only() -> None:
    params = SandboxCreateParams(runtime="python3.12")

    assert params.runtime == "python3.12"
    assert not hasattr(params, "request_timeout")
