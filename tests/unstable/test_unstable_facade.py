from __future__ import annotations

from collections.abc import Generator

import pytest

from vercel._internal.unstable.default import (
    DefaultSessionReconfigurationError,
    _default_session_ctx,
    _fallback_session,
)
from vercel._internal.unstable.errors import VercelError
from vercel.unstable import Session, SessionOptions
from vercel.unstable.testing import reset_default_session


@pytest.fixture(autouse=True)
def _reset_default_session() -> Generator[None, None, None]:
    """Reset default-session state before and after each test."""
    reset_default_session()
    yield
    reset_default_session()


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


def test_setup_default_session_reconfigures_before_initialization() -> None:
    from vercel import unstable as vercel

    vercel.setup_default_session(options=SessionOptions(client_pool_size=5))
    session = vercel.get_default_session()

    assert session.options.client_pool_size == 5


def test_setup_default_session_rejects_reconfiguration_after_initialization() -> None:
    from vercel import unstable as vercel

    vercel.setup_default_session(options=SessionOptions(client_pool_size=5))
    _ = vercel.get_default_session()  # triggers initialization

    with pytest.raises(DefaultSessionReconfigurationError) as raised:
        vercel.setup_default_session(options=SessionOptions(client_pool_size=10))

    assert isinstance(raised.value, VercelError)


def test_get_default_session_reuses_fallback_session() -> None:
    from vercel import unstable as vercel

    first = vercel.get_default_session()
    second = vercel.get_default_session()

    assert first is second
    assert isinstance(first, Session)


async def test_get_default_session_prefers_context_local_over_fallback() -> None:
    from vercel import unstable as vercel

    fallback = vercel.get_default_session()
    explicit = Session()

    async with vercel.use_session(explicit):
        current = vercel.get_default_session()
        assert current is explicit
        assert current is not fallback

    # After the context exits, fallback is restored
    assert vercel.get_default_session() is fallback


async def test_use_session_restores_previous_binding_on_nested_exit() -> None:
    from vercel import unstable as vercel

    first = Session()
    second = Session()

    async with vercel.use_session(first):
        assert vercel.get_default_session() is first
        async with vercel.use_session(second):
            assert vercel.get_default_session() is second
        assert vercel.get_default_session() is first


async def test_use_session_does_not_close_bound_session() -> None:
    from vercel import unstable as vercel

    session = Session()

    async with vercel.use_session(session):
        pass

    assert not session._closed


async def test_reset_default_session_clears_fallback_and_context() -> None:
    from vercel import unstable as vercel

    fallback = vercel.get_default_session()
    explicit = Session()

    async with vercel.use_session(explicit):
        reset_default_session()
        assert _default_session_ctx.get() is None

    assert _fallback_session is None
    assert vercel.get_default_session() is not fallback


def test_reset_default_session_is_not_in_unstable_all() -> None:
    from vercel import unstable

    assert "reset_default_session" not in unstable.__all__


async def test_default_bound_sandbox_create_delegates_to_effective_session(
    fake_sandbox_api: object,
) -> None:
    from tests.unstable.fake_sandbox_api import FakeSandboxAPI
    from vercel import unstable as vercel
    from vercel._internal.unstable.sandbox.request_client import _USER_AGENT
    from vercel.unstable.auth import AccessTokenCredentials, StaticCredentialProvider
    from vercel.unstable.sandbox import SandboxCreateParams, SandboxOptions, SandboxStatus

    api = FakeSandboxAPI()
    api.script_response(
        status_code=201,
        json={
            "sandbox": {
                "name": "default-sandbox",
                "persistent": False,
            },
            "session": {
                "id": "sbx_default123",
                "memory": 1024,
                "vcpus": 2,
                "region": "iad1",
                "runtime": "python3.12",
                "timeout": 300000,
                "status": "running",
                "requestedAt": 1,
                "startedAt": 2,
                "cwd": "/vercel/sandbox",
            },
            "routes": [],
        },
    )
    session = vercel.get_default_session()
    session._sandbox_transport = api

    sandbox = await vercel.sandbox.with_options(
        SandboxOptions(
            credential_provider=StaticCredentialProvider(
                AccessTokenCredentials(
                    token="token_default",
                    project_id="project_default",
                    team_id="team_default",
                )
            )
        )
    ).create(
        SandboxCreateParams(runtime="python3.12", ports=[], interactive=False),
        wait=False,
    )

    assert sandbox.name == "default-sandbox"
    assert sandbox.persistent is False
    assert sandbox.current_session is not None
    assert sandbox.current_session.id == "sbx_default123"
    assert sandbox.current_session.status == SandboxStatus.RUNNING
    assert len(api.requests) == 1
    request = api.requests[0]
    assert request.method == "POST"
    assert request.path == "/v2/sandboxes"
    assert request.headers["authorization"] == "Bearer token_default"
    assert request.headers["user-agent"] == _USER_AGENT


def test_default_bound_proxy_with_session_returns_accessor() -> None:
    from vercel import unstable as vercel
    from vercel.unstable.sandbox import SandboxOptions

    explicit = Session()
    accessor = vercel.sandbox.with_options(SandboxOptions(team_id="team_1")).with_session(explicit)

    assert accessor._session is explicit
