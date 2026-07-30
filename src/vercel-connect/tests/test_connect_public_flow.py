"""Session wiring, option seams, timeouts, and cancellation."""

import asyncio

import httpx
import pytest
import respx
from conftest import TEST_BASE_URL, session_options

from vercel.api import session
from vercel.connect import (
    ConnectAppTokenSubject,
    ConnectServiceOptions,
    get_token,
    sync as connect_sync,
)
from vercel.connect._internal.service import get_connect_service
from vercel.errors import VercelError, VercelSessionClosedError

TOKEN_URL = f"{TEST_BASE_URL}/v1/connect/token/slack%2Fmy-bot"


def token_route() -> respx.Route:
    return respx.post(TOKEN_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "token": "upstream",
                "expiresAt": 1_800_000_000_000,
                "connector": {"id": "scl_1", "uid": "slack/my-bot", "type": "slack"},
            },
        )
    )


def test_default_service_options_target_the_public_api() -> None:
    options = ConnectServiceOptions()

    assert options.base_url == "https://api.vercel.com"
    assert options.token_cache_size == 100
    assert options.validity_buffer.total_seconds() == 30
    assert options.oidc_issuer == "https://oidc.vercel.com"


def test_service_options_are_overridable() -> None:
    async def credentials_factory() -> str:
        return "t"

    options = ConnectServiceOptions(
        base_url="https://staging.example.com",
        credentials_factory=credentials_factory,
        token_cache_size=5,
    )

    assert options.base_url == "https://staging.example.com"
    assert options.credentials_factory is credentials_factory
    assert options.token_cache_size == 5


def test_service_options_are_frozen() -> None:
    options = ConnectServiceOptions()

    with pytest.raises((AttributeError, TypeError)):
        options.base_url = "https://elsewhere.example.com"  # type: ignore[misc]


@respx.mock
async def test_service_is_cached_per_session(mock_env_clear: None) -> None:
    from vercel._internal.core.session import get_active_session

    async with session(service_options=session_options()):
        first = get_connect_service(get_active_session())
        second = get_connect_service(get_active_session())
        assert first is second


@respx.mock
async def test_async_surface_rejects_a_sync_session(mock_env_clear: None) -> None:
    token_route()

    with pytest.raises(VercelError):
        with session(service_options=session_options()):
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())


@respx.mock
async def test_calls_after_session_close_are_rejected(mock_env_clear: None) -> None:
    token_route()

    from vercel._internal.core.session import get_active_session

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
        active = get_active_session()

    with pytest.raises(VercelSessionClosedError):
        active.check_open()


@respx.mock
async def test_credentials_failure_surfaces_as_credentials_error(mock_env_clear: None) -> None:
    from vercel.connect import ConnectCredentialsError

    async def failing_factory() -> str:
        raise ConnectCredentialsError("no token available")

    token_route()

    async with session(
        service_options=[
            ConnectServiceOptions(base_url=TEST_BASE_URL, credentials_factory=failing_factory)
        ]
    ):
        with pytest.raises(ConnectCredentialsError):
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())


@respx.mock
async def test_timeout_propagates_async(mock_env_clear: None) -> None:
    respx.post(TOKEN_URL).mock(side_effect=httpx.TimeoutException("timed out"))

    async with session(service_options=session_options()):
        with pytest.raises(httpx.TimeoutException):
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())


@respx.mock
def test_timeout_propagates_sync(mock_env_clear: None) -> None:
    respx.post(TOKEN_URL).mock(side_effect=httpx.TimeoutException("timed out"))

    with session(service_options=session_options()):
        with pytest.raises(httpx.TimeoutException):
            connect_sync.get_token("slack/my-bot", subject=ConnectAppTokenSubject())


@respx.mock
async def test_network_error_propagates(mock_env_clear: None) -> None:
    respx.post(TOKEN_URL).mock(side_effect=httpx.ConnectError("refused"))

    async with session(service_options=session_options()):
        with pytest.raises(httpx.ConnectError):
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())


@respx.mock
async def test_cancellation_propagates(mock_env_clear: None) -> None:
    async def slow(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(10)
        return httpx.Response(200, json={})

    respx.post(TOKEN_URL).mock(side_effect=slow)

    async with session(service_options=session_options()):
        task = asyncio.ensure_future(get_token("slack/my-bot", subject=ConnectAppTokenSubject()))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@respx.mock
async def test_sync_and_async_share_the_same_wire_behaviour(mock_env_clear: None) -> None:
    route = token_route()

    async with session(service_options=session_options()):
        async_token = await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
    async_body = route.calls.last.request.content

    # A sync session, entered only after the async one has exited.
    with session(service_options=session_options()):
        sync_token = connect_sync.get_token("slack/my-bot", subject=ConnectAppTokenSubject())
    sync_body = route.calls.last.request.content

    assert async_token == sync_token
    assert async_body == sync_body


@respx.mock
async def test_public_surface_works_without_an_explicit_session(
    mock_env_clear: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """There is an implicit default session, so `session()` is optional."""

    async def fake_oidc_token() -> str:
        return "env-oidc-token"

    monkeypatch.setattr("vercel.oidc.aio.get_vercel_oidc_token", fake_oidc_token)
    route = respx.post("https://api.vercel.com/v1/connect/token/slack%2Fmy-bot").mock(
        return_value=httpx.Response(
            200,
            json={
                "token": "upstream",
                "expiresAt": 1_800_000_000_000,
                "connector": {"id": "scl_1", "uid": "slack/my-bot", "type": "slack"},
            },
        )
    )

    token = await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert token == "upstream"
    assert route.calls.last.request.headers["authorization"] == "Bearer env-oidc-token"


@respx.mock
def test_sync_surface_tolerates_a_suspending_default_refresh(
    mock_env_clear: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default resolver must not suspend: `iter_coroutine` cannot allow it.

    The async resolver awaits an HTTP refresh on the local-dev path, so a sync
    session has to use the synchronous one or every call fails when a token
    needs refreshing.
    """
    token_route()

    async def suspending_async_resolver() -> str:
        await asyncio.sleep(0)
        return "should-not-be-used"

    def sync_resolver() -> str:
        return "sync-oidc-token"

    monkeypatch.setattr("vercel.oidc.aio.get_vercel_oidc_token", suspending_async_resolver)
    monkeypatch.setattr("vercel.oidc.get_vercel_oidc_token", sync_resolver)

    with session(service_options=[ConnectServiceOptions(base_url=TEST_BASE_URL)]):
        token = connect_sync.get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert token == "upstream"


@respx.mock
async def test_async_surface_uses_the_async_default_resolver(
    mock_env_clear: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    route = token_route()

    async def async_resolver() -> str:
        await asyncio.sleep(0)
        return "async-oidc-token"

    monkeypatch.setattr("vercel.oidc.aio.get_vercel_oidc_token", async_resolver)

    async with session(service_options=[ConnectServiceOptions(base_url=TEST_BASE_URL)]):
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert route.calls.last.request.headers["authorization"] == "Bearer async-oidc-token"


@respx.mock
async def test_empty_vercel_token_is_rejected(mock_env_clear: None) -> None:
    from vercel.connect import ConnectCredentialsError, ConnectOptions

    token_route()

    async with session(service_options=session_options()):
        with pytest.raises(ConnectCredentialsError, match="empty string"):
            await get_token(
                "slack/my-bot",
                subject=ConnectAppTokenSubject(),
                options=ConnectOptions(vercel_token=""),
            )


def test_unset_credentials_factory_defers_to_the_session_mode() -> None:
    assert ConnectServiceOptions().credentials_factory is None
