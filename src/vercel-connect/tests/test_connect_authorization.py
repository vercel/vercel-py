"""Authorization flow, URL validation, and detached interactive auth."""

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pytest
import respx
from conftest import TEST_BASE_URL, session_options

from vercel.api import session
from vercel.connect import (
    ConnectApiError,
    ConnectAppTokenSubject,
    ConnectOptions,
    ConnectTokenExchangeSubject,
    ConnectUserTokenSubject,
    ConnectValidationError,
    start_authorization,
    sync as connect_sync,
)

AUTHORIZE_URL = f"{TEST_BASE_URL}/v1/connect/authorize/linear%2Fmy-app"
EXPIRES_AT_MS = 1_800_000_000_000


def authorization_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "url": "https://connect.vercel.com/oauth/authorize?request=sca_123",
        "request": "sca_123",
        "verifier": "verifier-abc",
        "expiresAt": EXPIRES_AT_MS,
        "connector": {
            "id": "scl_123",
            "uid": "linear/my-app",
            "type": "linear",
            "name": "Linear",
            "service": "linear",
            "serviceName": "Linear",
        },
    }
    payload.update(overrides)
    return payload


def authorize_route(**overrides: Any) -> respx.Route:
    return respx.post(AUTHORIZE_URL).mock(
        return_value=httpx.Response(200, json=authorization_payload(**overrides))
    )


@respx.mock
async def test_start_authorization_returns_consent_url_async(mock_env_clear: None) -> None:
    route = authorize_route()

    async with session(service_options=session_options()):
        result = await start_authorization(
            "linear/my-app",
            subject=ConnectUserTokenSubject(id="u_1"),
            return_url="https://myapp.com/cb",
        )

    assert result.url == "https://connect.vercel.com/oauth/authorize?request=sca_123"
    assert result.request == "sca_123"
    assert result.verifier == "verifier-abc"
    assert result.device_code is None
    assert result.expires_at is not None
    assert result.expires_at.tzinfo is not None
    assert result.connector is not None
    assert result.connector.uid == "linear/my-app"
    assert result.connector.service_name == "Linear"

    assert json.loads(route.calls.last.request.content) == {
        "subject": {"type": "user", "id": "u_1"},
        "returnUrl": "https://myapp.com/cb",
    }


@respx.mock
def test_start_authorization_returns_consent_url_sync(mock_env_clear: None) -> None:
    route = authorize_route()

    with session(service_options=session_options()):
        result = connect_sync.start_authorization(
            "linear/my-app",
            subject=ConnectUserTokenSubject(id="u_1"),
            return_url="https://myapp.com/cb",
        )

    assert result.request == "sca_123"
    assert json.loads(route.calls.last.request.content)["returnUrl"] == "https://myapp.com/cb"


@respx.mock
async def test_start_authorization_serializes_full_body(mock_env_clear: None) -> None:
    route = authorize_route()

    async with session(service_options=session_options()):
        await start_authorization(
            "linear/my-app",
            subject=ConnectUserTokenSubject(id="u_1"),
            scopes=["read"],
            installation_id="T1",
            return_url="https://myapp.com/cb",
            webhook="https://myapp.com/hook",
            device_code=True,
            expires_in=timedelta(minutes=10),
        )

    assert json.loads(route.calls.last.request.content) == {
        "subject": {"type": "user", "id": "u_1"},
        "scopes": ["read"],
        "installationId": "T1",
        "returnUrl": "https://myapp.com/cb",
        "webhook": "https://myapp.com/hook",
        "deviceCode": True,
        "expiresInMs": 600000,
    }


@respx.mock
async def test_start_authorization_accepts_seconds_for_expires_in(mock_env_clear: None) -> None:
    route = authorize_route()

    async with session(service_options=session_options()):
        await start_authorization("linear/my-app", subject=ConnectAppTokenSubject(), expires_in=90)

    assert json.loads(route.calls.last.request.content)["expiresInMs"] == 90000


@respx.mock
async def test_start_authorization_surfaces_device_code(mock_env_clear: None) -> None:
    authorize_route(deviceCode="WXYZ-1234")

    async with session(service_options=session_options()):
        result = await start_authorization(
            "linear/my-app", subject=ConnectAppTokenSubject(), device_code=True
        )

    assert result.device_code == "WXYZ-1234"


@respx.mock
async def test_start_authorization_tolerates_absent_optional_fields(mock_env_clear: None) -> None:
    authorize_route(expiresAt=None, connector=None)

    async with session(service_options=session_options()):
        result = await start_authorization("linear/my-app", subject=ConnectAppTokenSubject())

    assert result.expires_at is None
    assert result.connector is None


@respx.mock
async def test_start_authorization_posts_token_exchange_subject(mock_env_clear: None) -> None:
    route = authorize_route()

    async with session(service_options=session_options()):
        await start_authorization(
            "linear/my-app", subject=ConnectTokenExchangeSubject(token="inbound")
        )

    assert json.loads(route.calls.last.request.content)["subject"] == {
        "type": "token",
        "token": "inbound",
    }


@pytest.mark.parametrize(
    "return_url",
    [
        "https://myapp.com/cb",
        "https://myapp.com:8443/cb",
        "http://localhost/cb",
        "http://localhost:3000/cb",
        "http://app.localhost:3000/cb",
        "http://deep.nested.localhost/cb",
        "http://127.0.0.1:3000/cb",
    ],
)
@respx.mock
async def test_start_authorization_accepts_allowed_return_urls(
    mock_env_clear: None,
    return_url: str,
) -> None:
    authorize_route()

    async with session(service_options=session_options()):
        await start_authorization(
            "linear/my-app", subject=ConnectAppTokenSubject(), return_url=return_url
        )


@pytest.mark.parametrize(
    "return_url",
    [
        "http://myapp.com/cb",
        "http://notlocalhost/cb",
        "http://localhost.evil.com/cb",
        "ftp://myapp.com/cb",
        "javascript:alert(1)",
        "not a url",
        "",
    ],
)
@respx.mock
async def test_start_authorization_rejects_disallowed_return_urls(
    mock_env_clear: None,
    return_url: str,
) -> None:
    route = authorize_route()

    async with session(service_options=session_options()):
        with pytest.raises(ConnectValidationError) as exc_info:
            await start_authorization(
                "linear/my-app", subject=ConnectAppTokenSubject(), return_url=return_url
            )

    assert "127.0.0.1" in str(exc_info.value) or "localhost" in str(exc_info.value)
    assert route.call_count == 0


@pytest.mark.parametrize(
    "webhook",
    ["http://myapp.com/hook", "http://localhost/hook", "ftp://myapp.com/hook", "nonsense"],
)
@respx.mock
async def test_start_authorization_requires_https_webhook(
    mock_env_clear: None,
    webhook: str,
) -> None:
    route = authorize_route()

    async with session(service_options=session_options()):
        with pytest.raises(ConnectValidationError):
            await start_authorization(
                "linear/my-app", subject=ConnectAppTokenSubject(), webhook=webhook
            )

    assert route.call_count == 0


@respx.mock
def test_start_authorization_url_validation_is_shared_with_sync(mock_env_clear: None) -> None:
    with session(service_options=session_options()):
        with pytest.raises(ConnectValidationError):
            connect_sync.start_authorization(
                "linear/my-app",
                subject=ConnectAppTokenSubject(),
                return_url="http://myapp.com/cb",
            )


@respx.mock
async def test_detached_env_defaults_to_device_code(
    mock_env_clear: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL_CONNECT_INTERACTIVE_AUTH_MODE", "detached")
    route = authorize_route(deviceCode="WXYZ-1234")

    async with session(service_options=session_options()):
        await start_authorization("linear/my-app", subject=ConnectAppTokenSubject())

    assert json.loads(route.calls.last.request.content)["deviceCode"] is True


@respx.mock
async def test_detached_env_warns_when_discarding_a_return_url(
    mock_env_clear: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The TypeScript SDK drops the caller's URL silently."""
    monkeypatch.setenv("VERCEL_CONNECT_INTERACTIVE_AUTH_MODE", "detached")
    route = authorize_route(deviceCode="WXYZ-1234")

    async with session(service_options=session_options()):
        with pytest.warns(UserWarning, match="return_url"):
            await start_authorization(
                "linear/my-app",
                subject=ConnectAppTokenSubject(),
                return_url="https://myapp.com/cb",
            )

    body = json.loads(route.calls.last.request.content)
    assert body["deviceCode"] is True
    assert "returnUrl" not in body


@respx.mock
async def test_explicit_device_code_false_overrides_detached_env(
    mock_env_clear: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL_CONNECT_INTERACTIVE_AUTH_MODE", "detached")
    route = authorize_route()

    async with session(service_options=session_options()):
        await start_authorization(
            "linear/my-app", subject=ConnectAppTokenSubject(), device_code=False
        )

    assert json.loads(route.calls.last.request.content)["deviceCode"] is False


@respx.mock
async def test_start_authorization_maps_errors_through_the_shared_parser(
    mock_env_clear: None,
) -> None:
    """The TypeScript SDK throws a bare `Error` here, unlike every sibling."""
    respx.post(AUTHORIZE_URL).mock(
        return_value=httpx.Response(
            403, json={"error": {"code": "forbidden", "message": "not attached"}}
        )
    )

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError) as exc_info:
            await start_authorization("linear/my-app", subject=ConnectAppTokenSubject())

    assert exc_info.value.code == "forbidden"
    assert exc_info.value.status_code == 403


@respx.mock
async def test_start_authorization_uses_explicit_identity(mock_env_clear: None) -> None:
    route = authorize_route()

    async with session(service_options=session_options()):
        await start_authorization(
            "linear/my-app",
            subject=ConnectAppTokenSubject(),
            options=ConnectOptions(vercel_token="explicit"),
        )

    assert route.calls.last.request.headers["authorization"] == "Bearer explicit"


@respx.mock
async def test_expires_at_is_converted_from_epoch_milliseconds(mock_env_clear: None) -> None:
    authorize_route(expiresAt=1_700_000_000_000)

    async with session(service_options=session_options()):
        result = await start_authorization("linear/my-app", subject=ConnectAppTokenSubject())

    assert result.expires_at == datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)
