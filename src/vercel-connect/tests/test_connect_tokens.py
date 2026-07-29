"""Token minting and revocation through the public surface, both modes.

Covers the headline API the TypeScript suite never tests directly: the token
endpoint's URL, headers, and request body for all four subject types.
"""

import json
from typing import Any

import httpx
import pytest
import respx
from conftest import TEST_BASE_URL, session_options

from vercel.api import session
from vercel.connect import (
    ConnectApiError,
    ConnectAppTokenSubject,
    ConnectCustomAuthorizationDetail,
    ConnectGitHubAppInstallationAuthorizationDetail,
    ConnectJwtBearerTokenSubject,
    ConnectOptions,
    ConnectTokenExchangeSubject,
    ConnectUserTokenSubject,
    get_token,
    get_token_response,
    revoke_token,
    sync as connect_sync,
)

EXPIRES_AT_MS = 1_800_000_000_000


def token_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "token": "xoxb-upstream",
        "tokenId": "stk_123",
        "expiresAt": EXPIRES_AT_MS,
        "connector": {"id": "scl_123", "uid": "slack/my-bot", "type": "slack"},
        "installationId": "T123",
        "tenantId": "tenant_1",
        "externalSubject": "U456",
        "metadata": {"team": "acme"},
        "claims": {"scope": "chat:write"},
    }
    payload.update(overrides)
    return payload


def token_route(connector: str = "slack%2Fmy-bot", **overrides: Any) -> respx.Route:
    return respx.post(f"{TEST_BASE_URL}/v1/connect/token/{connector}").mock(
        return_value=httpx.Response(200, json=token_payload(**overrides))
    )


@respx.mock
async def test_get_token_returns_credential_async(mock_env_clear: None) -> None:
    route = token_route()

    async with session(service_options=session_options()):
        token = await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert token == "xoxb-upstream"
    request = route.calls.last.request
    assert request.method == "POST"
    assert request.headers["authorization"] == "Bearer oidc-token"
    assert request.headers["user-agent"].startswith("vercel-connect/")
    assert "Python/" in request.headers["user-agent"]
    assert json.loads(request.content) == {"subject": {"type": "app"}}


@respx.mock
def test_get_token_returns_credential_sync(mock_env_clear: None) -> None:
    route = token_route()

    with session(service_options=session_options()):
        token = connect_sync.get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert token == "xoxb-upstream"
    assert json.loads(route.calls.last.request.content) == {"subject": {"type": "app"}}


@respx.mock
async def test_get_token_response_exposes_issuance_metadata_async(mock_env_clear: None) -> None:
    token_route()

    async with session(service_options=session_options()):
        response = await get_token_response("slack/my-bot", subject=ConnectAppTokenSubject())

    assert response.token == "xoxb-upstream"
    assert response.token_id == "stk_123"
    assert response.expires_at.timestamp() == pytest.approx(EXPIRES_AT_MS / 1000)
    assert response.expires_at.tzinfo is not None
    assert response.connector.id == "scl_123"
    assert response.connector.uid == "slack/my-bot"
    assert response.connector.type == "slack"
    assert response.installation_id == "T123"
    assert response.tenant_id == "tenant_1"
    assert response.external_subject == "U456"
    assert response.metadata == {"team": "acme"}
    assert response.claims == {"scope": "chat:write"}


@respx.mock
def test_get_token_response_exposes_issuance_metadata_sync(mock_env_clear: None) -> None:
    token_route()

    with session(service_options=session_options()):
        response = connect_sync.get_token_response("slack/my-bot", subject=ConnectAppTokenSubject())

    assert response.token_id == "stk_123"
    assert response.expires_at.tzinfo is not None
    assert response.connector.uid == "slack/my-bot"


@respx.mock
async def test_get_token_omits_absent_optional_fields(mock_env_clear: None) -> None:
    token_route(tokenId=None, installationId=None, tenantId=None, metadata=None, claims=None)

    async with session(service_options=session_options()):
        response = await get_token_response("slack/my-bot", subject=ConnectAppTokenSubject())

    assert response.token_id is None
    assert response.installation_id is None
    assert response.tenant_id is None
    assert response.metadata is None
    assert response.claims is None


@pytest.mark.parametrize(
    ("subject", "expected"),
    [
        (ConnectAppTokenSubject(), {"type": "app"}),
        (ConnectUserTokenSubject(id="u_1"), {"type": "user", "id": "u_1"}),
        (
            ConnectUserTokenSubject(id="u_1", issuer="https://idp.example.com"),
            {"type": "user", "id": "u_1", "issuer": "https://idp.example.com"},
        ),
        (
            ConnectJwtBearerTokenSubject(sub="u_1"),
            {"type": "jwt-bearer", "sub": "u_1"},
        ),
        (
            ConnectJwtBearerTokenSubject(
                sub="u_1",
                iss="client-id",
                aud="https://provider.example.com/token",
                additional_claims={"tenant": "acme"},
            ),
            {
                "type": "jwt-bearer",
                "sub": "u_1",
                "iss": "client-id",
                "aud": "https://provider.example.com/token",
                "additionalClaims": {"tenant": "acme"},
            },
        ),
        (
            ConnectTokenExchangeSubject(token="inbound-token"),
            {"type": "token", "token": "inbound-token"},
        ),
    ],
)
@respx.mock
async def test_get_token_serializes_every_subject_type(
    mock_env_clear: None,
    subject: Any,
    expected: dict[str, Any],
) -> None:
    route = token_route()

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=subject)

    assert json.loads(route.calls.last.request.content)["subject"] == expected


@respx.mock
async def test_get_token_serializes_full_request_body(mock_env_clear: None) -> None:
    route = token_route()

    async with session(service_options=session_options()):
        await get_token(
            "slack/my-bot",
            subject=ConnectUserTokenSubject(id="u_1"),
            scopes=["chat:write", "channels:read"],
            installation_id="T123",
            audience=["https://api.example.com"],
            resources=["https://api.example.com/v1"],
        )

    assert json.loads(route.calls.last.request.content) == {
        "subject": {"type": "user", "id": "u_1"},
        "scopes": ["chat:write", "channels:read"],
        "installationId": "T123",
        "audience": ["https://api.example.com"],
        "resources": ["https://api.example.com/v1"],
    }


@respx.mock
async def test_get_token_omitted_scopes_request_connector_defaults(mock_env_clear: None) -> None:
    route = token_route()

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert json.loads(route.calls.last.request.content) == {"subject": {"type": "app"}}


@respx.mock
async def test_get_token_serializes_github_authorization_detail(mock_env_clear: None) -> None:
    route = token_route("github%2Fmy-app")

    async with session(service_options=session_options()):
        await get_token(
            "github/my-app",
            subject=ConnectAppTokenSubject(),
            authorization_details=[
                ConnectGitHubAppInstallationAuthorizationDetail(
                    org="acme",
                    permissions=["contents:read"],
                    repositories=["web"],
                )
            ],
        )

    assert json.loads(route.calls.last.request.content)["authorizationDetails"] == [
        {
            "type": "github_app_installation",
            "org": "acme",
            "permissions": ["contents:read"],
            "repositories": ["web"],
        }
    ]


@respx.mock
async def test_get_token_serializes_custom_authorization_detail(mock_env_clear: None) -> None:
    route = token_route("oauth%2Fthing")

    async with session(service_options=session_options()):
        await get_token(
            "oauth/thing",
            subject=ConnectAppTokenSubject(),
            authorization_details=[
                ConnectCustomAuthorizationDetail(
                    type="payment_initiation",
                    details={"instructedAmount": {"currency": "EUR", "amount": "1.00"}},
                )
            ],
        )

    assert json.loads(route.calls.last.request.content)["authorizationDetails"] == [
        {
            "type": "payment_initiation",
            "instructedAmount": {"currency": "EUR", "amount": "1.00"},
        }
    ]


@respx.mock
async def test_get_token_never_sends_client_only_validity_buffer(mock_env_clear: None) -> None:
    route = token_route()

    async with session(service_options=session_options()):
        await get_token(
            "slack/my-bot",
            subject=ConnectAppTokenSubject(),
            options=ConnectOptions(validity_buffer=90),
        )

    body = json.loads(route.calls.last.request.content)
    assert "validityBufferMs" not in body
    assert "validityBuffer" not in body
    assert "validity_buffer_ms" not in body


@respx.mock
async def test_get_token_uses_explicit_vercel_token(mock_env_clear: None) -> None:
    route = token_route()

    async with session(service_options=session_options()):
        await get_token(
            "slack/my-bot",
            subject=ConnectAppTokenSubject(),
            options=ConnectOptions(vercel_token="explicit-token"),
        )

    assert route.calls.last.request.headers["authorization"] == "Bearer explicit-token"


@respx.mock
async def test_get_token_resolves_callable_vercel_token(mock_env_clear: None) -> None:
    route = token_route()
    calls: list[int] = []

    def resolve() -> str:
        calls.append(1)
        return f"rotating-{len(calls)}"

    async with session(service_options=session_options()):
        await get_token(
            "slack/my-bot",
            subject=ConnectAppTokenSubject(),
            options=ConnectOptions(vercel_token=resolve),
        )

    assert route.calls.last.request.headers["authorization"] == "Bearer rotating-1"


@respx.mock
async def test_get_token_resolves_async_callable_vercel_token(mock_env_clear: None) -> None:
    route = token_route()

    async def resolve() -> str:
        return "awaited-token"

    async with session(service_options=session_options()):
        await get_token(
            "slack/my-bot",
            subject=ConnectAppTokenSubject(),
            options=ConnectOptions(vercel_token=resolve),
        )

    assert route.calls.last.request.headers["authorization"] == "Bearer awaited-token"


@respx.mock
async def test_revoke_token_posts_subject_async(mock_env_clear: None) -> None:
    route = respx.delete(f"{TEST_BASE_URL}/v1/connect/connectors/slack%2Fmy-bot/tokens").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )

    async with session(service_options=session_options()):
        await revoke_token(
            "slack/my-bot",
            subject=ConnectUserTokenSubject(id="u_1"),
            installation_id="T123",
        )

    request = route.calls.last.request
    assert request.method == "DELETE"
    assert json.loads(request.content) == {
        "subject": {"type": "user", "id": "u_1"},
        "installationId": "T123",
    }


@respx.mock
def test_revoke_token_posts_subject_sync(mock_env_clear: None) -> None:
    route = respx.delete(f"{TEST_BASE_URL}/v1/connect/connectors/slack%2Fmy-bot/tokens").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )

    with session(service_options=session_options()):
        connect_sync.revoke_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert json.loads(route.calls.last.request.content) == {"subject": {"type": "app"}}


@respx.mock
async def test_revoke_token_accepts_empty_body(mock_env_clear: None) -> None:
    respx.delete(f"{TEST_BASE_URL}/v1/connect/connectors/slack%2Fmy-bot/tokens").mock(
        return_value=httpx.Response(204, content=b"")
    )

    async with session(service_options=session_options()):
        await revoke_token("slack/my-bot", subject=ConnectAppTokenSubject())


@respx.mock
async def test_revoke_token_maps_errors(mock_env_clear: None) -> None:
    respx.delete(f"{TEST_BASE_URL}/v1/connect/connectors/slack%2Fmy-bot/tokens").mock(
        return_value=httpx.Response(403, json={"error": {"code": "forbidden", "message": "nope"}})
    )

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError) as exc_info:
            await revoke_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert exc_info.value.code == "forbidden"
    assert exc_info.value.status_code == 403


@respx.mock
async def test_revoke_token_falls_back_to_resolved_identity(mock_env_clear: None) -> None:
    route = respx.delete(f"{TEST_BASE_URL}/v1/connect/connectors/slack%2Fmy-bot/tokens").mock(
        return_value=httpx.Response(204, content=b"")
    )

    async with session(service_options=session_options(token="from-factory")):
        await revoke_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert route.calls.last.request.headers["authorization"] == "Bearer from-factory"
