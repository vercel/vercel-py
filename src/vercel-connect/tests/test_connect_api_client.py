"""Api-client level tests over a fake transport.

Covers serialization, path encoding, and malformed-response handling without a
network layer. Connector UIDs contain `/`, so percent-encoding is asserted on
every endpoint: the TypeScript suite never tests it.
"""

import json
from datetime import timedelta

import httpx
import pytest

from vercel._internal.core.http import BaseTransport, JSONBody, ReadResponsePolicy, RequestBody
from vercel._internal.core.http.transport import HeaderTypes, QueryParamTypes
from vercel.connect import (
    ConnectAppTokenSubject,
    ConnectResponseError,
    ConnectUserTokenSubject,
)
from vercel.connect._internal.api_client import ConnectApiClient


class FakeTransport(BaseTransport):
    """Records requests and replays canned responses."""

    def __init__(self, *responses: httpx.Response) -> None:
        self.requests: list[httpx.Request] = []
        self._responses = list(responses) or [httpx.Response(200, json={})]

    async def send(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: QueryParamTypes | None = None,
        body: RequestBody = None,
        headers: HeaderTypes | None = None,
        timeout: timedelta | None = None,
        follow_redirects: bool | None = None,
        stream: bool = False,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NEVER,
    ) -> httpx.Response:
        content = b""
        if body is not None:
            data = getattr(body, "data", None)
            content = data if isinstance(data, bytes) else json.dumps(data).encode()
        merged = httpx.Headers(headers)
        if token is not None:
            merged["authorization"] = f"Bearer {token}"
        if isinstance(body, JSONBody) and "content-type" not in merged:
            merged["content-type"] = "application/json"
        request = httpx.Request(method, path, params=params, headers=merged, content=content)
        self.requests.append(request)
        response = self._responses.pop(0) if self._responses else httpx.Response(200, json={})
        response.request = request
        return response


def client(*responses: httpx.Response) -> tuple[ConnectApiClient, FakeTransport]:
    transport = FakeTransport(*responses)

    async def credentials_factory() -> str:
        return "oidc-token"

    api_client = ConnectApiClient(
        base_url="https://connect.test",
        credentials_factory=credentials_factory,
        transport=transport,
        timeout=timedelta(seconds=30),
    )
    return api_client, transport


def token_response() -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "token": "upstream",
            "expiresAt": 1_800_000_000_000,
            "connector": {"id": "scl_1", "uid": "slack/my-bot", "type": "slack"},
        },
    )


@pytest.mark.parametrize(
    ("connector", "expected"),
    [
        ("scl_123", "scl_123"),
        ("slack/my-bot", "slack%2Fmy-bot"),
        ("oauth/linear", "oauth%2Flinear"),
        ("../../etc/passwd", "..%2F..%2Fetc%2Fpasswd"),
        ("has space", "has%20space"),
        ("emoji/✨", "emoji%2F%E2%9C%A8"),
        ("q?a=b#c", "q%3Fa%3Db%23c"),
    ],
)
async def test_create_token_percent_encodes_the_connector(connector: str, expected: str) -> None:
    api_client, transport = client(token_response())

    await api_client.create_token(
        connector, subject=ConnectAppTokenSubject(), vercel_token="oidc-token"
    )

    assert transport.requests[0].url.raw_path.decode() == f"/v1/connect/token/{expected}"


async def test_revoke_token_percent_encodes_the_connector() -> None:
    api_client, transport = client(httpx.Response(204, content=b""))

    await api_client.revoke_token(
        "slack/my-bot", subject=ConnectAppTokenSubject(), vercel_token="oidc-token"
    )

    request = transport.requests[0]
    assert request.method == "DELETE"
    assert request.url.raw_path.decode() == "/v1/connect/connectors/slack%2Fmy-bot/tokens"


async def test_create_authorization_percent_encodes_the_connector() -> None:
    api_client, transport = client(
        httpx.Response(200, json={"url": "https://c.example", "request": "sca_1", "verifier": "v"})
    )

    await api_client.create_authorization(
        "oauth/linear", subject=ConnectAppTokenSubject(), vercel_token="oidc-token"
    )

    assert transport.requests[0].url.raw_path.decode() == "/v1/connect/authorize/oauth%2Flinear"


async def test_get_connector_percent_encodes_the_connector() -> None:
    api_client, transport = client(
        httpx.Response(200, json={"id": "scl_1", "uid": "oauth/linear", "type": "oauth"})
    )

    await api_client.get_connector("oauth/linear", vercel_token="oidc-token")

    request = transport.requests[0]
    assert request.method == "GET"
    assert request.url.raw_path.decode() == "/v1/connect/connectors/oauth%2Flinear"


async def test_requests_send_the_identity_and_user_agent() -> None:
    api_client, transport = client(token_response())

    await api_client.create_token(
        "slack/my-bot", subject=ConnectAppTokenSubject(), vercel_token="explicit-identity"
    )

    headers = transport.requests[0].headers
    assert headers["authorization"] == "Bearer explicit-identity"
    assert headers["user-agent"].startswith("vercel-connect/")
    assert "Python/" in headers["user-agent"]
    assert headers["content-type"].startswith("application/json")


async def test_token_body_uses_camel_case_keys() -> None:
    api_client, transport = client(token_response())

    await api_client.create_token(
        "slack/my-bot",
        subject=ConnectUserTokenSubject(id="u_1"),
        vercel_token="oidc-token",
        scopes=["read"],
        installation_id="T1",
        audience=["aud"],
        resources=["res"],
    )

    assert json.loads(transport.requests[0].content) == {
        "subject": {"type": "user", "id": "u_1"},
        "scopes": ["read"],
        "installationId": "T1",
        "audience": ["aud"],
        "resources": ["res"],
    }


async def test_token_body_omits_unset_fields() -> None:
    api_client, transport = client(token_response())

    await api_client.create_token(
        "slack/my-bot", subject=ConnectAppTokenSubject(), vercel_token="oidc-token"
    )

    assert json.loads(transport.requests[0].content) == {"subject": {"type": "app"}}


async def test_authorization_body_uses_camel_case_keys() -> None:
    api_client, transport = client(
        httpx.Response(200, json={"url": "https://c", "request": "sca_1", "verifier": "v"})
    )

    await api_client.create_authorization(
        "oauth/linear",
        subject=ConnectAppTokenSubject(),
        vercel_token="oidc-token",
        return_url="https://myapp.com/cb",
        webhook="https://myapp.com/hook",
        device_code=True,
        expires_in=timedelta(minutes=5),
    )

    assert json.loads(transport.requests[0].content) == {
        "subject": {"type": "app"},
        "returnUrl": "https://myapp.com/cb",
        "webhook": "https://myapp.com/hook",
        "deviceCode": True,
        "expiresInMs": 300000,
    }


async def test_revoke_accepts_an_empty_success_body() -> None:
    api_client, transport = client(httpx.Response(204, content=b""))

    await api_client.revoke_token(
        "slack/my-bot", subject=ConnectAppTokenSubject(), vercel_token="oidc-token"
    )

    assert len(transport.requests) == 1


async def test_revoke_accepts_a_json_success_body() -> None:
    api_client, transport = client(httpx.Response(200, json={"revoked": 2}))

    await api_client.revoke_token(
        "slack/my-bot", subject=ConnectAppTokenSubject(), vercel_token="oidc-token"
    )

    assert len(transport.requests) == 1


@pytest.mark.parametrize(
    "body",
    [
        b"not json at all",
        b"[]",
        b"null",
        b'{"expiresAt": 1}',
        b'{"token": "t"}',
        b'{"token": "t", "expiresAt": "not-a-number", "connector": {}}',
        b'{"token": "t", "expiresAt": 1, "connector": {"id": "x"}}',
    ],
)
async def test_malformed_token_success_body_raises_response_error(body: bytes) -> None:
    api_client, _ = client(httpx.Response(200, content=body))

    with pytest.raises(ConnectResponseError):
        await api_client.create_token(
            "slack/my-bot", subject=ConnectAppTokenSubject(), vercel_token="oidc-token"
        )


@pytest.mark.parametrize(
    "body",
    [b"not json", b'{"request": "sca_1"}', b'{"url": "https://c", "verifier": "v"}'],
)
async def test_malformed_authorization_success_body_raises_response_error(body: bytes) -> None:
    api_client, _ = client(httpx.Response(200, content=body))

    with pytest.raises(ConnectResponseError):
        await api_client.create_authorization(
            "oauth/linear", subject=ConnectAppTokenSubject(), vercel_token="oidc-token"
        )


@pytest.mark.parametrize("body", [b"not json", b"[]", b'{"uid": "oauth/linear"}'])
async def test_malformed_metadata_success_body_raises_response_error(body: bytes) -> None:
    api_client, _ = client(httpx.Response(200, content=body))

    with pytest.raises(ConnectResponseError):
        await api_client.get_connector("oauth/linear", vercel_token="oidc-token")


async def test_returned_state_is_neutral_of_wire_concerns() -> None:
    api_client, _ = client(token_response())

    state = await api_client.create_token(
        "slack/my-bot", subject=ConnectAppTokenSubject(), vercel_token="oidc-token"
    )

    assert state.token == "upstream"
    assert state.expires_at.tzinfo is not None
    assert state.connector.uid == "slack/my-bot"


async def test_base_url_trailing_slash_is_normalized() -> None:
    transport = FakeTransport(token_response())

    async def credentials_factory() -> str:
        return "oidc-token"

    api_client = ConnectApiClient(
        base_url="https://connect.test/",
        credentials_factory=credentials_factory,
        transport=transport,
        timeout=timedelta(seconds=30),
    )
    await api_client.create_token(
        "slack/my-bot", subject=ConnectAppTokenSubject(), vercel_token="oidc-token"
    )

    assert str(transport.requests[0].url) == "https://connect.test/v1/connect/token/slack%2Fmy-bot"
