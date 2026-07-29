"""Inbound trigger verification through the Connect surface.

The Connect layer only extracts the bearer credential and delegates to
`vercel.oidc` verification; the cryptographic vectors live in
`src/vercel-oidc/tests/test_verify.py`.
"""

from typing import Any

import pytest
from conftest import session_options

from vercel.api import session
from vercel.connect import (
    ConnectWebhookVerificationError,
    create_connect_webhook_verifier,
    sync as connect_sync,
    verify_connect_webhook,
)

VERIFIED_CLAIMS: dict[str, Any] = {
    "iss": "https://oidc.vercel.com",
    "sub": "owner:acme:project:my-app:environment:production",
    "aud": "https://vercel.com/acme",
    "owner_id": "team_123",
    "project_id": "prj_123",
    "environment": "production",
    "iat": 1_700_000_000,
    "exp": 1_700_003_600,
}


@pytest.fixture
def fake_verifier(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Record verification calls instead of performing real crypto."""
    calls: list[dict[str, Any]] = []

    def record(token: str, **kwargs: Any) -> dict[str, Any]:
        calls.append({"token": token, **kwargs})
        if token == "invalid":
            from vercel.oidc.verify import VercelOidcVerificationError

            raise VercelOidcVerificationError("signature did not verify")
        return dict(VERIFIED_CLAIMS)

    async def fake_verify_async(token: str, **kwargs: Any) -> dict[str, Any]:
        return record(token, **kwargs)

    # The sync surface uses the sync verifier and the async surface the async one,
    # so both are substituted to keep this test mode-agnostic.
    monkeypatch.setattr("vercel.oidc.verify.verify_vercel_oidc_token_async", fake_verify_async)
    monkeypatch.setattr("vercel.oidc.verify.verify_vercel_oidc_token", record)
    return calls


async def test_verifies_a_bearer_token_async(
    mock_env_clear: None,
    fake_verifier: list[dict[str, Any]],
) -> None:
    async with session(service_options=session_options()):
        claims = await verify_connect_webhook({"Authorization": "Bearer inbound-token"})

    assert fake_verifier[0]["token"] == "inbound-token"
    assert claims.project_id == "prj_123"
    assert claims.environment == "production"
    assert claims.owner_id == "team_123"
    assert claims.issuer == "https://oidc.vercel.com"
    assert claims.subject == VERIFIED_CLAIMS["sub"]
    assert claims.audience == ["https://vercel.com/acme"]
    assert claims.issued_at is not None
    assert claims.expires_at is not None
    assert claims.claims == VERIFIED_CLAIMS


def test_verifies_a_bearer_token_sync(
    mock_env_clear: None,
    fake_verifier: list[dict[str, Any]],
) -> None:
    with session(service_options=session_options()):
        claims = connect_sync.verify_connect_webhook({"Authorization": "Bearer inbound-token"})

    assert claims.project_id == "prj_123"


@pytest.mark.parametrize(
    "header",
    ["Bearer abc", "bearer abc", "BEARER abc", "Bearer   abc   "],
)
async def test_accepts_case_insensitive_scheme_and_trims_whitespace(
    mock_env_clear: None,
    fake_verifier: list[dict[str, Any]],
    header: str,
) -> None:
    async with session(service_options=session_options()):
        await verify_connect_webhook({"Authorization": header})

    assert fake_verifier[0]["token"] == "abc"


@pytest.mark.parametrize(
    "headers",
    [{}, {"Authorization": "Basic abc"}, {"Authorization": ""}, {"Authorization": "Bearer"}],
)
async def test_rejects_missing_or_non_bearer_credentials(
    mock_env_clear: None,
    fake_verifier: list[dict[str, Any]],
    headers: dict[str, str],
) -> None:
    async with session(service_options=session_options()):
        with pytest.raises(ConnectWebhookVerificationError):
            await verify_connect_webhook(headers)

    assert fake_verifier == []


async def test_forwards_expectations_to_the_verifier(
    mock_env_clear: None,
    fake_verifier: list[dict[str, Any]],
) -> None:
    async with session(service_options=session_options()):
        await verify_connect_webhook(
            {"Authorization": "Bearer abc"},
            project_id="prj_other",
            environment="preview",
            owner_id="team_999",
            audience="https://vercel.com/acme",
        )

    call = fake_verifier[0]
    assert call["project_id"] == "prj_other"
    assert call["environment"] == "preview"
    assert call["owner_id"] == "team_999"
    assert call["audience"] == "https://vercel.com/acme"


async def test_verification_failure_becomes_a_connect_error(
    mock_env_clear: None,
    fake_verifier: list[dict[str, Any]],
) -> None:
    async with session(service_options=session_options()):
        with pytest.raises(ConnectWebhookVerificationError):
            await verify_connect_webhook({"Authorization": "Bearer invalid"})


async def test_factory_binds_expectations_async(
    mock_env_clear: None,
    fake_verifier: list[dict[str, Any]],
) -> None:
    async with session(service_options=session_options()):
        verify = create_connect_webhook_verifier(project_id="prj_bound", environment="preview")
        await verify({"Authorization": "Bearer abc"})

    assert fake_verifier[0]["project_id"] == "prj_bound"
    assert fake_verifier[0]["environment"] == "preview"


def test_factory_binds_expectations_sync(
    mock_env_clear: None,
    fake_verifier: list[dict[str, Any]],
) -> None:
    with session(service_options=session_options()):
        verify = connect_sync.create_connect_webhook_verifier(project_id="prj_bound")
        claims = verify({"Authorization": "Bearer abc"})

    assert claims.project_id == "prj_123"
    assert fake_verifier[0]["project_id"] == "prj_bound"


async def test_factory_is_reusable(
    mock_env_clear: None,
    fake_verifier: list[dict[str, Any]],
) -> None:
    async with session(service_options=session_options()):
        verify = create_connect_webhook_verifier()
        await verify({"Authorization": "Bearer one"})
        await verify({"Authorization": "Bearer two"})

    assert [call["token"] for call in fake_verifier] == ["one", "two"]


async def test_trust_boundary_is_documented() -> None:
    """The accepted set is any Vercel OIDC token for this project and environment."""
    assert verify_connect_webhook.__doc__ is not None
    assert "Trust boundary" in verify_connect_webhook.__doc__
    assert "fails closed" in verify_connect_webhook.__doc__.lower()


async def test_accepts_a_request_object_exposing_headers(
    mock_env_clear: None,
    fake_verifier: list[dict[str, Any]],
) -> None:
    """A request object is the natural thing to reach for, so accept one."""
    import httpx

    request = httpx.Request(
        "POST", "https://myapp.com/hook", headers={"Authorization": "Bearer from-request"}
    )

    async with session(service_options=session_options()):
        claims = await verify_connect_webhook(request)

    assert fake_verifier[0]["token"] == "from-request"
    assert claims.project_id == "prj_123"


def test_accepts_a_request_object_exposing_headers_sync(
    mock_env_clear: None,
    fake_verifier: list[dict[str, Any]],
) -> None:
    import httpx

    request = httpx.Request(
        "POST", "https://myapp.com/hook", headers={"Authorization": "Bearer from-request"}
    )

    with session(service_options=session_options()):
        connect_sync.verify_connect_webhook(request)

    assert fake_verifier[0]["token"] == "from-request"


@pytest.mark.parametrize("headers", [None, 42, "Bearer abc", ["Authorization", "Bearer abc"]])
async def test_rejects_input_that_is_not_headers(
    mock_env_clear: None,
    fake_verifier: list[dict[str, Any]],
    headers: Any,
) -> None:
    from vercel.connect import ConnectValidationError

    async with session(service_options=session_options()):
        with pytest.raises(ConnectValidationError, match="must be a mapping"):
            await verify_connect_webhook(headers)

    assert fake_verifier == []
