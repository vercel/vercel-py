from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import ClassVar

import pytest

from vercel._internal.unstable.errors import (
    AuthError,
    CredentialProviderError,
    CredentialResolutionError,
)
from vercel._internal.unstable.sandbox.auth import (
    resolve_sandbox_credentials,
    resolve_sync_sandbox_credentials,
)
from vercel.cache.context import set_headers
from vercel.unstable import Session, SyncSession, VercelError
from vercel.unstable.auth import (
    AccessTokenCredentials,
    CredentialProvider,
    OIDCCredentials,
    StaticCredentialProvider,
    SyncCredentialProvider,
    SyncStaticCredentialProvider,
)
from vercel.unstable.sandbox import SandboxOptions


def _oidc_token(payload: dict[str, object]) -> str:
    encoded_payload = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=")
    return f"header.{encoded_payload.decode()}.signature"


async def test_static_async_provider_resolves_complete_credentials() -> None:
    credentials = AccessTokenCredentials(
        token="token",
        project_id="prj_123",
        team_id="team_123",
    )
    provider: CredentialProvider[AccessTokenCredentials] = StaticCredentialProvider(credentials)

    assert await provider.resolve() is credentials


def test_static_sync_provider_resolves_complete_credentials() -> None:
    credentials = OIDCCredentials(
        token="oidc",
        project_id="prj_123",
        team_id="team_123",
    )
    provider: SyncCredentialProvider[OIDCCredentials] = SyncStaticCredentialProvider(credentials)

    assert provider.resolve() is credentials


def test_construction_paths_do_not_resolve_credentials(mock_env_clear: None) -> None:
    _ = mock_env_clear

    AccessTokenCredentials(token="token", project_id="prj_123", team_id="team_123")
    OIDCCredentials(token="oidc", project_id="prj_123", team_id="team_123")
    StaticCredentialProvider(
        AccessTokenCredentials(token="token", project_id="prj_123", team_id="team_123")
    )
    SyncStaticCredentialProvider(
        OIDCCredentials(token="oidc", project_id="prj_123", team_id="team_123")
    )
    SandboxOptions(project_id="prj_123", team_id="team_123")
    Session()
    SyncSession()


async def test_explicit_provider_precedes_ambient_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL_TOKEN", "env_token")
    monkeypatch.setenv("VERCEL_PROJECT_ID", "env_project")
    monkeypatch.setenv("VERCEL_TEAM_ID", "env_team")

    credentials = AccessTokenCredentials(
        token="explicit_token",
        project_id="explicit_project",
        team_id="explicit_team",
    )

    resolved = await resolve_sandbox_credentials(
        credential_provider=StaticCredentialProvider(credentials)
    )

    assert resolved is credentials


async def test_default_provider_prefers_oidc_over_access_token(
    monkeypatch: pytest.MonkeyPatch,
    mock_env_clear: None,
) -> None:
    _ = mock_env_clear
    monkeypatch.setenv("VERCEL_TOKEN", "env_token")
    monkeypatch.setenv("VERCEL_PROJECT_ID", "env_project")
    monkeypatch.setenv("VERCEL_TEAM_ID", "env_team")
    set_headers({"x-vercel-oidc-token": _oidc_token({"project_id": "oidc_project"})})

    resolved = await resolve_sandbox_credentials(team_id="explicit_team")

    assert resolved == OIDCCredentials(
        token=_oidc_token({"project_id": "oidc_project"}),
        project_id="env_project",
        team_id="explicit_team",
    )


async def test_oidc_scope_resolves_field_by_field(
    monkeypatch: pytest.MonkeyPatch,
    mock_env_clear: None,
) -> None:
    _ = mock_env_clear
    monkeypatch.setenv("VERCEL_TEAM_ID", "env_team")
    token = _oidc_token({"project_id": "payload_project", "owner_id": "payload_team"})
    set_headers({"x-vercel-oidc-token": token})

    resolved = await resolve_sandbox_credentials(project_id="explicit_project")

    assert resolved == OIDCCredentials(
        token=token,
        project_id="explicit_project",
        team_id="env_team",
    )


async def test_access_token_default_requires_complete_scope(
    monkeypatch: pytest.MonkeyPatch,
    mock_env_clear: None,
) -> None:
    _ = mock_env_clear
    monkeypatch.setenv("VERCEL_TOKEN", "env_token")
    monkeypatch.setenv("VERCEL_TEAM_ID", "env_team")

    with pytest.raises(CredentialResolutionError, match="project_id"):
        await resolve_sandbox_credentials()


async def test_malformed_oidc_token_raises_typed_auth_error(
    mock_env_clear: None,
) -> None:
    _ = mock_env_clear
    set_headers({"x-vercel-oidc-token": "not-a-jwt"})

    with pytest.raises(CredentialResolutionError, match="OIDC token payload"):
        await resolve_sandbox_credentials(project_id="prj_123", team_id="team_123")


async def test_missing_default_credentials_raise_typed_auth_error(mock_env_clear: None) -> None:
    _ = mock_env_clear

    with pytest.raises(CredentialResolutionError) as error:
        await resolve_sandbox_credentials()

    assert isinstance(error.value, AuthError)
    assert isinstance(error.value, VercelError)


@dataclass(frozen=True)
class FailingProvider:
    attempts: ClassVar[int] = 0

    async def resolve(self) -> AccessTokenCredentials:
        type(self).attempts += 1
        raise RuntimeError("boom")


async def test_provider_failures_are_typed() -> None:
    with pytest.raises(CredentialProviderError) as error:
        await resolve_sandbox_credentials(credential_provider=FailingProvider())

    assert isinstance(error.value, VercelError)
    assert FailingProvider.attempts == 1


def test_sync_default_credentials_use_access_token(
    monkeypatch: pytest.MonkeyPatch,
    mock_env_clear: None,
) -> None:
    _ = mock_env_clear
    monkeypatch.setenv("VERCEL_TOKEN", "env_token")
    monkeypatch.setenv("VERCEL_PROJECT_ID", "env_project")
    monkeypatch.setenv("VERCEL_TEAM_ID", "env_team")

    assert resolve_sync_sandbox_credentials() == AccessTokenCredentials(
        token="env_token",
        project_id="env_project",
        team_id="env_team",
    )
