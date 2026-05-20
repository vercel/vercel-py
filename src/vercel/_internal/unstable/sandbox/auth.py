"""Internal auth policy for unstable Sandbox request plumbing."""

from __future__ import annotations

from typing import TypeAlias

from vercel._internal.runtime_context import get_headers
from vercel._internal.unstable.auth import (
    AccessTokenCredentials,
    CredentialProvider,
    Credentials,
    OIDCCredentials,
    SyncCredentialProvider,
    decode_jwt_payload,
    header_value,
    optional_string_setting,
    require_scope,
    resolve_provider,
    resolve_sync_provider,
    validate_provider_credentials,
)
from vercel._internal.unstable.errors import CredentialResolutionError
from vercel._internal.unstable.settings import (
    EnvironmentSettingsSource,
    ExplicitSettingsSource,
    SettingSource,
)

SandboxCredentials: TypeAlias = OIDCCredentials | AccessTokenCredentials
SandboxCredentialProvider: TypeAlias = CredentialProvider[SandboxCredentials]
SyncSandboxCredentialProvider: TypeAlias = SyncCredentialProvider[SandboxCredentials]

_AUTH_ENVIRONMENT_FIELDS = {
    "oidc_token": "VERCEL_OIDC_TOKEN",
    "access_token": "VERCEL_TOKEN",
    "project_id": "VERCEL_PROJECT_ID",
    "team_id": "VERCEL_TEAM_ID",
}


async def resolve_sandbox_credentials(
    *,
    credential_provider: SandboxCredentialProvider | None = None,
    project_id: str | None = None,
    team_id: str | None = None,
) -> SandboxCredentials:
    """Resolve credentials for async unstable Sandbox request plumbing."""

    if credential_provider is not None:
        return _validate_sandbox_credentials(
            await resolve_provider(credential_provider.resolve),
            source="credential provider",
        )
    return _resolve_default_sandbox_credentials(project_id=project_id, team_id=team_id)


def resolve_sync_sandbox_credentials(
    *,
    credential_provider: SyncSandboxCredentialProvider | None = None,
    project_id: str | None = None,
    team_id: str | None = None,
) -> SandboxCredentials:
    """Resolve credentials for sync unstable Sandbox request plumbing."""

    if credential_provider is not None:
        return _validate_sandbox_credentials(
            resolve_sync_provider(credential_provider.resolve),
            source="credential provider",
        )
    return _resolve_default_sandbox_credentials(project_id=project_id, team_id=team_id)


def _resolve_default_sandbox_credentials(
    *,
    project_id: str | None,
    team_id: str | None,
) -> SandboxCredentials:
    setting_sources = _sandbox_credential_setting_sources(project_id=project_id, team_id=team_id)
    oidc = _get_ambient_oidc_token(setting_sources)
    if oidc is not None:
        payload = decode_jwt_payload(oidc, token_name="OIDC token")
        return OIDCCredentials(
            token=oidc,
            project_id=require_scope(
                "project_id",
                sources=setting_sources,
                payload=payload,
                payload_key="project_id",
                source="OIDC credentials",
            ),
            team_id=require_scope(
                "team_id",
                sources=setting_sources,
                payload=payload,
                payload_key="owner_id",
                source="OIDC credentials",
            ),
        )

    token = optional_string_setting(
        "access_token",
        setting_sources,
        label="Sandbox credential",
    )
    if token is None or token == "":
        raise CredentialResolutionError(
            "missing Sandbox credentials: set VERCEL_OIDC_TOKEN or VERCEL_TOKEN"
        )

    return AccessTokenCredentials(
        token=token,
        project_id=require_scope(
            "project_id",
            sources=setting_sources,
            payload=None,
            payload_key="project_id",
            source="access-token credentials",
        ),
        team_id=require_scope(
            "team_id",
            sources=setting_sources,
            payload=None,
            payload_key="owner_id",
            source="access-token credentials",
        ),
    )


def _sandbox_credential_setting_sources(
    *,
    project_id: str | None,
    team_id: str | None,
) -> tuple[SettingSource, ...]:
    return (
        ExplicitSettingsSource(
            {
                "project_id": project_id,
                "team_id": team_id,
            },
            name="sandbox options",
        ),
        EnvironmentSettingsSource(_AUTH_ENVIRONMENT_FIELDS),
    )


def _get_ambient_oidc_token(sources: tuple[SettingSource, ...]) -> str | None:
    token = _get_oidc_token_from_context()
    if token is None:
        token = optional_string_setting(
            "oidc_token",
            sources,
            label="Sandbox credential",
        )
    if token == "":
        raise CredentialResolutionError("OIDC token was present but empty")
    return token


def _get_oidc_token_from_context() -> str | None:
    headers = get_headers()
    if headers is None:
        return None
    return header_value(headers, "x-vercel-oidc-token")


def _validate_sandbox_credentials(
    credentials: Credentials,
    *,
    source: str,
) -> SandboxCredentials:
    credentials = validate_provider_credentials(credentials, source=source)
    if isinstance(credentials, (OIDCCredentials, AccessTokenCredentials)):
        return credentials
    raise AssertionError("unreachable credential type after validation")


__all__ = [
    "SandboxCredentialProvider",
    "SandboxCredentials",
    "SyncSandboxCredentialProvider",
    "resolve_sandbox_credentials",
    "resolve_sync_sandbox_credentials",
]
