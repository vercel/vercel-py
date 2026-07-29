"""Async runtime entry points for Connect operations."""

from collections.abc import Mapping, Sequence

from vercel.connect._internal.models import (
    ConnectAuthorizationDetail,
    ConnectAuthorizationResponse,
    ConnectorMetadata,
    ConnectTokenResponse,
    ConnectTokenSubject,
    ConnectWebhookClaims,
    DurationInput,
)
from vercel.connect._internal.options import ConnectOptions
from vercel.connect._internal.service import ConnectService


async def get_token(
    service: ConnectService,
    connector: str,
    *,
    subject: ConnectTokenSubject,
    scopes: Sequence[str] | None = None,
    installation_id: str | None = None,
    audience: Sequence[str] | None = None,
    resources: Sequence[str] | None = None,
    authorization_details: Sequence[ConnectAuthorizationDetail] | None = None,
    options: ConnectOptions | None = None,
) -> str:
    response = await service.get_token_response(
        connector,
        subject=subject,
        scopes=scopes,
        installation_id=installation_id,
        audience=audience,
        resources=resources,
        authorization_details=authorization_details,
        options=options,
    )
    return response.token


async def get_token_response(
    service: ConnectService,
    connector: str,
    *,
    subject: ConnectTokenSubject,
    scopes: Sequence[str] | None = None,
    installation_id: str | None = None,
    audience: Sequence[str] | None = None,
    resources: Sequence[str] | None = None,
    authorization_details: Sequence[ConnectAuthorizationDetail] | None = None,
    options: ConnectOptions | None = None,
) -> ConnectTokenResponse:
    return await service.get_token_response(
        connector,
        subject=subject,
        scopes=scopes,
        installation_id=installation_id,
        audience=audience,
        resources=resources,
        authorization_details=authorization_details,
        options=options,
    )


async def revoke_token(
    service: ConnectService,
    connector: str,
    *,
    subject: ConnectTokenSubject,
    installation_id: str | None = None,
    options: ConnectOptions | None = None,
) -> None:
    await service.revoke_token(
        connector,
        subject=subject,
        installation_id=installation_id,
        options=options,
    )


async def start_authorization(
    service: ConnectService,
    connector: str,
    *,
    subject: ConnectTokenSubject,
    scopes: Sequence[str] | None = None,
    installation_id: str | None = None,
    return_url: str | None = None,
    webhook: str | None = None,
    device_code: bool | None = None,
    expires_in: DurationInput | None = None,
    options: ConnectOptions | None = None,
) -> ConnectAuthorizationResponse:
    return await service.start_authorization(
        connector,
        subject=subject,
        scopes=scopes,
        installation_id=installation_id,
        return_url=return_url,
        webhook=webhook,
        device_code=device_code,
        expires_in=expires_in,
        options=options,
    )


async def get_connector_metadata(
    service: ConnectService,
    connector: str,
    *,
    options: ConnectOptions | None = None,
) -> ConnectorMetadata:
    return await service.get_connector_metadata(connector, options=options)


async def verify_connect_webhook(
    service: ConnectService,
    headers: Mapping[str, str],
    *,
    project_id: str | None = None,
    environment: str | None = None,
    owner_id: str | None = None,
    audience: str | Sequence[str] | None = None,
) -> ConnectWebhookClaims:
    return await service.verify_webhook(
        headers,
        project_id=project_id,
        environment=environment,
        owner_id=owner_id,
        audience=audience,
    )


__all__ = [
    "get_connector_metadata",
    "get_token",
    "get_token_response",
    "revoke_token",
    "start_authorization",
    "verify_connect_webhook",
]
