"""Sync runtime entry points for Connect operations.

Each call steps the async service exactly once through `iter_coroutine`, which is
valid because the sync transport never suspends.
"""

from collections.abc import Mapping, Sequence

from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel.connect._internal.base import StringContainer
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


def get_token(
    service: ConnectService,
    connector: str,
    *,
    subject: ConnectTokenSubject,
    scopes: StringContainer | None = None,
    installation_id: str | None = None,
    audience: StringContainer | None = None,
    resources: StringContainer | None = None,
    authorization_details: Sequence[ConnectAuthorizationDetail] | None = None,
    options: ConnectOptions | None = None,
) -> str:
    response = iter_coroutine(
        service.get_token_response(
            connector,
            subject=subject,
            scopes=scopes,
            installation_id=installation_id,
            audience=audience,
            resources=resources,
            authorization_details=authorization_details,
            options=options,
        )
    )
    return response.token


def get_token_response(
    service: ConnectService,
    connector: str,
    *,
    subject: ConnectTokenSubject,
    scopes: StringContainer | None = None,
    installation_id: str | None = None,
    audience: StringContainer | None = None,
    resources: StringContainer | None = None,
    authorization_details: Sequence[ConnectAuthorizationDetail] | None = None,
    options: ConnectOptions | None = None,
) -> ConnectTokenResponse:
    return iter_coroutine(
        service.get_token_response(
            connector,
            subject=subject,
            scopes=scopes,
            installation_id=installation_id,
            audience=audience,
            resources=resources,
            authorization_details=authorization_details,
            options=options,
        )
    )


def revoke_token(
    service: ConnectService,
    connector: str,
    *,
    subject: ConnectTokenSubject,
    installation_id: str | None = None,
    options: ConnectOptions | None = None,
) -> None:
    iter_coroutine(
        service.revoke_token(
            connector,
            subject=subject,
            installation_id=installation_id,
            options=options,
        )
    )


def start_authorization(
    service: ConnectService,
    connector: str,
    *,
    subject: ConnectTokenSubject,
    scopes: StringContainer | None = None,
    installation_id: str | None = None,
    return_url: str | None = None,
    webhook: str | None = None,
    device_code: bool | None = None,
    expires_in: DurationInput | None = None,
    options: ConnectOptions | None = None,
) -> ConnectAuthorizationResponse:
    return iter_coroutine(
        service.start_authorization(
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
    )


def get_connector_metadata(
    service: ConnectService,
    connector: str,
    *,
    options: ConnectOptions | None = None,
) -> ConnectorMetadata:
    return iter_coroutine(service.get_connector_metadata(connector, options=options))


def verify_connect_webhook(
    service: ConnectService,
    headers: Mapping[str, str],
    *,
    project_id: str | None = None,
    environment: str | None = None,
    owner_id: str | None = None,
    audience: str | Sequence[str] | None = None,
) -> ConnectWebhookClaims:
    return iter_coroutine(
        service.verify_webhook(
            headers,
            project_id=project_id,
            environment=environment,
            owner_id=owner_id,
            audience=audience,
        )
    )


__all__ = [
    "get_connector_metadata",
    "get_token",
    "get_token_response",
    "revoke_token",
    "start_authorization",
    "verify_connect_webhook",
]
