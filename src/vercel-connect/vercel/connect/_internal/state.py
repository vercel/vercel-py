"""Neutral domain values exchanged between the Connect api client and service."""

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

from pydantic import Field, field_serializer, field_validator

from vercel._internal.core.time import to_ms_int
from vercel.connect._internal.base import ConnectModel, StringField, reject_bool
from vercel.connect._internal.errors import ConnectValidationError
from vercel.connect._internal.models import (
    ConnectAuthorizationDetail,
    ConnectTokenSubject,
    JSONObject,
)

DEFAULT_SCOPES_SENTINEL = "*"
_LOCAL_HOSTS = ("localhost", "127.0.0.1", "[::1]", "::1")


def _is_local_host(host: str) -> bool:
    host = host.lower()
    return host in _LOCAL_HOSTS or host.endswith(".localhost")


class _SubjectRequest(ConnectModel):
    """What every credential request names: a connector and a subject."""

    connector: str
    subject: ConnectTokenSubject = Field(discriminator="type")
    installation_id: str | None = Field(default=None, serialization_alias="installationId")

    def to_api_body(self) -> dict[str, Any]:
        """Render the request body. `connector` names the path, not the body."""
        return self.model_dump(mode="json", by_alias=True, exclude_none=True, exclude={"connector"})


class ConnectRevokeRequest(_SubjectRequest):
    """A grant to revoke."""


class _ScopedRequest(_SubjectRequest):
    """A credential request that can narrow what the credential may do."""

    scopes: StringField | None = None

    @field_validator("scopes")
    @classmethod
    def _collapse_default_scopes(cls, scopes: Sequence[str] | None) -> Sequence[str] | None:
        # `None` and `["*"]` request the same credential, so they must serialize
        # the same way and share one cache entry.
        return (
            None if scopes is not None and tuple(scopes) == (DEFAULT_SCOPES_SENTINEL,) else scopes
        )


class ConnectTokenRequest(_ScopedRequest):
    """Everything that decides which credential the server mints.

    One value serves both the request body and the cache key, so a field cannot
    reach the wire without also partitioning the cache.
    """

    audience: StringField | None = None
    resources: StringField | None = None
    authorization_details: Sequence[ConnectAuthorizationDetail] | None = Field(
        default=None, serialization_alias="authorizationDetails"
    )


class ConnectAuthorizationRequest(_ScopedRequest):
    """An end-user consent flow to start."""

    return_url: str | None = Field(default=None, serialization_alias="returnUrl")
    webhook: str | None = None
    device_code: bool | None = Field(default=None, serialization_alias="deviceCode")
    expires_in: timedelta | None = Field(default=None, serialization_alias="expiresInMs")

    @field_serializer("expires_in")
    def _serialize_expires_in(self, expires_in: timedelta | None) -> int | None:
        return None if expires_in is None else to_ms_int(expires_in)

    @field_validator("return_url")
    @classmethod
    def _check_return_url(cls, url: str | None) -> str | None:
        """Allow https anywhere, and http only on a loopback host."""
        if url is None:
            return None
        parts = urlsplit(url)
        if parts.hostname and (
            parts.scheme == "https" or (parts.scheme == "http" and _is_local_host(parts.hostname))
        ):
            return url
        raise ConnectValidationError(
            f"must be https, or http on localhost, *.localhost or 127.0.0.1; got {url!r}"
        )

    @field_validator("webhook")
    @classmethod
    def _check_webhook(cls, url: str | None) -> str | None:
        if url is None:
            return None
        parts = urlsplit(url)
        if parts.scheme != "https" or not parts.hostname:
            raise ConnectValidationError(f"must be an https URL; got {url!r}")
        return url

    @field_validator("expires_in", mode="before")
    @classmethod
    def _check_expires_in(cls, value: object) -> object:
        # A bare number is seconds, which is what pydantic reads it as.
        return reject_bool(value)


class ConnectorRefState(ConnectModel):
    """Connector identity as carried on a Connect response."""

    id: str
    uid: str
    type: str
    name: str | None = None
    service: str | None = None
    service_name: str | None = None


class ConnectTokenState(ConnectModel):
    """A minted upstream credential, wire concerns removed."""

    token: str
    expires_at: datetime
    connector: ConnectorRefState
    token_id: str | None = None
    name: str | None = None
    installation_id: str | None = None
    tenant_id: str | None = None
    external_subject: str | None = None
    metadata: JSONObject | None = None
    claims: JSONObject | None = None


class ConnectAuthorizationState(ConnectModel):
    """A started authorization request, wire concerns removed."""

    url: str
    request: str
    verifier: str
    device_code: str | None = None
    expires_at: datetime | None = None
    connector: ConnectorRefState | None = None


class ConnectorMetadataState(ConnectModel):
    """Connector metadata, with unrecognized top-level fields preserved."""

    id: str
    uid: str
    type: str
    name: str | None = None
    service: str | None = None
    client_url: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    vendor: JSONObject = Field(default_factory=dict)
    extra: JSONObject = Field(default_factory=dict)


__all__ = [
    "ConnectAuthorizationRequest",
    "ConnectAuthorizationState",
    "ConnectRevokeRequest",
    "ConnectTokenRequest",
    "ConnectTokenState",
    "ConnectorMetadataState",
    "ConnectorRefState",
]
