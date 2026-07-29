"""Neutral domain state returned by the Connect api client and service."""

from dataclasses import dataclass, field
from datetime import datetime

from vercel.connect._internal.models import JSONObject


@dataclass(frozen=True, slots=True)
class ConnectorRefState:
    """Connector identity as carried on a Connect response."""

    id: str
    uid: str
    type: str
    name: str | None = None
    service: str | None = None
    service_name: str | None = None


@dataclass(frozen=True, slots=True)
class ConnectTokenState:
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


@dataclass(frozen=True, slots=True)
class ConnectAuthorizationState:
    """A started authorization request, wire concerns removed."""

    url: str
    request: str
    verifier: str
    device_code: str | None = None
    expires_at: datetime | None = None
    connector: ConnectorRefState | None = None


@dataclass(frozen=True, slots=True)
class ConnectorMetadataState:
    """Connector metadata, with unrecognized top-level fields preserved."""

    id: str
    uid: str
    type: str
    name: str | None = None
    service: str | None = None
    client_url: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    vendor: JSONObject = field(default_factory=dict)
    extra: JSONObject = field(default_factory=dict)


__all__ = [
    "ConnectAuthorizationState",
    "ConnectTokenState",
    "ConnectorMetadataState",
    "ConnectorRefState",
]
