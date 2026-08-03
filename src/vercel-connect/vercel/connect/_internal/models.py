"""Public value types for the Connect SDK surface.

Every type is a frozen pydantic model: constructed by keyword, validated on
construction, and immutable afterwards.
"""

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any, Literal, TypeAlias

from pydantic import Field, model_serializer

from vercel.connect._internal.base import ConnectModel, StringContainer, StringField

DurationInput: TypeAlias = int | float | timedelta
"""A duration accepted at the public boundary: seconds as a number, or a timedelta."""

JSONValue: TypeAlias = Any
JSONObject: TypeAlias = Mapping[str, JSONValue]


class ConnectAppTokenSubject(ConnectModel):
    """Authority of the integration itself, scoped to one installation.

    One shared credential per installation. Always available once a connector is
    installed, but it is ambient authority: every user of your app gets whatever
    the bot can do.
    """

    type: Literal["app"] = "app"


class ConnectUserTokenSubject(ConnectModel):
    """Authority of one named end user, requiring that user's consent.

    Preserves the upstream provider's own permission model per person and names
    them in the provider's audit log, at the cost of a consent flow.
    """

    id: str
    issuer: str | None = None
    type: Literal["user"] = "user"


class ConnectJwtBearerTokenSubject(ConnectModel):
    """Authority of a user asserted by your app (RFC 7523), with no consent screen.

    Requires trust to be pre-established with the upstream provider. `iss`
    defaults to the connector's OAuth client id and `aud` to its token endpoint.
    """

    sub: str
    iss: str | None = None
    aud: str | None = None
    additional_claims: JSONObject | None = Field(
        default=None, serialization_alias="additionalClaims"
    )
    type: Literal["jwt-bearer"] = "jwt-bearer"


class ConnectTokenExchangeSubject(ConnectModel):
    """A credential you already hold, exchanged for an upstream credential."""

    token: str
    type: Literal["token"] = "token"


ConnectTokenSubject: TypeAlias = (
    ConnectAppTokenSubject
    | ConnectUserTokenSubject
    | ConnectJwtBearerTokenSubject
    | ConnectTokenExchangeSubject
)


class ConnectGitHubAppInstallationAuthorizationDetail(ConnectModel):
    """A GitHub App installation authorization detail (RFC 9396)."""

    org: str | None = None
    permissions: StringField | None = None
    repositories: StringField | None = None
    type: str = "github_app_installation"


class ConnectCustomAuthorizationDetail(ConnectModel):
    """An open-ended authorization detail (RFC 9396) for any other type."""

    type: str
    details: JSONObject = Field(default_factory=dict)

    @model_serializer
    def _to_wire(self) -> dict[str, Any]:
        # `details` is spread, and `type` is written last so a stray "type" key in
        # it cannot silently change which kind of authorization is requested.
        return {**dict(self.details), "type": self.type}


ConnectAuthorizationDetail: TypeAlias = (
    ConnectGitHubAppInstallationAuthorizationDetail | ConnectCustomAuthorizationDetail
)


class ConnectorRef(ConnectModel):
    """Identity of the connector that issued or owns a response."""

    id: str
    uid: str
    type: str
    name: str | None = None
    service: str | None = None
    service_name: str | None = None


class ConnectTokenResponse(ConnectModel):
    """A minted upstream credential and the metadata worth logging."""

    token: str
    expires_at: datetime
    connector: ConnectorRef
    token_id: str | None = None
    name: str | None = None
    installation_id: str | None = None
    tenant_id: str | None = None
    external_subject: str | None = None
    metadata: JSONObject | None = None
    claims: JSONObject | None = None


class ConnectAuthorizationResponse(ConnectModel):
    """A started end-user authorization request.

    Send the user to `url`. `request` and `verifier` are returned for parity with
    the TypeScript SDK; Connect exposes no endpoint that consumes them today, so
    treat them as opaque. For device-code flows, poll `get_token` with
    `force_refresh=True` until it succeeds.
    """

    url: str
    request: str
    verifier: str
    device_code: str | None = None
    expires_at: datetime | None = None
    connector: ConnectorRef | None = None


class ConnectorMetadata(ConnectModel):
    """Connector identity and configuration.

    Only the documented fields are typed. Every other top-level field the API
    returns is preserved verbatim in `extra`, so newer server fields are never
    dropped and can be promoted to typed attributes without a breaking change.
    """

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


class ConnectWebhookClaims(ConnectModel):
    """Verified claims from a Connect trigger's Vercel OIDC token."""

    issuer: str
    subject: str
    project_id: str | None
    environment: str | None
    owner_id: str | None
    audience: list[str]
    issued_at: datetime | None
    expires_at: datetime | None
    claims: JSONObject


__all__ = [
    "ConnectAppTokenSubject",
    "ConnectAuthorizationDetail",
    "ConnectAuthorizationResponse",
    "ConnectCustomAuthorizationDetail",
    "ConnectGitHubAppInstallationAuthorizationDetail",
    "ConnectJwtBearerTokenSubject",
    "ConnectTokenExchangeSubject",
    "ConnectTokenResponse",
    "ConnectTokenSubject",
    "ConnectUserTokenSubject",
    "ConnectWebhookClaims",
    "ConnectorMetadata",
    "ConnectorRef",
    "DurationInput",
    "JSONObject",
    "JSONValue",
    "StringContainer",
]
