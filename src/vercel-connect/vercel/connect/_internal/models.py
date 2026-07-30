"""Public value types for the Connect SDK surface."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Literal, TypeAlias

from vercel.connect._internal.errors import ConnectValidationError


def _validate_string_sequence(name: str, value: object) -> None:
    """Reject a bare string where a sequence of strings is required.

    `str` satisfies `Sequence[str]`, so `permissions="contents:read"` type-checks
    and then serializes as one entry per character.
    """
    if isinstance(value, str):
        raise ConnectValidationError(
            f"{name} must be a sequence of strings, not a single string; "
            f"pass [{value!r}] for one value"
        )


DurationInput: TypeAlias = int | float | timedelta
"""A duration accepted at the public boundary: seconds as a number, or a timedelta."""

JSONValue: TypeAlias = Any
JSONObject: TypeAlias = Mapping[str, JSONValue]


@dataclass(frozen=True, slots=True)
class ConnectAppTokenSubject:
    """Authority of the integration itself, scoped to one installation.

    One shared credential per installation. Always available once a connector is
    installed, but it is ambient authority: every user of your app gets whatever
    the bot can do.
    """

    type: Literal["app"] = "app"


@dataclass(frozen=True, slots=True)
class ConnectUserTokenSubject:
    """Authority of one named end user, requiring that user's consent.

    Preserves the upstream provider's own permission model per person and names
    them in the provider's audit log, at the cost of a consent flow.
    """

    id: str
    issuer: str | None = None
    type: Literal["user"] = "user"


@dataclass(frozen=True, slots=True)
class ConnectJwtBearerTokenSubject:
    """Authority of a user asserted by your app (RFC 7523), with no consent screen.

    Requires trust to be pre-established with the upstream provider. `iss`
    defaults to the connector's OAuth client id and `aud` to its token endpoint.
    """

    sub: str
    iss: str | None = None
    aud: str | None = None
    additional_claims: JSONObject | None = None
    type: Literal["jwt-bearer"] = "jwt-bearer"


@dataclass(frozen=True, slots=True)
class ConnectTokenExchangeSubject:
    """A credential you already hold, exchanged for an upstream credential."""

    token: str
    type: Literal["token"] = "token"


ConnectTokenSubject: TypeAlias = (
    ConnectAppTokenSubject
    | ConnectUserTokenSubject
    | ConnectJwtBearerTokenSubject
    | ConnectTokenExchangeSubject
)


@dataclass(frozen=True, slots=True)
class ConnectGitHubAppInstallationAuthorizationDetail:
    """A GitHub App installation authorization detail (RFC 9396)."""

    org: str | None = None
    permissions: Sequence[str] | None = None
    repositories: Sequence[str] | None = None
    type: str = "github_app_installation"

    def __post_init__(self) -> None:
        _validate_string_sequence("permissions", self.permissions)
        _validate_string_sequence("repositories", self.repositories)


@dataclass(frozen=True, slots=True)
class ConnectCustomAuthorizationDetail:
    """An open-ended authorization detail (RFC 9396) for any other type."""

    type: str
    details: JSONObject = field(default_factory=dict)


ConnectAuthorizationDetail: TypeAlias = (
    ConnectGitHubAppInstallationAuthorizationDetail | ConnectCustomAuthorizationDetail
)


@dataclass(frozen=True, slots=True)
class ConnectorRef:
    """Identity of the connector that issued or owns a response."""

    id: str
    uid: str
    type: str
    name: str | None = None
    service: str | None = None
    service_name: str | None = None


@dataclass(frozen=True, slots=True)
class ConnectTokenResponse:
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


@dataclass(frozen=True, slots=True)
class ConnectAuthorizationResponse:
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


@dataclass(frozen=True, slots=True)
class ConnectorMetadata:
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
    vendor: JSONObject = field(default_factory=dict)
    extra: JSONObject = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ConnectWebhookClaims:
    """Verified claims from a Connect trigger's Vercel OIDC token."""

    issuer: str
    subject: str
    project_id: str | None
    environment: str | None
    owner_id: str | None
    audience: Sequence[str]
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
]
