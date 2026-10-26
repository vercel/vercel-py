"""Passport identity values and errors."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from vercel.oidc.verify import VERCEL_PASSPORT_ISSUER

TokenSource: TypeAlias = Literal["header", "cookie", "local"]
"""Where an identity's token came from.

``"header"`` is the trusted Passport header or an ``Authorization`` bearer,
``"cookie"`` is the Passport cookie, and ``"local"`` is a token or identity the
caller supplied directly (an argument, an environment variable, or the
development identity).
"""

PassportClaims: TypeAlias = Mapping[str, Any]
"""Every claim on a Passport token. JSON, so its values stay dynamic."""


class PassportError(Exception):
    """Base error for Vercel Passport operations."""


class PassportValidationError(PassportError, ValueError):
    """Raised when a Passport value is built with invalid arguments.

    Covers a misspelled or unknown field, a value of the wrong type, and an
    attempt to change a field of a frozen value. Also a `ValueError`, so the
    `except ValueError` a caller would reach for on a bad argument catches it.
    """


class PassportIdentityError(PassportError):
    """Raised when a Passport identity is missing, malformed, or not allowed.

    Covers a token that is not a JWT, a payload that is not Passport-shaped, a
    missing token where one is required, and an unverified token the caller
    refused to accept.
    """


class PassportVerificationError(PassportIdentityError):
    """Raised when a Passport token does not verify.

    Covers an invalid signature, an issuer outside Vercel Passport, an expired
    token, a claim that does not match the expected project, environment,
    owner, or audience, and an expectation that could not be resolved.
    Verification fails closed, so this is never raised to indicate success.
    """


class PassportDevelopmentIdentityWarning(UserWarning):
    """Emitted once per process when a synthesized development identity is used."""


def as_passport_error(exc: ValidationError) -> PassportValidationError:
    """Report a pydantic validation failure as the package's own error."""
    details = "; ".join(
        f"{'.'.join(str(part) for part in error['loc']) or 'value'}: {error['msg']}"
        for error in exc.errors()
    )
    return PassportValidationError(details)


class PassportModel(BaseModel):
    """Frozen, validated value type.

    `extra="forbid"` so a misspelled keyword is an error rather than a silently
    dropped field, and validation failures surface as `PassportValidationError`
    rather than pydantic's own error, which spares callers an import of pydantic.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except ValidationError as exc:
            raise as_passport_error(exc) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        # Assigning to a frozen model is a pydantic ValidationError; report it the
        # same way as every other misuse of a Passport value.
        try:
            super().__setattr__(name, value)
        except ValidationError as exc:
            raise as_passport_error(exc) from exc


# The environment variables each development-identity field falls back to, in
# order. An empty value counts as unset, so it never becomes an empty claim.
_DEVELOPMENT_ENV: Mapping[str, tuple[str, ...]] = {
    "owner": ("VERCEL_PASSPORT_DEV_OWNER",),
    "owner_id": ("VERCEL_PASSPORT_DEV_OWNER_ID",),
    "connector_id": ("VERCEL_PASSPORT_DEV_CONNECTOR_ID",),
    "external_issuer": ("VERCEL_PASSPORT_DEV_EXTERNAL_ISS",),
    "external_subject": ("VERCEL_PASSPORT_DEV_EXTERNAL_SUB",),
    "environment": ("VERCEL_PASSPORT_DEV_ENVIRONMENT",),
    "project": ("VERCEL_PASSPORT_DEV_PROJECT",),
    "project_id": ("VERCEL_PASSPORT_DEV_PROJECT_ID", "VERCEL_PROJECT_ID"),
    "issuer": ("VERCEL_PASSPORT_DEV_ISSUER",),
    "audience": ("VERCEL_PASSPORT_DEV_AUDIENCE",),
}


class PassportDevelopmentIdentity(PassportModel):
    """The identity synthesized for local development.

    Each field is resolved as the explicit value, then its `VERCEL_PASSPORT_DEV_*`
    environment variable, then the default, whichever way the model is built.
    An explicit `None` clears an optional field even when its variable is set.
    The environment is read when the model is constructed.
    """

    # Declared before `issuer` and `audience`, whose defaults are derived from them.
    owner: str = "local"
    owner_id: str | None = None
    connector_id: str = "local"
    external_issuer: str | None = Field(default=None, serialization_alias="external_iss")
    external_subject: str = Field(default="test-user", serialization_alias="external_sub")
    environment: str = "development"
    project: str = "local"
    project_id: str | None = None
    issuer: str = Field(
        default_factory=lambda data: f"{VERCEL_PASSPORT_ISSUER}/{data['owner']}",
        serialization_alias="iss",
    )
    audience: str = Field(
        default_factory=lambda data: (
            f"https://vercel.com/{data['owner']}/{data['project']}/{data['environment']}"
        ),
        serialization_alias="aud",
    )

    @model_validator(mode="before")
    @classmethod
    def _environment_beneath_explicit(cls, data: Any) -> Any:
        # The environment is read here rather than through a validation context:
        # `PassportModel.__init__` wraps validation errors, and pydantic routes
        # `model_validate` through a custom `__init__`, which drops the context.
        if not isinstance(data, Mapping):
            return data
        from_env = {
            name: value
            for name, variables in _DEVELOPMENT_ENV.items()
            if (value := next((os.environ[v] for v in variables if os.environ.get(v)), None))
        }
        return {**from_env, **data}

    def to_claims(self) -> PassportClaims:
        """Render the identity as the claims of a Passport token."""
        subject = (
            f"owner:{self.owner}:connector:{self.connector_id}:principal:{self.external_subject}"
        )
        return self.model_dump(by_alias=True, exclude_none=True) | {
            "email": "test-user@passport.local",
            "name": "Test User",
            "scope": subject,
            "sub": subject,
            "typ": "passport",
        }


class PassportOwner(PassportModel):
    """The Vercel team or account that owns the Passport-protected project."""

    slug: str
    id: str | None = None


class PassportProject(PassportModel):
    """The Vercel project the Passport token was issued for.

    An identity has a project only when its token names a project ID, which every
    verified token does, because verification requires it to match.
    """

    id: str
    name: str | None = None


class PassportIdentity(PassportModel):
    """A visitor identity established by Vercel Passport.

    `token` and `payload` are omitted from the repr so that logging an identity
    never writes a bearer credential to the logs.

    The fields are frozen, but `payload` holds a copy of the claims as a plain
    dict, so an identity can be pickled, copied, and serialized with
    `model_dump`. Equality compares every field, `payload` included; the hash
    leaves `payload` out, because a dict cannot be hashed.
    """

    subject: str
    """The Passport subject, `owner:<owner>:connector:<connector>:principal:<sub>`."""
    external_subject: str
    """The subject at the upstream identity provider."""
    connector_id: str
    """The Passport connector that authenticated the visitor."""
    owner: PassportOwner
    token_source: TokenSource
    verified: bool
    """Whether the token's signature and claims were verified."""
    external_issuer: str | None = None
    """The upstream identity provider's issuer, when known."""
    email: str | None = None
    name: str | None = None
    project: PassportProject | None = None
    environment: str | None = None
    token: str | None = Field(default=None, repr=False)
    """The encoded token, or `None` for an identity supplied without one."""
    payload: dict[str, Any] = Field(default_factory=dict, repr=False)
    """Every claim on the token, including ones without a dedicated field."""

    def __hash__(self) -> int:
        # Defined here so pydantic does not install its own, which hashes every
        # field and fails on the `payload` dict. Equal identities have equal
        # payloads, so leaving it out keeps the hash consistent with equality.
        return hash(
            (
                type(self),
                *(getattr(self, name) for name in type(self).model_fields if name != "payload"),
            )
        )
