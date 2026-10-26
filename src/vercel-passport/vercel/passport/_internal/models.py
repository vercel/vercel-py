"""Passport identity values and errors."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any, Literal, TypeAlias, TypeVar

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

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
    """Report a pydantic validation failure as the package's own error.

    A nested model's own `PassportValidationError` is unwrapped, so its message
    keeps the field path rather than pydantic's "Value error" wrapping. Only
    pydantic's message is kept, never the input, which could hold a token.
    """
    for error in exc.errors():
        original = (error.get("ctx") or {}).get("error")
        if isinstance(original, PassportValidationError):
            location = ".".join(str(part) for part in error["loc"])
            return PassportValidationError(f"{location}.{original}" if location else str(original))
    details = "; ".join(
        f"{'.'.join(str(part) for part in error['loc']) or 'value'}: {error['msg']}"
        for error in exc.errors()
    )
    return PassportValidationError(details)


_Model = TypeVar("_Model", bound="PassportModel")


class PassportModel(BaseModel):
    """Frozen, validated value type.

    `extra="forbid"` so a misspelled keyword is an error rather than a silently
    dropped field. Construction, `model_validate`, `model_validate_json`,
    assignment, and deletion all report failures as `PassportValidationError`
    rather than pydantic's own error, which spares callers an import of pydantic.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except ValidationError as exc:
            raise as_passport_error(exc) from exc

    @classmethod
    def model_validate(cls: type[_Model], obj: Any, *args: Any, **kwargs: Any) -> _Model:
        try:
            return super().model_validate(obj, *args, **kwargs)  # type: ignore[misc]
        except ValidationError as exc:
            raise as_passport_error(exc) from exc

    @classmethod
    def model_validate_json(
        cls: type[_Model], json_data: str | bytes | bytearray, *args: Any, **kwargs: Any
    ) -> _Model:
        try:
            return super().model_validate_json(json_data, *args, **kwargs)  # type: ignore[misc]
        except ValidationError as exc:
            raise as_passport_error(exc) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        # Assigning to a frozen model is a pydantic ValidationError; report it the
        # same way as every other misuse of a Passport value.
        try:
            super().__setattr__(name, value)
        except ValidationError as exc:
            raise as_passport_error(exc) from exc

    def __delattr__(self, name: str) -> None:
        try:
            super().__delattr__(name)
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
    # The factories read with `get`: before pydantic 2.12 they run even when the
    # field they read failed validation, and a `KeyError` would escape unwrapped.
    issuer: str = Field(
        default_factory=lambda data: f"{VERCEL_PASSPORT_ISSUER}/{data.get('owner')}",
        serialization_alias="iss",
    )
    audience: str = Field(
        default_factory=lambda data: (
            f"https://vercel.com/{data.get('owner')}/{data.get('project')}/{data.get('environment')}"
        ),
        serialization_alias="aud",
    )

    @field_validator("owner_id", "external_issuer", "project_id", mode="before")
    @classmethod
    def _empty_is_unset(cls, value: Any) -> Any:
        # An optional claim is omitted rather than emitted empty, as in the
        # TypeScript package.
        return None if value == "" else value

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

    `token` and `payload` are omitted from the repr, and `token` from
    `model_dump`, so neither logging an identity nor returning one from an
    endpoint exposes the bearer credential. Read it as `identity.token`.

    The fields are frozen, but `payload` holds a deep copy of the claims as a
    plain dict, so an identity can be pickled, copied, and serialized with
    `model_dump`. `payload` takes no part in equality or hashing: it stays
    mutable, and an identity used as a set member or dict key must not change.
    Two identities without a `token` that differ only in claims lacking a
    dedicated field therefore compare equal; key on `payload` if that matters.
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
    # Left out of the repr and of `model_dump`, so neither a log line nor an
    # identity returned from an endpoint ever carries the bearer credential.
    token: str | None = Field(default=None, repr=False, exclude=True)
    """The encoded token, or `None` for an identity supplied without one."""
    payload: dict[str, Any] = Field(default_factory=dict, repr=False)
    """Every claim on the token, including ones without a dedicated field."""

    def _key(self) -> tuple[Any, ...]:
        return (
            type(self),
            *(getattr(self, name) for name in type(self).model_fields if name != "payload"),
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PassportIdentity):
            return NotImplemented
        return self._key() == other._key()

    def __hash__(self) -> int:
        # Defined here so pydantic does not install its own, which hashes every
        # field and fails on the `payload` dict.
        return hash(self._key())
