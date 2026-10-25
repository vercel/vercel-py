"""Passport identity values and errors."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, TypeAlias

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


@dataclass(frozen=True)
class PassportDevelopmentIdentity:
    """Overrides for the identity synthesized in local development.

    Each field falls back to its `VERCEL_PASSPORT_DEV_*` environment variable,
    then to a placeholder value.
    """

    owner: str | None = None
    owner_id: str | None = None
    connector_id: str | None = None
    external_issuer: str | None = None
    external_subject: str | None = None
    environment: str | None = None
    project: str | None = None
    project_id: str | None = None
    issuer: str | None = None
    audience: str | None = None


@dataclass(frozen=True)
class PassportOwner:
    """The Vercel team or account that owns the Passport-protected project."""

    slug: str
    id: str | None = None


@dataclass(frozen=True)
class PassportProject:
    """The Vercel project the Passport token was issued for."""

    id: str | None = None
    name: str | None = None


@dataclass(frozen=True)
class PassportIdentity:
    """A visitor identity established by Vercel Passport.

    `token` and `payload` are omitted from the repr so that logging an identity
    never writes a bearer credential to the logs.

    The fields are frozen, but `payload` holds a copy of the claims as a plain
    dict, so an identity can be pickled, copied, and serialized. `payload` takes
    no part in equality or hashing, so two identities without a `token` that
    differ only in claims lacking a dedicated field compare equal. Key on
    `payload` explicitly if those claims matter.
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
    token: str | None = field(default=None, repr=False)
    """The encoded token, or `None` for an identity supplied without one."""
    payload: PassportClaims = field(default_factory=dict, repr=False, compare=False, hash=False)
    """Every claim on the token, including ones without a dedicated field."""
