"""Sync mirror of `vercel.passport`.

Every name here matches `vercel.passport`, with no `await`. Verification fetches
Vercel's JWKS when its cache is cold, so in async code prefer the async surface,
which does not block the event loop while it does.
"""

from collections.abc import Mapping, Sequence
from typing import Any

from vercel.passport._internal.models import (
    PassportClaims,
    PassportDevelopmentIdentity,
    PassportDevelopmentIdentityWarning,
    PassportError,
    PassportIdentity,
    PassportIdentityError,
    PassportOwner,
    PassportProject,
    PassportVerificationError,
    TokenSource,
)
from vercel.passport._internal.resolve import (
    PASSPORT_COOKIE_NAME,
    PASSPORT_HEADER_NAME,
    Expectations,
    PendingVerification,
    required_identity,
    resolve_identity,
    resolve_token,
    verified_identity,
    verify_sync,
)


def get_identity(
    request: object | None = None,
    *,
    token: str | None = None,
    cookies: Mapping[str, str] | None = None,
    verify: bool = False,
    allow_unverified: bool | None = None,
    development: bool | PassportDevelopmentIdentity = True,
    local_identity: Mapping[str, Any] | str | None = None,
    local_identity_env: str = "VERCEL_PASSPORT_IDENTITY",
    local_token_env: str = "VERCEL_PASSPORT_TOKEN",
    project_id: str | None = None,
    environment: str | None = None,
    owner_id: str | None = None,
    audience: str | Sequence[str] | None = None,
) -> PassportIdentity | None:
    """Read the Passport identity of the current request.

    See `vercel.passport.get_identity`, which takes the same arguments.
    """
    outcome = resolve_identity(
        request,
        token=token,
        cookies=cookies,
        verify=verify,
        allow_unverified=allow_unverified,
        development=development,
        local_identity_value=local_identity,
        local_identity_env=local_identity_env,
        local_token_env=local_token_env,
    )
    if not isinstance(outcome, PendingVerification):
        return outcome
    expectations = Expectations(
        project_id=project_id, environment=environment, owner_id=owner_id, audience=audience
    )
    return verified_identity(outcome, verify_sync(outcome, expectations))


def require_identity(
    request: object | None = None,
    *,
    token: str | None = None,
    cookies: Mapping[str, str] | None = None,
    verify: bool = False,
    allow_unverified: bool | None = None,
    development: bool | PassportDevelopmentIdentity = True,
    local_identity: Mapping[str, Any] | str | None = None,
    local_identity_env: str = "VERCEL_PASSPORT_IDENTITY",
    local_token_env: str = "VERCEL_PASSPORT_TOKEN",
    project_id: str | None = None,
    environment: str | None = None,
    owner_id: str | None = None,
    audience: str | Sequence[str] | None = None,
) -> PassportIdentity:
    """Read the Passport identity of the current request, requiring a token.

    See `vercel.passport.require_identity`, which takes the same arguments.
    """
    outcome = resolve_identity(
        request,
        token=token,
        cookies=cookies,
        verify=verify,
        allow_unverified=allow_unverified,
        development=development,
        local_identity_value=local_identity,
        local_identity_env=local_identity_env,
        local_token_env=local_token_env,
    )
    if not isinstance(outcome, PendingVerification):
        return required_identity(outcome)
    expectations = Expectations(
        project_id=project_id, environment=environment, owner_id=owner_id, audience=audience
    )
    return verified_identity(outcome, verify_sync(outcome, expectations))


def verify_identity(
    source: str | object | None,
    *,
    cookies: Mapping[str, str] | None = None,
    project_id: str | None = None,
    environment: str | None = None,
    owner_id: str | None = None,
    audience: str | Sequence[str] | None = None,
) -> PassportIdentity:
    """Verify a forwarded Passport token and return its identity.

    See `vercel.passport.verify_identity`, which takes the same arguments.
    """
    pending = resolve_token(source, cookies=cookies)
    expectations = Expectations(
        project_id=project_id, environment=environment, owner_id=owner_id, audience=audience
    )
    return verified_identity(pending, verify_sync(pending, expectations))


__all__ = [
    "PASSPORT_COOKIE_NAME",
    "PASSPORT_HEADER_NAME",
    "PassportClaims",
    "PassportDevelopmentIdentity",
    "PassportDevelopmentIdentityWarning",
    "PassportError",
    "PassportIdentity",
    "PassportIdentityError",
    "PassportOwner",
    "PassportProject",
    "PassportVerificationError",
    "TokenSource",
    "get_identity",
    "require_identity",
    "verify_identity",
]
