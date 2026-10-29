"""Sync mirror of `vercel.passport`.

Every name here matches `vercel.passport`, with no `await`. Verification fetches
Vercel's JWKS when its cache is cold, so in async code prefer the async surface,
which does not block the event loop while it does.
"""

from collections.abc import Mapping, Sequence

from vercel.passport._internal.development import local_identity
from vercel.passport._internal.models import (
    PassportClaims,
    PassportDevelopmentIdentity,
    PassportDevelopmentIdentityWarning,
    PassportError,
    PassportIdentity,
    PassportIdentityError,
    PassportOwner,
    PassportProject,
    PassportValidationError,
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
    project_id: str | None = None,
    environment: str | None = None,
    owner_id: str | None = None,
    audience: str | Sequence[str] | None = None,
) -> PassportIdentity | None:
    """Read the Passport identity of the current request.

    Blocks while it fetches Vercel's JWKS on a cold cache. In async code, use
    `vercel.passport.get_identity`, which does not block the event loop.

    With no `request`, the headers of the current request come from the Vercel
    Python runtime, which records them for every request it serves. The trusted
    `x-vercel-oidc-passport-token` header is preferred, then the
    `_vercel_passport` cookie, which may be split across several cookies. A
    token from the request is always verified.

    When the request carries no token, local development can still have an
    identity: the one set with `local_identity`, then the JSON in
    `VERCEL_PASSPORT_IDENTITY`, then, when `VERCEL_PASSPORT_DEV=1`, a synthesized
    `PassportDevelopmentIdentity`. None of these is verified, and none is used on
    a Vercel deployment other than `VERCEL_ENV=development`.

    Args:
        request: Request headers, or any request object exposing a `headers`
            mapping (Starlette, FastAPI, Django, Flask, httpx).
        token: A Passport token to use instead of the request's, for local
            debugging. It is verified only when `verify` is true.
        cookies: Request cookies, such as `request.cookies`. Checked before the
            `Cookie` header.
        verify: Verify `token` or the `VERCEL_PASSPORT_TOKEN` token.
        allow_unverified: Accept `token` or the `VERCEL_PASSPORT_TOKEN` token
            unverified when `verify` is false. Defaults to accepting it only in
            local development, so on a Vercel deployment other than
            `VERCEL_ENV=development` an unverified token raises unless this is
            true.
        project_id: Expected project. Defaults to `VERCEL_PROJECT_ID`.
        environment: Expected environment. Defaults to `VERCEL_TARGET_ENV`, then
            `VERCEL_ENV`.
        owner_id: Expected team owner. Checked only when supplied.
        audience: Expected audience. Checked only when supplied.

    Returns:
        The identity, or `None` when there is none.

    Raises:
        PassportVerificationError: If a token does not verify, a claim does not
            match, or the expected project and environment cannot be resolved.
        PassportIdentityError: If a token, chunked cookie, or identity is
            malformed or not Passport-shaped, or an unverified token is not
            allowed.
    """
    outcome = resolve_identity(
        request,
        token=token,
        cookies=cookies,
        verify=verify,
        allow_unverified=allow_unverified,
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
    project_id: str | None = None,
    environment: str | None = None,
    owner_id: str | None = None,
    audience: str | Sequence[str] | None = None,
) -> PassportIdentity:
    """Read the Passport identity of the current request, requiring a token.

    Blocks while it fetches Vercel's JWKS on a cold cache. In async code, use
    `vercel.passport.require_identity`, which does not block the event loop.

    Resolves the identity exactly as `get_identity` does, but raises instead of
    returning `None`, and also rejects an identity without a token, such as a
    local or synthesized development identity. The returned identity's `token`
    is always set, so it can be forwarded to another backend.

    Args:
        request: Request headers, or any request object exposing a `headers`
            mapping (Starlette, FastAPI, Django, Flask, httpx).
        token: A Passport token to use instead of the request's, for local
            debugging. It is verified only when `verify` is true.
        cookies: Request cookies, such as `request.cookies`. Checked before the
            `Cookie` header.
        verify: Verify `token` or the `VERCEL_PASSPORT_TOKEN` token.
        allow_unverified: Accept `token` or the `VERCEL_PASSPORT_TOKEN` token
            unverified when `verify` is false. Defaults to accepting it only in
            local development, so on a Vercel deployment other than
            `VERCEL_ENV=development` an unverified token raises unless this is
            true.
        project_id: Expected project. Defaults to `VERCEL_PROJECT_ID`.
        environment: Expected environment. Defaults to `VERCEL_TARGET_ENV`, then
            `VERCEL_ENV`.
        owner_id: Expected team owner. Checked only when supplied.
        audience: Expected audience. Checked only when supplied.

    Returns:
        The identity, with its `token` set.

    Raises:
        PassportIdentityError: If there is no identity or it has no token, or a
            token, chunked cookie, or identity is malformed or not
            Passport-shaped, or an unverified token is not allowed.
        PassportVerificationError: If a token does not verify, a claim does not
            match, or the expected project and environment cannot be resolved.
    """
    outcome = resolve_identity(
        request,
        token=token,
        cookies=cookies,
        verify=verify,
        allow_unverified=allow_unverified,
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

    Blocks while it fetches Vercel's JWKS on a cold cache. In async code, use
    `vercel.passport.verify_identity`, which does not block the event loop.

    Use this in a backend that receives a Passport token from another app.
    Unlike `get_identity`, it always verifies and never falls back to a local or
    development identity.

    Verification pins the issuer to Vercel Passport, allows only RS256, resolves
    the signing key from Vercel's JWKS, and requires the Passport token shape, so
    a regular Vercel OIDC token is never accepted. Time claims allow 60 seconds
    of clock skew.

    Expectations compare by equality: `"*"` is not a wildcard and lists are not
    accepted. It fails closed: if the expected project or environment cannot be
    resolved, every token is rejected. If the token was minted for a different
    project than the one verifying it, pass that project's `project_id` and
    `environment`.

    Args:
        source: The token itself, request headers, or any request object exposing
            a `headers` mapping. Headers are searched for an `Authorization:
            Bearer` token, then the Passport header, then the Passport cookie.
        cookies: Request cookies, checked before the `Cookie` header.
        project_id: Expected project. Defaults to `VERCEL_PROJECT_ID`.
        environment: Expected environment. Defaults to `VERCEL_TARGET_ENV`, then
            `VERCEL_ENV`.
        owner_id: Expected team owner. Checked only when supplied.
        audience: Expected audience. Checked only when supplied.

    Returns:
        The verified identity.

    Raises:
        PassportVerificationError: If the token does not verify, a claim does not
            match, or the expected project and environment cannot be resolved.
        PassportIdentityError: If no token is found, a chunked cookie is
            malformed, or the token is not Passport-shaped.
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
    "PassportValidationError",
    "PassportVerificationError",
    "TokenSource",
    "get_identity",
    "local_identity",
    "require_identity",
    "verify_identity",
]
