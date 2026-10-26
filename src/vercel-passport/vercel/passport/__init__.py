"""Vercel Passport identity helpers.

Passport authenticates visitors at Vercel's edge, then forwards their identity
to the app as a signed token in the `x-vercel-oidc-passport-token` header. These
helpers read that identity and verify it.

`get_identity` reads the identity of the current request, and `require_identity`
does the same but raises unless there is one with a token. `verify_identity`
verifies a Passport token another app forwarded, typically as an
`Authorization: Bearer` header. `vercel.passport.sync` mirrors all three with
the same names and arguments, without `await`.
"""

from collections.abc import Mapping, Sequence
from typing import Any, Literal

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
    verify_async,
)
from vercel.passport.version import __version__

from . import sync


async def get_identity(
    request: object | None = None,
    *,
    token: str | None = None,
    cookies: Mapping[str, str] | None = None,
    verify: bool = False,
    allow_unverified: bool | None = None,
    development: PassportDevelopmentIdentity | Literal[False] | None = None,
    local_identity: Mapping[str, Any] | str | None = None,
    local_identity_env: str = "VERCEL_PASSPORT_IDENTITY",
    local_token_env: str = "VERCEL_PASSPORT_TOKEN",
    project_id: str | None = None,
    environment: str | None = None,
    owner_id: str | None = None,
    audience: str | Sequence[str] | None = None,
) -> PassportIdentity | None:
    """Read the Passport identity of the current request.

    With no `request`, the headers of the current request come from the Vercel
    Python runtime, which records them for every request it serves. The trusted
    `x-vercel-oidc-passport-token` header is preferred, then the
    `_vercel_passport` cookie, which may be split across several cookies. A
    token from the request is always verified.

    When the request carries no token, the identity comes from `local_identity`,
    then the `VERCEL_PASSPORT_IDENTITY` environment variable, then, in local
    development only, a synthesized test identity. None of these is verified.

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
        development: The identity to synthesize in local development. `None`
            builds a default `PassportDevelopmentIdentity`, whose fields come
            from `VERCEL_PASSPORT_DEV_*` environment variables, and `False`
            never synthesizes one. `VERCEL_PASSPORT_DEV=0` also disables it.
        local_identity: Identity claims to use when the request has no token,
            as a mapping or a JSON string.
        local_identity_env: Environment variable holding JSON identity claims.
        local_token_env: Environment variable holding a Passport token for local
            debugging.
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
    return verified_identity(outcome, await verify_async(outcome, expectations))


async def require_identity(
    request: object | None = None,
    *,
    token: str | None = None,
    cookies: Mapping[str, str] | None = None,
    verify: bool = False,
    allow_unverified: bool | None = None,
    development: PassportDevelopmentIdentity | Literal[False] | None = None,
    local_identity: Mapping[str, Any] | str | None = None,
    local_identity_env: str = "VERCEL_PASSPORT_IDENTITY",
    local_token_env: str = "VERCEL_PASSPORT_TOKEN",
    project_id: str | None = None,
    environment: str | None = None,
    owner_id: str | None = None,
    audience: str | Sequence[str] | None = None,
) -> PassportIdentity:
    """Read the Passport identity of the current request, requiring a token.

    Takes the same arguments as `get_identity`, but raises instead of returning
    `None`, and also rejects an identity without a token, such as a local or
    synthesized development identity. The returned identity's `token` is
    always set.

    Raises:
        PassportIdentityError: If there is no identity or it has no token, or
            for any reason `get_identity` raises it.
        PassportVerificationError: For any reason `get_identity` raises it.
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
    return verified_identity(outcome, await verify_async(outcome, expectations))


async def verify_identity(
    source: str | object | None,
    *,
    cookies: Mapping[str, str] | None = None,
    project_id: str | None = None,
    environment: str | None = None,
    owner_id: str | None = None,
    audience: str | Sequence[str] | None = None,
) -> PassportIdentity:
    """Verify a forwarded Passport token and return its identity.

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
    return verified_identity(pending, await verify_async(pending, expectations))


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
    "__version__",
    "get_identity",
    "require_identity",
    "sync",
    "verify_identity",
]
