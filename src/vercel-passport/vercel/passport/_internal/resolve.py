"""Token resolution shared by the async and sync surfaces.

Resolution is pure: it decides which token to use, where it came from, and
whether it must be verified. Only verification differs between the two surfaces,
so each surface awaits or calls it and then finishes with `verified_identity`.
"""

from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from vercel.headers import get_headers
from vercel.oidc import verify as oidc_verify
from vercel.passport._internal.development import is_local_development, local_identity
from vercel.passport._internal.models import (
    PassportClaims,
    PassportDevelopmentIdentity,
    PassportIdentity,
    PassportIdentityError,
    PassportVerificationError,
    TokenSource,
)
from vercel.passport._internal.payload import build_identity, decode_unverified

PASSPORT_HEADER_NAME = "x-vercel-oidc-passport-token"
PASSPORT_COOKIE_NAME = "_vercel_passport"

_BEARER = re.compile(r"bearer\s+(\S.*)", re.IGNORECASE)

# A session token too large for one cookie is split across several. The
# `_vercel_passport` cookie then holds a manifest naming the chunk count and the
# SHA-256 of the joined token, and chunk `i` is the cookie
# `_vercel_passport.<i>.<generation>`, where the generation is the checksum's
# first 32 hex digits, so chunks from an older session never mix with a newer one.
_CHUNKED_PREFIX = "__vc_chunked_v1:"
# The count is bounded in the pattern too, so an absurd digit string is rejected
# as an invalid manifest rather than reaching `int()`. Leading zeros are allowed,
# as in the TypeScript reader.
_CHUNKED_MANIFEST = re.compile(r"__vc_chunked_v1:0*([0-9]{1,3}):([0-9a-f]{64})")
_CHUNK_GENERATION_LENGTH = 32
# A defensive reader bound, deliberately above the writer's current limit so the
# two can evolve independently. The same bound as the TypeScript reader.
_MAX_CHUNKS = 100


@dataclass(frozen=True)
class Expectations:
    """What a verified token must match. Unset values come from the environment."""

    project_id: str | None = None
    environment: str | None = None
    owner_id: str | None = None
    audience: str | Sequence[str] | None = None


@dataclass(frozen=True)
class PendingVerification:
    """A token that must be verified before it becomes an identity."""

    token: str
    token_source: TokenSource


def resolve_identity(
    request: object | None,
    *,
    token: str | None,
    cookies: Mapping[str, str] | None,
    verify: bool,
    allow_unverified: bool | None,
    development: bool | PassportDevelopmentIdentity,
    local_identity_value: Mapping[str, Any] | str | None,
    local_identity_env: str,
    local_token_env: str,
) -> PassportIdentity | PendingVerification | None:
    """Decide what `get_identity` returns, short of verifying a token."""
    headers = _resolve_headers(request) if request is not None else get_headers()

    resolved: str | None
    source: TokenSource
    if token:
        resolved, source = token, "local"
    elif header_token := _header(headers, PASSPORT_HEADER_NAME):
        resolved, source = header_token, "header"
    # Read only when no better source exists: reassembling a chunked cookie can
    # fail, and a corrupt cookie must not break a request with a valid header.
    elif cookie_token := _cookie(cookies, headers):
        resolved, source = cookie_token, "cookie"
    else:
        resolved, source = os.environ.get(local_token_env) or None, "local"

    if resolved:
        # A token from the request is always verified. Only a token the caller
        # supplied directly may skip verification, and only when asked to.
        if source != "local" or verify:
            return PendingVerification(token=resolved, token_source=source)
        # Unset means "only in local development", so a debugging token left in
        # deployed code or its environment never becomes an unverified identity.
        if allow_unverified is None:
            allow_unverified = is_local_development()
        if not allow_unverified:
            raise PassportIdentityError(
                "Passport identity token verification is disabled and unverified tokens "
                "are not allowed outside local development; pass verify=True, use "
                "verify_identity(), or pass allow_unverified=True"
            )
        return build_identity(
            decode_unverified(resolved), token=resolved, token_source=source, verified=False
        )

    claims = local_identity(
        explicit=local_identity_value,
        identity_env=local_identity_env,
        development=development,
    )
    if claims is None:
        return None
    return build_identity(claims, token=None, token_source="local", verified=False)


def resolve_token(
    source: object | None,
    *,
    cookies: Mapping[str, str] | None,
) -> PendingVerification:
    """Find the token `verify_identity` must verify."""
    if isinstance(source, str):
        if not source:
            raise PassportIdentityError("Passport identity token is empty")
        return PendingVerification(token=source, token_source="local")

    headers = _resolve_headers(source) if source is not None else None
    bearer = _bearer(headers)
    if bearer:
        return PendingVerification(token=bearer, token_source="header")
    header_token = _header(headers, PASSPORT_HEADER_NAME)
    if header_token:
        return PendingVerification(token=header_token, token_source="header")
    cookie_token = _cookie(cookies, headers)
    if cookie_token:
        return PendingVerification(token=cookie_token, token_source="cookie")
    raise PassportIdentityError("Passport identity token was not found")


def verify_sync(pending: PendingVerification, expectations: Expectations) -> PassportClaims:
    try:
        return oidc_verify.verify_vercel_oidc_token(
            pending.token,
            project_id=expectations.project_id,
            environment=expectations.environment,
            owner_id=expectations.owner_id,
            audience=expectations.audience,
            issuer="passport",
        )
    except oidc_verify.VercelOidcVerificationError as exc:
        raise PassportVerificationError(str(exc)) from exc


async def verify_async(pending: PendingVerification, expectations: Expectations) -> PassportClaims:
    try:
        return await oidc_verify.verify_vercel_oidc_token_async(
            pending.token,
            project_id=expectations.project_id,
            environment=expectations.environment,
            owner_id=expectations.owner_id,
            audience=expectations.audience,
            issuer="passport",
        )
    except oidc_verify.VercelOidcVerificationError as exc:
        raise PassportVerificationError(str(exc)) from exc


def verified_identity(pending: PendingVerification, claims: PassportClaims) -> PassportIdentity:
    return build_identity(
        claims, token=pending.token, token_source=pending.token_source, verified=True
    )


def required_identity(identity: PassportIdentity | None) -> PassportIdentity:
    """Reject a missing identity, or one without a token, for `require_identity`.

    A local or development identity carries no token, so it is rejected too.
    """
    if identity is None or not identity.token:
        raise PassportIdentityError("Passport identity token was not found")
    return identity


def _resolve_headers(request: object) -> Mapping[str, Any]:
    """Accept a header mapping, or any request object exposing one."""
    candidate = getattr(request, "headers", request)
    if not hasattr(candidate, "items") or not hasattr(candidate, "get"):
        raise TypeError(
            "request must be a mapping of header names to values, or a request object "
            f"exposing one; got {type(request).__name__}"
        )
    return cast("Mapping[str, Any]", candidate)


def _header(headers: Mapping[str, Any] | None, name: str) -> str | None:
    if headers is None:
        return None
    # Framework header objects match case-insensitively on `get`; a plain dict
    # does not, so fall back to a scan.
    value = headers.get(name)
    if value is None:
        value = next((v for k, v in headers.items() if str(k).lower() == name), None)
    if isinstance(value, (list, tuple)):
        value = value[0] if value else None
    return value if isinstance(value, str) and value else None


def _bearer(headers: Mapping[str, Any] | None) -> str | None:
    # Anything other than a bearer credential is not a Passport token, so it is
    # skipped rather than rejected and the remaining sources are still checked.
    authorization = _header(headers, "authorization")
    match = _BEARER.fullmatch(authorization.strip()) if authorization else None
    return match.group(1).strip() if match else None


def _cookie(cookies: Mapping[str, str] | None, headers: Mapping[str, Any] | None) -> str | None:
    """Return the Passport cookie's token, reassembling it if it is chunked."""
    stored = _cookie_value(cookies, headers, PASSPORT_COOKIE_NAME)
    if not stored or not stored.startswith(_CHUNKED_PREFIX):
        return stored

    manifest = _CHUNKED_MANIFEST.fullmatch(stored)
    count = int(manifest.group(1)) if manifest else 0
    if manifest is None or not 1 <= count <= _MAX_CHUNKS:
        raise PassportIdentityError("Passport session cookie manifest is invalid")
    checksum = manifest.group(2)
    generation = checksum[:_CHUNK_GENERATION_LENGTH]

    chunks: list[str] = []
    for index in range(1, count + 1):
        chunk = _cookie_value(cookies, headers, f"{PASSPORT_COOKIE_NAME}.{index}.{generation}")
        if chunk is None:
            raise PassportIdentityError(
                f"Passport session cookie is missing chunk {index} of {count}"
            )
        chunks.append(chunk)

    token = "".join(chunks)
    # A caller-supplied mapping can hold text UTF-8 cannot encode, such as a lone
    # surrogate. Replacing it can only produce a mismatch, so it is reported as
    # one rather than escaping as a `UnicodeEncodeError`.
    if hashlib.sha256(token.encode(errors="replace")).hexdigest() != checksum:
        raise PassportIdentityError("Passport session cookie checksum does not match its chunks")
    return token


def _cookie_value(
    cookies: Mapping[str, str] | None, headers: Mapping[str, Any] | None, name: str
) -> str | None:
    """Look up one cookie in `cookies`, then in the `Cookie` header."""
    if cookies is not None:
        value = cookies.get(name)
        if value:
            return value
    cookie_header = _header(headers, "cookie")
    if not cookie_header:
        return None
    for part in cookie_header.split(";"):
        key, separator, value = part.strip().partition("=")
        if separator and key == name and value:
            return value
    return None
