"""Passport payload decoding, shape validation, and identity construction."""

from __future__ import annotations

import base64
import binascii
import json
import re
from collections.abc import Mapping
from typing import Any, cast

from vercel.oidc.verify import VERCEL_PASSPORT_ISSUER
from vercel.passport._internal.models import (
    PassportClaims,
    PassportIdentity,
    PassportIdentityError,
    PassportOwner,
    PassportProject,
    TokenSource,
)

PASSPORT_ISSUER = VERCEL_PASSPORT_ISSUER
# Matches the owner segment `vercel.oidc` accepts for verified tokens, so an
# unverified identity is held to the same issuer shape as a verified one.
_OWNER_SLUG = re.compile(r"[A-Za-z0-9_-]+")


def decode_unverified(token: str) -> PassportClaims:
    """Decode a JWT payload without checking its signature."""
    parts = token.split(".")
    if len(parts) != 3 or not parts[1]:
        raise PassportIdentityError("Passport identity token is not a JWT")
    encoded = parts[1]
    try:
        raw = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        payload: object = json.loads(raw)
    except (binascii.Error, ValueError) as exc:
        raise PassportIdentityError(f"Passport identity token payload is invalid: {exc}") from exc
    if not isinstance(payload, dict):
        raise PassportIdentityError("Passport identity token payload is not a JSON object")
    return cast("dict[str, Any]", payload)


def is_passport_issuer(value: object) -> bool:
    if value == PASSPORT_ISSUER:
        return True
    prefix = f"{PASSPORT_ISSUER}/"
    return (
        isinstance(value, str)
        and value.startswith(prefix)
        and _OWNER_SLUG.fullmatch(value[len(prefix) :]) is not None
    )


def build_identity(
    claims: PassportClaims,
    *,
    token: str | None,
    token_source: TokenSource,
    verified: bool,
) -> PassportIdentity:
    """Validate that `claims` are Passport-shaped and build the identity.

    The shape is checked for verified tokens too, so a valid Vercel OIDC token
    that is not a Passport identity is never accepted as one.
    """
    if claims.get("typ") != "passport":
        raise PassportIdentityError('Passport identity token is missing typ "passport"')
    subject = _string(claims, "sub")
    if not subject:
        raise PassportIdentityError('Passport identity is missing a "sub" claim')
    issuer = claims.get("iss")
    if not is_passport_issuer(issuer):
        raise PassportIdentityError(
            f"Passport identity issuer {issuer!r} is not {PASSPORT_ISSUER} "
            "or an owner-scoped issuer under it"
        )
    owner = _string(claims, "owner")
    if not owner:
        raise PassportIdentityError('Passport identity is missing an "owner" claim')
    connector_id = _string(claims, "connector_id")
    if not connector_id:
        raise PassportIdentityError('Passport identity is missing a "connector_id" claim')
    external_subject = _string(claims, "external_sub")
    if not external_subject:
        raise PassportIdentityError('Passport identity is missing an "external_sub" claim')

    project_id = _string(claims, "project_id")
    # Only a project ID identifies a project; every verified token has one.
    project = (
        PassportProject(id=project_id, name=_string(claims, "project")) if project_id else None
    )

    return PassportIdentity(
        subject=subject,
        external_subject=external_subject,
        connector_id=connector_id,
        owner=PassportOwner(slug=owner, id=_string(claims, "owner_id")),
        token_source=token_source,
        verified=verified,
        external_issuer=_string(claims, "external_iss"),
        email=_string(claims, "email"),
        name=_string(claims, "name"),
        project=project,
        environment=_string(claims, "environment"),
        token=token,
        # A copy, so the identity is unaffected by later changes to `claims`.
        payload=dict(claims),
    )


def _string(claims: Mapping[str, Any], name: str) -> str | None:
    value = claims.get(name)
    return value if isinstance(value, str) else None
