"""Decoding a Passport token's payload without verifying it."""

from __future__ import annotations

import base64
import binascii
import json
from typing import Any, cast

from vercel.passport._internal.models import PassportClaims, PassportIdentityError


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
