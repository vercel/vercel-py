"""Identities used when no Passport token reaches the app.

Local development has no Passport header or cookie, because Passport creates
them in Vercel's edge network. So outside a deployment the caller can supply an
identity, or one is synthesized. None of these identities is verified, and a
synthesized one is never produced on a non-development Vercel deployment.
"""

from __future__ import annotations

import json
import os
import threading
import warnings
from collections.abc import Mapping
from typing import Any, Literal, cast

from vercel.passport._internal.models import (
    PassportClaims,
    PassportDevelopmentIdentity,
    PassportDevelopmentIdentityWarning,
    PassportIdentityError,
)

DEVELOPMENT_WARNING = (
    "using a local development Passport identity; set VERCEL_PASSPORT_DEV=0 or pass "
    "development=False to disable it"
)

_warned_lock = threading.Lock()
_warned = False


def local_identity(
    *,
    explicit: Mapping[str, Any] | str | None,
    identity_env: str,
    development: PassportDevelopmentIdentity | Literal[False] | None,
) -> PassportClaims | None:
    """Return the caller-supplied or synthesized identity claims, if any."""
    if isinstance(explicit, str):
        if explicit:
            return _parse(explicit)
    elif explicit is not None:
        return explicit

    from_env = os.environ.get(identity_env)
    if from_env:
        return _parse(from_env)

    return _development_identity(development)


def is_local_development() -> bool:
    """Whether this process is local development rather than a deployment."""
    vercel_env = os.environ.get("VERCEL_ENV") or None
    if os.environ.get("VERCEL") == "1" and vercel_env != "development":
        return False
    return vercel_env in (None, "development")


def reset_development_warning() -> None:
    """Allow the development warning to be emitted again. Intended for tests."""
    global _warned
    with _warned_lock:
        _warned = False


def _parse(value: str) -> PassportClaims:
    try:
        parsed: object = json.loads(value)
    except ValueError as exc:
        raise PassportIdentityError(f"local Passport identity is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise PassportIdentityError(
            f"local Passport identity must be a JSON object, not {type(parsed).__name__}"
        )
    return cast("dict[str, Any]", parsed)


def _development_identity(
    development: PassportDevelopmentIdentity | Literal[False] | None,
) -> PassportClaims | None:
    if development is False or not is_local_development():
        return None
    if os.environ.get("VERCEL_PASSPORT_DEV") in ("0", "false"):
        return None
    # Built here rather than as a parameter default, so the environment is read
    # on every call instead of once at import.
    identity = PassportDevelopmentIdentity() if development is None else development
    _warn_once()
    return identity.to_claims()


def _warn_once() -> None:
    global _warned
    with _warned_lock:
        if _warned:
            return
        _warned = True
    # Attributed to the line that called `get_identity`: this frame, then
    # `_development_identity`, `local_identity`, `resolve_identity`, and the
    # public `get_identity` of either surface.
    warnings.warn(DEVELOPMENT_WARNING, PassportDevelopmentIdentityWarning, stacklevel=6)
