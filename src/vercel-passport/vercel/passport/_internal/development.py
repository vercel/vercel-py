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
from typing import Any, cast

from vercel.passport._internal.models import (
    PassportClaims,
    PassportDevelopmentIdentity,
    PassportDevelopmentIdentityWarning,
    PassportIdentityError,
)
from vercel.passport._internal.payload import PASSPORT_ISSUER

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
    development: bool | PassportDevelopmentIdentity,
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
    development: bool | PassportDevelopmentIdentity,
) -> PassportClaims | None:
    if development is False or not is_local_development():
        return None
    if os.environ.get("VERCEL_PASSPORT_DEV") in ("0", "false"):
        return None

    config = (
        development
        if isinstance(development, PassportDevelopmentIdentity)
        else PassportDevelopmentIdentity()
    )

    def setting(value: str | None, env: str, default: str | None = None) -> str | None:
        return value if value is not None else os.environ.get(env, default)

    owner = setting(config.owner, "VERCEL_PASSPORT_DEV_OWNER", "local")
    owner_id = setting(config.owner_id, "VERCEL_PASSPORT_DEV_OWNER_ID")
    connector_id = setting(config.connector_id, "VERCEL_PASSPORT_DEV_CONNECTOR_ID", "local")
    external_issuer = setting(config.external_issuer, "VERCEL_PASSPORT_DEV_EXTERNAL_ISS")
    external_subject = setting(
        config.external_subject, "VERCEL_PASSPORT_DEV_EXTERNAL_SUB", "test-user"
    )
    environment = setting(config.environment, "VERCEL_PASSPORT_DEV_ENVIRONMENT", "development")
    project = setting(config.project, "VERCEL_PASSPORT_DEV_PROJECT", "local")
    project_id = setting(
        config.project_id,
        "VERCEL_PASSPORT_DEV_PROJECT_ID",
        os.environ.get("VERCEL_PROJECT_ID"),
    )
    issuer = setting(config.issuer, "VERCEL_PASSPORT_DEV_ISSUER", f"{PASSPORT_ISSUER}/{owner}")
    audience = setting(
        config.audience,
        "VERCEL_PASSPORT_DEV_AUDIENCE",
        f"https://vercel.com/{owner}/{project}/{environment}",
    )
    subject = f"owner:{owner}:connector:{connector_id}:principal:{external_subject}"

    _warn_once()

    claims: dict[str, Any] = {
        "aud": audience,
        "connector_id": connector_id,
        "email": "test-user@passport.local",
        "environment": environment,
        "external_sub": external_subject,
        "iss": issuer,
        "name": "Test User",
        "owner": owner,
        "project": project,
        "scope": subject,
        "sub": subject,
        "typ": "passport",
    }
    # Optional claims are omitted when unset or empty, as in the TypeScript
    # package, so an empty `VERCEL_PROJECT_ID` yields no `project_id` claim.
    optional = {"external_iss": external_issuer, "owner_id": owner_id, "project_id": project_id}
    claims.update({name: value for name, value in optional.items() if value})
    return claims


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
