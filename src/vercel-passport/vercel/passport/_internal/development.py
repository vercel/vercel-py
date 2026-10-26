"""Identities used when no Passport token reaches the app.

Local development has no Passport header or cookie, because Passport creates
them in Vercel's edge network. So in local development an identity can be set
for the current context with `local_identity`, supplied as JSON in
`VERCEL_PASSPORT_IDENTITY`, or synthesized when `VERCEL_PASSPORT_DEV=1` opts in.
None of these identities is verified, and none is ever used on a Vercel
deployment other than `VERCEL_ENV=development`.
"""

from __future__ import annotations

import json
import os
import threading
import warnings
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, cast

from vercel.passport._internal.models import (
    PassportClaims,
    PassportDevelopmentIdentity,
    PassportDevelopmentIdentityWarning,
    PassportIdentityError,
)
from vercel.passport._internal.payload import build_identity

IDENTITY_ENV = "VERCEL_PASSPORT_IDENTITY"
DEVELOPMENT_ENV = "VERCEL_PASSPORT_DEV"
_ENABLED = ("1", "true")

DEVELOPMENT_WARNING = (
    f"using a synthesized development Passport identity because {DEVELOPMENT_ENV} is set; "
    "unset it to disable"
)

_override: ContextVar[PassportClaims | None] = ContextVar(
    "vercel_passport_local_identity", default=None
)

_warned_lock = threading.Lock()
_warned = False


@contextmanager
def local_identity(identity: PassportDevelopmentIdentity | Mapping[str, Any]) -> Iterator[None]:
    """Use `identity` for requests without a Passport token in this context.

    For tests and local development: inside the block, `get_identity` returns
    this identity, unverified, when the request carries no token. It is ignored
    on a Vercel deployment other than `VERCEL_ENV=development`. Like any context
    variable, it is visible to tasks and threads started inside the block.

    Args:
        identity: A `PassportDevelopmentIdentity`, or the full claims of a
            Passport token.

    Raises:
        PassportIdentityError: If the claims are not Passport-shaped.
    """
    claims = (
        identity.to_claims()
        if isinstance(identity, PassportDevelopmentIdentity)
        else dict(identity)
    )
    # Checked on entry, so a malformed identity fails where it is set rather
    # than at some later `get_identity`.
    build_identity(claims, token=None, token_source="local", verified=False)
    token = _override.set(claims)
    try:
        yield
    finally:
        _override.reset(token)


def local_claims() -> PassportClaims | None:
    """Return the claims of the local identity to use, if any.

    The context's `local_identity` comes first, then `VERCEL_PASSPORT_IDENTITY`,
    then a synthesized identity when `VERCEL_PASSPORT_DEV` opts in.
    """
    if not is_local_development():
        return None
    overridden = _override.get()
    if overridden is not None:
        return overridden
    from_env = os.environ.get(IDENTITY_ENV)
    if from_env:
        return _parse(from_env)
    return _development_identity()


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


def _development_identity() -> PassportClaims | None:
    # Opt-in: an identity that authenticates every request must never appear
    # just because a process is not on a Vercel deployment, such as a
    # self-hosted production server without `VERCEL_ENV`.
    if os.environ.get(DEVELOPMENT_ENV, "").strip().lower() not in _ENABLED:
        return None
    # Built per call rather than once, so the environment is read every time.
    claims = PassportDevelopmentIdentity().to_claims()
    _warn_once()
    return claims


def _warn_once() -> None:
    global _warned
    with _warned_lock:
        if _warned:
            return
        _warned = True
    # Attributed to the line that called `get_identity`: this frame, then
    # `_development_identity`, `local_claims`, `resolve_identity`, and the
    # public `get_identity` or `require_identity` of either surface.
    warnings.warn(DEVELOPMENT_WARNING, PassportDevelopmentIdentityWarning, stacklevel=6)
