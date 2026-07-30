"""Signature verification for Vercel OIDC tokens.

Verification is deliberately narrow, because every historical JWT library
vulnerability in this space is an algorithm- or key-confusion bug:

- ``RS256`` is the only accepted algorithm, passed explicitly. The token's own
  ``alg`` header is never consulted to select a verifier, so ``alg: none`` and
  HMAC confusion are structurally impossible.
- Key material is an RSA public key object resolved by ``kid`` from Vercel's
  JWKS, never a string that could be reinterpreted as an HMAC secret. An unknown
  ``kid`` is rejected; there is no "try every key" fallback.
- The JWKS is fetched by this module over https only, with a bounded cache and a
  rate-limited refetch on rotation. PyJWT's ``PyJWKClient`` is intentionally not
  used: it fetches with ``urllib``, cannot be injected for tests, and carries the
  SSRF and unbounded-refetch issues fixed in PyJWT 2.13.0.
- Claims are checked only after the signature verifies, and verification fails
  closed when the expected project or environment cannot be determined.

Requires the ``verify`` extra: ``pip install "vercel-oidc[verify]"``.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any

import httpx

from .token import VercelOidcTokenError

VERCEL_OIDC_ISSUER = "https://oidc.vercel.com"
JWKS_PATH = "/.well-known/jwks"
ALLOWED_ALGORITHMS = ("RS256",)
DEFAULT_LEEWAY = timedelta(seconds=60)
DEFAULT_JWKS_CACHE_TTL = timedelta(minutes=10)
DEFAULT_JWKS_MIN_REFETCH_INTERVAL = timedelta(seconds=30)
_JWKS_TIMEOUT_SECONDS = 10.0
_JWKS_TIMEOUT = httpx.Timeout(_JWKS_TIMEOUT_SECONDS)
_WILDCARD = "*"

_jwks_lock = threading.Lock()
_jwks_cache: dict[str, tuple[float, dict[str, Any]]] = {}
# Set only by an *unproductive* fetch: one that failed, or that returned a document
# still missing the requested `kid`. A productive fetch clears it, so a rotation is
# picked up immediately while an attacker cycling unknown `kid` values, or an
# outage, stays limited to one request per interval.
_jwks_throttled_until: dict[str, float] = {}
_jwks_fetch_in_progress: set[str] = set()
# Longer than the fetch itself, so a waiter gives up only if the owner neither
# succeeded nor failed.
FETCH_WAIT_TIMEOUT = _JWKS_TIMEOUT_SECONDS + 1.0
_FETCH_WAIT_INTERVAL = 0.02

_MISSING_EXTRA_MESSAGE = (
    "OIDC token verification requires the 'verify' extra: pip install \"vercel-oidc[verify]\""
)


class VercelOidcVerificationError(VercelOidcTokenError):
    """Raised when a Vercel OIDC token cannot be verified.

    Covers an unparseable token, an unknown or unusable signing key, a signature
    that does not verify, a claim that does not match, and an expectation that
    could not be resolved. Never raised to indicate success.
    """


def _require_pyjwt() -> Any:
    try:
        import jwt
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised by packaging
        raise VercelOidcVerificationError(_MISSING_EXTRA_MESSAGE, exc) from exc
    # PyJWT installs without `cryptography` by default, and defines RSAAlgorithm
    # only when it is present. Detect that here rather than failing later with an
    # AttributeError that hides the real remedy.
    if not hasattr(jwt.algorithms, "RSAAlgorithm"):  # pragma: no cover - env dependent
        raise VercelOidcVerificationError(_MISSING_EXTRA_MESSAGE)
    return jwt


def _jwks_url(issuer: str) -> str:
    # https only: a plaintext JWKS endpoint would let a network attacker choose
    # the signing key, which defeats the entire verification.
    if not issuer.startswith("https://"):
        raise VercelOidcVerificationError(f"OIDC issuer must be https, got {issuer!r}")
    return f"{issuer.rstrip('/')}{JWKS_PATH}"


def _cached_jwks(url: str) -> dict[str, Any] | None:
    with _jwks_lock:
        entry = _jwks_cache.get(url)
        if entry is None:
            return None
        fetched_at, document = entry
        if time.monotonic() - fetched_at > DEFAULT_JWKS_CACHE_TTL.total_seconds():
            return None
        return document


def _store_jwks(url: str, document: dict[str, Any]) -> None:
    with _jwks_lock:
        _jwks_cache[url] = (time.monotonic(), document)


def _throttle_fetches(url: str) -> None:
    """Suppress fetches for the rate-limit interval after an unproductive one."""
    with _jwks_lock:
        _jwks_throttled_until[url] = (
            time.monotonic() + DEFAULT_JWKS_MIN_REFETCH_INTERVAL.total_seconds()
        )


def _allow_fetches(url: str) -> None:
    """Clear the throttle after a fetch that produced the requested key."""
    with _jwks_lock:
        _jwks_throttled_until.pop(url, None)


def _parse_jwks(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict) or not isinstance(payload.get("keys"), list):
        raise VercelOidcVerificationError("JWKS document is malformed")
    return payload


def _claim_fetch(url: str) -> bool:
    """Atomically claim the right to fetch this JWKS URL.

    Checking the in-flight marker and setting it must be one critical section, or
    a sync and an async caller can both decide to fetch. This single claim also
    replaces the per-mode locks that previously coordinated only within a mode.
    """
    with _jwks_lock:
        if url in _jwks_fetch_in_progress:
            return False
        until = _jwks_throttled_until.get(url)
        if until is not None and time.monotonic() < until:
            return False
        _jwks_fetch_in_progress.add(url)
        return True


def _end_fetch(url: str) -> None:
    with _jwks_lock:
        _jwks_fetch_in_progress.discard(url)


def _fetch_is_in_progress(url: str) -> bool:
    with _jwks_lock:
        return url in _jwks_fetch_in_progress


def _select_and_record(url: str, kid: str, document: dict[str, Any]) -> Any:
    """Select the key and record whether the fetch was productive."""
    key = _select_key(document, kid)
    if key is None:
        _throttle_fetches(url)
    else:
        _allow_fetches(url)
    return key


def _record_fetch_outcome(url: str, kid: str, fetch: Any) -> Any:
    try:
        document = fetch(url)
    except Exception:
        # A failing endpoint must not be retried by every caller.
        _throttle_fetches(url)
        raise
    return _select_and_record(url, kid, document)


async def _fetch_or_throttle_async(url: str) -> dict[str, Any]:
    try:
        return await _fetch_jwks_async(url)
    except Exception:
        _throttle_fetches(url)
        raise


def _cached_key(url: str, kid: str) -> Any:
    document = _cached_jwks(url)
    return None if document is None else _select_key(document, kid)


def _resolve_key_sync(url: str, kid: str) -> Any:
    """Find the signing key for `kid`, refreshing the JWKS when it is unknown."""
    key = _cached_key(url, kid)
    if key is not None:
        return key

    # One fetch per URL across every thread and both execution modes.
    if _claim_fetch(url):
        return _record_fetch_outcome(url, kid, _fetch_jwks_sync_uncached)

    # Someone else owns the fetch, or fetches are throttled. Wait for their result
    # rather than rejecting a token that is about to become verifiable.
    deadline = time.monotonic() + FETCH_WAIT_TIMEOUT
    while _fetch_is_in_progress(url) and time.monotonic() < deadline:
        time.sleep(_FETCH_WAIT_INTERVAL)
        key = _cached_key(url, kid)
        if key is not None:
            return key
    return _cached_key(url, kid)


def _fetch_jwks_sync_uncached(url: str) -> dict[str, Any]:
    try:
        with httpx.Client(timeout=_JWKS_TIMEOUT) as client:
            response = client.get(url)
            response.raise_for_status()
            document = _parse_jwks(response.json())
        # Stored before the marker is cleared, so a waiter that observes the fetch
        # finishing always sees the document it produced.
        _store_jwks(url, document)
        return document
    except VercelOidcVerificationError:
        raise
    except Exception as exc:
        raise VercelOidcVerificationError(f"could not fetch JWKS from {url}", exc) from exc
    finally:
        _end_fetch(url)


async def _resolve_key_async(url: str, kid: str) -> Any:
    """Async twin of `_resolve_key_sync`.

    Coordination is through the shared atomic claim rather than an async lock, so
    it holds across threads, tasks, and both execution modes without binding to an
    async backend.
    """
    import anyio

    key = _cached_key(url, kid)
    if key is not None:
        return key

    if _claim_fetch(url):
        document = await _fetch_or_throttle_async(url)
        return _select_and_record(url, kid, document)

    deadline = time.monotonic() + FETCH_WAIT_TIMEOUT
    while _fetch_is_in_progress(url) and time.monotonic() < deadline:
        await anyio.sleep(_FETCH_WAIT_INTERVAL)
        key = _cached_key(url, kid)
        if key is not None:
            return key
    return _cached_key(url, kid)


async def _fetch_jwks_async(url: str) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=_JWKS_TIMEOUT) as client:
            response = await client.get(url)
            response.raise_for_status()
            document = _parse_jwks(response.json())
        # Stored before the marker is cleared; see the sync twin.
        _store_jwks(url, document)
        return document
    except VercelOidcVerificationError:
        raise
    except Exception as exc:
        raise VercelOidcVerificationError(f"could not fetch JWKS from {url}", exc) from exc
    finally:
        _end_fetch(url)


def _unverified_kid(token: str) -> str:
    jwt = _require_pyjwt()
    try:
        header = jwt.get_unverified_header(token)
    except Exception as exc:
        raise VercelOidcVerificationError("token header is not readable", exc) from exc
    kid = header.get("kid")
    if not isinstance(kid, str) or not kid:
        # Without a kid there is no way to select a key, and trying every key in
        # the JWKS is exactly the fallback that enables key-confusion attacks.
        raise VercelOidcVerificationError("token has no 'kid' header")
    return kid


def _select_key(document: Mapping[str, Any], kid: str) -> Any:
    jwt = _require_pyjwt()
    for candidate in document.get("keys", []):
        if not isinstance(candidate, dict) or candidate.get("kid") != kid:
            continue
        if candidate.get("kty") != "RSA":
            raise VercelOidcVerificationError(
                f"signing key {kid!r} is {candidate.get('kty')!r}, expected RSA"
            )
        algorithm = candidate.get("alg")
        if algorithm is not None and algorithm not in ALLOWED_ALGORITHMS:
            raise VercelOidcVerificationError(
                f"signing key {kid!r} declares algorithm {algorithm!r}"
            )
        try:
            return jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(candidate))
        except Exception as exc:
            raise VercelOidcVerificationError(f"signing key {kid!r} is unusable", exc) from exc
    return None


def _decode(
    token: str, key: Any, *, issuer: str, audience: Any, leeway: timedelta
) -> dict[str, Any]:
    jwt = _require_pyjwt()
    try:
        return dict(
            jwt.decode(
                token,
                key=key,
                # Explicit and closed: the token's own `alg` never selects the
                # verifier, and `key` is an RSA public key object rather than a
                # string that could be reused as an HMAC secret.
                algorithms=list(ALLOWED_ALGORITHMS),
                issuer=issuer,
                audience=audience,
                leeway=leeway.total_seconds(),
                options={
                    "verify_signature": True,
                    "verify_exp": True,
                    "verify_nbf": True,
                    "verify_iat": True,
                    "verify_iss": True,
                    "verify_aud": audience is not None,
                    "require": ["exp", "iss"],
                },
            )
        )
    except Exception as exc:
        raise VercelOidcVerificationError("token failed verification", exc) from exc


def _resolve_expectations(
    project_id: str | None,
    environment: str | None,
) -> tuple[str, str]:
    resolved_project = project_id or os.environ.get("VERCEL_PROJECT_ID")
    resolved_environment = (
        environment or os.environ.get("VERCEL_TARGET_ENV") or os.environ.get("VERCEL_ENV")
    )
    # Fail closed: an unknown expectation must never widen into "accept anything".
    if not resolved_project:
        raise VercelOidcVerificationError(
            "expected project could not be determined; pass project_id or set VERCEL_PROJECT_ID"
        )
    if not resolved_environment:
        raise VercelOidcVerificationError(
            "expected environment could not be determined; pass environment or set "
            "VERCEL_TARGET_ENV or VERCEL_ENV"
        )
    return resolved_project, resolved_environment


def _check_claims(
    claims: Mapping[str, Any],
    *,
    project_id: str,
    environment: str,
    owner_id: str | None,
    audience: str | Sequence[str] | None,
) -> None:
    token_project = claims.get("project_id")
    token_environment = claims.get("environment")
    token_owner = claims.get("owner_id")

    if token_environment != environment:
        raise VercelOidcVerificationError(
            f"token environment {token_environment!r} does not match expected {environment!r}"
        )

    if project_id == _WILDCARD or token_project == _WILDCARD:
        # A wildcard project claim authorizes every project in a team, so it is
        # only meaningful when narrowed by an owner or an audience.
        if owner_id is None and audience is None:
            raise VercelOidcVerificationError(
                "wildcard project claims require owner_id or audience to be supplied"
            )
    elif token_project != project_id:
        raise VercelOidcVerificationError(
            f"token project {token_project!r} does not match expected {project_id!r}"
        )

    if owner_id is not None and token_owner != owner_id:
        raise VercelOidcVerificationError(
            f"token owner {token_owner!r} does not match expected {owner_id!r}"
        )


def verify_vercel_oidc_token(
    token: str,
    *,
    project_id: str | None = None,
    environment: str | None = None,
    owner_id: str | None = None,
    audience: str | Sequence[str] | None = None,
    issuer: str = VERCEL_OIDC_ISSUER,
    leeway: timedelta = DEFAULT_LEEWAY,
) -> dict[str, Any]:
    """Verify a Vercel OIDC token and return its claims.

    Args:
        token: The encoded JWT.
        project_id: Expected ``project_id`` claim. Defaults to
            ``VERCEL_PROJECT_ID``.
        environment: Expected ``environment`` claim. Defaults to
            ``VERCEL_TARGET_ENV``, then ``VERCEL_ENV``.
        owner_id: Expected ``owner_id`` claim. Required when the token's project
            claim is the ``*`` wildcard, unless ``audience`` is supplied.
        audience: Expected ``aud`` claim.
        issuer: Expected issuer. Defaults to ``https://oidc.vercel.com`` and
            should not normally be changed.
        leeway: Clock-skew allowance for time-based claims.

    Returns:
        The verified claims.

    Raises:
        VercelOidcVerificationError: If the token does not verify, a claim does
            not match, or the expected project and environment cannot be
            resolved.
    """
    url = _jwks_url(issuer)
    expected_project, expected_environment = _resolve_expectations(project_id, environment)
    kid = _unverified_kid(token)

    key = _resolve_key_sync(url, kid)
    if key is None:
        raise VercelOidcVerificationError(f"no signing key matches kid {kid!r}")

    claims = _decode(token, key, issuer=issuer, audience=audience, leeway=leeway)
    _check_claims(
        claims,
        project_id=expected_project,
        environment=expected_environment,
        owner_id=owner_id,
        audience=audience,
    )
    return claims


async def verify_vercel_oidc_token_async(
    token: str,
    *,
    project_id: str | None = None,
    environment: str | None = None,
    owner_id: str | None = None,
    audience: str | Sequence[str] | None = None,
    issuer: str = VERCEL_OIDC_ISSUER,
    leeway: timedelta = DEFAULT_LEEWAY,
) -> dict[str, Any]:
    """Verify a Vercel OIDC token and return its claims.

    Args:
        token: The encoded JWT.
        project_id: Expected ``project_id`` claim. Defaults to
            ``VERCEL_PROJECT_ID``.
        environment: Expected ``environment`` claim. Defaults to
            ``VERCEL_TARGET_ENV``, then ``VERCEL_ENV``.
        owner_id: Expected ``owner_id`` claim. Required when the token's project
            claim is the ``*`` wildcard, unless ``audience`` is supplied.
        audience: Expected ``aud`` claim.
        issuer: Expected issuer. Defaults to ``https://oidc.vercel.com``.
        leeway: Clock-skew allowance for time-based claims.

    Returns:
        The verified claims.

    Raises:
        VercelOidcVerificationError: If the token does not verify, a claim does
            not match, or the expected project and environment cannot be
            resolved.
    """
    url = _jwks_url(issuer)
    expected_project, expected_environment = _resolve_expectations(project_id, environment)
    kid = _unverified_kid(token)

    key = await _resolve_key_async(url, kid)
    if key is None:
        raise VercelOidcVerificationError(f"no signing key matches kid {kid!r}")

    claims = _decode(token, key, issuer=issuer, audience=audience, leeway=leeway)
    _check_claims(
        claims,
        project_id=expected_project,
        environment=expected_environment,
        owner_id=owner_id,
        audience=audience,
    )
    return claims


def extract_bearer_token(headers: Mapping[str, str]) -> str:
    """Extract a bearer token from request headers.

    The scheme match is case-insensitive and surrounding whitespace is trimmed.

    Args:
        headers: Request headers. Only ``Authorization`` is read.

    Returns:
        The bearer token.

    Raises:
        VercelOidcVerificationError: If the header is absent or is not a bearer
            credential.
    """
    header = None
    for name, value in headers.items():
        if name.lower() == "authorization":
            header = value
            break
    if not header:
        raise VercelOidcVerificationError("request has no Authorization header")

    # Split on any whitespace run, so a tab-separated scheme is handled too.
    parts = header.strip().split(maxsplit=1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise VercelOidcVerificationError("Authorization header is not a bearer credential")
    token = parts[1].strip()
    if not token:
        raise VercelOidcVerificationError("Authorization header has an empty bearer credential")
    return token


def clear_jwks_cache() -> None:
    """Drop the cached JWKS. Intended for tests."""
    with _jwks_lock:
        _jwks_cache.clear()
        _jwks_throttled_until.clear()
        _jwks_fetch_in_progress.clear()


__all__ = [
    "ALLOWED_ALGORITHMS",
    "VERCEL_OIDC_ISSUER",
    "VercelOidcVerificationError",
    "clear_jwks_cache",
    "extract_bearer_token",
    "verify_vercel_oidc_token",
    "verify_vercel_oidc_token_async",
]
