"""Signature verification for Vercel OIDC tokens.

Verification is deliberately narrow, because every historical JWT library
vulnerability in this space is an algorithm- or key-confusion bug:

- ``RS256`` is the only accepted algorithm, passed explicitly. The token's own
  ``alg`` header is never consulted to select a verifier, so ``alg: none`` and
  HMAC confusion are structurally impossible.
- The issuer is pinned to Vercel's OIDC service and is not configurable. The
  JWKS URL is a constant, so a token can never influence where keys come from.
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

import hashlib
import json
import os
import re
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import timedelta
from types import ModuleType
from typing import TYPE_CHECKING, Any, TypeAlias, cast

import httpx

from .token import VercelOidcTokenError

if TYPE_CHECKING:
    # Only for annotations: `cryptography` ships in the `verify` extra, so it must
    # not be imported when the extra is absent.
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

JwksDocument: TypeAlias = dict[str, Any]
"""A parsed JWKS document. JSON, so its values stay dynamic."""

VERCEL_OIDC_ISSUER = "https://oidc.vercel.com"
JWKS_PATH = "/.well-known/jwks"
# Fixed, never derived from the token: one global key signs every Vercel OIDC
# token, including the team-scoped issuers, so there is nothing to discover and
# no reason to let a claim choose a URL.
JWKS_URL = f"{VERCEL_OIDC_ISSUER}{JWKS_PATH}"
# Rejects traversal, extra path segments and query strings.
_TEAM_SLUG = re.compile(r"[A-Za-z0-9_-]+")

ALLOWED_ALGORITHMS = ("RS256",)
DEFAULT_LEEWAY = timedelta(seconds=60)
DEFAULT_JWKS_CACHE_TTL = timedelta(minutes=10)
DEFAULT_JWKS_MIN_REFETCH_INTERVAL = timedelta(seconds=30)
_JWKS_TIMEOUT_SECONDS = 10.0
_JWKS_TIMEOUT = httpx.Timeout(_JWKS_TIMEOUT_SECONDS)

_jwks_lock = threading.Lock()
_jwks_cache: dict[str, tuple[float, JwksDocument]] = {}
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


def _require_pyjwt() -> ModuleType:
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


def _cached_jwks(url: str) -> JwksDocument | None:
    with _jwks_lock:
        entry = _jwks_cache.get(url)
        if entry is None:
            return None
        fetched_at, document = entry
        if time.monotonic() - fetched_at > DEFAULT_JWKS_CACHE_TTL.total_seconds():
            return None
        return document


def _store_jwks(url: str, document: JwksDocument) -> None:
    with _jwks_lock:
        _jwks_cache[url] = (time.monotonic(), document)


def _parse_jwks(payload: object) -> JwksDocument:
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


def _end_fetch(url: str, *, productive: bool | None = None) -> None:
    """Release the fetch claim, recording its outcome in the same critical section.

    An unproductive fetch must set the throttle before the claim is released, or a
    concurrent unknown-kid caller can claim a fresh fetch in the gap and a burst
    fans out into repeated refetches. ``productive=None`` releases without touching
    the throttle, for a fetch that did not run to completion.
    """
    with _jwks_lock:
        _jwks_fetch_in_progress.discard(url)
        if productive is True:
            _jwks_throttled_until.pop(url, None)
        elif productive is False:
            _jwks_throttled_until[url] = (
                time.monotonic() + DEFAULT_JWKS_MIN_REFETCH_INTERVAL.total_seconds()
            )


def _fetch_is_in_progress(url: str) -> bool:
    with _jwks_lock:
        return url in _jwks_fetch_in_progress


def _select_and_record(url: str, kid: str, document: JwksDocument) -> RSAPublicKey | None:
    """Select the key, then release the fetch claim with the outcome recorded."""
    try:
        key = _select_key(document, kid)
    except BaseException:
        _end_fetch(url, productive=False)
        raise
    _end_fetch(url, productive=key is not None)
    return key


def _record_fetch_outcome(
    url: str, kid: str, fetch: Callable[[str], JwksDocument]
) -> RSAPublicKey | None:
    try:
        document = fetch(url)
    except Exception:
        # A failing endpoint must not be retried by every caller.
        _end_fetch(url, productive=False)
        raise
    except BaseException:
        _end_fetch(url)
        raise
    return _select_and_record(url, kid, document)


async def _fetch_or_throttle_async(url: str) -> JwksDocument:
    try:
        return await _fetch_jwks_async(url)
    except Exception:
        _end_fetch(url, productive=False)
        raise
    except BaseException:
        # Cancelled mid-fetch: release the claim without recording an outcome.
        _end_fetch(url)
        raise


def _cached_key(url: str, kid: str) -> RSAPublicKey | None:
    document = _cached_jwks(url)
    return None if document is None else _select_key(document, kid)


def _resolve_key_sync(url: str, kid: str) -> RSAPublicKey | None:
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


def _fetch_jwks_sync_uncached(url: str) -> JwksDocument:
    try:
        with httpx.Client(timeout=_JWKS_TIMEOUT) as client:
            response = client.get(url)
            response.raise_for_status()
            document = _parse_jwks(response.json())
        # Stored before the caller releases the claim, so a waiter that observes
        # the fetch finishing always sees the document it produced.
        _store_jwks(url, document)
        return document
    except VercelOidcVerificationError:
        raise
    except Exception as exc:
        raise VercelOidcVerificationError(f"could not fetch JWKS from {url}", exc) from exc


async def _resolve_key_async(url: str, kid: str) -> RSAPublicKey | None:
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


async def _fetch_jwks_async(url: str) -> JwksDocument:
    try:
        async with httpx.AsyncClient(timeout=_JWKS_TIMEOUT) as client:
            response = await client.get(url)
            response.raise_for_status()
            document = _parse_jwks(response.json())
        # Stored before the caller releases the claim; see the sync twin.
        _store_jwks(url, document)
        return document
    except VercelOidcVerificationError:
        raise
    except Exception as exc:
        raise VercelOidcVerificationError(f"could not fetch JWKS from {url}", exc) from exc


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


def _select_key(document: Mapping[str, Any], kid: str) -> RSAPublicKey | None:
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
            # pyjwt is untyped here; the JWKS carries public keys only, and a
            # non-RSA `kty` was rejected above.
            return cast("RSAPublicKey", jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(candidate)))
        except Exception as exc:
            raise VercelOidcVerificationError(f"signing key {kid!r} is unusable", exc) from exc
    return None


def _decode(
    token: str,
    key: RSAPublicKey,
    *,
    audience: str | Sequence[str] | None,
    leeway: timedelta,
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
                audience=audience,
                leeway=leeway.total_seconds(),
                options={
                    "verify_signature": True,
                    "verify_exp": True,
                    "verify_nbf": True,
                    "verify_iat": True,
                    # Checked by `_check_issuer`: PyJWT compares `iss` for exact
                    # equality, which would reject the team-scoped issuers.
                    "verify_iss": False,
                    "verify_aud": audience is not None,
                    "require": ["exp", "iss"],
                },
            )
        )
    except Exception as exc:
        raise VercelOidcVerificationError("token failed verification", exc) from exc


def _check_issuer(claims: Mapping[str, Any]) -> None:
    """Pin the issuer to Vercel's OIDC service, root or team-scoped.

    Vercel mints both `https://oidc.vercel.com` and `https://oidc.vercel.com/<team>`,
    signed by the same global key. The accepted prefix includes the host and the
    next character must be `/`, so a lookalike host such as
    `https://oidc.vercel.com.evil.example` cannot match.
    """
    issuer = claims.get("iss")
    if not isinstance(issuer, str):
        raise VercelOidcVerificationError("token has no 'iss' claim")
    if issuer == VERCEL_OIDC_ISSUER:
        return
    prefix = f"{VERCEL_OIDC_ISSUER}/"
    if issuer.startswith(prefix) and _TEAM_SLUG.fullmatch(issuer[len(prefix) :]):
        return
    raise VercelOidcVerificationError(
        f"token issuer {issuer!r} is not {VERCEL_OIDC_ISSUER} or a team-scoped issuer under it"
    )


def _verified_claims_sync(
    token: str, *, audience: str | Sequence[str] | None, leeway: timedelta
) -> dict[str, Any]:
    """Resolve the signing key, verify the signature, and pin the issuer."""
    kid = _unverified_kid(token)
    key = _resolve_key_sync(JWKS_URL, kid)
    if key is None:
        raise VercelOidcVerificationError(f"no signing key matches kid {kid!r}")
    claims = _decode(token, key, audience=audience, leeway=leeway)
    _check_issuer(claims)
    return claims


async def _verified_claims_async(
    token: str, *, audience: str | Sequence[str] | None, leeway: timedelta
) -> dict[str, Any]:
    kid = _unverified_kid(token)
    key = await _resolve_key_async(JWKS_URL, kid)
    if key is None:
        raise VercelOidcVerificationError(f"no signing key matches kid {kid!r}")
    claims = _decode(token, key, audience=audience, leeway=leeway)
    _check_issuer(claims)
    return claims


# Every claim that names the identity rather than the token. `sub` alone is
# expected to be unique per identity, but a cache partition must not rest on that
# one claim staying unique, and none of these change when a token is reissued.
_IDENTITY_CLAIMS = ("iss", "sub", "owner_id", "project_id", "environment")


def _identity_from_claims(claims: Mapping[str, Any]) -> str:
    """Reduce the verified identity claims to one opaque, stable digest.

    A single identity is issued many tokens over time, because a token is a
    signature over the identity plus an expiry, so the token itself is not a
    usable identity. These claims are what survive a reissue.
    """
    if not isinstance(claims.get("sub"), str) or not claims.get("sub"):
        raise VercelOidcVerificationError("token has no 'sub' claim")
    raw_audience = claims.get("aud")
    if isinstance(raw_audience, str):
        audiences = [raw_audience]
    elif isinstance(raw_audience, (list, tuple)):
        # Sorted: audience order is not part of the identity.
        audiences = sorted(str(entry) for entry in raw_audience)
    else:
        audiences = []
    identity: dict[str, Any] = {name: claims.get(name) for name in _IDENTITY_CLAIMS}
    identity["aud"] = audiences
    payload = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


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
) -> None:
    token_project = claims.get("project_id")
    token_environment = claims.get("environment")
    token_owner = claims.get("owner_id")

    if token_environment != environment:
        raise VercelOidcVerificationError(
            f"token environment {token_environment!r} does not match expected {environment!r}"
        )

    # Compared for equality only. There is no wildcard: `"*"` is an ordinary
    # string here, so a token claiming it matches nothing but an expectation of
    # `"*"`, and a project expectation can never widen to "any project".
    if token_project != project_id:
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
    leeway: timedelta = DEFAULT_LEEWAY,
) -> dict[str, Any]:
    """Verify a Vercel OIDC token and return its claims.

    The issuer is pinned to Vercel's OIDC service, which mints both
    ``https://oidc.vercel.com`` and the team-scoped
    ``https://oidc.vercel.com/<team>``.

    Args:
        token: The encoded JWT.
        project_id: Expected ``project_id`` claim. Defaults to
            ``VERCEL_PROJECT_ID``.
        environment: Expected ``environment`` claim. Defaults to
            ``VERCEL_TARGET_ENV``, then ``VERCEL_ENV``.
        owner_id: Expected ``owner_id`` claim.
        audience: Expected ``aud`` claim.
        leeway: Clock-skew allowance for time-based claims.

    Returns:
        The verified claims.

    Raises:
        VercelOidcVerificationError: If the token does not verify, a claim does
            not match, or the expected project and environment cannot be
            resolved.
    """
    expected_project, expected_environment = _resolve_expectations(project_id, environment)
    claims = _verified_claims_sync(token, audience=audience, leeway=leeway)
    _check_claims(
        claims,
        project_id=expected_project,
        environment=expected_environment,
        owner_id=owner_id,
    )
    return claims


async def verify_vercel_oidc_token_async(
    token: str,
    *,
    project_id: str | None = None,
    environment: str | None = None,
    owner_id: str | None = None,
    audience: str | Sequence[str] | None = None,
    leeway: timedelta = DEFAULT_LEEWAY,
) -> dict[str, Any]:
    """Verify a Vercel OIDC token and return its claims.

    The issuer is pinned to Vercel's OIDC service, which mints both
    ``https://oidc.vercel.com`` and the team-scoped
    ``https://oidc.vercel.com/<team>``.

    Args:
        token: The encoded JWT.
        project_id: Expected ``project_id`` claim. Defaults to
            ``VERCEL_PROJECT_ID``.
        environment: Expected ``environment`` claim. Defaults to
            ``VERCEL_TARGET_ENV``, then ``VERCEL_ENV``.
        owner_id: Expected ``owner_id`` claim.
        audience: Expected ``aud`` claim.
        leeway: Clock-skew allowance for time-based claims.

    Returns:
        The verified claims.

    Raises:
        VercelOidcVerificationError: If the token does not verify, a claim does
            not match, or the expected project and environment cannot be
            resolved.
    """
    expected_project, expected_environment = _resolve_expectations(project_id, environment)
    claims = await _verified_claims_async(token, audience=audience, leeway=leeway)
    _check_claims(
        claims,
        project_id=expected_project,
        environment=expected_environment,
        owner_id=owner_id,
    )
    return claims


def resolve_vercel_oidc_token_identity(token: str, *, leeway: timedelta = DEFAULT_LEEWAY) -> str:
    """Verify a Vercel OIDC token and return the stable identity it names.

    The signature, the issuer and the expiry are verified before anything is read,
    and the identity claims are then reduced to an opaque digest.

    A deployment identity is issued a new token whenever the previous one nears
    expiry, and every one of those tokens yields the same identity, so this is
    what a client should key identity-scoped state on rather than the token
    itself.

    What it deliberately does **not** do is authorize: it returns no claims and
    does not check the project, environment, owner or audience, so it must not be
    used to decide whether a request is allowed. Use `verify_vercel_oidc_token`
    for that.

    Args:
        token: The encoded JWT.
        leeway: Clock-skew allowance for time-based claims.

    Returns:
        An opaque digest of the token's identity claims. It carries no credential
        and is safe to log.

    Raises:
        VercelOidcVerificationError: If the token does not verify, or carries no
            subject.
    """
    return _identity_from_claims(_verified_claims_sync(token, audience=None, leeway=leeway))


async def resolve_vercel_oidc_token_identity_async(
    token: str, *, leeway: timedelta = DEFAULT_LEEWAY
) -> str:
    """Verify a Vercel OIDC token and return the stable identity it names.

    See `resolve_vercel_oidc_token_identity`. This is not an authorization check.

    Args:
        token: The encoded JWT.
        leeway: Clock-skew allowance for time-based claims.

    Returns:
        An opaque digest of the token's identity claims.

    Raises:
        VercelOidcVerificationError: If the token does not verify, or carries no
            subject.
    """
    return _identity_from_claims(await _verified_claims_async(token, audience=None, leeway=leeway))


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
    "JWKS_URL",
    "VERCEL_OIDC_ISSUER",
    "VercelOidcVerificationError",
    "clear_jwks_cache",
    "extract_bearer_token",
    "verify_vercel_oidc_token",
    "verify_vercel_oidc_token_async",
    "resolve_vercel_oidc_token_identity",
    "resolve_vercel_oidc_token_identity_async",
]
