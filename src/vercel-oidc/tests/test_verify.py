"""Vercel OIDC token signature verification.

Attack vectors come first, deliberately. Every historical CVE in this space is an
algorithm- or key-confusion bug, so each of those classes gets an explicit test
vector before the happy path is exercised at all.
"""

import base64
import json
import time
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

import httpx
import pytest
import respx
from conftest import ISSUER, JWKS_URL, KID, b64url, claims, forge_hs256, jwks_for, sign
from cryptography.hazmat.primitives.asymmetric import rsa

from vercel.oidc import resolve_vercel_oidc_token_identity, verify_vercel_oidc_token
from vercel.oidc.aio import (
    resolve_vercel_oidc_token_identity as resolve_vercel_oidc_token_identity_async,
    verify_vercel_oidc_token as verify_vercel_oidc_token_async,
)
from vercel.oidc.verify import (
    FETCH_WAIT_TIMEOUT,
    VercelOidcVerificationError,
    clear_jwks_cache,
    extract_bearer_token,
)

pytestmark = pytest.mark.usefixtures("mock_env_clear")

EXPECTATIONS: dict[str, Any] = {"project_id": "prj_123", "environment": "production"}


def jwks_route(key: rsa.RSAPrivateKey, *, kid: str = KID) -> respx.Route:
    return respx.get(JWKS_URL).mock(return_value=httpx.Response(200, json=jwks_for(key, kid=kid)))


# --------------------------------------------------------------------------
# Attack vectors
# --------------------------------------------------------------------------


@respx.mock
def test_rejects_alg_none(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    header = b64url(json.dumps({"alg": "none", "typ": "JWT", "kid": KID}).encode())
    payload = b64url(json.dumps(claims()).encode())
    token = f"{header}.{payload}."

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, **EXPECTATIONS)


@respx.mock
def test_rejects_hs256_signed_with_the_jwks_public_key(signing_key: rsa.RSAPrivateKey) -> None:
    """CVE-2026-48526 class: a public key must never be usable as an HMAC secret."""
    from cryptography.hazmat.primitives import serialization

    jwks_route(signing_key)
    public_pem = signing_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(forge_hs256(public_pem), **EXPECTATIONS)


@respx.mock
def test_rejects_hs256_signed_with_the_jwk_modulus(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    modulus: str = jwks_for(signing_key)["keys"][0]["n"]

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(forge_hs256(modulus.encode()), **EXPECTATIONS)


@respx.mock
def test_rejects_hs256_signed_with_the_der_public_key(signing_key: rsa.RSAPrivateKey) -> None:
    from cryptography.hazmat.primitives import serialization

    jwks_route(signing_key)
    public_der = signing_key.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(forge_hs256(public_der), **EXPECTATIONS)


@respx.mock
def test_rejects_a_token_signed_by_another_key(
    signing_key: rsa.RSAPrivateKey,
    other_signing_key: rsa.RSAPrivateKey,
) -> None:
    """Key substitution: the JWKS advertises one key, the token uses another."""
    jwks_route(signing_key)
    token = sign(other_signing_key)

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, **EXPECTATIONS)


@respx.mock
def test_rejects_unknown_kid(signing_key: rsa.RSAPrivateKey) -> None:
    """No fallback to trying every key in the JWKS."""
    jwks_route(signing_key, kid="rotated-key")
    token = sign(signing_key, kid="unknown-key")

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, **EXPECTATIONS)


@respx.mock
def test_rejects_missing_kid(signing_key: rsa.RSAPrivateKey) -> None:
    import jwt

    jwks_route(signing_key)
    token = jwt.encode(claims(), signing_key, algorithm="RS256")

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, **EXPECTATIONS)


@respx.mock
def test_rejects_a_tampered_payload(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    header, payload, signature = sign(signing_key).split(".")
    tampered_claims = claims(project_id="prj_attacker")
    tampered = f"{header}.{b64url(json.dumps(tampered_claims).encode())}.{signature}"

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(tampered, project_id="prj_attacker", environment="production")


@pytest.mark.parametrize(
    "issuer",
    [
        "https://evil.example.com",
        # Lookalike host: the accepted prefix must include the host, or a
        # registrable suffix attack passes.
        "https://oidc.vercel.com.evil.example",
        "https://oidc.vercel.com.evil.example/acme",
        # Prefix without the separator.
        "https://oidc.vercel.comevil",
        # More than one path segment, and traversal back off the service.
        "https://oidc.vercel.com/acme/extra",
        "https://oidc.vercel.com/../evil",
        "https://oidc.vercel.com/",
        # Scheme downgrade on the right host.
        "http://oidc.vercel.com",
        "http://oidc.vercel.com/acme",
    ],
)
@respx.mock
def test_rejects_a_wrong_issuer(signing_key: rsa.RSAPrivateKey, issuer: str) -> None:
    jwks_route(signing_key)
    token = sign(signing_key, claims(iss=issuer))

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, **EXPECTATIONS)


@respx.mock
def test_rejects_a_missing_issuer(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    token = sign(signing_key, claims(iss=None))

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, **EXPECTATIONS)


@respx.mock
def test_rejects_an_expired_token(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    now = int(time.time())
    token = sign(signing_key, claims(iat=now - 7200, nbf=now - 7200, exp=now - 3600))

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, **EXPECTATIONS)


@respx.mock
def test_rejects_a_not_yet_valid_token(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    now = int(time.time())
    token = sign(signing_key, claims(nbf=now + 3600, exp=now + 7200))

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, **EXPECTATIONS)


@respx.mock
def test_rejects_a_wrong_project(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    token = sign(signing_key)

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, project_id="prj_other", environment="production")


@respx.mock
def test_rejects_a_wrong_environment(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    token = sign(signing_key)

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, project_id="prj_123", environment="preview")


@respx.mock
def test_rejects_a_wrong_audience(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    token = sign(signing_key)

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, audience="https://vercel.com/other-team", **EXPECTATIONS)


@respx.mock
def test_fails_closed_when_expectations_cannot_be_resolved(
    signing_key: rsa.RSAPrivateKey,
) -> None:
    """No project or environment in the arguments or the environment: reject."""
    jwks_route(signing_key)
    token = sign(signing_key)

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token)


@respx.mock
def test_a_wildcard_project_claim_is_not_a_wildcard(signing_key: rsa.RSAPrivateKey) -> None:
    """`"*"` is an ordinary string, so it must not authorize another project."""
    jwks_route(signing_key)
    token = sign(signing_key, claims(project_id="*"))

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, project_id="prj_123", environment="production")

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(
            token, project_id="prj_123", environment="production", owner_id="team_123"
        )


@pytest.mark.parametrize(
    "token",
    [
        "",
        "not-a-jwt",
        "only.two",
        "a.b.c.d",
        "!!!.###.$$$",
        base64.urlsafe_b64encode(b"{not json}").decode() + ".e30.sig",
    ],
)
@respx.mock
def test_rejects_malformed_tokens(signing_key: rsa.RSAPrivateKey, token: str) -> None:
    jwks_route(signing_key)

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, **EXPECTATIONS)


@respx.mock
def test_jwks_fetch_failure_rejects_the_request(signing_key: rsa.RSAPrivateKey) -> None:
    respx.get(JWKS_URL).mock(return_value=httpx.Response(503, text="unavailable"))
    token = sign(signing_key)

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, **EXPECTATIONS)


# --------------------------------------------------------------------------
# Valid tokens
# --------------------------------------------------------------------------


@respx.mock
def test_verifies_a_valid_token_sync(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    token = sign(signing_key)

    verified = verify_vercel_oidc_token(token, **EXPECTATIONS)

    assert verified["iss"] == ISSUER
    assert verified["project_id"] == "prj_123"
    assert verified["environment"] == "production"
    assert verified["owner_id"] == "team_123"


@respx.mock
async def test_verifies_a_valid_token_async(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    token = sign(signing_key)

    verified = await verify_vercel_oidc_token_async(token, **EXPECTATIONS)

    assert verified["project_id"] == "prj_123"


@pytest.mark.parametrize(
    "issuer",
    [
        ISSUER,
        f"{ISSUER}/team1",
        f"{ISSUER}/acme-team_2",
    ],
)
@respx.mock
def test_accepts_the_root_and_team_scoped_issuers(
    signing_key: rsa.RSAPrivateKey, issuer: str
) -> None:
    """Vercel mints both forms, signed by one global key.

    `https://oidc.vercel.com/.well-known/openid-configuration` declares the root
    issuer, and `.../<team>/.well-known/openid-configuration` declares a
    team-scoped one, both advertising the same `kid`. Pinning `iss` by equality
    rejected every team-scoped token.
    """
    jwks_route(signing_key)
    token = sign(signing_key, claims(iss=issuer))

    assert verify_vercel_oidc_token(token, **EXPECTATIONS)["iss"] == issuer


@respx.mock
def test_team_scoped_issuer_still_uses_the_pinned_jwks_url(
    signing_key: rsa.RSAPrivateKey,
) -> None:
    """A team-scoped `iss` must not redirect key resolution to a derived URL."""
    root = jwks_route(signing_key)
    scoped = respx.get(f"{ISSUER}/team1/.well-known/jwks").mock(
        return_value=httpx.Response(500, text="must not be called")
    )

    verify_vercel_oidc_token(sign(signing_key, claims(iss=f"{ISSUER}/team1")), **EXPECTATIONS)

    assert root.call_count == 1
    assert scoped.call_count == 0


@respx.mock
def test_reads_expectations_from_the_environment(
    signing_key: rsa.RSAPrivateKey,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_123")
    monkeypatch.setenv("VERCEL_ENV", "production")
    jwks_route(signing_key)

    assert verify_vercel_oidc_token(sign(signing_key))["project_id"] == "prj_123"


@respx.mock
def test_target_env_takes_precedence_over_env(
    signing_key: rsa.RSAPrivateKey,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_123")
    monkeypatch.setenv("VERCEL_TARGET_ENV", "production")
    monkeypatch.setenv("VERCEL_ENV", "preview")
    jwks_route(signing_key)

    assert verify_vercel_oidc_token(sign(signing_key))["environment"] == "production"


@respx.mock
def test_accepts_a_matching_audience(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)

    verified = verify_vercel_oidc_token(
        sign(signing_key), audience="https://vercel.com/acme", **EXPECTATIONS
    )

    assert verified["aud"] == "https://vercel.com/acme"


@respx.mock
def test_clock_skew_leeway_is_applied(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    now = int(time.time())
    token = sign(signing_key, claims(exp=now - 10))

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, leeway=timedelta(0), **EXPECTATIONS)

    assert verify_vercel_oidc_token(token, leeway=timedelta(seconds=60), **EXPECTATIONS)


# --------------------------------------------------------------------------
# Token identity
# --------------------------------------------------------------------------


@respx.mock
def test_identity_is_stable_across_a_reissued_token(signing_key: rsa.RSAPrivateKey) -> None:
    """A refreshed token names the same identity, so it must digest the same."""
    jwks_route(signing_key)
    now = int(time.time())
    first = sign(signing_key, claims(iat=now - 1800, exp=now + 1800))
    second = sign(signing_key, claims(iat=now, exp=now + 3600, jti="other"))

    assert first != second
    assert resolve_vercel_oidc_token_identity(first) == resolve_vercel_oidc_token_identity(second)


@respx.mock
async def test_identity_is_stable_across_a_reissued_token_async(
    signing_key: rsa.RSAPrivateKey,
) -> None:
    jwks_route(signing_key)
    now = int(time.time())
    first = sign(signing_key, claims(exp=now + 1800))
    second = sign(signing_key, claims(exp=now + 3600))

    identity = await resolve_vercel_oidc_token_identity_async(first)

    assert identity == await resolve_vercel_oidc_token_identity_async(second)
    assert identity == resolve_vercel_oidc_token_identity(second)


@pytest.mark.parametrize(
    "overrides",
    [
        {"sub": "owner:acme:project:other-app:environment:production"},
        {"iss": f"{ISSUER}/other-team"},
        {"aud": "https://vercel.com/other"},
    ],
)
@respx.mock
def test_identity_separates_distinct_identities(
    signing_key: rsa.RSAPrivateKey, overrides: dict[str, Any]
) -> None:
    jwks_route(signing_key)

    baseline = resolve_vercel_oidc_token_identity(sign(signing_key))

    assert resolve_vercel_oidc_token_identity(sign(signing_key, claims(**overrides))) != baseline


@respx.mock
def test_identity_ignores_audience_order(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    forward = sign(signing_key, claims(aud=["https://vercel.com/a", "https://vercel.com/b"]))
    reversed_ = sign(signing_key, claims(aud=["https://vercel.com/b", "https://vercel.com/a"]))

    assert resolve_vercel_oidc_token_identity(forward) == resolve_vercel_oidc_token_identity(
        reversed_
    )


@respx.mock
def test_identity_rejects_an_unverifiable_token(
    signing_key: rsa.RSAPrivateKey,
    other_signing_key: rsa.RSAPrivateKey,
) -> None:
    """Unverified claims must never yield an identity, or a forged token could be
    handed the state cached under a real one."""
    jwks_route(signing_key)

    with pytest.raises(VercelOidcVerificationError):
        resolve_vercel_oidc_token_identity(sign(other_signing_key))


@respx.mock
def test_identity_rejects_a_foreign_issuer(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)

    with pytest.raises(VercelOidcVerificationError):
        resolve_vercel_oidc_token_identity(
            sign(signing_key, claims(iss="https://evil.example.com"))
        )


@respx.mock
def test_identity_rejects_an_expired_token(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    now = int(time.time())

    with pytest.raises(VercelOidcVerificationError):
        resolve_vercel_oidc_token_identity(
            sign(signing_key, claims(exp=now - 3600)), leeway=timedelta(0)
        )


@respx.mock
def test_identity_rejects_a_token_without_a_subject(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)

    with pytest.raises(VercelOidcVerificationError):
        resolve_vercel_oidc_token_identity(sign(signing_key, claims(sub=None)))


@respx.mock
def test_identity_needs_no_project_or_environment_expectations(
    signing_key: rsa.RSAPrivateKey,
) -> None:
    """Identity is not authorization, so it must work where `VERCEL_PROJECT_ID`
    and `VERCEL_ENV` are unset, which is the normal local-development case."""
    jwks_route(signing_key)
    token = sign(signing_key)

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token)

    assert resolve_vercel_oidc_token_identity(token)


@respx.mock
def test_identity_carries_no_credential(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    token = sign(signing_key)

    identity = resolve_vercel_oidc_token_identity(token)

    assert len(identity) == 64 and all(c in "0123456789abcdef" for c in identity)
    for part in token.split("."):
        assert part not in identity


# --------------------------------------------------------------------------
# JWKS cache
# --------------------------------------------------------------------------


@respx.mock
def test_jwks_is_cached_between_verifications(signing_key: rsa.RSAPrivateKey) -> None:
    route = jwks_route(signing_key)
    token = sign(signing_key)

    verify_vercel_oidc_token(token, **EXPECTATIONS)
    verify_vercel_oidc_token(token, **EXPECTATIONS)

    assert route.call_count == 1


@respx.mock
def test_jwks_refetch_is_rate_limited_on_unknown_kid(signing_key: rsa.RSAPrivateKey) -> None:
    """An attacker-controlled `kid` must not drive unbounded refetching."""
    route = jwks_route(signing_key)

    for index in range(10):
        with pytest.raises(VercelOidcVerificationError):
            verify_vercel_oidc_token(sign(signing_key, kid=f"forged-{index}"), **EXPECTATIONS)

    assert route.call_count <= 2


@respx.mock
def test_clear_jwks_cache_forces_a_refetch(signing_key: rsa.RSAPrivateKey) -> None:
    route = jwks_route(signing_key)
    token = sign(signing_key)

    verify_vercel_oidc_token(token, **EXPECTATIONS)
    clear_jwks_cache()
    verify_vercel_oidc_token(token, **EXPECTATIONS)

    assert route.call_count == 2


# --------------------------------------------------------------------------
# Bearer extraction
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("Bearer abc", "abc"),
        ("bearer abc", "abc"),
        ("BEARER abc", "abc"),
        ("Bearer   abc  ", "abc"),
        ("Bearer\tabc", "abc"),
    ],
)
def test_extract_bearer_token_accepts_valid_headers(header: str, expected: str) -> None:
    assert extract_bearer_token({"Authorization": header}) == expected


def test_extract_bearer_token_is_header_name_insensitive() -> None:
    assert extract_bearer_token({"authorization": "Bearer abc"}) == "abc"


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": ""},
        {"Authorization": "Bearer"},
        {"Authorization": "Bearer "},
        {"Authorization": "Basic abc"},
        {"Authorization": "abc"},
        {"X-Other": "Bearer abc"},
    ],
)
def test_extract_bearer_token_rejects_invalid_headers(headers: dict[str, str]) -> None:
    with pytest.raises(VercelOidcVerificationError):
        extract_bearer_token(headers)


@respx.mock
def test_unknown_kid_refetch_is_rate_limited_under_concurrency(
    signing_key: rsa.RSAPrivateKey,
) -> None:
    """An attacker-chosen `kid` must not amplify into one JWKS request per caller."""
    from concurrent.futures import ThreadPoolExecutor

    route = jwks_route(signing_key)
    clear_jwks_cache()
    verify_vercel_oidc_token(sign(signing_key), **EXPECTATIONS)  # warm the cache
    assert route.call_count == 1

    def attempt(index: int) -> None:
        with pytest.raises(VercelOidcVerificationError):
            verify_vercel_oidc_token(sign(signing_key, kid=f"forged-{index}"), **EXPECTATIONS)

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(attempt, range(12)))

    # Exactly one refetch for the whole burst: more would be amplification, none
    # would mean rotation could never be picked up.
    assert route.call_count == 2


@respx.mock
def test_refetch_throttle_engages_before_the_claim_is_released(
    signing_key: rsa.RSAPrivateKey,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Deterministic twin of the burst test above, pinned to the exact window.

    The fetch claim used to be released in the fetch helper's `finally`, with the
    throttle recorded only after key selection. A second unknown-kid caller
    arriving in between claimed a fresh fetch, so a burst amplified anyway. Pause
    the first caller's post-refetch selection and steer a second caller into that
    window: it must wait on the claim, not fetch.
    """
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from contextvars import ContextVar

    from vercel.oidc import verify as oidc_verify

    route = jwks_route(signing_key)
    clear_jwks_cache()
    verify_vercel_oidc_token(sign(signing_key), **EXPECTATIONS)  # warm the cache
    assert route.call_count == 1

    original_select = oidc_verify._select_key
    original_claim_fetch = oidc_verify._claim_fetch
    outcome_pending = threading.Event()
    second_claim_checked = threading.Event()
    record_outcome = threading.Event()
    caller_kid: ContextVar[str | None] = ContextVar("caller_kid", default=None)
    second_claim_results: list[bool] = []
    first_kid_selections = 0

    def paused_select(document: Mapping[str, Any], kid: str) -> Any:
        nonlocal first_kid_selections
        if kid == "forged-first":
            first_kid_selections += 1
            # Call 1 is the cache probe; call 2 inspects the refetched document,
            # where the outcome (throttle) has not been recorded yet.
            if first_kid_selections == 2:
                outcome_pending.set()
                assert record_outcome.wait(timeout=5)
        return original_select(document, kid)

    monkeypatch.setattr(oidc_verify, "_select_key", paused_select)

    def observed_claim_fetch(url: str) -> bool:
        claimed = original_claim_fetch(url)
        if caller_kid.get() == "forged-second":
            second_claim_results.append(claimed)
            second_claim_checked.set()
        return claimed

    monkeypatch.setattr(oidc_verify, "_claim_fetch", observed_claim_fetch)

    def attempt(kid: str) -> None:
        token = caller_kid.set(kid)
        try:
            with pytest.raises(VercelOidcVerificationError):
                verify_vercel_oidc_token(sign(signing_key, kid=kid), **EXPECTATIONS)
        finally:
            caller_kid.reset(token)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(attempt, "forged-first")
        assert outcome_pending.wait(timeout=5)
        second = pool.submit(attempt, "forged-second")
        assert second_claim_checked.wait(timeout=5)
        record_outcome.set()
        assert second_claim_results == [False]
        first.result(timeout=10)
        second.result(timeout=10)

    assert route.call_count == 2


@respx.mock
def test_concurrent_cold_verifications_fetch_jwks_once(
    signing_key: rsa.RSAPrivateKey,
) -> None:
    from concurrent.futures import ThreadPoolExecutor

    route = jwks_route(signing_key)
    clear_jwks_cache()
    token = sign(signing_key)

    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(
            pool.map(lambda _: verify_vercel_oidc_token(token, **EXPECTATIONS), range(12))
        )

    assert all(r["project_id"] == "prj_123" for r in results)
    assert route.call_count == 1


def test_missing_crypto_backend_reports_the_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    """PyJWT without `cryptography` must name the remedy, not raise AttributeError."""
    # The published-wheel test environment installs the package without the
    # `verify` extra, so PyJWT may legitimately be absent there.
    jwt = pytest.importorskip("jwt")

    from vercel.oidc import verify as verify_module

    monkeypatch.delattr(jwt.algorithms, "RSAAlgorithm")
    with pytest.raises(VercelOidcVerificationError, match="verify"):
        verify_module._require_pyjwt()


@respx.mock
def test_rotated_signing_key_is_picked_up_sync(
    signing_key: rsa.RSAPrivateKey,
    other_signing_key: rsa.RSAPrivateKey,
) -> None:
    """A rotated key must verify immediately, not after the cache TTL.

    Two defects made this fail: the refetch rate limit shared its timestamp with
    the cache store, so a successful fetch suppressed the next refetch, and the
    sync fetch short-circuited on the still-warm cache.
    """
    route = jwks_route(signing_key, kid="k-old")
    clear_jwks_cache()
    verify_vercel_oidc_token(sign(signing_key, kid="k-old"), **EXPECTATIONS)
    assert route.call_count == 1

    route.mock(return_value=httpx.Response(200, json=jwks_for(other_signing_key, kid="k-new")))
    verified = verify_vercel_oidc_token(sign(other_signing_key, kid="k-new"), **EXPECTATIONS)

    assert verified["project_id"] == "prj_123"
    assert route.call_count == 2


@respx.mock
async def test_rotated_signing_key_is_picked_up_async(
    signing_key: rsa.RSAPrivateKey,
    other_signing_key: rsa.RSAPrivateKey,
) -> None:
    route = jwks_route(signing_key, kid="k-old")
    clear_jwks_cache()
    await verify_vercel_oidc_token_async(sign(signing_key, kid="k-old"), **EXPECTATIONS)

    route.mock(return_value=httpx.Response(200, json=jwks_for(other_signing_key, kid="k-new")))
    verified = await verify_vercel_oidc_token_async(
        sign(other_signing_key, kid="k-new"), **EXPECTATIONS
    )

    assert verified["project_id"] == "prj_123"
    assert route.call_count == 2


@respx.mock
def test_rotation_still_rejects_a_key_absent_from_the_new_jwks(
    signing_key: rsa.RSAPrivateKey,
    other_signing_key: rsa.RSAPrivateKey,
) -> None:
    """Refreshing on an unknown kid must not become a way to accept any key."""
    route = jwks_route(signing_key, kid="k-old")
    clear_jwks_cache()
    verify_vercel_oidc_token(sign(signing_key, kid="k-old"), **EXPECTATIONS)
    route.mock(return_value=httpx.Response(200, json=jwks_for(signing_key, kid="k-old")))

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(sign(other_signing_key, kid="k-unknown"), **EXPECTATIONS)


@respx.mock
async def test_concurrent_rotation_waiters_do_not_fail(
    signing_key: rsa.RSAPrivateKey,
    other_signing_key: rsa.RSAPrivateKey,
) -> None:
    """Tasks that lose the refetch race must wait for it, not reject a valid token."""
    import anyio

    clear_jwks_cache()
    route = jwks_route(signing_key, kid="k-old")
    await verify_vercel_oidc_token_async(sign(signing_key, kid="k-old"), **EXPECTATIONS)

    async def rotated(request: httpx.Request) -> httpx.Response:
        await anyio.sleep(0.05)
        return httpx.Response(200, json=jwks_for(other_signing_key, kid="k-new"))

    route.mock(side_effect=rotated)
    token = sign(other_signing_key, kid="k-new")

    results = []

    async def verify_one() -> None:
        results.append(await verify_vercel_oidc_token_async(token, **EXPECTATIONS))

    async with anyio.create_task_group() as group:
        for _ in range(6):
            group.start_soon(verify_one)

    assert len(results) == 6
    assert all(r["project_id"] == "prj_123" for r in results)
    # One refetch shared by every waiter.
    assert route.call_count == 2


def test_async_lock_is_not_reused_across_backends(signing_key: rsa.RSAPrivateKey) -> None:
    """An anyio lock binds to the backend of first use.

    A lock created under asyncio cannot be awaited under trio, so the cache is
    keyed by backend. Without that, a process using both raises
    `RuntimeError: no running event loop`.
    """
    import asyncio

    import trio

    clear_jwks_cache()

    @respx.mock
    async def verify_current(kid: str) -> dict[str, Any]:
        respx.get(JWKS_URL).mock(
            return_value=httpx.Response(200, json=jwks_for(signing_key, kid=kid))
        )
        return await verify_vercel_oidc_token_async(sign(signing_key, kid=kid), **EXPECTATIONS)

    assert asyncio.run(verify_current("k-asyncio"))["project_id"] == "prj_123"
    # Unknown kid, so this takes the lock path rather than the cache fast path.
    assert trio.run(verify_current, "k-trio")["project_id"] == "prj_123"


@respx.mock
def test_cold_cache_outage_is_not_amplified(signing_key: rsa.RSAPrivateKey) -> None:
    """With nothing cached, a failing endpoint must be hit once, not once per caller."""
    from concurrent.futures import ThreadPoolExecutor

    clear_jwks_cache()
    route = respx.get(JWKS_URL).mock(return_value=httpx.Response(503, text="unavailable"))
    token = sign(signing_key)

    def attempt(_: int) -> None:
        with pytest.raises(VercelOidcVerificationError):
            verify_vercel_oidc_token(token, **EXPECTATIONS)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(attempt, range(8)))

    assert route.call_count == 1


@respx.mock
def test_throttled_unknown_kid_fails_fast(signing_key: rsa.RSAPrivateKey) -> None:
    """Waiting is only for a fetch actually in flight, so a throttled miss is quick."""
    import time as time_module

    clear_jwks_cache()
    jwks_route(signing_key)
    verify_vercel_oidc_token(sign(signing_key), **EXPECTATIONS)

    # First forged kid triggers the one permitted refetch and sets the throttle.
    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(sign(signing_key, kid="forged-1"), **EXPECTATIONS)

    started = time_module.monotonic()
    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(sign(signing_key, kid="forged-2"), **EXPECTATIONS)
    elapsed = time_module.monotonic() - started

    assert elapsed < FETCH_WAIT_TIMEOUT, (
        "a throttled miss must not wait for a fetch that is not running"
    )


@respx.mock
def test_sync_waits_for_an_async_rotation_refresh(
    signing_key: rsa.RSAPrivateKey,
    other_signing_key: rsa.RSAPrivateKey,
) -> None:
    """The two modes hold different locks, so they coordinate through the
    in-progress marker rather than one rejecting a token the other is fetching."""
    import asyncio
    import threading

    clear_jwks_cache()
    route = jwks_route(signing_key, kid="k-old")
    verify_vercel_oidc_token(sign(signing_key, kid="k-old"), **EXPECTATIONS)

    fetch_started = threading.Event()

    async def slow(request: httpx.Request) -> httpx.Response:
        fetch_started.set()
        await asyncio.sleep(0.1)
        return httpx.Response(200, json=jwks_for(other_signing_key, kid="k-new"))

    route.mock(side_effect=slow)
    token = sign(other_signing_key, kid="k-new")
    outcome: dict[str, object] = {}

    def sync_side() -> None:
        fetch_started.wait(2.0)
        try:
            outcome["claims"] = verify_vercel_oidc_token(token, **EXPECTATIONS)
        except VercelOidcVerificationError as error:
            outcome["error"] = str(error)

    async def drive() -> None:
        thread = threading.Thread(target=sync_side)
        thread.start()
        await verify_vercel_oidc_token_async(token, **EXPECTATIONS)
        thread.join(5.0)

    asyncio.run(drive())

    assert "error" not in outcome, outcome.get("error")
    assert outcome["claims"]  # type: ignore[truthy-bool]


@respx.mock
def test_waiter_survives_a_fetch_slower_than_the_old_wait_cap(
    signing_key: rsa.RSAPrivateKey,
    other_signing_key: rsa.RSAPrivateKey,
) -> None:
    """A cold TLS handshake routinely takes longer than half a second.

    The waiting mode cannot fetch for itself, so giving up early rejected a valid
    token. The wait now ends when the owner finishes, bounded by the fetch timeout.
    """
    import asyncio
    import threading

    clear_jwks_cache()
    route = jwks_route(signing_key, kid="k-old")
    verify_vercel_oidc_token(sign(signing_key, kid="k-old"), **EXPECTATIONS)

    fetch_started = threading.Event()

    async def slow(request: httpx.Request) -> httpx.Response:
        fetch_started.set()
        await asyncio.sleep(0.7)
        return httpx.Response(200, json=jwks_for(other_signing_key, kid="k-new"))

    route.mock(side_effect=slow)
    token = sign(other_signing_key, kid="k-new")
    outcome: dict[str, object] = {}

    def sync_side() -> None:
        fetch_started.wait(3.0)
        try:
            outcome["claims"] = verify_vercel_oidc_token(token, **EXPECTATIONS)
        except VercelOidcVerificationError as error:
            outcome["error"] = str(error)

    async def drive() -> None:
        thread = threading.Thread(target=sync_side)
        thread.start()
        await verify_vercel_oidc_token_async(token, **EXPECTATIONS)
        thread.join(10.0)

    asyncio.run(drive())

    assert "error" not in outcome, outcome.get("error")
    assert FETCH_WAIT_TIMEOUT > 0.7


@respx.mock
def test_only_one_caller_fetches_across_both_modes(signing_key: rsa.RSAPrivateKey) -> None:
    """The claim is atomic, so a sync and an async caller cannot both fetch."""
    import asyncio
    import threading

    clear_jwks_cache()
    fetch_count = {"n": 0}
    barrier = threading.Event()

    async def counting(request: httpx.Request) -> httpx.Response:
        fetch_count["n"] += 1
        await asyncio.sleep(0.2)
        return httpx.Response(200, json=jwks_for(signing_key))

    respx.get(JWKS_URL).mock(side_effect=counting)
    token = sign(signing_key)
    results: list[object] = []

    def sync_side() -> None:
        barrier.wait(3.0)
        try:
            results.append(verify_vercel_oidc_token(token, **EXPECTATIONS))
        except VercelOidcVerificationError as error:  # pragma: no cover - diagnostic
            results.append(error)

    async def drive() -> None:
        thread = threading.Thread(target=sync_side)
        thread.start()
        barrier.set()
        results.append(await verify_vercel_oidc_token_async(token, **EXPECTATIONS))
        thread.join(10.0)

    asyncio.run(drive())

    assert len(results) == 2
    assert all(isinstance(r, dict) for r in results), results
    assert fetch_count["n"] == 1


@respx.mock
def test_document_is_visible_before_the_fetch_marker_clears(
    signing_key: rsa.RSAPrivateKey,
) -> None:
    """A waiter observing the fetch finish must always see its document.

    Clearing the marker first left a window where the loop exited with nothing
    cached, rejecting a token whose key was about to be stored.
    """
    import vercel.oidc.verify as verify_module

    clear_jwks_cache()
    jwks_route(signing_key)
    observed: list[bool] = []

    original_store = verify_module._store_jwks

    def observing_store(url: str, document: dict[str, Any]) -> None:
        # While storing, the fetch must still be marked in progress.
        observed.append(verify_module._fetch_is_in_progress(url))
        original_store(url, document)

    verify_module._store_jwks = observing_store  # type: ignore[assignment]
    try:
        verify_vercel_oidc_token(sign(signing_key), **EXPECTATIONS)
    finally:
        verify_module._store_jwks = original_store  # type: ignore[assignment]

    assert observed == [True]
