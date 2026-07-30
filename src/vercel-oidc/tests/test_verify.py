"""Vercel OIDC token signature verification.

Attack vectors come first, deliberately. Every historical CVE in this space is an
algorithm- or key-confusion bug, so each of those classes gets an explicit test
vector before the happy path is exercised at all.
"""

import base64
import json
import time
from typing import Any

import httpx
import pytest
import respx
from conftest import ISSUER, JWKS_URL, KID, b64url, claims, forge_hs256, jwks_for, sign
from cryptography.hazmat.primitives.asymmetric import rsa

from vercel.oidc import verify_vercel_oidc_token
from vercel.oidc.aio import verify_vercel_oidc_token as verify_vercel_oidc_token_async
from vercel.oidc.verify import (
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


@respx.mock
def test_rejects_a_wrong_issuer(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    token = sign(signing_key, claims(iss="https://evil.example.com"))

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
def test_wildcard_project_requires_owner_or_audience(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    token = sign(signing_key, claims(project_id="*"))

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, project_id="*", environment="production")


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
def test_rejects_a_non_https_jwks_issuer(signing_key: rsa.RSAPrivateKey) -> None:
    token = sign(signing_key)

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, issuer="http://oidc.vercel.com", **EXPECTATIONS)


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
def test_wildcard_project_accepted_with_owner_id(signing_key: rsa.RSAPrivateKey) -> None:
    jwks_route(signing_key)
    token = sign(signing_key, claims(project_id="*"))

    verified = verify_vercel_oidc_token(
        token, project_id="*", environment="production", owner_id="team_123"
    )

    assert verified["owner_id"] == "team_123"


@respx.mock
def test_clock_skew_leeway_is_applied(signing_key: rsa.RSAPrivateKey) -> None:
    from datetime import timedelta

    jwks_route(signing_key)
    now = int(time.time())
    token = sign(signing_key, claims(exp=now - 10))

    with pytest.raises(VercelOidcVerificationError):
        verify_vercel_oidc_token(token, leeway=timedelta(0), **EXPECTATIONS)

    assert verify_vercel_oidc_token(token, leeway=timedelta(seconds=60), **EXPECTATIONS)


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
    import jwt

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
