"""Cache identity across a platform token refresh.

A Vercel OIDC token is a signature over an identity plus an expiry, so one
identity is issued many tokens over time. Keying the token cache on the token
itself discards every cached credential on each refresh, so the key is derived
from the token's verified identity claims instead. These tests use real signed
tokens and a mocked JWKS, because the fallback path is what runs when
verification fails and it would hide a broken verified path.
"""

import json
import time
from collections.abc import Sequence
from typing import Any

import httpx2 as httpx
import pytest
from conftest import TEST_BASE_URL, session_options
from cryptography.hazmat.primitives.asymmetric import rsa

import vendor.respx as respx
from vercel.api import session
from vercel.connect import (
    ConnectAppTokenSubject,
    ConnectOptions,
    ConnectServiceOptions,
    get_token,
    sync as connect_sync,
)
from vercel.connect._internal.identity import IdentityResolver
from vercel.oidc.verify import JWKS_URL, VercelOidcVerificationError, clear_jwks_cache

TOKEN_URL = f"{TEST_BASE_URL}/v1/connect/token/slack%2Fmy-bot"
ISSUER = "https://oidc.vercel.com/acme"
KID = "identity-key"
SUBJECT = "owner:acme:project:my-app:environment:production"


@pytest.fixture(scope="module")
def signing_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="module")
def other_signing_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(autouse=True)
def _clean_jwks() -> Any:
    clear_jwks_cache()
    yield
    clear_jwks_cache()


def jwks_route(key: rsa.RSAPrivateKey, *, kid: str = KID) -> respx.Route:
    import jwt

    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
    jwk.update({"kid": kid, "use": "sig", "alg": "RS256"})
    return respx.get(JWKS_URL).mock(return_value=httpx.Response(200, json={"keys": [jwk]}))


def platform_token(
    key: rsa.RSAPrivateKey,
    *,
    subject: str = SUBJECT,
    issuer: str = ISSUER,
    expires_in: int = 3600,
    kid: str = KID,
) -> str:
    """Mint a platform token the way the runtime would."""
    import jwt

    now = int(time.time())
    return jwt.encode(
        {
            "iss": issuer,
            "sub": subject,
            "aud": "https://vercel.com/acme",
            "owner_id": "team_123",
            "project_id": "prj_123",
            "environment": "production",
            "iat": now,
            "nbf": now,
            "exp": now + expires_in,
        },
        key,
        algorithm="RS256",
        headers={"kid": kid},
    )


def counting_route() -> respx.Route:
    counter = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        counter["n"] += 1
        return httpx.Response(
            200,
            json={
                "token": f"upstream-{counter['n']}",
                "expiresAt": int((time.time() + 3600) * 1000),
                "connector": {"id": "scl_123", "uid": "slack/my-bot", "type": "slack"},
            },
        )

    return respx.post(TOKEN_URL).mock(side_effect=handler)


def rotating_options(tokens: Sequence[str]) -> list[ConnectServiceOptions]:
    """Service options whose credential resolver hands out a new token per call."""
    remaining = list(tokens)

    async def credentials_factory() -> str:
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    return [
        ConnectServiceOptions(
            base_url=TEST_BASE_URL,
            credentials_factory=credentials_factory,
        )
    ]


# --------------------------------------------------------------------------
# A refresh must not re-mint
# --------------------------------------------------------------------------


@respx.mock
async def test_refreshed_platform_token_reuses_the_cached_credential(
    mock_env_clear: None,
    signing_key: rsa.RSAPrivateKey,
) -> None:
    """The whole point: two tokens, one identity, one mint."""
    jwks_route(signing_key)
    route = counting_route()
    first = platform_token(signing_key, expires_in=1800)
    second = platform_token(signing_key, expires_in=3600)
    assert first != second

    async with session(service_options=rotating_options([first, second])):
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert route.call_count == 1


@respx.mock
def test_refreshed_platform_token_reuses_the_cached_credential_sync(
    mock_env_clear: None,
    signing_key: rsa.RSAPrivateKey,
) -> None:
    jwks_route(signing_key)
    route = counting_route()
    first = platform_token(signing_key, expires_in=1800)
    second = platform_token(signing_key, expires_in=3600)

    with session(service_options=rotating_options([first, second])):
        connect_sync.get_token("slack/my-bot", subject=ConnectAppTokenSubject())
        connect_sync.get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert route.call_count == 1


@respx.mock
async def test_refresh_reuse_also_applies_to_an_explicit_token(
    mock_env_clear: None,
    signing_key: rsa.RSAPrivateKey,
) -> None:
    """A caller passing the token themselves refreshes it themselves too."""
    jwks_route(signing_key)
    route = counting_route()

    async with session(service_options=session_options()):
        for expires_in in (1800, 3600):
            await get_token(
                "slack/my-bot",
                subject=ConnectAppTokenSubject(),
                options=ConnectOptions(
                    vercel_token=platform_token(signing_key, expires_in=expires_in)
                ),
            )

    assert route.call_count == 1


@respx.mock
async def test_jwks_is_fetched_once_per_distinct_token(
    mock_env_clear: None,
    signing_key: rsa.RSAPrivateKey,
) -> None:
    """Identity resolution must not add a round trip to every call."""
    jwks = jwks_route(signing_key)
    counting_route()
    token = platform_token(signing_key)

    async with session(service_options=rotating_options([token])):
        for _ in range(3):
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert jwks.call_count == 1


# --------------------------------------------------------------------------
# Distinct identities must stay separate
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "overrides",
    [
        {"subject": "owner:acme:project:other-app:environment:production"},
        {"issuer": "https://oidc.vercel.com/other-team"},
    ],
)
@respx.mock
async def test_distinct_identities_do_not_share_a_credential(
    mock_env_clear: None,
    signing_key: rsa.RSAPrivateKey,
    overrides: dict[str, Any],
) -> None:
    jwks_route(signing_key)
    route = counting_route()
    tokens = [platform_token(signing_key), platform_token(signing_key, **overrides)]

    async with session(service_options=rotating_options(tokens)):
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert route.call_count == 2


@respx.mock
async def test_a_forged_token_cannot_read_a_verified_entry(
    mock_env_clear: None,
    signing_key: rsa.RSAPrivateKey,
    other_signing_key: rsa.RSAPrivateKey,
) -> None:
    """Security regression: identity comes from verified claims only.

    The forged token asserts exactly the same identity, so keying on unverified
    claims would hand it the credential minted for the real one without the
    Connect API ever seeing it.
    """
    jwks_route(signing_key)
    route = counting_route()
    genuine = platform_token(signing_key)
    forged = platform_token(other_signing_key)

    async with session(service_options=rotating_options([genuine, forged])):
        real = await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
        impostor = await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert route.call_count == 2
    assert real != impostor


@respx.mock
async def test_an_opaque_token_keys_per_token(
    mock_env_clear: None,
) -> None:
    """No JWT to verify, so each token gets its own partition, as before."""
    route = counting_route()

    async with session(service_options=rotating_options(["opaque-one", "opaque-two"])):
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert route.call_count == 2


@respx.mock
async def test_an_unreachable_jwks_degrades_instead_of_failing(
    mock_env_clear: None,
    signing_key: rsa.RSAPrivateKey,
) -> None:
    """A JWKS outage must cost an extra mint, never the call."""
    respx.get(JWKS_URL).mock(return_value=httpx.Response(503, text="unavailable"))
    route = counting_route()
    tokens = [platform_token(signing_key, expires_in=1800), platform_token(signing_key)]

    async with session(service_options=rotating_options(tokens)):
        assert await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
        assert await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert route.call_count == 2


@respx.mock
async def test_an_expired_platform_token_is_not_given_an_identity(
    mock_env_clear: None,
    signing_key: rsa.RSAPrivateKey,
) -> None:
    """An expired token must not inherit the live token's cache entry."""
    jwks_route(signing_key)
    route = counting_route()
    tokens = [platform_token(signing_key), platform_token(signing_key, expires_in=-3600)]

    async with session(service_options=rotating_options(tokens)):
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert route.call_count == 2


# --------------------------------------------------------------------------
# Resolver
# --------------------------------------------------------------------------


async def test_resolver_verifies_once_per_distinct_token() -> None:
    calls: list[str] = []

    def resolve(token: str) -> str:
        calls.append(token)
        return "identity"

    resolver = IdentityResolver(resolve_identity=resolve)

    assert await resolver.resolve("a") == await resolver.resolve("a")
    await resolver.resolve("b")

    assert calls == ["a", "b"]


async def test_resolver_reverifies_after_the_ttl() -> None:
    """A token that verified once must stop naming a live identity once it expires."""
    calls: list[str] = []

    def resolve(token: str) -> str:
        calls.append(token)
        if len(calls) > 1:
            raise VercelOidcVerificationError("token failed verification")
        return "identity"

    resolver = IdentityResolver(resolve_identity=resolve, ttl_seconds=0)

    first = await resolver.resolve("a")
    second = await resolver.resolve("a")

    assert len(calls) == 2
    assert first.startswith("oidc:")
    assert second.startswith("token:")


async def test_resolver_awaits_an_async_implementation() -> None:
    async def resolve(token: str) -> str:
        return f"identity-{token}"

    resolver = IdentityResolver(resolve_identity=resolve)

    assert await resolver.resolve("a") == "oidc:identity-a"


async def test_resolver_is_bounded() -> None:
    resolver = IdentityResolver(resolve_identity=lambda token: token, max_size=2)

    for index in range(10):
        await resolver.resolve(f"token-{index}")

    assert len(resolver) == 2


async def test_resolver_never_stores_the_token_itself() -> None:
    resolver = IdentityResolver(resolve_identity=lambda token: None)

    assert "super-secret" not in await resolver.resolve("super-secret")


async def test_resolver_namespaces_verified_identities() -> None:
    """A verified identity must not be forgeable by choosing a token digest."""
    from vercel.connect._internal.identity import token_digest

    verified = IdentityResolver(resolve_identity=lambda token: token_digest("x"))
    unverifiable = IdentityResolver(resolve_identity=lambda token: None)

    assert await verified.resolve("x") != await unverifiable.resolve("x")
