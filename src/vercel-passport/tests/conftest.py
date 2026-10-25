"""Shared fixtures for vercel-passport tests.

Tokens are signed for real with a throwaway RSA key and verified against a mocked
JWKS, so these tests exercise the actual verifier rather than a stub of it.
"""

import json
import time
from collections.abc import Awaitable, Callable, Generator
from dataclasses import dataclass
from typing import Any

import httpx2 as httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

import vendor.respx as respx
import vercel.passport as passport_async
import vercel.passport.sync as passport_sync
from vercel.headers import set_headers
from vercel.oidc.verify import clear_jwks_cache
from vercel.passport import PASSPORT_COOKIE_NAME, PassportIdentity
from vercel.passport._internal.development import reset_development_warning

pytest_plugins = ["vendor.respx.plugin"]

JWKS_URL = "https://oidc.vercel.com/.well-known/jwks"
KID = "test-key-1"

PAYLOAD: dict[str, Any] = {
    "aud": "https://vercel.com/team-slug/my-project/production",
    "connector_id": "scl_123",
    "email": "user@example.com",
    "environment": "production",
    "external_iss": "https://example.okta.com/oauth2/default",
    "external_sub": "user_123",
    "iss": "https://passport.vercel.com/team-slug",
    "name": "Example User",
    "owner": "team-slug",
    "owner_id": "team_123",
    "project": "my-project",
    "project_id": "prj_123",
    "scope": "owner:team-slug:connector:scl_123:principal:user_123",
    "sub": "owner:team-slug:connector:scl_123:principal:user_123",
    "typ": "passport",
}

REALISTIC_PAYLOAD: dict[str, Any] = {
    **PAYLOAD,
    "aud": "owner:team_example:project:prj_example:environment:production",
    "connector_id": "scl_example",
    "email": "person@example.com",
    "email_verified": True,
    "external_iss": "https://idp.example.com",
    "external_sub": "00u_example_user",
    "iss": "https://passport.vercel.com/example-team",
    "name": "Example Person",
    "owner": "example-team",
    "owner_id": "team_example",
    "plan": "enterprise",
    "project": "example-project",
    "project_id": "prj_example",
    "scope": "owner:team_example:project:prj_example:host:example.vercel.app",
    "sid": "oauth_example-session",
    "sub": "owner:team_example:connector:scl_example:principal:00u_example_user",
}

EXPECTATIONS: dict[str, Any] = {"project_id": "prj_123", "environment": "production"}
REALISTIC_EXPECTATIONS: dict[str, Any] = {
    "project_id": "prj_example",
    "environment": "production",
}

_ENV = (
    "VERCEL",
    "VERCEL_ENV",
    "VERCEL_TARGET_ENV",
    "VERCEL_PROJECT_ID",
    "VERCEL_PASSPORT_TOKEN",
    "VERCEL_PASSPORT_IDENTITY",
    "VERCEL_PASSPORT_DEV",
    "VERCEL_PASSPORT_DEV_OWNER",
    "VERCEL_PASSPORT_DEV_OWNER_ID",
    "VERCEL_PASSPORT_DEV_CONNECTOR_ID",
    "VERCEL_PASSPORT_DEV_EXTERNAL_ISS",
    "VERCEL_PASSPORT_DEV_EXTERNAL_SUB",
    "VERCEL_PASSPORT_DEV_ENVIRONMENT",
    "VERCEL_PASSPORT_DEV_PROJECT",
    "VERCEL_PASSPORT_DEV_PROJECT_ID",
    "VERCEL_PASSPORT_DEV_ISSUER",
    "VERCEL_PASSPORT_DEV_AUDIENCE",
)


@pytest.fixture(autouse=True)
def isolated(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Isolate each test from the developer environment and from each other."""
    for name in _ENV:
        monkeypatch.delenv(name, raising=False)
    set_headers(None)
    clear_jwks_cache()
    reset_development_warning()
    yield
    set_headers(None)
    clear_jwks_cache()
    reset_development_warning()


@pytest.fixture(scope="session")
def signing_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="session")
def other_signing_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture
def jwks(signing_key: rsa.RSAPrivateKey) -> Generator[respx.Route, None, None]:
    """Serve the test key from Vercel's JWKS URL."""
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(signing_key.public_key()))
    jwk.update({"kid": KID, "use": "sig", "alg": "RS256"})
    with respx.mock(assert_all_called=False) as mock:
        yield mock.get(JWKS_URL).mock(return_value=httpx.Response(200, json={"keys": [jwk]}))


@pytest.fixture
def sign(signing_key: rsa.RSAPrivateKey) -> Callable[..., str]:
    """Sign claims (PAYLOAD by default) with fresh, valid time claims."""

    def sign_claims(
        claims: dict[str, Any] | None = None,
        *,
        key: rsa.RSAPrivateKey | None = None,
        **overrides: Any,
    ) -> str:
        now = int(time.time())
        payload = {"iat": now, "nbf": now, "exp": now + 3600, **(claims or PAYLOAD), **overrides}
        payload = {name: value for name, value in payload.items() if value is not None}
        return jwt.encode(payload, key or signing_key, algorithm="RS256", headers={"kid": KID})

    return sign_claims


@dataclass(frozen=True)
class Surface:
    """One public surface, with the sync one adapted to be awaited."""

    name: str
    get_identity: Callable[..., Awaitable[PassportIdentity | None]]
    require_identity: Callable[..., Awaitable[PassportIdentity]]
    verify_identity: Callable[..., Awaitable[PassportIdentity]]


def _awaitable(func: Callable[..., Any]) -> Callable[..., Awaitable[Any]]:
    async def call(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    return call


@pytest.fixture(params=["async", "sync"])
def surface(request: pytest.FixtureRequest) -> Surface:
    """Run a test against both `vercel.passport` and `vercel.passport.sync`."""
    if request.param == "async":
        return Surface(
            "async",
            passport_async.get_identity,
            passport_async.require_identity,
            passport_async.verify_identity,
        )
    return Surface(
        "sync",
        _awaitable(passport_sync.get_identity),
        _awaitable(passport_sync.require_identity),
        _awaitable(passport_sync.verify_identity),
    )


def chunked_cookies(
    token: str, chunk_size: int = 3500, *, count: int | None = None
) -> dict[str, str]:
    """Split a token across Passport session cookies the way Proxy writes them.

    Splits into `chunk_size` pieces, or into exactly `count` near-equal pieces.
    Returns the manifest cookie first, then the chunks in order, so the result
    works both as a cookie mapping and, through `cookie_header`, as a header.
    """
    import hashlib

    checksum = hashlib.sha256(token.encode()).hexdigest()
    if count is not None:
        size, extra = divmod(len(token), count)
        bounds = [index * size + min(index, extra) for index in range(count + 1)]
        chunks = [token[start:end] for start, end in zip(bounds[:-1], bounds[1:], strict=True)]
    else:
        chunks = [token[start : start + chunk_size] for start in range(0, len(token), chunk_size)]
    cookies = {PASSPORT_COOKIE_NAME: f"__vc_chunked_v1:{len(chunks)}:{checksum}"}
    for index, chunk in enumerate(chunks, start=1):
        cookies[f"{PASSPORT_COOKIE_NAME}.{index}.{checksum[:32]}"] = chunk
    return cookies


def cookie_header(cookies: dict[str, str]) -> str:
    return "; ".join(f"{name}={value}" for name, value in cookies.items())


def unsigned(claims: dict[str, Any]) -> str:
    """Encode claims as a JWT without a usable signature."""
    import base64

    def b64(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    header = b64(json.dumps({"alg": "RS256", "kid": KID}).encode())
    return f"{header}.{b64(json.dumps(claims).encode())}.signature"
