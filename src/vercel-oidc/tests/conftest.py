"""Shared fixtures for vercel-oidc tests."""

import base64
import json
import time
from collections.abc import Generator
from typing import Any

import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

pytest_plugins = ["vendor.respx.plugin"]

JWKS_URL = "https://oidc.vercel.com/.well-known/jwks"
ISSUER = "https://oidc.vercel.com"
KID = "test-key-1"


@pytest.fixture
def mock_env_clear(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Prevent tests from resolving credentials from the developer environment."""
    for name in (
        "VERCEL_TOKEN",
        "VERCEL_TEAM_ID",
        "VERCEL_PROJECT_ID",
        "VERCEL_OIDC_TOKEN",
        "VERCEL_OIDC_TOKEN_HEADER",
        "VERCEL_ENV",
        "VERCEL_TARGET_ENV",
    ):
        monkeypatch.delenv(name, raising=False)

    from vercel.oidc.token import _clear_cached_oidc_token
    from vercel.oidc.verify import clear_jwks_cache

    _clear_cached_oidc_token()
    try:
        clear_jwks_cache()
    except NotImplementedError:
        pass
    yield
    _clear_cached_oidc_token()
    try:
        clear_jwks_cache()
    except NotImplementedError:
        pass


@pytest.fixture(scope="session")
def signing_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="session")
def other_signing_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def jwks_for(key: rsa.RSAPrivateKey, *, kid: str = KID) -> dict[str, Any]:
    """Serialize a public key as a JWKS document."""
    import jwt

    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
    jwk.update({"kid": kid, "use": "sig", "alg": "RS256"})
    return {"keys": [jwk]}


def claims(**overrides: Any) -> dict[str, Any]:
    now = int(time.time())
    payload: dict[str, Any] = {
        "iss": ISSUER,
        "sub": "owner:acme:project:my-app:environment:production",
        "aud": "https://vercel.com/acme",
        "owner": "acme",
        "owner_id": "team_123",
        "project": "my-app",
        "project_id": "prj_123",
        "environment": "production",
        "iat": now,
        "nbf": now,
        "exp": now + 3600,
    }
    payload.update(overrides)
    return {name: value for name, value in payload.items() if value is not None}


def forge_hs256(secret: bytes, payload: dict[str, Any] | None = None, *, kid: str = KID) -> str:
    """Hand-roll an HS256 token, bypassing any library's key-type guards.

    An attacker exploiting algorithm confusion has no reason to respect PyJWT's
    refusal to use an asymmetric key as an HMAC secret, so neither does this.
    """
    import hashlib
    import hmac

    header = b64url(json.dumps({"alg": "HS256", "typ": "JWT", "kid": kid}).encode())
    body = b64url(json.dumps(payload if payload is not None else claims()).encode())
    signing_input = f"{header}.{body}".encode()
    signature = hmac.new(secret, signing_input, hashlib.sha256).digest()
    return f"{header}.{body}.{b64url(signature)}"


def sign(
    key: rsa.RSAPrivateKey,
    payload: dict[str, Any] | None = None,
    *,
    kid: str = KID,
    algorithm: str = "RS256",
) -> str:
    import jwt

    return jwt.encode(
        payload if payload is not None else claims(),
        key,
        algorithm=algorithm,
        headers={"kid": kid},
    )
