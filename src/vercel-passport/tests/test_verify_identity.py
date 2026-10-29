"""`verify_identity`: verifying a forwarded Passport token.

Ported from `packages/passport/test/unit/index.test.ts` in vercel/vercel, with
real signatures in place of a mocked JWT library.
"""

from collections.abc import Callable

import pytest
from conftest import (
    EXPECTATIONS,
    PAYLOAD,
    REALISTIC_EXPECTATIONS,
    REALISTIC_PAYLOAD,
    Surface,
    chunked_cookies,
    unsigned,
)
from cryptography.hazmat.primitives.asymmetric import rsa

import vendor.respx as respx
from vercel.headers import set_headers
from vercel.passport import (
    PASSPORT_COOKIE_NAME,
    PASSPORT_HEADER_NAME,
    PassportIdentityError,
    PassportOwner,
    PassportProject,
    PassportVerificationError,
)

Sign = Callable[..., str]


async def test_verifies_an_explicit_token(surface: Surface, jwks: respx.Route, sign: Sign) -> None:
    token = sign()

    identity = await surface.verify_identity(token, **EXPECTATIONS)

    assert identity.external_subject == "user_123"
    assert identity.subject == PAYLOAD["sub"]
    assert identity.token == token
    assert identity.token_source == "local"
    assert identity.verified is True
    assert jwks.call_count == 1


async def test_verifies_a_realistic_payload(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    identity = await surface.verify_identity(sign(REALISTIC_PAYLOAD), **REALISTIC_EXPECTATIONS)

    assert identity.external_subject == "00u_example_user"
    assert identity.owner == PassportOwner(slug="example-team", id="team_example")
    assert identity.project == PassportProject(id="prj_example", name="example-project")
    assert identity.payload["email_verified"] is True
    assert identity.payload["plan"] == "enterprise"
    assert identity.verified is True


@pytest.mark.parametrize("scheme", ["Bearer", "bearer", "BEARER"])
async def test_verifies_a_token_from_the_authorization_header(
    surface: Surface, jwks: respx.Route, sign: Sign, scheme: str
) -> None:
    token = sign()

    identity = await surface.verify_identity({"authorization": f"{scheme} {token}"}, **EXPECTATIONS)

    assert identity.external_subject == "user_123"
    assert identity.token == token
    assert identity.token_source == "header"
    assert identity.verified is True


async def test_verifies_a_token_from_a_request_objects_passport_header(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    import httpx2 as httpx

    token = sign()
    request = httpx.Request(
        "POST", "https://api.example.com", headers={PASSPORT_HEADER_NAME: token}
    )

    identity = await surface.verify_identity(request, **EXPECTATIONS)

    assert identity.token == token
    assert identity.token_source == "header"
    assert identity.verified is True


async def test_prefers_the_bearer_token_over_the_passport_header(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    bearer = sign()

    identity = await surface.verify_identity(
        {"authorization": f"Bearer {bearer}", PASSPORT_HEADER_NAME: sign(external_sub="other")},
        **EXPECTATIONS,
    )

    assert identity.token == bearer


async def test_verifies_a_chunked_token_from_explicit_cookies(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    token = sign(padding="x" * 4000)

    identity = await surface.verify_identity(None, cookies=chunked_cookies(token), **EXPECTATIONS)

    assert identity.token == token
    assert identity.token_source == "cookie"
    assert identity.external_subject == "user_123"
    assert identity.verified is True


async def test_prefers_the_bearer_token_over_a_malformed_chunked_cookie(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    token = sign()

    identity = await surface.verify_identity(
        {
            "authorization": f"Bearer {token}",
            "cookie": f"{PASSPORT_COOKIE_NAME}=__vc_chunked_v1:invalid",
        },
        **EXPECTATIONS,
    )

    assert identity.token == token


async def test_verifies_a_token_from_explicit_cookies(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    token = sign()

    identity = await surface.verify_identity(
        None, cookies={PASSPORT_COOKIE_NAME: token}, **EXPECTATIONS
    )

    assert identity.token == token
    assert identity.token_source == "cookie"
    assert identity.verified is True


async def test_does_not_read_the_runtime_request_context(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    """The token to verify is always named explicitly."""
    set_headers({PASSPORT_HEADER_NAME: sign()})

    with pytest.raises(PassportIdentityError, match="was not found"):
        await surface.verify_identity(None, **EXPECTATIONS)


async def test_never_falls_back_to_a_local_identity(
    surface: Surface, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json

    monkeypatch.setenv("VERCEL_PASSPORT_IDENTITY", json.dumps(PAYLOAD))

    with pytest.raises(PassportIdentityError, match="was not found"):
        await surface.verify_identity({}, **EXPECTATIONS)


async def test_accepts_a_matching_owner(surface: Surface, jwks: respx.Route, sign: Sign) -> None:
    identity = await surface.verify_identity(sign(), owner_id="team_123", **EXPECTATIONS)

    assert identity.verified is True


async def test_checks_the_audience_when_supplied(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    token = sign()

    identity = await surface.verify_identity(token, audience=PAYLOAD["aud"], **EXPECTATIONS)
    assert identity.verified is True

    with pytest.raises(PassportVerificationError):
        await surface.verify_identity(token, audience="https://vercel.com/other", **EXPECTATIONS)


@pytest.mark.parametrize("field", ["project_id", "environment"])
async def test_a_star_is_not_a_wildcard(
    surface: Surface, jwks: respx.Route, sign: Sign, field: str
) -> None:
    """Unlike the TypeScript SDK, `"*"` never widens verification.

    This matches `vercel.oidc`, which compares expectations by equality only, so
    a backend cannot accidentally accept tokens minted for any project.
    """
    with pytest.raises(PassportVerificationError):
        await surface.verify_identity(sign(), **{**EXPECTATIONS, field: "*"}, owner_id="team_123")


async def test_rejects_a_different_project(surface: Surface, jwks: respx.Route, sign: Sign) -> None:
    with pytest.raises(PassportVerificationError, match="'prj_other'"):
        await surface.verify_identity(sign(), **{**EXPECTATIONS, "project_id": "prj_other"})


async def test_rejects_a_different_environment(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    with pytest.raises(PassportVerificationError, match="'preview'"):
        await surface.verify_identity(sign(), **{**EXPECTATIONS, "environment": "preview"})


async def test_rejects_a_different_owner(surface: Surface, jwks: respx.Route, sign: Sign) -> None:
    with pytest.raises(PassportVerificationError, match="'team_other'"):
        await surface.verify_identity(sign(), owner_id="team_other", **EXPECTATIONS)


async def test_rejects_a_verified_token_that_is_not_passport_shaped(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    with pytest.raises(PassportIdentityError, match='missing typ "passport"'):
        await surface.verify_identity(sign(typ="vercel-oidc"), **EXPECTATIONS)


async def test_rejects_a_verified_token_from_a_non_passport_issuer(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    with pytest.raises(PassportVerificationError, match="is not https://passport.vercel.com"):
        await surface.verify_identity(sign(iss="https://oidc.vercel.com/team-slug"), **EXPECTATIONS)


async def test_skips_a_non_bearer_authorization_header(surface: Surface) -> None:
    with pytest.raises(PassportIdentityError, match="was not found"):
        await surface.verify_identity({"authorization": "Basic abc"}, **EXPECTATIONS)


async def test_propagates_verification_failures(
    surface: Surface, jwks: respx.Route, sign: Sign, other_signing_key: rsa.RSAPrivateKey
) -> None:
    with pytest.raises(PassportVerificationError, match="token failed verification"):
        await surface.verify_identity(sign(key=other_signing_key), **EXPECTATIONS)


async def test_rejects_an_unsigned_token(surface: Surface, jwks: respx.Route) -> None:
    with pytest.raises(PassportVerificationError):
        await surface.verify_identity(unsigned(PAYLOAD), **EXPECTATIONS)


async def test_verification_errors_are_identity_errors(surface: Surface, jwks: respx.Route) -> None:
    """One `except PassportIdentityError` handles every rejection."""
    with pytest.raises(PassportIdentityError):
        await surface.verify_identity(unsigned(PAYLOAD), **EXPECTATIONS)


@pytest.mark.parametrize("source", [{}, ""])
async def test_throws_when_no_token_is_provided(surface: Surface, source: object) -> None:
    with pytest.raises(PassportIdentityError):
        await surface.verify_identity(source, **EXPECTATIONS)


# --------------------------------------------------------------------------
# Clock tolerance: vercel-oidc's 60 seconds
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"exp": -120}, id="expired 2m ago"),
        pytest.param({"nbf": 120}, id="not valid for another 2m"),
        # Unlike `jose`, which never checks a future `iat`; documented in the README.
        pytest.param({"iat": 120}, id="issued 2m in the future"),
    ],
)
async def test_rejects_time_claims_beyond_the_clock_skew_allowance(
    surface: Surface, jwks: respx.Route, sign: Sign, overrides: dict[str, int]
) -> None:
    import time

    now = int(time.time())
    claims = {name: now + offset for name, offset in overrides.items()}

    with pytest.raises(PassportVerificationError):
        await surface.verify_identity(sign(**claims), **EXPECTATIONS)


@pytest.mark.parametrize(
    "offset",
    [pytest.param(2, id="2s ahead"), pytest.param(30, id="30s ahead")],
)
async def test_tolerates_an_issuing_clock_ahead_of_ours(
    surface: Surface, jwks: respx.Route, sign: Sign, offset: int
) -> None:
    """A token minted by an edge clock running ahead has `iat` and `nbf` in our future.

    `jose` in the TypeScript package never rejects a future `iat`, so a fresh
    token from a slightly fast clock must not be rejected here either.
    """
    import time

    ahead = int(time.time()) + offset

    identity = await surface.verify_identity(sign(iat=ahead, nbf=ahead), **EXPECTATIONS)

    assert identity.verified is True


async def test_accepts_a_token_expired_within_the_clock_skew_allowance(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    import time

    identity = await surface.verify_identity(sign(exp=int(time.time()) - 30), **EXPECTATIONS)

    assert identity.verified is True


async def test_accepts_a_token_about_to_expire(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    import time

    identity = await surface.verify_identity(sign(exp=int(time.time()) + 30), **EXPECTATIONS)

    assert identity.verified is True
