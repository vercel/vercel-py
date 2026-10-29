"""`get_identity`: reading the current request's Passport identity.

Ported from `packages/passport/test/unit/index.test.ts` in vercel/vercel, with
real signatures in place of a mocked JWT library.
"""

from collections.abc import Callable
from typing import Any

import pytest
from conftest import (
    EXPECTATIONS,
    PAYLOAD,
    REALISTIC_EXPECTATIONS,
    REALISTIC_PAYLOAD,
    Surface,
    chunked_cookies,
    cookie_header,
    unsigned,
)
from cryptography.hazmat.primitives.asymmetric import rsa

import vendor.respx as respx
from vercel.headers import set_headers
from vercel.passport import (
    PASSPORT_COOKIE_NAME,
    PASSPORT_HEADER_NAME,
    PassportDevelopmentIdentity,
    PassportDevelopmentIdentityWarning,
    PassportIdentity,
    PassportIdentityError,
    PassportOwner,
    PassportProject,
    PassportValidationError,
    PassportVerificationError,
    local_identity,
)

Sign = Callable[..., str]

# --------------------------------------------------------------------------
# Request tokens
# --------------------------------------------------------------------------


async def test_reads_the_header_from_the_runtime_request_context(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    token = sign()
    set_headers({PASSPORT_HEADER_NAME: token})

    identity = await surface.get_identity(**EXPECTATIONS)

    assert identity is not None
    assert identity.token_source == "header"
    assert identity.external_subject == "user_123"
    assert identity.verified is True
    # Keys come from the pinned OIDC JWKS, not from anything in the token.
    assert jwks.call_count == 1


async def test_reads_the_cookie_from_the_runtime_request_context(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    set_headers({"cookie": f"other=value; {PASSPORT_COOKIE_NAME}={sign()}"})

    identity = await surface.get_identity(**EXPECTATIONS)

    assert identity is not None
    assert identity.token_source == "cookie"
    assert identity.external_subject == "user_123"
    assert identity.verified is True


async def test_reassembles_a_chunked_cookie_from_the_runtime_request_context(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    token = sign(padding="x" * 4000)
    set_headers({"Cookie": cookie_header(chunked_cookies(token))})

    identity = await surface.get_identity(**EXPECTATIONS)

    assert identity is not None
    assert identity.token == token
    assert identity.token_source == "cookie"


async def test_reads_the_trusted_header_from_explicit_headers(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    token = sign()

    identity = await surface.get_identity({PASSPORT_HEADER_NAME: token}, **EXPECTATIONS)

    assert identity is not None
    assert identity.connector_id == "scl_123"
    assert identity.environment == "production"
    assert identity.email == "user@example.com"
    assert identity.external_issuer == "https://example.okta.com/oauth2/default"
    assert identity.external_subject == "user_123"
    assert identity.name == "Example User"
    assert identity.owner == PassportOwner(slug="team-slug", id="team_123")
    assert identity.project == PassportProject(id="prj_123", name="my-project")
    assert identity.subject == PAYLOAD["sub"]
    assert identity.token == token
    assert identity.token_source == "header"
    assert identity.verified is True


async def test_explicit_headers_take_precedence_over_the_request_context(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    set_headers({PASSPORT_HEADER_NAME: sign(external_sub="context-user")})

    identity = await surface.get_identity({PASSPORT_HEADER_NAME: sign()}, **EXPECTATIONS)

    assert identity is not None
    assert identity.external_subject == "user_123"


async def test_matches_header_names_case_insensitively(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    identity = await surface.get_identity({"X-Vercel-OIDC-Passport-Token": sign()}, **EXPECTATIONS)

    assert identity is not None
    assert identity.token_source == "header"


async def test_accepts_a_request_object(surface: Surface, jwks: respx.Route, sign: Sign) -> None:
    from starlette.requests import Request

    token = sign()
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [(PASSPORT_HEADER_NAME.encode(), token.encode())],
        }
    )

    identity = await surface.get_identity(request, **EXPECTATIONS)

    assert identity is not None
    assert identity.token == token


async def test_falls_back_to_explicit_cookies(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    identity = await surface.get_identity(cookies={PASSPORT_COOKIE_NAME: sign()}, **EXPECTATIONS)

    assert identity is not None
    assert identity.token_source == "cookie"
    assert identity.external_subject == "user_123"
    assert identity.verified is True


async def test_falls_back_to_the_cookie_header_in_explicit_headers(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    identity = await surface.get_identity(
        {"cookie": f"other=value; {PASSPORT_COOKIE_NAME}={sign()}"}, **EXPECTATIONS
    )

    assert identity is not None
    assert identity.token_source == "cookie"
    assert identity.verified is True


async def test_prefers_the_header_over_a_cookie(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    cookie_token = sign(
        external_sub="cookie-user",
        scope="owner:team-slug:connector:scl_123:principal:cookie-user",
        sub="owner:team-slug:connector:scl_123:principal:cookie-user",
    )

    identity = await surface.get_identity(
        {PASSPORT_HEADER_NAME: sign()},
        cookies={PASSPORT_COOKIE_NAME: cookie_token},
        **EXPECTATIONS,
    )

    assert identity is not None
    assert identity.token_source == "header"
    assert identity.external_subject == "user_123"


# Large enough to need two cookies at Proxy's 3500-character chunk size.
PADDING = "x" * 4000


async def test_reassembles_a_chunked_cookie(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    token = sign(padding=PADDING)
    cookies = chunked_cookies(token)
    assert len(cookies) == 3

    identity = await surface.get_identity({"cookie": cookie_header(cookies)}, **EXPECTATIONS)

    assert identity is not None
    assert identity.token == token
    assert identity.token_source == "cookie"
    assert identity.external_subject == "user_123"
    assert identity.verified is True


async def test_reassembles_a_chunked_cookie_from_explicit_cookies(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    token = sign(padding=PADDING)

    identity = await surface.get_identity(cookies=chunked_cookies(token), **EXPECTATIONS)

    assert identity is not None
    assert identity.token == token
    assert identity.token_source == "cookie"


@pytest.mark.parametrize(
    "chunks",
    [pytest.param(1, id="one chunk"), pytest.param(11, id="eleven chunks")],
)
async def test_does_not_depend_on_the_writers_chunk_count(
    surface: Surface, jwks: respx.Route, sign: Sign, chunks: int
) -> None:
    token = sign(padding="x" * 12000)
    cookies = chunked_cookies(token, count=chunks)
    assert cookies[PASSPORT_COOKIE_NAME].startswith(f"__vc_chunked_v1:{chunks}:")

    identity = await surface.get_identity({"cookie": cookie_header(cookies)}, **EXPECTATIONS)

    assert identity is not None
    assert identity.token == token


async def test_accepts_the_maximum_chunk_count(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    token = sign()
    cookies = chunked_cookies(token, count=100)
    assert cookies[PASSPORT_COOKIE_NAME].startswith("__vc_chunked_v1:100:")

    identity = await surface.get_identity(cookies=cookies, **EXPECTATIONS)

    assert identity is not None
    assert identity.token == token


async def test_reads_chunks_only_from_the_manifests_generation(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    current = sign(padding=PADDING)
    stale = chunked_cookies(sign(padding=PADDING, external_sub="stale-user"))
    cookies = chunked_cookies(current)
    # Chunks of a previous session sit beside the current ones, as after a
    # re-login whose old chunk cookies have not expired yet.
    header = cookie_header({**{n: v for n, v in stale.items() if "." in n}, **cookies})

    identity = await surface.get_identity({"cookie": header}, **EXPECTATIONS)

    assert identity is not None
    assert identity.token == current
    assert identity.external_subject == "user_123"


def test_reassembles_a_fixed_chunked_cookie_vector() -> None:
    """A literal vector, so the reader is not only checked against the test helper."""
    from vercel.passport._internal.resolve import _cookie

    checksum = "256d04db4e5e4ac308751ed0885b722b758630567c53a7125ed9fbd068e5c3f6"
    cookies = {
        "_vercel_passport": f"__vc_chunked_v1:002:{checksum}",
        "_vercel_passport.1.256d04db4e5e4ac308751ed0885b722b": "header.payl",
        "_vercel_passport.2.256d04db4e5e4ac308751ed0885b722b": "oad.signature",
    }

    assert _cookie(cookies, None) == "header.payload.signature"


async def test_rejects_an_unencodable_chunk_as_an_identity_error(surface: Surface) -> None:
    checksum = "a" * 64
    cookies = {
        PASSPORT_COOKIE_NAME: f"__vc_chunked_v1:1:{checksum}",
        f"{PASSPORT_COOKIE_NAME}.1.{checksum[:32]}": "\udc80",
    }

    with pytest.raises(PassportIdentityError, match="checksum does not match"):
        await surface.get_identity(cookies=cookies, **EXPECTATIONS)


async def test_an_explicit_token_skips_a_malformed_chunked_cookie(surface: Surface) -> None:
    identity = await surface.get_identity(
        {"cookie": f"{PASSPORT_COOKIE_NAME}=__vc_chunked_v1:invalid"}, token=unsigned(PAYLOAD)
    )

    assert identity is not None
    assert identity.token_source == "local"


async def test_rejects_a_chunked_cookie_with_a_missing_chunk(surface: Surface, sign: Sign) -> None:
    cookies = chunked_cookies(sign(padding=PADDING))
    del cookies[next(name for name in cookies if name.startswith(f"{PASSPORT_COOKIE_NAME}.2."))]

    with pytest.raises(PassportIdentityError, match="missing chunk 2 of 2"):
        await surface.get_identity({"cookie": cookie_header(cookies)}, **EXPECTATIONS)


async def test_rejects_a_chunked_cookie_with_a_checksum_mismatch(
    surface: Surface, sign: Sign
) -> None:
    cookies = chunked_cookies(sign(padding=PADDING))
    first = next(name for name in cookies if name.startswith(f"{PASSPORT_COOKIE_NAME}.1."))
    cookies[first] = ("y" if cookies[first][0] == "x" else "x") + cookies[first][1:]

    with pytest.raises(PassportIdentityError, match="checksum does not match"):
        await surface.get_identity({"cookie": cookie_header(cookies)}, **EXPECTATIONS)


@pytest.mark.parametrize(
    "manifest",
    [
        pytest.param(f"__vc_chunked_v1:101:{'a' * 64}", id="above the chunk limit"),
        pytest.param(f"__vc_chunked_v1:0:{'a' * 64}", id="zero chunks"),
        pytest.param(f"__vc_chunked_v1:{'9' * 5000}:{'a' * 64}", id="absurd count"),
        pytest.param(f"__vc_chunked_v1:2:{'A' * 64}", id="uppercase checksum"),
        pytest.param("__vc_chunked_v1:invalid", id="malformed"),
    ],
)
async def test_rejects_an_invalid_chunked_cookie_manifest(surface: Surface, manifest: str) -> None:
    with pytest.raises(PassportIdentityError, match="manifest is invalid") as caught:
        await surface.get_identity({"cookie": f"{PASSPORT_COOKIE_NAME}={manifest}"}, **EXPECTATIONS)
    assert not isinstance(caught.value, PassportVerificationError)


async def test_prefers_the_header_over_a_malformed_chunked_cookie(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    token = sign()

    identity = await surface.get_identity(
        {
            "cookie": f"{PASSPORT_COOKIE_NAME}=__vc_chunked_v1:invalid",
            PASSPORT_HEADER_NAME: token,
        },
        **EXPECTATIONS,
    )

    assert identity is not None
    assert identity.token == token
    assert identity.token_source == "header"


async def test_accepts_an_owner_id_subject_and_deployment_scope(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    token = sign(
        aud="owner:team_123:project:prj_123:environment:production",
        scope="owner:team_123:project:prj_123:deployment:dpl_123",
        sub="owner:team_123:connector:scl_123:principal:user_123",
    )

    identity = await surface.get_identity({PASSPORT_HEADER_NAME: token}, **EXPECTATIONS)

    assert identity is not None
    assert identity.external_subject == "user_123"
    assert identity.owner == PassportOwner(slug="team-slug", id="team_123")
    assert identity.subject == "owner:team_123:connector:scl_123:principal:user_123"
    assert identity.verified is True


async def test_reads_a_realistic_payload(surface: Surface, jwks: respx.Route, sign: Sign) -> None:
    token = sign(REALISTIC_PAYLOAD)

    identity = await surface.get_identity({PASSPORT_HEADER_NAME: token}, **REALISTIC_EXPECTATIONS)

    assert identity is not None
    assert identity.external_subject == "00u_example_user"
    assert identity.owner == PassportOwner(slug="example-team", id="team_example")
    assert identity.project == PassportProject(id="prj_example", name="example-project")
    assert identity.verified is True
    # Claims without a dedicated field are still available.
    assert identity.payload["email_verified"] is True
    assert identity.payload["plan"] == "enterprise"
    assert identity.payload["sid"] == "oauth_example-session"
    assert identity.payload["scope"] == REALISTIC_PAYLOAD["scope"]


# --------------------------------------------------------------------------
# Request tokens are always verified
# --------------------------------------------------------------------------


async def test_verifies_a_request_token_even_when_verify_is_false(
    surface: Surface, jwks: respx.Route
) -> None:
    """`verify=False` only applies to tokens the caller supplied directly."""
    with pytest.raises(PassportVerificationError):
        await surface.get_identity(
            {PASSPORT_HEADER_NAME: unsigned(PAYLOAD)}, verify=False, **EXPECTATIONS
        )


async def test_rejects_a_request_token_signed_by_another_key(
    surface: Surface, jwks: respx.Route, sign: Sign, other_signing_key: rsa.RSAPrivateKey
) -> None:
    token = sign(key=other_signing_key)

    with pytest.raises(PassportVerificationError):
        await surface.get_identity({PASSPORT_HEADER_NAME: token}, **EXPECTATIONS)


async def test_rejects_an_expired_request_token(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    token = sign(iat=1_700_000_000, nbf=1_700_000_000, exp=1_700_043_200)

    with pytest.raises(PassportVerificationError):
        await surface.get_identity({PASSPORT_HEADER_NAME: token}, **EXPECTATIONS)


async def test_rejects_a_vercel_oidc_token_in_the_passport_header(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    """A valid OIDC token is signed by the same key, so the issuer must decide."""
    token = sign(iss="https://oidc.vercel.com/team-slug")

    with pytest.raises(PassportVerificationError):
        await surface.get_identity({PASSPORT_HEADER_NAME: token}, **EXPECTATIONS)


async def test_fails_closed_without_expectations(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    with pytest.raises(PassportVerificationError, match="expected project"):
        await surface.get_identity({PASSPORT_HEADER_NAME: sign()})


async def test_reads_expectations_from_the_environment(
    surface: Surface, jwks: respx.Route, sign: Sign, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_123")
    monkeypatch.setenv("VERCEL_TARGET_ENV", "production")

    identity = await surface.get_identity({PASSPORT_HEADER_NAME: sign()})

    assert identity is not None
    assert identity.verified is True


# --------------------------------------------------------------------------
# Tokens supplied directly
# --------------------------------------------------------------------------


async def test_decodes_an_explicit_token_without_verifying_by_default(surface: Surface) -> None:
    token = unsigned(PAYLOAD)

    identity = await surface.get_identity(token=token)

    assert identity is not None
    assert identity.token_source == "local"
    assert identity.verified is False
    assert identity.external_subject == "user_123"


async def test_verifies_an_explicit_token_when_asked(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    identity = await surface.get_identity(token=sign(), verify=True, **EXPECTATIONS)

    assert identity is not None
    assert identity.token_source == "local"
    assert identity.verified is True


async def test_rejects_an_unverified_explicit_token_when_not_allowed(surface: Surface) -> None:
    with pytest.raises(PassportIdentityError, match="unverified tokens are not allowed"):
        await surface.get_identity(token=unsigned(PAYLOAD), allow_unverified=False)


async def test_reads_a_debugging_token_from_the_environment(
    surface: Surface, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERCEL_PASSPORT_TOKEN", unsigned(PAYLOAD))

    identity = await surface.get_identity()

    assert identity is not None
    assert identity.token_source == "local"
    assert identity.verified is False


@pytest.mark.parametrize("from_env", [False, True], ids=["argument", "environment"])
async def test_rejects_an_unverified_token_on_a_deployment_by_default(
    surface: Surface, monkeypatch: pytest.MonkeyPatch, from_env: bool
) -> None:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_ENV", "production")
    if from_env:
        monkeypatch.setenv("VERCEL_PASSPORT_TOKEN", unsigned(PAYLOAD))

    with pytest.raises(PassportIdentityError, match="pass verify=True") as caught:
        await surface.get_identity(token=None if from_env else unsigned(PAYLOAD))
    assert not isinstance(caught.value, PassportVerificationError)


async def test_accepts_an_unverified_token_on_a_deployment_when_allowed(
    surface: Surface, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_ENV", "production")

    identity = await surface.get_identity(token=unsigned(PAYLOAD), allow_unverified=True)

    assert identity is not None
    assert identity.verified is False


async def test_accepts_an_unverified_token_in_vercel_development(
    surface: Surface, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_ENV", "development")

    identity = await surface.get_identity(token=unsigned(PAYLOAD))

    assert identity is not None
    assert identity.verified is False


async def test_rejects_vercel_oidc_issuer_tokens(surface: Surface) -> None:
    token = unsigned({**PAYLOAD, "iss": "https://oidc.vercel.com/team-slug"})

    with pytest.raises(PassportIdentityError, match="is not https://passport.vercel.com"):
        await surface.get_identity(token=token, verify=False)


async def test_rejects_non_passport_shaped_tokens(surface: Surface) -> None:
    token = unsigned({**PAYLOAD, "sub": "owner:team-slug:project:my-project", "typ": "vercel-oidc"})

    with pytest.raises(PassportIdentityError, match='missing typ "passport"'):
        await surface.get_identity(token=token, verify=False)


@pytest.mark.parametrize("claim", ["sub", "owner", "connector_id", "external_sub", "iss"])
async def test_rejects_a_payload_missing_a_required_claim(surface: Surface, claim: str) -> None:
    claims = {name: value for name, value in PAYLOAD.items() if name != claim}

    with pytest.raises(PassportIdentityError):
        await surface.get_identity(token=unsigned(claims))


@pytest.mark.parametrize("token", ["not-a-jwt", "a..c", "a.!!!.c", "a.bnVsbA.c"])
async def test_rejects_a_malformed_token(surface: Surface, token: str) -> None:
    with pytest.raises(PassportIdentityError):
        await surface.get_identity(token=token)


# --------------------------------------------------------------------------
# Local and development identities
# --------------------------------------------------------------------------


async def test_uses_the_local_identity_set_for_the_context(surface: Surface) -> None:
    with local_identity(PAYLOAD):
        identity = await surface.get_identity()

    assert identity is not None
    assert identity.external_subject == "user_123"
    assert identity.token is None
    assert identity.token_source == "local"
    assert identity.verified is False
    assert await surface.get_identity() is None


async def test_uses_a_development_identity_set_for_the_context(surface: Surface) -> None:
    import warnings

    with warnings.catch_warnings():
        # Set explicitly in code, so it is not worth a warning.
        warnings.simplefilter("error", PassportDevelopmentIdentityWarning)
        with local_identity(PassportDevelopmentIdentity(owner="acme", external_subject="dev")):
            identity = await surface.get_identity()

    assert identity is not None
    assert identity.owner.slug == "acme"
    assert identity.subject == "owner:acme:connector:local:principal:dev"
    assert identity.payload["iss"] == "https://passport.vercel.com/acme"


async def test_nested_local_identities_restore_the_outer_one(surface: Surface) -> None:
    with local_identity(PAYLOAD):
        with local_identity(PassportDevelopmentIdentity(external_subject="inner")):
            inner = await surface.get_identity()
        outer = await surface.get_identity()

    assert inner is not None
    assert inner.external_subject == "inner"
    assert outer is not None
    assert outer.external_subject == "user_123"


async def test_a_local_identity_reaches_tasks_and_threads(surface: Surface) -> None:
    import anyio
    import anyio.to_thread

    from vercel.passport.sync import get_identity as get_identity_sync

    results: list[str] = []

    async def read() -> None:
        identity = await surface.get_identity()
        assert identity is not None
        results.append(identity.external_subject)

    def read_in_thread() -> str:
        identity = get_identity_sync()
        assert identity is not None
        return identity.external_subject

    with local_identity(PAYLOAD):
        async with anyio.create_task_group() as group:
            group.start_soon(read)
        results.append(await anyio.to_thread.run_sync(read_in_thread))

    assert results == ["user_123", "user_123"]


async def test_rejects_a_local_identity_that_is_not_passport_shaped() -> None:
    with pytest.raises(PassportIdentityError, match='missing typ "passport"'):
        with local_identity({**PAYLOAD, "typ": "vercel-oidc"}):
            pass


async def test_uses_a_local_identity_from_the_environment(
    surface: Surface, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json

    monkeypatch.setenv("VERCEL_PASSPORT_IDENTITY", json.dumps(PAYLOAD))

    identity = await surface.get_identity()

    assert identity is not None
    assert identity.token_source == "local"
    assert identity.external_subject == "user_123"


async def test_the_context_identity_takes_precedence_over_the_environment(
    surface: Surface, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json

    monkeypatch.setenv("VERCEL_PASSPORT_IDENTITY", json.dumps(PAYLOAD))
    monkeypatch.setenv("VERCEL_PASSPORT_DEV", "1")

    with local_identity(PassportDevelopmentIdentity(external_subject="from-context")):
        identity = await surface.get_identity()

    assert identity is not None
    assert identity.external_subject == "from-context"


@pytest.mark.parametrize("value", ["{not json", "[1, 2]"])
async def test_rejects_an_invalid_local_identity_from_the_environment(
    surface: Surface, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("VERCEL_PASSPORT_IDENTITY", value)

    with pytest.raises(PassportIdentityError, match="local Passport identity"):
        await surface.get_identity()


async def test_a_request_token_takes_precedence_over_a_local_identity(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    with local_identity(PAYLOAD):
        identity = await surface.get_identity({PASSPORT_HEADER_NAME: sign()}, **EXPECTATIONS)

    assert identity is not None
    assert identity.verified is True


async def test_a_debugging_token_takes_precedence_over_a_local_identity(
    surface: Surface, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERCEL_PASSPORT_TOKEN", unsigned({**PAYLOAD, "external_sub": "token"}))

    with local_identity(PAYLOAD):
        identity = await surface.get_identity()

    assert identity is not None
    assert identity.external_subject == "token"


async def test_has_no_identity_by_default(surface: Surface) -> None:
    """A development identity is opt-in, so local development without one has none."""
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error", PassportDevelopmentIdentityWarning)
        assert await surface.get_identity() is None


@pytest.mark.parametrize("value", ["1", "true", "TRUE"])
async def test_synthesizes_a_development_identity_when_opted_in(
    surface: Surface, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("VERCEL_PASSPORT_DEV", value)

    with pytest.warns(PassportDevelopmentIdentityWarning, match="VERCEL_PASSPORT_DEV is set"):
        identity = await surface.get_identity()

    assert identity is not None
    assert identity.connector_id == "local"
    assert identity.environment == "development"
    assert identity.external_subject == "test-user"
    assert identity.owner == PassportOwner(slug="local", id=None)
    # No project ID locally, so no project: only a project ID identifies one.
    assert identity.project is None
    assert identity.subject == "owner:local:connector:local:principal:test-user"
    assert identity.token is None
    assert identity.token_source == "local"
    assert identity.verified is False
    assert identity.payload["aud"] == "https://vercel.com/local/local/development"
    assert identity.payload["email"] == "test-user@passport.local"
    assert identity.payload["iss"] == "https://passport.vercel.com/local"
    assert identity.payload["name"] == "Test User"


@pytest.mark.parametrize("value", ["0", "false", "yes", ""])
async def test_does_not_synthesize_unless_opted_in(
    surface: Surface, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("VERCEL_PASSPORT_DEV", value)

    assert await surface.get_identity() is None


async def test_warns_about_the_development_identity_once(
    surface: Surface, monkeypatch: pytest.MonkeyPatch
) -> None:
    import warnings

    monkeypatch.setenv("VERCEL_PASSPORT_DEV", "1")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        await surface.get_identity()
        await surface.get_identity()

    assert [w.category for w in caught] == [PassportDevelopmentIdentityWarning]
    # Attributed to the caller, not to the package internals.
    assert caught[0].filename.endswith("conftest.py") or caught[0].filename == __file__


async def test_synthesizes_a_development_identity_from_the_environment(
    surface: Surface, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name, value in {
        "VERCEL_PASSPORT_DEV": "1",
        "VERCEL_PASSPORT_DEV_OWNER": "acme",
        "VERCEL_PASSPORT_DEV_OWNER_ID": "team_acme",
        "VERCEL_PASSPORT_DEV_CONNECTOR_ID": "scl_dev",
        "VERCEL_PASSPORT_DEV_EXTERNAL_ISS": "https://idp.example.com",
        "VERCEL_PASSPORT_DEV_EXTERNAL_SUB": "user_dev",
        "VERCEL_PASSPORT_DEV_PROJECT": "demo",
        "VERCEL_PASSPORT_DEV_PROJECT_ID": "prj_demo",
    }.items():
        monkeypatch.setenv(name, value)

    with pytest.warns(PassportDevelopmentIdentityWarning):
        identity = await surface.get_identity()

    assert identity is not None
    assert identity.connector_id == "scl_dev"
    assert identity.external_issuer == "https://idp.example.com"
    assert identity.external_subject == "user_dev"
    assert identity.owner == PassportOwner(slug="acme", id="team_acme")
    assert identity.project == PassportProject(id="prj_demo", name="demo")
    assert identity.subject == "owner:acme:connector:scl_dev:principal:user_dev"
    assert identity.token_source == "local"
    assert identity.verified is False


async def test_omits_empty_optional_development_claims(
    surface: Surface, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERCEL_PASSPORT_DEV", "1")
    monkeypatch.setenv("VERCEL_PROJECT_ID", "")
    monkeypatch.setenv("VERCEL_PASSPORT_DEV_OWNER_ID", "")
    monkeypatch.setenv("VERCEL_PASSPORT_DEV_EXTERNAL_ISS", "")

    with pytest.warns(PassportDevelopmentIdentityWarning):
        identity = await surface.get_identity()

    assert identity is not None
    assert not {"project_id", "owner_id", "external_iss"} & identity.payload.keys()
    assert identity.owner.id is None
    assert identity.external_issuer is None
    assert identity.project is None


@pytest.mark.parametrize(
    "env",
    [
        {"VERCEL": "1", "VERCEL_ENV": "production"},
        {"VERCEL": "1", "VERCEL_ENV": "preview"},
        {"VERCEL": "1"},
        {"VERCEL_ENV": "production"},
    ],
)
async def test_never_uses_a_local_identity_on_a_deployment(
    surface: Surface, monkeypatch: pytest.MonkeyPatch, env: dict[str, str]
) -> None:
    import json

    for name, value in {
        **env,
        "VERCEL_PASSPORT_DEV": "1",
        "VERCEL_PASSPORT_IDENTITY": json.dumps(PAYLOAD),
    }.items():
        monkeypatch.setenv(name, value)

    assert await surface.get_identity() is None
    with local_identity(PAYLOAD):
        assert await surface.get_identity() is None


async def test_uses_local_identities_on_a_vercel_development_environment(
    surface: Surface, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_ENV", "development")
    monkeypatch.setenv("VERCEL_PASSPORT_DEV", "1")

    with local_identity(PAYLOAD):
        assert await surface.get_identity() is not None
    with pytest.warns(PassportDevelopmentIdentityWarning):
        assert await surface.get_identity() is not None


# --------------------------------------------------------------------------
# Identity values
# --------------------------------------------------------------------------


async def test_repr_never_includes_the_token(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    token = sign(tenant_id="tenant-only-in-payload")

    identity = await surface.get_identity({PASSPORT_HEADER_NAME: token}, **EXPECTATIONS)

    assert token not in repr(identity)
    assert token.split(".")[1] not in repr(identity)
    # Claims without a dedicated field stay out too.
    assert "tenant-only-in-payload" not in repr(identity)
    assert "tenant-only-in-payload" not in str(identity)


async def test_serialization_never_includes_the_token(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    """Returning an identity from an endpoint must not hand the bearer token to the client."""
    token = sign()

    identity = await surface.get_identity({PASSPORT_HEADER_NAME: token}, **EXPECTATIONS)

    assert identity is not None
    assert identity.token == token
    assert "token" not in identity.model_dump()
    assert token not in identity.model_dump_json()


async def test_identity_is_immutable(surface: Surface) -> None:
    with local_identity(PAYLOAD):
        identity = await surface.get_identity()
    assert identity is not None

    with pytest.raises(PassportValidationError, match="frozen"):
        identity.subject = "someone-else"  # type: ignore[misc]
    with pytest.raises(PassportValidationError, match="frozen"):
        identity.payload = {}  # type: ignore[misc]


async def test_identity_payload_is_a_deep_copy_of_the_claims(surface: Surface) -> None:
    claims = {**PAYLOAD, "groups": ["staff"]}

    with local_identity(claims):
        claims["sub"] = "someone-else"
        claims["groups"].append("admin")
        first = await surface.get_identity()
        assert first is not None
        first.payload["groups"].append("root")
        second = await surface.get_identity()

    assert second is not None
    assert first.payload["sub"] == PAYLOAD["sub"]
    assert second.payload["groups"] == ["staff"]
    assert claims["groups"] == ["staff", "admin"]


@pytest.mark.parametrize(
    "value",
    [pytest.param('{"typ": "passport"}', id="string"), pytest.param(None, id="none")],
)
async def test_local_identity_rejects_a_value_that_is_not_an_identity(value: object) -> None:
    with pytest.raises(TypeError, match="PassportDevelopmentIdentity or a mapping"):
        with local_identity(value):  # type: ignore[arg-type]
            pass


async def test_identity_misuse_raises_a_validation_error(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    import pydantic

    identity = await surface.get_identity({PASSPORT_HEADER_NAME: sign()}, **EXPECTATIONS)
    assert identity is not None

    for misuse in (
        lambda: delattr(identity, "token"),
        lambda: PassportIdentity.model_validate({"subject": 1}),
        lambda: PassportIdentity.model_validate_json('{"subject": 1}'),
        lambda: PassportOwner.model_validate({"slug": 1}),
        lambda: PassportOwner(slug=1),  # type: ignore[arg-type]
    ):
        with pytest.raises(PassportValidationError) as caught:
            misuse()
        assert not isinstance(caught.value, pydantic.ValidationError)


async def test_identity_can_be_copied_pickled_serialized_and_hashed(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    import copy
    import json
    import pickle

    identity = await surface.get_identity({PASSPORT_HEADER_NAME: sign()}, **EXPECTATIONS)
    assert identity is not None

    assert pickle.loads(pickle.dumps(identity)) == identity
    assert copy.deepcopy(identity) == identity
    serialized = json.loads(identity.model_dump_json())
    assert serialized["payload"]["sub"] == PAYLOAD["sub"]
    assert "token" not in serialized
    assert serialized["owner"] == {"slug": "team-slug", "id": "team_123"}
    assert serialized["project"] == {"id": "prj_123", "name": "my-project"}
    assert hash(identity) == hash(copy.deepcopy(identity))


async def test_identity_equality_and_hash_ignore_the_payload(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    """`payload` is mutable, so an identity in a set or dict must not depend on it."""
    import copy

    identity = await surface.get_identity({PASSPORT_HEADER_NAME: sign()}, **EXPECTATIONS)
    assert identity is not None
    snapshot = copy.deepcopy(identity)
    members = {identity}

    identity.payload["role"] = "admin"

    assert identity == snapshot
    assert hash(identity) == hash(snapshot)
    assert snapshot in members


async def test_rejects_a_request_that_is_not_a_header_mapping(surface: Surface) -> None:
    request: Any = 42

    with pytest.raises(TypeError, match="request must be a mapping"):
        await surface.get_identity(request)
