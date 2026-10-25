"""`require_identity`: reading the current request's identity, requiring a token.

Mirrors `requireIdentity`, added in `@vercel/passport` 1.2.0, which calls
`getIdentity` and rejects a missing identity or one without a token.
"""

from collections.abc import Callable

import pytest
from conftest import EXPECTATIONS, PAYLOAD, Surface, chunked_cookies, cookie_header, unsigned

import vendor.respx as respx
from vercel.headers import set_headers
from vercel.passport import (
    PASSPORT_HEADER_NAME,
    PassportDevelopmentIdentityWarning,
    PassportIdentityError,
    PassportVerificationError,
)

Sign = Callable[..., str]


async def test_returns_a_verified_identity_from_the_request(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    token = sign()

    identity = await surface.require_identity({PASSPORT_HEADER_NAME: token}, **EXPECTATIONS)

    assert identity.token == token
    assert identity.token_source == "header"
    assert identity.verified is True


async def test_reads_the_runtime_request_context(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    token = sign(padding="x" * 4000)
    set_headers({"cookie": cookie_header(chunked_cookies(token))})

    identity = await surface.require_identity(**EXPECTATIONS)

    assert identity.token == token
    assert identity.token_source == "cookie"


async def test_returns_an_unverified_explicit_token_locally(surface: Surface) -> None:
    token = unsigned(PAYLOAD)

    identity = await surface.require_identity(token=token)

    assert identity.token == token
    assert identity.verified is False


async def test_rejects_a_missing_identity(surface: Surface) -> None:
    with pytest.raises(PassportIdentityError, match="token was not found") as caught:
        await surface.require_identity({}, development=False)
    assert not isinstance(caught.value, PassportVerificationError)


async def test_rejects_a_development_identity(surface: Surface) -> None:
    """A synthesized identity has no token, so it never satisfies the requirement."""
    with (
        pytest.warns(PassportDevelopmentIdentityWarning),
        pytest.raises(PassportIdentityError, match="token was not found"),
    ):
        await surface.require_identity({})


async def test_rejects_a_local_identity(surface: Surface) -> None:
    with pytest.raises(PassportIdentityError, match="token was not found"):
        await surface.require_identity({}, local_identity=PAYLOAD)


async def test_rejects_a_local_identity_from_the_environment(
    surface: Surface, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json

    monkeypatch.setenv("VERCEL_PASSPORT_IDENTITY", json.dumps(PAYLOAD))

    with pytest.raises(PassportIdentityError, match="token was not found"):
        await surface.require_identity({})


async def test_propagates_verification_failures(
    surface: Surface, jwks: respx.Route, sign: Sign
) -> None:
    with pytest.raises(PassportVerificationError):
        await surface.require_identity(
            {PASSPORT_HEADER_NAME: sign(project_id="prj_other")}, **EXPECTATIONS
        )


async def test_attributes_the_development_warning_to_the_caller(surface: Surface) -> None:
    import warnings

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(PassportIdentityError):
            await surface.require_identity({})

    assert [w.category for w in caught] == [PassportDevelopmentIdentityWarning]
    assert caught[0].filename.endswith("conftest.py") or caught[0].filename == __file__
