"""`PassportIdentity`: the Passport rules apply however an identity is built."""

from typing import Any

import pydantic
import pytest
from conftest import PAYLOAD

from vercel.passport import (
    PassportIdentity,
    PassportIdentityError,
    PassportOwner,
    PassportProject,
    PassportValidationError,
)


def test_derives_its_fields_from_the_claims() -> None:
    identity = PassportIdentity(payload=PAYLOAD, token="t", token_source="header", verified=True)

    assert identity.subject == PAYLOAD["sub"]
    assert identity.external_subject == "user_123"
    assert identity.connector_id == "scl_123"
    assert identity.owner == PassportOwner(slug="team-slug", id="team_123")
    assert identity.project == PassportProject(id="prj_123", name="my-project")
    assert identity.email == "user@example.com"
    assert identity.environment == "production"
    assert identity.token == "t"


@pytest.mark.parametrize(
    ("claims", "message"),
    [
        pytest.param({**PAYLOAD, "typ": "vercel-oidc"}, 'missing typ "passport"', id="typ"),
        pytest.param(
            {**PAYLOAD, "iss": "https://oidc.vercel.com/team-slug"},
            "is not https://passport.vercel.com",
            id="issuer",
        ),
        pytest.param(
            {k: v for k, v in PAYLOAD.items() if k != "connector_id"}, "connector_id", id="claim"
        ),
    ],
)
def test_constructing_directly_enforces_the_passport_shape(
    claims: dict[str, object], message: str
) -> None:
    with pytest.raises(PassportIdentityError, match=message):
        PassportIdentity(payload=claims, token_source="local", verified=False)


def test_model_validate_enforces_the_passport_shape() -> None:
    with pytest.raises(PassportIdentityError, match='missing typ "passport"'):
        PassportIdentity.model_validate(
            {"payload": {**PAYLOAD, "typ": "x"}, "token_source": "local", "verified": False}
        )


@pytest.mark.parametrize(
    "field",
    [
        pytest.param({"subject": "someone-else"}, id="subject"),
        pytest.param({"owner": PassportOwner(slug="other")}, id="owner model"),
        pytest.param({"owner": {"slug": "other"}}, id="owner mapping"),
        pytest.param({"project": None}, id="project"),
    ],
)
def test_rejects_a_field_that_contradicts_the_claims(field: dict[str, Any]) -> None:
    with pytest.raises(
        PassportValidationError, match="does not match the identity's claims"
    ) as caught:
        PassportIdentity(payload=PAYLOAD, token_source="local", verified=False, **field)
    assert not isinstance(caught.value, pydantic.ValidationError)


def test_accepts_fields_that_match_the_claims() -> None:
    identity = PassportIdentity(
        payload=PAYLOAD,
        token_source="local",
        verified=False,
        subject=PAYLOAD["sub"],
        owner=PassportOwner(slug="team-slug", id="team_123"),
    )

    assert identity.subject == PAYLOAD["sub"]


def test_round_trips_through_model_dump() -> None:
    identity = PassportIdentity(payload=PAYLOAD, token_source="header", verified=True)

    assert PassportIdentity.model_validate(identity.model_dump()) == identity
    assert PassportIdentity.model_validate_json(identity.model_dump_json()) == identity


def test_requires_the_claims() -> None:
    with pytest.raises(PassportValidationError, match="payload: Field required"):
        PassportIdentity.model_validate({"token_source": "local", "verified": False})
