"""`PassportDevelopmentIdentity`: explicit values over environment over defaults."""

import pickle

import pytest

from vercel.passport import PassportDevelopmentIdentity, PassportValidationError


def test_uses_placeholder_defaults() -> None:
    identity = PassportDevelopmentIdentity()

    assert identity.owner == "local"
    assert identity.owner_id is None
    assert identity.connector_id == "local"
    assert identity.external_subject == "test-user"
    assert identity.environment == "development"
    assert identity.project == "local"
    assert identity.project_id is None
    assert identity.issuer == "https://passport.vercel.com/local"
    assert identity.audience == "https://vercel.com/local/local/development"


def test_reads_the_environment_beneath_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL_PASSPORT_DEV_OWNER", "acme")
    monkeypatch.setenv("VERCEL_PASSPORT_DEV_OWNER_ID", "team_acme")
    monkeypatch.setenv("VERCEL_PASSPORT_DEV_PROJECT", "demo")

    identity = PassportDevelopmentIdentity()

    assert identity.owner == "acme"
    assert identity.owner_id == "team_acme"
    # Derived defaults follow the resolved fields, wherever they came from.
    assert identity.issuer == "https://passport.vercel.com/acme"
    assert identity.audience == "https://vercel.com/acme/demo/development"


def test_explicit_values_take_precedence_over_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL_PASSPORT_DEV_OWNER", "from-env")

    identity = PassportDevelopmentIdentity(owner="explicit")

    assert identity.owner == "explicit"
    assert identity.issuer == "https://passport.vercel.com/explicit"


def test_an_explicit_none_clears_an_environment_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL_PASSPORT_DEV_OWNER_ID", "team_acme")
    monkeypatch.setenv("VERCEL_PASSPORT_DEV_PROJECT_ID", "prj_env")

    identity = PassportDevelopmentIdentity(owner_id=None, project_id=None)

    assert identity.owner_id is None
    assert identity.project_id is None
    assert not {"owner_id", "project_id"} & identity.to_claims().keys()


def test_falls_back_to_the_deployment_project_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_deployment")

    assert PassportDevelopmentIdentity().project_id == "prj_deployment"

    monkeypatch.setenv("VERCEL_PASSPORT_DEV_PROJECT_ID", "prj_dev")
    assert PassportDevelopmentIdentity().project_id == "prj_dev"


def test_ignores_empty_environment_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL_PASSPORT_DEV_OWNER", "")
    monkeypatch.setenv("VERCEL_PASSPORT_DEV_EXTERNAL_ISS", "")

    identity = PassportDevelopmentIdentity()

    assert identity.owner == "local"
    assert "external_iss" not in identity.to_claims()


def test_model_validate_resolves_the_same_way(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL_PASSPORT_DEV_OWNER", "from-env")
    monkeypatch.setenv("VERCEL_PASSPORT_DEV_PROJECT", "demo")

    identity = PassportDevelopmentIdentity.model_validate({"owner": "explicit"})

    assert identity.owner == "explicit"
    assert identity.project == "demo"


def test_renders_passport_claims() -> None:
    identity = PassportDevelopmentIdentity(
        owner="acme", external_subject="user_dev", external_issuer="https://idp"
    )

    assert identity.to_claims() == {
        "aud": "https://vercel.com/acme/local/development",
        "connector_id": "local",
        "email": "test-user@passport.local",
        "environment": "development",
        "external_iss": "https://idp",
        "external_sub": "user_dev",
        "iss": "https://passport.vercel.com/acme",
        "name": "Test User",
        "owner": "acme",
        "project": "local",
        "scope": "owner:acme:connector:local:principal:user_dev",
        "sub": "owner:acme:connector:local:principal:user_dev",
        "typ": "passport",
    }


def test_rejects_an_unknown_field() -> None:
    with pytest.raises(PassportValidationError, match="ownr"):
        PassportDevelopmentIdentity(ownr="acme")  # type: ignore[call-arg]


@pytest.mark.parametrize(
    "values",
    [
        pytest.param({"owner": 42}, id="owner"),
        pytest.param({"owner": None}, id="owner none"),
        pytest.param({"project": 42}, id="project"),
        pytest.param({"environment": ["x"]}, id="environment"),
    ],
)
def test_rejects_a_value_of_the_wrong_type(values: dict[str, object]) -> None:
    """Including the fields the derived defaults read, which pydantic before 2.12
    still passes to the default factories after they fail validation."""
    import pydantic

    with pytest.raises(PassportValidationError) as caught:
        PassportDevelopmentIdentity(**values)  # type: ignore[arg-type]
    assert not isinstance(caught.value, pydantic.ValidationError)


def test_model_validate_reports_errors_as_validation_errors() -> None:
    import pydantic

    with pytest.raises(PassportValidationError, match="ownr") as caught:
        PassportDevelopmentIdentity.model_validate({"ownr": "acme"})
    assert not isinstance(caught.value, pydantic.ValidationError)


def test_omits_an_explicitly_empty_optional_claim() -> None:
    identity = PassportDevelopmentIdentity(project_id="", owner_id="", external_issuer="")

    assert not {"project_id", "owner_id", "external_iss"} & identity.to_claims().keys()


def test_is_frozen_and_picklable() -> None:
    identity = PassportDevelopmentIdentity(owner="acme")

    with pytest.raises(PassportValidationError, match="frozen"):
        identity.owner = "other"  # type: ignore[misc]
    assert pickle.loads(pickle.dumps(identity)) == identity
