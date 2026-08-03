"""Property tests for cache-key canonicalization."""

from typing import Any

from hypothesis import given, settings, strategies as st

from vercel.connect import (
    ConnectAppTokenSubject,
    ConnectGitHubAppInstallationAuthorizationDetail,
    ConnectTokenExchangeSubject,
    ConnectUserTokenSubject,
)
from vercel.connect._internal.cache import build_cache_key
from vercel.connect._internal.state import ConnectTokenRequest

scopes = st.lists(st.text(min_size=1, max_size=8), max_size=4)
identifiers = st.text(min_size=1, max_size=12)


def request(connector: str, **overrides: Any) -> ConnectTokenRequest:
    return ConnectTokenRequest(connector=connector, subject=ConnectAppTokenSubject(), **overrides)


@settings(max_examples=50, deadline=None)
@given(
    connector=identifiers,
    token=identifiers,
    scope_list=scopes,
    installation=st.one_of(st.none(), identifiers),
)
def test_permuted_sequence_order_still_identifies_one_token(
    connector: str,
    token: str,
    scope_list: list[str],
    installation: str | None,
) -> None:
    """Scope order is not part of a token's identity."""
    first = request(connector, scopes=list(scope_list), installation_id=installation)
    second = request(connector, scopes=list(reversed(scope_list)), installation_id=installation)

    assert build_cache_key(first, identity=token) == build_cache_key(second, identity=token)


@settings(max_examples=50, deadline=None)
@given(connector=identifiers, token=identifiers, user=identifiers)
def test_distinct_subjects_never_collide(connector: str, token: str, user: str) -> None:
    subjects = (
        ConnectAppTokenSubject(),
        ConnectUserTokenSubject(id=user),
        ConnectTokenExchangeSubject(token=user),
    )
    keys = {
        build_cache_key(ConnectTokenRequest(connector=connector, subject=subject), identity=token)
        for subject in subjects
    }

    assert len(keys) == len(subjects)


@settings(max_examples=50, deadline=None)
@given(connector=identifiers, identity_one=identifiers, identity_two=identifiers)
def test_identity_is_always_part_of_the_key(
    connector: str, identity_one: str, identity_two: str
) -> None:
    first = build_cache_key(request(connector), identity=identity_one)
    second = build_cache_key(request(connector), identity=identity_two)

    assert (first == second) is (identity_one == identity_two)


# One distinguishable value per request field. Anything reaching the wire has to
# partition the cache, so a new field must be added here too.
_FIELD_VALUES: dict[str, tuple[Any, Any]] = {
    "connector": ("slack/my-bot", "slack/other-bot"),
    "subject": (ConnectAppTokenSubject(), ConnectUserTokenSubject(id="u_1")),
    "scopes": (None, ["chat:write"]),
    "installation_id": (None, "T1"),
    "audience": (None, ["https://api.example.com"]),
    "resources": (None, ["https://api.example.com/orders"]),
    "authorization_details": (
        None,
        [ConnectGitHubAppInstallationAuthorizationDetail(org="acme")],
    ),
}


def test_every_request_field_participates_in_the_key() -> None:
    """A field that changes the minted credential must change the key.

    Generic over the dataclass, so adding a field to `ConnectTokenRequest` without
    keying it fails here rather than silently colliding two different requests.
    """
    assert set(ConnectTokenRequest.model_fields) == set(_FIELD_VALUES), (
        "every request field needs a value pair here"
    )

    baseline = ConnectTokenRequest(**{name: pair[0] for name, pair in _FIELD_VALUES.items()})
    for name, (_, changed) in _FIELD_VALUES.items():
        changed_request = baseline.model_copy(update={name: changed})
        assert build_cache_key(changed_request, identity="identity") != build_cache_key(
            baseline, identity="identity"
        ), f"{name} does not reach the key"
