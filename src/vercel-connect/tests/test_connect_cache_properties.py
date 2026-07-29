"""Property tests for cache-key canonicalization."""

from hypothesis import given, settings, strategies as st

from vercel.connect import (
    ConnectAppTokenSubject,
    ConnectTokenExchangeSubject,
    ConnectUserTokenSubject,
)
from vercel.connect._internal.cache import build_cache_key

scopes = st.lists(st.text(min_size=1, max_size=8), max_size=4)
identifiers = st.text(min_size=1, max_size=12)


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
    first = build_cache_key(
        connector,
        subject=ConnectAppTokenSubject(),
        vercel_token=token,
        scopes=list(scope_list),
        installation_id=installation,
    )
    second = build_cache_key(
        connector,
        subject=ConnectAppTokenSubject(),
        vercel_token=token,
        scopes=list(reversed(scope_list)),
        installation_id=installation,
    )

    assert first == second


@settings(max_examples=50, deadline=None)
@given(connector=identifiers, token=identifiers, user_one=identifiers, user_two=identifiers)
def test_distinct_subjects_never_collide(
    connector: str,
    token: str,
    user_one: str,
    user_two: str,
) -> None:
    keys = {
        build_cache_key(connector, subject=ConnectAppTokenSubject(), vercel_token=token),
        build_cache_key(
            connector, subject=ConnectUserTokenSubject(id=user_one), vercel_token=token
        ),
        build_cache_key(
            connector, subject=ConnectTokenExchangeSubject(token=user_one), vercel_token=token
        ),
    }

    expected = 3 if user_one != user_two else 3
    assert len(keys) == expected


@settings(max_examples=50, deadline=None)
@given(connector=identifiers, identity_one=identifiers, identity_two=identifiers)
def test_identity_is_always_part_of_the_key(
    connector: str,
    identity_one: str,
    identity_two: str,
) -> None:
    first = build_cache_key(connector, subject=ConnectAppTokenSubject(), vercel_token=identity_one)
    second = build_cache_key(connector, subject=ConnectAppTokenSubject(), vercel_token=identity_two)

    assert (first == second) is (identity_one == identity_two)
