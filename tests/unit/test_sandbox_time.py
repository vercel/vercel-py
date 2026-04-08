"""Tests for sandbox time helpers."""

from __future__ import annotations

from datetime import timedelta

import pytest
from hypothesis import given, strategies as st

from vercel._internal.sandbox.time import normalize_duration_ms

MAX_DURATION_MS = timedelta.max // timedelta(milliseconds=1)
MIN_DURATION_MS = timedelta.min // timedelta(milliseconds=1)


@given(st.one_of(st.none(), st.integers()))
def test_normalize_duration_ms_preserves_none_and_ints(value: int | None) -> None:
    assert normalize_duration_ms(value) == value


@given(st.integers(min_value=MIN_DURATION_MS, max_value=MAX_DURATION_MS))
def test_normalize_duration_ms_matches_equivalent_milliseconds(value: int) -> None:
    assert normalize_duration_ms(timedelta(milliseconds=value)) == normalize_duration_ms(value)


@given(
    st.one_of(
        st.floats(),
        st.text(),
        st.binary(),
        st.lists(st.integers()),
        st.dictionaries(st.text(), st.integers()),
        st.tuples(st.integers(), st.integers()),
    )
)
def test_normalize_duration_ms_rejects_unsupported_values(value: object) -> None:
    with pytest.raises(TypeError, match="duration must be an int, timedelta, or None"):
        normalize_duration_ms(value)  # type: ignore[arg-type]
