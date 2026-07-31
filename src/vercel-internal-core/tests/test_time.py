"""Tests for time helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from hypothesis import given, strategies as st

from vercel._internal.core.time import (
    MILLISECOND,
    SECOND,
    coerce_duration,
    from_epoch_ms,
    from_epoch_seconds,
    parse_duration,
    parse_duration_seconds,
    parse_epoch_ms,
    parse_epoch_seconds,
    parse_required_duration_seconds,
    to_ms_int,
)

MAX_DURATION_MS = to_ms_int(timedelta.max)
MIN_DURATION_MS = to_ms_int(timedelta.min)


@pytest.mark.parametrize(
    ("unit", "primitive", "equivalent_delta"),
    [
        pytest.param(
            MILLISECOND,
            st.integers(min_value=MIN_DURATION_MS, max_value=MAX_DURATION_MS),
            lambda value: timedelta(milliseconds=value),
            id="milliseconds",
        ),
        pytest.param(
            SECOND,
            st.floats(
                min_value=-1_000_000.0,
                max_value=1_000_000.0,
                allow_nan=False,
                allow_infinity=False,
            ),
            lambda value: timedelta(seconds=value),
            id="seconds",
        ),
    ],
)
def test_coerce_duration_matches_equivalent_timedelta(
    unit: timedelta,
    primitive: st.SearchStrategy[Any],
    equivalent_delta: Any,
) -> None:
    @given(primitive)
    def run(value: int | float) -> None:
        assert coerce_duration(value, unit) == equivalent_delta(value)

    run()


@given(
    st.one_of(
        st.text(),
        st.binary(),
        st.lists(st.integers()),
        st.dictionaries(st.text(), st.integers()),
        st.tuples(st.integers(), st.integers()),
    )
)
def test_coerce_duration_rejects_unsupported_values(value: object) -> None:
    with pytest.raises(TypeError, match="duration must be an int, float, or timedelta"):
        coerce_duration(value, MILLISECOND)  # type: ignore[arg-type]


@given(
    st.booleans(),
)
def test_coerce_duration_rejects_bool_values(value: bool) -> None:
    with pytest.raises(TypeError, match="duration must be an int, float, or timedelta"):
        coerce_duration(value, SECOND)  # type: ignore[arg-type]


@given(st.none())
def test_parse_duration_preserves_none(value: None) -> None:
    assert parse_duration(value, MILLISECOND) is None


@pytest.mark.parametrize(
    ("unit", "primitive", "equivalent_delta"),
    [
        pytest.param(
            MILLISECOND,
            st.integers(min_value=MIN_DURATION_MS, max_value=MAX_DURATION_MS),
            lambda value: timedelta(milliseconds=value),
            id="milliseconds",
        ),
        pytest.param(
            SECOND,
            st.floats(
                min_value=-1_000_000.0,
                max_value=1_000_000.0,
                allow_nan=False,
                allow_infinity=False,
            ),
            lambda value: timedelta(seconds=value),
            id="seconds",
        ),
    ],
)
def test_parse_duration_matches_equivalent_timedelta(
    unit: timedelta,
    primitive: st.SearchStrategy[Any],
    equivalent_delta: Any,
) -> None:
    @given(primitive)
    def run(value: int | float) -> None:
        assert parse_duration(value, unit) == equivalent_delta(value)

    run()


@given(
    st.one_of(
        st.text(),
        st.binary(),
        st.lists(st.integers()),
        st.dictionaries(st.text(), st.integers()),
        st.tuples(st.integers(), st.integers()),
    )
)
def test_parse_duration_rejects_unsupported_values(value: object) -> None:
    with pytest.raises(TypeError, match="duration must be an int, float, timedelta, or None"):
        parse_duration(value, MILLISECOND)


@given(st.booleans())
def test_parse_duration_rejects_bool_values(value: bool) -> None:
    with pytest.raises(TypeError, match="duration must be an int, float, timedelta, or None"):
        parse_duration(value, SECOND)


def test_parse_duration_seconds_uses_seconds_for_numeric_values() -> None:
    assert parse_duration_seconds(1.5) == timedelta(seconds=1.5)


def test_parse_required_duration_seconds_rejects_none() -> None:
    with pytest.raises(TypeError, match="duration is required"):
        parse_required_duration_seconds(None)


EPOCH_SECONDS = 1_800_000_000
EPOCH = datetime(2027, 1, 15, 8, 0, tzinfo=timezone.utc)


def test_from_epoch_reads_utc() -> None:
    assert from_epoch_seconds(EPOCH_SECONDS) == EPOCH
    assert from_epoch_ms(EPOCH_SECONDS * 1000) == EPOCH


def test_from_epoch_keeps_sub_second_precision() -> None:
    assert from_epoch_ms(EPOCH_SECONDS * 1000 + 250) == EPOCH + timedelta(milliseconds=250)


@pytest.mark.parametrize("parse", [parse_epoch_seconds, parse_epoch_ms])
@pytest.mark.parametrize("value", [None, "1800000000", True, False, [], {}, object()])
def test_parse_epoch_returns_none_for_anything_but_a_number(parse: Any, value: object) -> None:
    """A server may omit a timestamp or send the wrong type; neither should raise.

    `bool` is excluded deliberately: it is an `int`, so `True` would otherwise read
    as one second past the epoch.
    """
    assert parse(value) is None


def test_parse_epoch_reads_numbers_in_its_own_unit() -> None:
    assert parse_epoch_seconds(EPOCH_SECONDS) == EPOCH
    assert parse_epoch_ms(EPOCH_SECONDS * 1000) == EPOCH
