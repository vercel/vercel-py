"""Tests for sandbox time helpers."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
from hypothesis import given, strategies as st

from vercel._internal.sandbox.models import CreateSandboxRequest
from vercel._internal.sandbox.time import (
    MILLISECOND,
    SECOND,
    coerce_duration,
    parse_duration,
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


def test_create_sandbox_request_preserves_timedelta_until_serialization() -> None:
    request = CreateSandboxRequest(project_id="prj_test123", timeout=timedelta(minutes=10))

    assert request.timeout == timedelta(minutes=10)
    assert request.model_dump(by_alias=True, exclude_none=True)["timeout"] == 600_000
