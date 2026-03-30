"""Tests for snapshot helpers."""

from __future__ import annotations

from datetime import timedelta

import pytest
from hypothesis import given, strategies as st

from vercel.sandbox import MIN_SNAPSHOT_EXPIRATION_MS, SnapshotExpiration


class TestSnapshotExpiration:
    def test_allows_zero(self) -> None:
        expiration = SnapshotExpiration(0)

        assert expiration == 0

    def test_allows_minimum(self) -> None:
        expiration = SnapshotExpiration(MIN_SNAPSHOT_EXPIRATION_MS)

        assert expiration == MIN_SNAPSHOT_EXPIRATION_MS

    def test_rejects_values_below_minimum(self) -> None:
        with pytest.raises(ValueError, match="0 for no expiration or >= 86400000"):
            SnapshotExpiration(MIN_SNAPSHOT_EXPIRATION_MS - 1)

    def test_rejects_timedelta_below_minimum(self) -> None:
        with pytest.raises(ValueError, match="0 for no expiration or >= 86400000"):
            SnapshotExpiration(timedelta(milliseconds=MIN_SNAPSHOT_EXPIRATION_MS - 1))

    @given(
        st.one_of(
            st.just(0),
            st.integers(
                min_value=MIN_SNAPSHOT_EXPIRATION_MS,
                max_value=365 * MIN_SNAPSHOT_EXPIRATION_MS,
            ),
        )
    )
    def test_roundtrips_valid_inputs(self, milliseconds: int) -> None:
        int_expiration = SnapshotExpiration(milliseconds)
        timedelta_expiration = SnapshotExpiration(timedelta(milliseconds=milliseconds))

        assert int_expiration == SnapshotExpiration(milliseconds)
        assert isinstance(int_expiration, SnapshotExpiration)
        assert timedelta_expiration == SnapshotExpiration(milliseconds)
        assert isinstance(timedelta_expiration, SnapshotExpiration)

    @given(
        st.integers(
            min_value=-(365 * MIN_SNAPSHOT_EXPIRATION_MS),
            max_value=MIN_SNAPSHOT_EXPIRATION_MS - 1,
        ).filter(lambda value: value != 0)
    )
    def test_rejects_invalid_inputs(self, milliseconds: int) -> None:
        with pytest.raises(ValueError, match="0 for no expiration or >= 86400000"):
            SnapshotExpiration(milliseconds)

        with pytest.raises(ValueError, match="0 for no expiration or >= 86400000"):
            SnapshotExpiration(timedelta(milliseconds=milliseconds))
