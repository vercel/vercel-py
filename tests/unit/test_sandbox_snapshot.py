"""Tests for snapshot helpers."""

from __future__ import annotations

import pytest

from vercel.sandbox import MIN_SNAPSHOT_EXPIRATION_MS, SnapshotExpiration
from vercel.sandbox.snapshot import normalize_snapshot_expiration


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

    def test_normalize_coerces_int(self) -> None:
        expiration = normalize_snapshot_expiration(MIN_SNAPSHOT_EXPIRATION_MS)

        assert expiration == SnapshotExpiration(MIN_SNAPSHOT_EXPIRATION_MS)
        assert isinstance(expiration, SnapshotExpiration)
