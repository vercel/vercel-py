from datetime import timedelta

import pytest

from vercel.sandbox._internal.models import (
    SnapshotExpiration,
    _parse_snapshot_expiration,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, timedelta(0)),
        (86400, timedelta(days=1)),
        (86400.5, timedelta(days=1, microseconds=500000)),
        (timedelta(days=1), timedelta(days=1)),
        (timedelta(days=365 * 10), timedelta(days=365 * 10)),
    ],
)
def test_snapshot_expiration_accepts_valid_values(
    value: int | float | timedelta, expected: timedelta
) -> None:
    assert SnapshotExpiration(value).value == expected


def test_snapshot_expiration_parser_preserves_wrapper() -> None:
    expiration = SnapshotExpiration(timedelta(days=1))

    assert _parse_snapshot_expiration(expiration) is expiration


@pytest.mark.parametrize(
    "value",
    [
        -1,
        1,
        timedelta(microseconds=1),
        timedelta(days=365 * 10, microseconds=1),
    ],
)
def test_snapshot_expiration_rejects_out_of_range_values(
    value: int | timedelta,
) -> None:
    with pytest.raises(ValueError):
        SnapshotExpiration(value)


@pytest.mark.parametrize("value", [True, "86400", object()])
def test_snapshot_expiration_rejects_unsupported_values(value: object) -> None:
    with pytest.raises(TypeError):
        _parse_snapshot_expiration(value)
