from __future__ import annotations

from datetime import datetime, timedelta, timezone

MILLISECOND = timedelta(milliseconds=1)
SECOND = timedelta(seconds=1)


def coerce_duration(value: int | float | timedelta, unit: timedelta) -> timedelta:
    match value:
        case bool():
            raise TypeError("duration must be an int, float, or timedelta")
        case timedelta():
            return value
        case int() | float():
            return value * unit
        case _:
            raise TypeError("duration must be an int, float, or timedelta")


def parse_duration(value: object, unit: timedelta) -> timedelta | None:
    match value:
        case None:
            return None
        case bool():
            raise TypeError("duration must be an int, float, timedelta, or None")
        case int() | float() | timedelta():
            return coerce_duration(value, unit)
        case _:
            raise TypeError("duration must be an int, float, timedelta, or None")


def parse_duration_seconds(value: object) -> timedelta | None:
    return parse_duration(value, SECOND)


def parse_required_duration_seconds(value: object) -> timedelta:
    duration = parse_duration_seconds(value)
    if duration is None:
        raise TypeError("duration is required")
    return duration


def to_ms_int(td: timedelta) -> int:
    return td // MILLISECOND


def to_seconds_float(td: timedelta) -> float:
    return td / SECOND


def from_epoch_ms(value: int | float) -> datetime:
    """Read a millisecond epoch timestamp as an aware UTC datetime."""
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def from_epoch_seconds(value: int | float) -> datetime:
    """Read a second epoch timestamp as an aware UTC datetime."""
    return datetime.fromtimestamp(value, tz=timezone.utc)


def parse_epoch_seconds(value: object) -> datetime | None:
    """Read a second epoch timestamp that a server may have omitted or mistyped."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return from_epoch_seconds(value)


def parse_epoch_ms(value: object) -> datetime | None:
    """Read a millisecond epoch timestamp that a server may have omitted."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return from_epoch_ms(value)
