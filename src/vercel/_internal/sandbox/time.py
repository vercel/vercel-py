from __future__ import annotations

from datetime import timedelta

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


def to_ms_int(td: timedelta) -> int:
    return td // MILLISECOND


def to_seconds_float(td: timedelta) -> float:
    return td / SECOND
