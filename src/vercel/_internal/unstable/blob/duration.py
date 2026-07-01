"""Duration parsing helpers for unstable Blob API arguments."""

from datetime import timedelta
from typing import TypeAlias

from vercel._internal.time import SECOND

DurationInput: TypeAlias = int | float | timedelta | None


def _coerce_duration(value: int | float | timedelta, unit: timedelta, *, name: str) -> timedelta:
    match value:
        case bool():
            raise TypeError(f"{name} must be an int, float, or timedelta")
        case timedelta():
            return value
        case int() | float():
            return value * unit
        case _:
            raise TypeError(f"{name} must be an int, float, or timedelta")


def parse_duration_seconds(value: object, *, name: str = "duration") -> timedelta | None:
    match value:
        case None:
            return None
        case bool():
            raise TypeError(f"{name} must be an int, float, timedelta, or None")
        case int() | float() | timedelta():
            return _coerce_duration(value, SECOND, name=name)
        case _:
            raise TypeError(f"{name} must be an int, float, timedelta, or None")


def parse_required_duration_seconds(value: object, *, name: str = "duration") -> timedelta:
    duration = parse_duration_seconds(value, name=name)
    if duration is None:
        raise TypeError(f"{name} is required")
    return duration
