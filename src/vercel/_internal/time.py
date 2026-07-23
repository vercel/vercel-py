"""Transitional aliases for time helpers now owned by internal core."""

from vercel.internal.core.time import (
    MILLISECOND,
    SECOND,
    coerce_duration,
    parse_duration,
    parse_duration_seconds,
    parse_required_duration_seconds,
    to_ms_int,
    to_seconds_float,
)

__all__ = [
    "MILLISECOND",
    "SECOND",
    "coerce_duration",
    "parse_duration",
    "parse_duration_seconds",
    "parse_required_duration_seconds",
    "to_ms_int",
    "to_seconds_float",
]
