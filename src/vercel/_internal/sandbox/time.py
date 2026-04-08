from __future__ import annotations

from datetime import timedelta


def normalize_duration_ms(value: int | timedelta | None) -> int | None:
    match value:
        case None:
            return None
        case int():
            return value
        case timedelta():
            return value // timedelta(milliseconds=1)
        case _:
            raise TypeError("duration must be an int, timedelta, or None")
