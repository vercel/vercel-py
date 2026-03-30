from __future__ import annotations

from datetime import timedelta


def normalize_duration_ms(value: int | timedelta | None) -> int | None:
    match value:
        case None:
            return None
        case timedelta():
            return int(value.total_seconds() * 1000)
        case _:
            return int(value)
