from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from vercel._internal.sandbox.constants import MIN_SNAPSHOT_EXPIRATION
from vercel._internal.sandbox.time import (
    MILLISECOND,
    coerce_duration,
    to_ms_int,
)

_ZERO_DELTA = timedelta(seconds=0)


@dataclass(eq=False)
class SnapshotExpiration:
    _td: timedelta
    """Snapshot expiration in milliseconds.

    Valid values are ``0`` for no expiration or any value greater than or equal
    to ``86_400_000`` (24 hours).
    """

    def __init__(self, value: int | timedelta):
        normalized_delta = coerce_duration(value, MILLISECOND)
        if normalized_delta != _ZERO_DELTA and normalized_delta < MIN_SNAPSHOT_EXPIRATION:
            raise ValueError(
                "Snapshot expiration must be 0 for no expiration or >= 86400000 milliseconds"
            )
        self._td = normalized_delta

    def __int__(self) -> int:
        return to_ms_int(self._td)

    def __eq__(self, other: object) -> bool:
        match other:
            case SnapshotExpiration():
                return int(self) == int(other)
            case int() if not isinstance(other, bool):
                return int(self) == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(int(self))
