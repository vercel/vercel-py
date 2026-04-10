from __future__ import annotations

from datetime import timedelta
from typing import Final

from vercel._internal.sandbox.time import MILLISECOND, coerce_duration

MIN_SNAPSHOT_EXPIRATION_MS: Final[int] = 86_400_000


class SnapshotExpiration(int):
    """Snapshot expiration in milliseconds.

    Valid values are ``0`` for no expiration or any value greater than or equal
    to ``86_400_000`` (24 hours).
    """

    def __new__(cls, value: int | timedelta) -> SnapshotExpiration:
        normalized_delta = coerce_duration(value, MILLISECOND)
        normalized_value = normalized_delta // MILLISECOND
        if normalized_value != 0 and normalized_value < MIN_SNAPSHOT_EXPIRATION_MS:
            raise ValueError(
                "Snapshot expiration must be 0 for no expiration or >= 86400000 milliseconds"
            )
        return int.__new__(cls, normalized_value)
