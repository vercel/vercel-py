from __future__ import annotations

from datetime import timedelta
from typing import Final

from vercel._internal.sandbox.time import normalize_duration_ms

MIN_SNAPSHOT_EXPIRATION_MS: Final[int] = 86_400_000


class SnapshotExpiration(int):
    """Snapshot expiration in milliseconds.

    Valid values are ``0`` for no expiration or any value greater than or equal
    to ``86_400_000`` (24 hours).
    """

    def __new__(cls, value: int | timedelta) -> SnapshotExpiration:
        normalized_value = normalize_duration_ms(value)
        if normalized_value is None:
            raise TypeError("Snapshot expiration cannot be None")
        if normalized_value != 0 and normalized_value < MIN_SNAPSHOT_EXPIRATION_MS:
            raise ValueError(
                "Snapshot expiration must be 0 for no expiration or >= 86400000 milliseconds"
            )
        return int.__new__(cls, normalized_value)
