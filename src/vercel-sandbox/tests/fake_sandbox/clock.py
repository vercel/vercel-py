"""Virtual time for the fake Sandbox API."""

from __future__ import annotations

from datetime import datetime, timezone

import anyio.lowlevel

_EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


class VirtualClock:
    """Milliseconds since the epoch that only move when something waits.

    The SDK's readiness polling and the fake's command durations both advance
    it, so a test that waits five minutes of sandbox time takes none of the
    wall clock's.
    """

    def __init__(self, start: datetime = _EPOCH) -> None:
        self._now_ms = int(start.timestamp() * 1000)
        self.sleeps: list[float] = []

    def now_ms(self) -> int:
        return self._now_ms

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("time cannot move backwards")
        self._now_ms += round(seconds * 1000)

    def advance_to(self, ms: int) -> None:
        self._now_ms = max(self._now_ms, ms)

    async def sleep(self, seconds: float) -> None:
        """Sleep without suspending, as the sync runtime requires."""
        self.sleeps.append(seconds)
        self.advance(seconds)

    async def async_sleep(self, seconds: float) -> None:
        """Sleep virtually but yield, so concurrent tasks get to run."""
        self.sleeps.append(seconds)
        self.advance(seconds)
        await anyio.lowlevel.checkpoint()


__all__ = ["VirtualClock"]
