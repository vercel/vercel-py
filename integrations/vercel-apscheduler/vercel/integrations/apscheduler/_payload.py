"""Stable Queue payloads for the durable APScheduler driver."""

from __future__ import annotations

from typing import Any

from dataclasses import dataclass
from datetime import datetime

from ._driver import WakeToken
from ._time import as_utc

START_KIND = "apscheduler.start"
START_VERSION = 1
WAKEUP_KIND = "apscheduler.wakeup"
WAKEUP_VERSION = 1

__all__ = ["StartPayload", "WakeupPayload"]


@dataclass(frozen=True, slots=True)
class StartPayload:
    scheduler_id: str
    generation: int

    def __post_init__(self) -> None:
        if not self.scheduler_id:
            raise ValueError("scheduler_id must be non-empty")
        if self.generation < 1:
            raise ValueError("generation must be greater than or equal to 1")

    def to_payload(self) -> dict[str, Any]:
        return {
            "vercel": {"kind": START_KIND, "version": START_VERSION},
            "scheduler_id": self.scheduler_id,
            "generation": self.generation,
        }

    @classmethod
    def from_payload(cls, payload: Any) -> StartPayload:
        envelope = _envelope(payload, kind=START_KIND, version=START_VERSION)
        scheduler_id = envelope.get("scheduler_id")
        if not isinstance(scheduler_id, str) or not scheduler_id:
            raise ValueError("Invalid start payload: missing scheduler_id")
        generation = _positive_int(envelope.get("generation"), name="generation")
        return cls(scheduler_id=scheduler_id, generation=generation)


@dataclass(frozen=True, slots=True)
class WakeupPayload:
    scheduler_id: str
    generation: int
    sequence: int
    logical_time: datetime

    def __post_init__(self) -> None:
        if not self.scheduler_id:
            raise ValueError("scheduler_id must be non-empty")
        if self.generation < 1:
            raise ValueError("generation must be greater than or equal to 1")
        if self.sequence < 1:
            raise ValueError("sequence must be greater than or equal to 1")
        object.__setattr__(
            self,
            "logical_time",
            as_utc(self.logical_time, name="logical_time"),
        )

    @classmethod
    def from_token(cls, scheduler_id: str, token: WakeToken) -> WakeupPayload:
        return cls(
            scheduler_id=scheduler_id,
            generation=token.generation,
            sequence=token.sequence,
            logical_time=token.logical_time,
        )

    def to_token(self) -> WakeToken:
        return WakeToken(
            generation=self.generation,
            sequence=self.sequence,
            logical_time=self.logical_time,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "vercel": {"kind": WAKEUP_KIND, "version": WAKEUP_VERSION},
            "scheduler_id": self.scheduler_id,
            "generation": self.generation,
            "sequence": self.sequence,
            "logical_time": self.logical_time.isoformat(),
        }

    @classmethod
    def from_payload(cls, payload: Any) -> WakeupPayload:
        envelope = _envelope(payload, kind=WAKEUP_KIND, version=WAKEUP_VERSION)
        scheduler_id = envelope.get("scheduler_id")
        if not isinstance(scheduler_id, str) or not scheduler_id:
            raise ValueError("Invalid wakeup payload: missing scheduler_id")
        logical_time_raw = envelope.get("logical_time")
        if not isinstance(logical_time_raw, str) or not logical_time_raw:
            raise ValueError("Invalid wakeup payload: missing logical_time")
        try:
            logical_time = datetime.fromisoformat(logical_time_raw)
        except ValueError as exc:
            raise ValueError("Invalid wakeup payload: logical_time must be ISO-8601") from exc
        return cls(
            scheduler_id=scheduler_id,
            generation=_positive_int(
                envelope.get("generation"),
                name="generation",
            ),
            sequence=_positive_int(envelope.get("sequence"), name="sequence"),
            logical_time=logical_time,
        )


def _envelope(payload: Any, *, kind: str, version: int) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Invalid APScheduler payload: expected object")
    vercel_info = payload.get("vercel")
    if not isinstance(vercel_info, dict) or vercel_info.get("kind") != kind:
        raise ValueError("Invalid APScheduler payload: unexpected envelope kind")
    if int(vercel_info.get("version", 0)) != version:
        raise ValueError("Invalid APScheduler payload: unsupported version")
    return payload


def _positive_int(value: Any, *, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"Invalid APScheduler payload: {name} must be positive")
    return value
