from __future__ import annotations

from typing import Any, Literal

from dataclasses import dataclass, field
from datetime import datetime

from ._time import as_utc

WAKEUP_KIND = "apscheduler.wakeup"
WAKEUP_VERSION = 2

CursorState = Literal["scheduled", "paused", "finished"]

__all__ = [
    "WAKEUP_KIND",
    "WAKEUP_VERSION",
    "CursorEntry",
    "MemoryCursor",
    "WakeupPayload",
]


@dataclass(frozen=True, slots=True)
class CursorEntry:
    job_id: str
    fingerprint: str
    state: CursorState
    next_run_time: datetime | None = None

    def __post_init__(self) -> None:
        if self.state == "scheduled" and self.next_run_time is None:
            raise ValueError("scheduled cursor entry requires next_run_time")
        if self.next_run_time is not None:
            object.__setattr__(
                self,
                "next_run_time",
                as_utc(self.next_run_time, name="next_run_time"),
            )

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "job_id": self.job_id,
            "fingerprint": self.fingerprint,
            "state": self.state,
        }
        if self.next_run_time is not None:
            payload["next_run_time"] = self.next_run_time.isoformat()
        return payload

    @classmethod
    def from_payload(cls, payload: Any) -> CursorEntry:
        if not isinstance(payload, dict):
            raise ValueError("cursor entry must be an object")

        job_id = payload.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("cursor entry missing job_id")

        fingerprint = payload.get("fingerprint")
        if not isinstance(fingerprint, str) or not fingerprint:
            raise ValueError("cursor entry missing fingerprint")

        state = payload.get("state")
        if state not in {"scheduled", "paused", "finished"}:
            raise ValueError("cursor entry has invalid state")

        next_run_time: datetime | None = None
        next_run_time_raw = payload.get("next_run_time")
        if next_run_time_raw is not None:
            if not isinstance(next_run_time_raw, str):
                raise ValueError("cursor entry next_run_time must be a string")
            try:
                next_run_time = datetime.fromisoformat(next_run_time_raw)
            except ValueError as exc:
                raise ValueError("cursor entry next_run_time must be ISO-8601") from exc

        return cls(
            job_id=job_id,
            fingerprint=fingerprint,
            state=state,
            next_run_time=next_run_time,
        )


@dataclass(frozen=True, slots=True)
class MemoryCursor:
    jobs: dict[str, CursorEntry] = field(default_factory=dict)
    version: int = 1

    def to_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "jobs": {key: entry.to_payload() for key, entry in self.jobs.items()},
        }

    @classmethod
    def empty(cls) -> MemoryCursor:
        return cls()

    @classmethod
    def from_payload(cls, payload: Any) -> MemoryCursor:
        if payload is None:
            return cls.empty()
        if not isinstance(payload, dict):
            raise ValueError("cursor must be an object")
        if int(payload.get("version", 0)) != 1:
            raise ValueError("cursor has unsupported version")

        raw_jobs = payload.get("jobs", {})
        if not isinstance(raw_jobs, dict):
            raise ValueError("cursor jobs must be an object")

        jobs: dict[str, CursorEntry] = {}
        for key, raw_entry in raw_jobs.items():
            if not isinstance(key, str) or not key:
                raise ValueError("cursor job keys must be non-empty strings")
            jobs[key] = CursorEntry.from_payload(raw_entry)
        return cls(jobs=jobs)


@dataclass(frozen=True, slots=True)
class WakeupPayload:
    scheduler_id: str
    logical_time: datetime
    cursor: MemoryCursor = field(default_factory=MemoryCursor.empty)
    kind: str = "tick"

    def __post_init__(self) -> None:
        if not self.scheduler_id:
            raise ValueError("scheduler_id must be non-empty")
        object.__setattr__(self, "logical_time", as_utc(self.logical_time, name="logical_time"))

    def to_payload(self) -> dict[str, Any]:
        return {
            "vercel": {"kind": WAKEUP_KIND, "version": WAKEUP_VERSION},
            "scheduler_id": self.scheduler_id,
            "logical_time": self.logical_time.isoformat(),
            "kind": self.kind,
            "cursor": self.cursor.to_payload(),
        }

    @classmethod
    def from_payload(cls, payload: Any) -> WakeupPayload:
        if not isinstance(payload, dict):
            raise ValueError("Invalid wakeup payload: expected object")

        vercel_info = payload.get("vercel")
        if not isinstance(vercel_info, dict) or vercel_info.get("kind") != WAKEUP_KIND:
            raise ValueError("Invalid wakeup payload: not an APScheduler wakeup envelope")

        if int(vercel_info.get("version", 0)) != WAKEUP_VERSION:
            raise ValueError("Invalid wakeup payload: unsupported version")

        scheduler_id = payload.get("scheduler_id")
        if not isinstance(scheduler_id, str) or not scheduler_id:
            raise ValueError("Invalid wakeup payload: missing scheduler_id")

        logical_time_raw = payload.get("logical_time")
        if not isinstance(logical_time_raw, str) or not logical_time_raw:
            raise ValueError("Invalid wakeup payload: missing logical_time")

        try:
            logical_time = datetime.fromisoformat(logical_time_raw)
        except ValueError as exc:
            raise ValueError("Invalid wakeup payload: logical_time must be ISO-8601") from exc

        kind = payload.get("kind", "tick")
        if not isinstance(kind, str) or not kind:
            raise ValueError("Invalid wakeup payload: missing kind")

        return cls(
            scheduler_id=scheduler_id,
            logical_time=logical_time,
            kind=kind,
            cursor=MemoryCursor.from_payload(payload.get("cursor")),
        )
