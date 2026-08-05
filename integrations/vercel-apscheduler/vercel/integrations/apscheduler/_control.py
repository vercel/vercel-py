"""Queue-borne lifecycle control messages.

The cache backend's lifecycle flags are only as visible as the cache is —
per-process under ``vercel dev``, per-region in deployments. A ``pause()``
therefore also rides the queue: a small control message on the start topic,
applied to the local driver document by whichever subscriber processes it.
Redelivery is harmless (pausing a paused chain is a no-op), so these carry
no idempotency key. Resume needs no counterpart: it publishes a regular
start message whose new generation is adopted by ``claim_start``.
"""

from __future__ import annotations

from typing import Any

from dataclasses import dataclass
from datetime import datetime

from ._payload import _envelope
from ._time import as_utc

LIFECYCLE_KIND = "apscheduler.lifecycle"
LIFECYCLE_VERSION = 1
LIFECYCLE_ACTIONS = frozenset({"pause"})

__all__ = ["LIFECYCLE_ACTIONS", "LifecyclePayload"]


@dataclass(frozen=True, slots=True)
class LifecyclePayload:
    scheduler_id: str
    action: str
    issued_at: datetime

    def __post_init__(self) -> None:
        if not self.scheduler_id:
            raise ValueError("scheduler_id must be non-empty")
        if self.action not in LIFECYCLE_ACTIONS:
            raise ValueError(f"action must be one of {sorted(LIFECYCLE_ACTIONS)}")
        object.__setattr__(
            self,
            "issued_at",
            as_utc(self.issued_at, name="issued_at"),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "vercel": {"kind": LIFECYCLE_KIND, "version": LIFECYCLE_VERSION},
            "scheduler_id": self.scheduler_id,
            "action": self.action,
            "issued_at": self.issued_at.isoformat(),
        }

    @classmethod
    def from_payload(cls, payload: Any) -> LifecyclePayload:
        envelope = _envelope(payload, kind=LIFECYCLE_KIND, version=LIFECYCLE_VERSION)
        scheduler_id = envelope.get("scheduler_id")
        if not isinstance(scheduler_id, str) or not scheduler_id:
            raise ValueError("Invalid lifecycle payload: missing scheduler_id")
        action = envelope.get("action")
        if action not in LIFECYCLE_ACTIONS:
            raise ValueError("Invalid lifecycle payload: unsupported action")
        issued_at_raw = envelope.get("issued_at")
        if not isinstance(issued_at_raw, str) or not issued_at_raw:
            raise ValueError("Invalid lifecycle payload: missing issued_at")
        try:
            issued_at = datetime.fromisoformat(issued_at_raw)
        except ValueError as exc:
            raise ValueError("Invalid lifecycle payload: issued_at must be ISO-8601") from exc
        return cls(scheduler_id=scheduler_id, action=action, issued_at=issued_at)
