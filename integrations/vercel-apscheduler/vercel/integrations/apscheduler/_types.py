"""Backend-neutral durable-contract types.

These are the vocabulary the adapter, subscriber, and payloads speak with
whichever backend coordinates the chain; nothing here knows how state is
stored.
"""

from __future__ import annotations

from typing import Literal

from dataclasses import dataclass
from datetime import datetime

from ._time import as_utc

# Long enough that a merely slow delivery or an in-progress retry cycle is
# never declared dead, short enough that a stranded chain heals within one
# activation sweep.
WAKE_REPAIR_GRACE_SECONDS = 10 * 60

PROVENANCE_DECLARED = "declared"
PROVENANCE_RUNTIME = "runtime"

ClaimState = Literal["claimed", "busy", "stale"]
FinishState = Literal["advanced", "fenced", "lost"]
LifecycleState = Literal["running", "paused", "inactive"]

__all__ = [
    "PROVENANCE_DECLARED",
    "PROVENANCE_RUNTIME",
    "WAKE_REPAIR_GRACE_SECONDS",
    "APSchedulerConfigurationError",
    "ClaimResult",
    "ClaimState",
    "DriverSnapshot",
    "FinishResult",
    "FinishState",
    "LifecycleState",
    "NamespaceFencedError",
    "StartDecision",
    "WakeToken",
]


class APSchedulerConfigurationError(RuntimeError):
    """Raised when a scheduler cannot satisfy the Vercel runtime contract."""


class NamespaceFencedError(APSchedulerConfigurationError):
    """Raised when a write is refused because another deployment owns the chain."""


@dataclass(frozen=True, slots=True)
class WakeToken:
    """The one wake message currently allowed to advance a scheduler."""

    generation: int
    sequence: int
    logical_time: datetime
    status: str = "pending"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "logical_time",
            as_utc(self.logical_time, name="logical_time"),
        )


@dataclass(frozen=True, slots=True)
class StartDecision:
    """Atomic result of starting or resuming a scheduler."""

    generation: int
    changed: bool
    start_status: str
    current_wake: WakeToken | None
    state: LifecycleState = "running"
    # False when another deployment owns the chain and the caller was not
    # allowed to take it; every action besides serving must then be skipped.
    owned: bool = True


@dataclass(frozen=True, slots=True)
class ClaimResult:
    """Result of trying to own a start or wake delivery."""

    state: ClaimState
    activation_time: datetime | None = None

    def __post_init__(self) -> None:
        if self.activation_time is not None:
            object.__setattr__(
                self,
                "activation_time",
                as_utc(self.activation_time, name="activation_time"),
            )


@dataclass(frozen=True, slots=True)
class FinishResult:
    """Atomic outcome of completing a claimed start or wake."""

    state: FinishState
    wake: WakeToken | None = None


@dataclass(frozen=True, slots=True)
class DriverSnapshot:
    """Current durable driver state."""

    state: LifecycleState
    generation: int
    start_status: str | None
    current_wake: WakeToken | None
