"""The durable-coordination contract every backend satisfies.

A backend supplies two collaborators to the adapter:

- a *driver*: the lifecycle state machine (start/pause/resume, wake tokens,
  ownership, the reconciliation marker), and
- a *job coordinator*: atomic-as-possible job writes coupled to wake rearming.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from contextlib import AbstractContextManager
from datetime import datetime

from .._types import (
    ClaimResult,
    DriverSnapshot,
    FinishResult,
    StartDecision,
    WakeToken,
)

__all__ = ["Backend", "BoundRuntime", "Driver", "JobCoordinator"]


class Driver(Protocol):
    """Durable lifecycle and single-chain coordination."""

    scope: str
    scheduler_id: str
    deployment: str

    def start(
        self,
        now: datetime,
        *,
        idle_timeout_seconds: int | None = None,
    ) -> StartDecision: ...

    def auto_activate(
        self,
        now: datetime,
        *,
        idle_timeout_seconds: int | None = None,
        takeover_allowed: bool = False,
    ) -> StartDecision: ...

    def pause(self, now: datetime) -> bool: ...

    def mark_start_published(self, generation: int, now: datetime) -> None: ...

    def claim_start(
        self,
        generation: int,
        owner: str,
        now: datetime,
    ) -> ClaimResult: ...

    def finish_start(
        self,
        generation: int,
        owner: str,
        next_time: datetime | None,
        now: datetime,
    ) -> FinishResult: ...

    def mark_wake_published(
        self,
        generation: int,
        sequence: int,
        now: datetime,
    ) -> None: ...

    def claim_wake(self, token: WakeToken, owner: str, now: datetime) -> ClaimResult: ...

    def finish_wake(
        self,
        token: WakeToken,
        owner: str,
        next_time: datetime | None,
        now: datetime,
    ) -> FinishResult: ...

    def snapshot(self) -> DriverSnapshot: ...

    def repair_overdue_wake(self, now: datetime) -> WakeToken | bool | None:
        """Demote an overdue published wake back to pending.

        Truthy iff a repair actually happened (the adapter logs it loudly);
        a merely-pending wake is not a repair.
        """
        ...

    def owner_deployment(self) -> str | None: ...

    def reconciled_deployment(self) -> str | None: ...

    def mark_reconciled(self, deployment: str, now: datetime) -> bool: ...

    def renew(self, owner: str, now: datetime) -> bool: ...

    def release(self, owner: str) -> None: ...

    def renewing(self, owner: str) -> AbstractContextManager[None]: ...


class JobCoordinator(Protocol):
    """Job persistence coupled to atomic wake rearming."""

    store: Any
    driver: Any

    def install(self) -> None: ...

    def add_job(self, job: Any) -> None: ...

    def update_job(self, job: Any) -> None: ...

    def remove_job(self, job_id: str) -> None: ...

    def remove_all_jobs(self) -> None: ...

    def get_due_jobs_with_revisions(
        self,
        now: datetime,
    ) -> list[tuple[Any, int]]: ...

    def get_all_jobs_with_revisions(
        self,
    ) -> tuple[list[tuple[Any, int, str]], list[tuple[str, int, str]]]: ...

    def cas_update_job(self, job: Any, expected_revision: int) -> bool: ...

    def cas_remove_job(self, job_id: str, expected_revision: int) -> bool: ...

    def quarantine_job(self, job_id: str) -> None: ...


class BoundRuntime(Protocol):
    """What a backend hands the adapter after binding to a scheduler."""

    driver: Driver
    coordinator: JobCoordinator


@runtime_checkable
class Backend(Protocol):
    """Selects and constructs the durable substrate for one scheduler."""

    name: str

    def validate_configuration(self, scheduler: Any) -> dict[str, Any]:
        """Check the scheduler's job stores fit this backend; return them.

        Raises ``APSchedulerConfigurationError`` with a backend-specific
        message otherwise (e.g. Redis requires a default ``RedisJobStore``;
        cache mode rejects an explicitly configured Redis store).
        """
        ...

    def supports_store(self, store: Any) -> bool:
        """Whether ``store`` is one of this backend's durable store types."""
        ...

    def identity_ready(self, scheduler: Any) -> bool:
        """Whether a durable identity can already be derived for ``scheduler``."""
        ...

    def derive_identity(self, scheduler: Any) -> Any:
        """Derive the durable scheduler identity, or raise a configuration error."""
        ...

    def bind(
        self,
        adapter: Any,
        *,
        scope: str,
        deployment: str,
    ) -> BoundRuntime:
        """Namespace the stores and construct driver + coordinator."""
        ...
