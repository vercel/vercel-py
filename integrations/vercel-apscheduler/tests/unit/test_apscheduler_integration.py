from __future__ import annotations

from typing import Any, cast

import logging
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from itertools import starmap
from operator import itemgetter
from sys import modules
from threading import Lock
from types import ModuleType
from unittest.mock import patch

import pytest
from apscheduler.job import Job
from apscheduler.jobstores.base import BaseJobStore, ConflictingIdError, JobLookupError
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import STATE_PAUSED, STATE_RUNNING
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from vercel.integrations.apscheduler import (
    APSchedulerConfigurationError,
    _adapter as _adapter_module,
    _subscriber,
    install_vercel_apscheduler_integration,
    is_scheduler_subscriber,
)
from vercel.integrations.apscheduler._adapter import SchedulerAdapter, get_adapter
from vercel.integrations.apscheduler._backends.cache import CacheJobStore
from vercel.integrations.apscheduler._backends.redis import RedisJobCoordinator
from vercel.integrations.apscheduler._options import VercelAPSchedulerOptions
from vercel.integrations.apscheduler._payload import StartPayload, WakeupPayload
from vercel.integrations.apscheduler._subscriber import register_scheduler
from vercel.integrations.apscheduler._types import (
    PROVENANCE_DECLARED,
    PROVENANCE_RUNTIME,
    ClaimResult,
    DriverSnapshot,
    FinishResult,
    StartDecision,
    WakeToken,
)
from vercel.queue import (
    Message,
    MessageMetadata,
    RetryAfter,
    SanitizedName,
    get_subscriptions,
)
from vercel.queue.testing import clear_subscriptions

UTC = timezone.utc
TEST_SCHEDULER_MODULE = "test_scheduler"
# Identity derives from the store's jobs_key ("apscheduler.jobs" by default).
TEST_SCHEDULER_ID = "apscheduler-jobs"


def durable_noop_job() -> None:
    return


class InMemoryRedisJobStore(RedisJobStore):
    def __init__(self, jobs_key: str = "apscheduler.jobs") -> None:
        self.jobs_key = jobs_key
        self.run_times_key = "apscheduler.run_times"
        self.redis = object()
        self.jobs: dict[str, Any] = {}
        self._scheduler = None
        self._alias = None

    def lookup_job(self, job_id: str) -> Any | None:
        return self.jobs.get(job_id)

    def get_due_jobs(self, now: datetime) -> list[Any]:
        return sorted(
            (
                job
                for job in self.jobs.values()
                if job.next_run_time is not None and job.next_run_time <= now
            ),
            key=lambda job: job.next_run_time,
        )

    def get_next_run_time(self) -> datetime | None:
        times = [job.next_run_time for job in self.jobs.values() if job.next_run_time is not None]
        return min(times) if times else None

    def get_all_jobs(self) -> list[Any]:
        return sorted(
            self.jobs.values(),
            key=lambda job: job.next_run_time or datetime.max.replace(tzinfo=UTC),
        )

    def add_job(self, job: Any) -> None:
        if job.id in self.jobs:
            raise ConflictingIdError(job.id)
        self.jobs[job.id] = job

    def update_job(self, job: Any) -> None:
        if job.id not in self.jobs:
            raise JobLookupError(job.id)
        self.jobs[job.id] = job

    def remove_job(self, job_id: str) -> None:
        if job_id not in self.jobs:
            raise JobLookupError(job_id)
        del self.jobs[job_id]

    def remove_all_jobs(self) -> None:
        self.jobs.clear()

    def shutdown(self) -> None:
        return


class InMemorySourceJobStore(BaseJobStore):
    """A source store: external rows become one-shot date jobs
    and ``remove_job`` doubles as the dispatch step."""

    def __init__(self) -> None:
        super().__init__()
        self.rows: dict[str, datetime] = {}
        self.dispatched: list[str] = []
        self.updated: list[str] = []
        self.fail_reads = False
        self.executor_alias = "default"
        self.trigger_factory: Any = lambda run_date: DateTrigger(run_date, UTC)

    def lookup_job(self, job_id: str) -> Any | None:
        return None

    def get_due_jobs(self, now: datetime) -> list[Any]:
        self._check_reads()
        return [
            self._materialize(row_id, run_date)
            for row_id, run_date in sorted(self.rows.items(), key=itemgetter(1))
            if run_date <= now
        ]

    def get_next_run_time(self) -> datetime | None:
        self._check_reads()
        return min(self.rows.values(), default=None)

    def get_all_jobs(self) -> list[Any]:
        self._check_reads()
        rows: list[tuple[str, datetime]] = sorted(self.rows.items(), key=itemgetter(1))
        return list(starmap(self._materialize, rows))

    def add_job(self, job: Any) -> None:
        raise RuntimeError("This job store does not support managing jobs directly.")

    def update_job(self, job: Any) -> None:
        self.rows[job.id] = job.next_run_time
        self.updated.append(job.id)

    def remove_job(self, job_id: str) -> None:
        del self.rows[job_id]
        self.dispatched.append(job_id)

    def remove_all_jobs(self) -> None:
        raise RuntimeError("This job store does not support managing jobs directly.")

    def _check_reads(self) -> None:
        if self.fail_reads:
            raise ConnectionError("source database unavailable")

    def _materialize(self, row_id: str, run_date: datetime) -> Any:
        job_kwargs = {
            **(self._scheduler._job_defaults if self._scheduler else {}),
            "trigger": self.trigger_factory(run_date),
            "executor": self.executor_alias,
            "func": lambda: None,
            "args": (),
            "kwargs": {},
            "id": row_id,
            "name": None,
            "next_run_time": run_date,
            "misfire_grace_time": None,
        }
        return Job(self._scheduler, **job_kwargs)


class FakeDriver:
    def __init__(self, deployment: str = "dpl_test") -> None:
        self.lock = Lock()
        self.deployment = deployment
        self.state = "paused"
        self.generation = 0
        self.start_status: str | None = None
        self.activation_time: datetime | None = None
        self.current: WakeToken | None = None
        self.last_sequence = 0
        self.owner: str | None = None
        self.owner_deployment_value: str | None = None
        self.reconciled: str | None = None

    def owner_deployment(self) -> str | None:
        return self.owner_deployment_value

    def start(
        self,
        now: datetime,
        *,
        idle_timeout_seconds: int | None = None,
    ) -> StartDecision:
        del now, idle_timeout_seconds
        with self.lock:
            takeover = (
                self.owner_deployment_value is not None
                and self.owner_deployment_value != self.deployment
            )
            changed = self.state != "running" or takeover
            if changed:
                self.state = "running"
                self.generation += 1
                self.start_status = "pending"
                self.activation_time = None
                self.current = None
                self.last_sequence = 0
            self.owner_deployment_value = self.deployment
            return StartDecision(
                generation=self.generation,
                changed=changed,
                start_status=self.start_status or "",
                current_wake=self.current,
            )

    def auto_activate(
        self,
        now: datetime,
        *,
        idle_timeout_seconds: int | None = None,
        takeover_allowed: bool = False,
    ) -> StartDecision:
        del now, idle_timeout_seconds
        with self.lock:
            foreign = (
                self.owner_deployment_value is not None
                and self.owner_deployment_value != self.deployment
            )
            explicitly_paused = self.state == "paused" and self.generation > 0
            if foreign and (not takeover_allowed or explicitly_paused):
                return StartDecision(
                    generation=self.generation,
                    changed=False,
                    start_status=self.start_status or "",
                    current_wake=self.current,
                    state="paused" if explicitly_paused else "running",
                    owned=False,
                )
            if foreign or (not explicitly_paused and self.state != "running"):
                self.state = "running"
                self.generation += 1
                self.start_status = "pending"
                self.activation_time = None
                self.current = None
                self.last_sequence = 0
                changed = True
            else:
                changed = False
            self.owner_deployment_value = self.deployment
            return StartDecision(
                generation=self.generation,
                changed=changed,
                start_status=self.start_status or "",
                current_wake=self.current,
                state="paused" if explicitly_paused else "running",
            )

    def pause(self, now: datetime) -> bool:
        del now
        with self.lock:
            changed = self.state == "running"
            self.state = "paused"
            return changed

    def reconciled_deployment(self) -> str | None:
        return self.reconciled

    def mark_reconciled(self, deployment: str, now: datetime) -> bool:
        del now
        if self.owner_deployment_value != deployment:
            return False
        self.reconciled = deployment
        return True

    def repair_overdue_wake(self, now: datetime, *, grace_seconds: int = 600) -> bool:
        with self.lock:
            if (
                self.state != "running"
                or self.current is None
                or self.current.status not in {"published", "processing"}
                or self.owner is not None
                or self.current.logical_time >= now - timedelta(seconds=grace_seconds)
            ):
                return False
            self.current = WakeToken(
                self.current.generation,
                self.current.sequence,
                self.current.logical_time,
            )
            return True

    def mark_start_published(self, generation: int, now: datetime) -> None:
        del now
        with self.lock:
            if (
                self.state == "running"
                and generation == self.generation
                and self.start_status == "pending"
            ):
                self.start_status = "published"

    def claim_start(
        self,
        generation: int,
        owner: str,
        now: datetime,
    ) -> ClaimResult:
        with self.lock:
            if (
                self.state != "running"
                or generation != self.generation
                or self.start_status == "active"
            ):
                return ClaimResult("stale")
            if self.owner is not None and self.owner != owner:
                return ClaimResult("busy")
            self.owner = owner
            self.start_status = "processing"
            if self.activation_time is None:
                self.activation_time = now
            return ClaimResult("claimed", self.activation_time)

    def finish_start(
        self,
        generation: int,
        owner: str,
        next_logical_time: datetime | None,
        now: datetime,
    ) -> FinishResult:
        del now
        with self.lock:
            if self.owner != owner:
                return FinishResult("lost")
            if self.state != "running" or generation != self.generation:
                self.owner = None
                return FinishResult("fenced")
            self.owner = None
            self.start_status = "active"
            self.current = (
                WakeToken(generation, 1, next_logical_time)
                if next_logical_time is not None
                else None
            )
            self.last_sequence = 1 if self.current is not None else 0
            return FinishResult("advanced", self.current)

    def mark_wake_published(
        self,
        generation: int,
        sequence: int,
        now: datetime,
    ) -> None:
        del now
        with self.lock:
            if (
                self.current is not None
                and self.current.generation == generation
                and self.current.sequence == sequence
            ):
                self.current = WakeToken(
                    generation,
                    sequence,
                    self.current.logical_time,
                    status="published",
                )

    def claim_wake(
        self,
        token: WakeToken,
        owner: str,
        now: datetime,
    ) -> ClaimResult:
        del now
        with self.lock:
            if (
                self.state != "running"
                or self.current is None
                or token.generation != self.generation
                or token.sequence != self.current.sequence
                or token.logical_time != self.current.logical_time
            ):
                return ClaimResult("stale")
            if self.owner is not None and self.owner != owner:
                return ClaimResult("busy")
            self.owner = owner
            return ClaimResult("claimed")

    def finish_wake(
        self,
        token: WakeToken,
        owner: str,
        next_logical_time: datetime | None,
        now: datetime,
    ) -> FinishResult:
        del now
        with self.lock:
            if self.owner != owner:
                return FinishResult("lost")
            if (
                self.state != "running"
                or token.generation != self.generation
                or self.current is None
                or token.sequence != self.current.sequence
            ):
                self.owner = None
                return FinishResult("fenced")
            self.owner = None
            self.current = (
                WakeToken(
                    self.generation,
                    token.sequence + 1,
                    next_logical_time,
                )
                if next_logical_time is not None
                else None
            )
            self.last_sequence = (
                self.current.sequence if self.current is not None else token.sequence
            )
            return FinishResult("advanced", self.current)

    def rearm_wake(self, candidate: datetime, now: datetime) -> None:
        self.rearm(candidate, now)

    def rearm(self, logical_time: datetime, now: datetime) -> WakeToken | None:
        del now
        with self.lock:
            if self.state != "running" or self.start_status != "active":
                return None
            if self.owner is not None:
                return None
            if self.current is not None and self.current.logical_time <= logical_time:
                return None
            sequence = self.last_sequence + 1
            self.current = WakeToken(self.generation, sequence, logical_time)
            self.last_sequence = sequence
            return self.current

    def snapshot(self) -> DriverSnapshot:
        with self.lock:
            return DriverSnapshot(
                state="running" if self.state == "running" else "paused",
                generation=self.generation,
                start_status=self.start_status,
                current_wake=self.current,
            )

    def release(self, owner: str) -> None:
        with self.lock:
            if self.owner == owner:
                self.owner = None

    def renewing(self, owner: str) -> Any:
        del owner
        return nullcontext()


@pytest.fixture(autouse=True)
def runtime(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_DEPLOYMENT_ID", "dpl_test")
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.delenv("VERCEL_TARGET_ENV", raising=False)
    monkeypatch.delenv("VERCEL_PYTHON_SUBSCRIBER_ID", raising=False)
    monkeypatch.setenv(
        "VERCEL_APSCHEDULER_SUBSCRIBERS",
        (f'[{{"id":"{TEST_SCHEDULER_ID}","entrypoint":"{TEST_SCHEDULER_MODULE}:scheduler"}}]'),
    )
    monkeypatch.delenv("VERCEL_APSCHEDULER_DISCOVERY", raising=False)
    monkeypatch.delenv("VERCEL_SERVICE_TYPE", raising=False)
    monkeypatch.delenv("VERCEL_SERVICE_TRIGGER", raising=False)
    monkeypatch.delenv("VERCEL_DEV_QUEUE_SERVING", raising=False)
    monkeypatch.setitem(modules, TEST_SCHEDULER_MODULE, ModuleType(TEST_SCHEDULER_MODULE))
    _clear_scheduler_registrations()
    install_vercel_apscheduler_integration(register_queues=False)
    yield
    _clear_scheduler_registrations()


def _clear_scheduler_registrations() -> None:
    clear_subscriptions()
    _subscriber._registered_schedulers.clear()
    _subscriber._registered_callbacks.clear()
    _adapter_module._ACTIVE_IDENTITIES.clear()
    # install() latches register_queues once any caller passes True; reset so
    # test ordering cannot change when registration happens.
    _adapter_module._PATCH_STATE.register_queues = False


def bind_test_scheduler(scheduler: BlockingScheduler) -> None:
    modules[TEST_SCHEDULER_MODULE].__dict__["scheduler"] = scheduler


class InMemoryJobCoordinator(RedisJobCoordinator):
    """Mirrors the coordinator's Lua semantics against the in-memory store.

    Versions, provenance, and the original store methods live on the store
    object so a second coordinator (a takeover by another deployment in one
    test process) observes the first one's durable state.
    """

    def __init__(self, store: Any, driver: Any, adapter: Any) -> None:
        super().__init__(store, driver, adapter)
        originals = store.__dict__.setdefault(
            "_test_original_methods",
            {
                "add_job": store.add_job,
                "update_job": store.update_job,
                "remove_job": store.remove_job,
                "remove_all_jobs": store.remove_all_jobs,
            },
        )
        self._original_add_job = originals["add_job"]
        self._original_update_job = originals["update_job"]
        self._original_remove_job = originals["remove_job"]
        self._original_remove_all_jobs = originals["remove_all_jobs"]
        self._state: dict[str, Any] = store.__dict__.setdefault(
            "_test_durable_state",
            {"versions": {}, "provenance": {}, "revision": 0},
        )

    @property
    def _versions(self) -> dict[str, int]:
        return cast("dict[str, int]", self._state["versions"])

    @property
    def _provenance(self) -> dict[str, str]:
        return cast("dict[str, str]", self._state["provenance"])

    def add_job(self, job: Any) -> None:
        try:
            self._original_add_job(job)
        except ConflictingIdError:
            if self.adapter.is_runtime_mutation or self.adapter.is_wake_mutation:
                raise
            return
        self._versions[job.id] = self._next_revision()
        self._provenance[job.id] = (
            PROVENANCE_RUNTIME
            if self.adapter.is_runtime_mutation or self.adapter.is_wake_mutation
            else PROVENANCE_DECLARED
        )
        self._rearm(job, rearm=True)

    def update_job(self, job: Any) -> None:
        self._original_update_job(job)
        self._versions[job.id] = self._next_revision()
        self._rearm(job, rearm=self.adapter.is_runtime_mutation)

    def remove_job(self, job_id: str) -> None:
        self._original_remove_job(job_id)
        self._next_revision()
        self._versions.pop(job_id, None)
        self._provenance.pop(job_id, None)

    def remove_all_jobs(self) -> None:
        self._original_remove_all_jobs()
        self._next_revision()
        self._versions.clear()
        self._provenance.clear()

    def get_due_jobs_with_revisions(self, now: datetime) -> list[tuple[Any, int]]:
        return [(job, self._versions.get(job.id, 0)) for job in self.store.get_due_jobs(now)]

    def get_all_jobs_with_revisions(
        self,
    ) -> tuple[list[tuple[Any, int, str]], list[tuple[str, int, str]]]:
        return (
            [
                (job, self._versions.get(job.id, 0), self._provenance.get(job.id, ""))
                for job in self.store.get_all_jobs()
            ],
            [],
        )

    def cas_update_job(self, job: Any, expected_revision: int) -> bool:
        if self._versions.get(job.id, 0) != expected_revision:
            return False
        self._original_update_job(job)
        self._versions[job.id] = self._next_revision()
        return True

    def cas_remove_job(self, job_id: str, expected_revision: int) -> bool:
        store: Any = self.store
        if job_id not in store.jobs:
            return False
        if self._versions.get(job_id, 0) != expected_revision:
            return False
        self._original_remove_job(job_id)
        self._next_revision()
        self._versions.pop(job_id, None)
        self._provenance.pop(job_id, None)
        return True

    def _next_revision(self) -> int:
        self._state["revision"] += 1
        return cast("int", self._state["revision"])

    def _rearm(self, job: Any, *, rearm: bool) -> None:
        if not rearm:
            return
        driver_owner = self.driver.owner_deployment()
        if driver_owner is not None and driver_owner != self.driver.deployment:
            return
        next_run_time = getattr(job, "next_run_time", None)
        if next_run_time is None:
            return
        driver: Any = self.driver
        driver.rearm(
            self.adapter.canonical_wakeup_time(next_run_time),
            datetime.now(UTC),
        )


def scheduler_with_driver(
    jobs_key: str = "apscheduler.jobs",
) -> tuple[BlockingScheduler, Any, FakeDriver]:
    scheduler = BlockingScheduler(
        timezone=UTC,
        jobstores={"default": InMemoryRedisJobStore(jobs_key)},
    )
    bind_test_scheduler(scheduler)
    adapter = get_adapter(scheduler)
    assert adapter is not None
    with patch(
        "vercel.integrations.apscheduler._backends.redis.RedisJobCoordinator",
        InMemoryJobCoordinator,
    ):
        adapter._bind_runtime()
    driver = FakeDriver()
    adapter._driver = driver  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
    adapter.coordinator.driver = driver
    return scheduler, adapter, driver


def scheduler_with_source_store() -> tuple[
    BlockingScheduler,
    SchedulerAdapter,
    FakeDriver,
    InMemorySourceJobStore,
]:
    scheduler, adapter, driver = scheduler_with_driver()
    source = InMemorySourceJobStore()
    scheduler.add_jobstore(source, "subscription")
    return scheduler, adapter, driver, source


def message(
    payload: dict[str, Any],
    *,
    message_id: str,
    topic: str,
    consumer_group: str,
) -> Message[dict[str, Any]]:
    return Message(
        payload=payload,
        metadata=MessageMetadata(
            message_id=message_id,
            delivery_count=1,
            created_at=datetime.now(UTC),
            topic=topic,
            consumer_group=SanitizedName(consumer_group),
        ),
    )


def test_memory_job_store_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    scheduler = BlockingScheduler(
        timezone=UTC,
        jobstores={"default": MemoryJobStore()},
    )
    bind_test_scheduler(scheduler)

    with pytest.raises(
        APSchedulerConfigurationError,
        match="not suitable for the cache backend",
    ):
        scheduler.start()


def test_redis_backend_requires_a_redis_store(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL_APSCHEDULER_BACKEND", "redis")
    scheduler = BlockingScheduler(timezone=UTC)
    bind_test_scheduler(scheduler)

    with pytest.raises(
        APSchedulerConfigurationError,
        match="default RedisJobStore",
    ):
        scheduler.start()


def test_scheduler_lifecycle_remains_native_outside_vercel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class NativeBackgroundScheduler(BackgroundScheduler):
        pass

    monkeypatch.delenv("VERCEL", raising=False)
    scheduler = NativeBackgroundScheduler(timezone=UTC)
    scheduler.add_job(
        durable_noop_job,
        "interval",
        minutes=5,
        id="native-before-start",
    )

    scheduler.start()
    try:
        assert scheduler.state == STATE_RUNNING
        assert scheduler.get_job("native-before-start") is not None
        scheduler.add_job(
            durable_noop_job,
            "interval",
            minutes=10,
            id="native-after-start",
        )
        assert scheduler.get_job("native-after-start") is not None
        scheduler.pause()
        assert scheduler.state == STATE_PAUSED
        scheduler.resume()
        assert scheduler.state == STATE_RUNNING
    finally:
        scheduler.shutdown()


def test_identity_derives_from_the_job_store_key() -> None:
    """Variable and module renames never move a scheduler's durable identity."""
    _scheduler, adapter, _driver = scheduler_with_driver()

    assert adapter.identity.scheduler_id == TEST_SCHEDULER_ID
    assert adapter.identity.wakeup_topic == f"__aps_{TEST_SCHEDULER_ID}_wakeup"
    assert adapter.identity.start_topic == f"__aps_{TEST_SCHEDULER_ID}_start"
    assert adapter.identity.consumer_group == f"apscheduler-{TEST_SCHEDULER_ID}"


def test_scheduler_id_option_pins_the_identity() -> None:
    scheduler = BlockingScheduler(
        timezone=UTC,
        jobstores={"default": InMemoryRedisJobStore()},
    )
    adapter = SchedulerAdapter(
        scheduler,
        VercelAPSchedulerOptions(scheduler_id="pinned-id"),
    )

    assert adapter.identity.scheduler_id == "pinned-id"


def test_production_state_is_environment_scoped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production namespaces survive promotes; previews stay disposable."""
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_123")
    _scheduler, adapter, _driver = scheduler_with_driver()

    assert adapter.scope == "prj_123:production"
    store = adapter.scheduler._jobstores["default"]
    assert store.jobs_key == ("apscheduler.jobs:{prj_123:production:apscheduler-jobs}:jobs")


def test_custom_environment_state_is_environment_scoped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.setenv("VERCEL_TARGET_ENV", "staging")
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_123")
    _scheduler, adapter, _driver = scheduler_with_driver()

    assert adapter.scope == "prj_123:staging"


def test_named_environment_without_project_id_fails_loudly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A silent project-less scope could interleave two projects' state."""
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.delenv("VERCEL_PROJECT_ID", raising=False)
    scheduler = BlockingScheduler(
        timezone=UTC,
        jobstores={"default": InMemoryRedisJobStore()},
    )
    adapter = get_adapter(scheduler)
    assert adapter is not None

    with pytest.raises(APSchedulerConfigurationError, match="VERCEL_PROJECT_ID"):
        adapter._bind_runtime()


def test_missing_environment_fails_loudly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A silent deployment-scope fallback would fork a chain per deployment."""
    monkeypatch.delenv("VERCEL_ENV", raising=False)
    monkeypatch.delenv("VERCEL_TARGET_ENV", raising=False)
    scheduler = BlockingScheduler(
        timezone=UTC,
        jobstores={"default": InMemoryRedisJobStore()},
    )
    adapter = get_adapter(scheduler)
    assert adapter is not None

    with pytest.raises(APSchedulerConfigurationError, match="VERCEL_ENV"):
        adapter._bind_runtime()


def test_options_reject_limits_the_queue_cannot_serve() -> None:
    """A wake the queue would refuse must fail at import, not at publish."""
    with pytest.raises(ValueError, match="max_delay_seconds"):
        VercelAPSchedulerOptions(max_delay_seconds=6 * 24 * 60 * 60)
    with pytest.raises(ValueError, match="retention_seconds"):
        VercelAPSchedulerOptions(retention_seconds=8 * 24 * 60 * 60)
    with pytest.raises(ValueError, match="must exceed max_delay_seconds"):
        # Raising the delay without also raising retention would publish
        # wakes that expire before they become visible.
        VercelAPSchedulerOptions(max_delay_seconds=3 * 24 * 60 * 60)


def test_unset_misfire_grace_defaults_to_none() -> None:
    """Stock one-second grace is calibrated to in-process wakeup precision.

    Queue delivery rounds wake delays up to whole seconds and adds dispatch
    latency, so keeping the stock default would skip occurrences as misfires
    on routine jitter.
    """
    scheduler = BlockingScheduler(
        timezone=UTC,
        jobstores={"default": InMemoryRedisJobStore()},
    )

    assert scheduler._job_defaults["misfire_grace_time"] is None


def test_explicit_misfire_grace_default_is_preserved() -> None:
    scheduler = BlockingScheduler(
        timezone=UTC,
        job_defaults={"misfire_grace_time": 15},
        jobstores={"default": InMemoryRedisJobStore()},
    )

    assert scheduler._job_defaults["misfire_grace_time"] == 15


def test_gconfig_misfire_grace_default_is_preserved() -> None:
    scheduler = BlockingScheduler(
        {"apscheduler.job_defaults.misfire_grace_time": "7"},
        timezone=UTC,
        jobstores={"default": InMemoryRedisJobStore()},
    )

    assert scheduler._job_defaults["misfire_grace_time"] == 7


def test_misfire_grace_stays_stock_outside_vercel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VERCEL", raising=False)
    scheduler = BlockingScheduler(
        timezone=UTC,
        jobstores={"default": InMemoryRedisJobStore()},
    )

    assert scheduler._job_defaults["misfire_grace_time"] == 1


def test_short_explicit_misfire_grace_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An explicit sub-transport grace is honored but named for what it is."""
    scheduler, adapter, _driver = scheduler_with_driver()
    scheduler.add_job(
        durable_noop_job,
        "interval",
        hours=1,
        id="tight",
        misfire_grace_time=1,
        replace_existing=True,
    )

    with caplog.at_level(logging.WARNING):
        adapter.ensure_local_started()

    assert any("misfire_grace_time=1" in record.getMessage() for record in caplog.records)
    persisted = scheduler.get_job("tight")
    assert persisted is not None
    assert persisted.misfire_grace_time == 1


def test_preview_state_stays_deployment_scoped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.setenv("VERCEL_TARGET_ENV", "preview")
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_123")
    _scheduler, adapter, _driver = scheduler_with_driver()

    assert adapter.scope == "dpl_test"


def test_takeover_reconciles_declared_jobs_and_keeps_dynamic_ones(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A promote syncs code-declared jobs and never touches runtime jobs.

    Environment-scoped production state outlives deployments, so the new
    deployment's first touch must delete undeclared jobs before any wake
    plans them, restart changed schedules, and preserve progress for
    unchanged ones.
    """
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_123")

    first, _first_adapter, driver = scheduler_with_driver()
    first.add_job(durable_noop_job, "interval", hours=1, id="keep", replace_existing=True)
    first.add_job(durable_noop_job, "interval", hours=1, id="gone", replace_existing=True)
    first.add_job(durable_noop_job, "interval", hours=1, id="changed", replace_existing=True)
    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg",
    ):
        first.start()
        first.add_job(
            durable_noop_job,
            "date",
            run_date=datetime.now(UTC) + timedelta(hours=2),
            id="dynamic",
        )
    store: Any = first._jobstores["default"]
    kept_run_time = store.jobs["keep"].next_run_time
    changed_run_time = store.jobs["changed"].next_run_time
    assert driver.reconciled == "dpl_test"

    # A new deployment takes over the shared store and driver.
    _clear_scheduler_registrations()
    monkeypatch.setenv("VERCEL_DEPLOYMENT_ID", "dpl_two")
    second = BlockingScheduler(timezone=UTC, jobstores={"default": store})
    second.add_job(durable_noop_job, "interval", hours=1, id="keep", replace_existing=True)
    second.add_job(durable_noop_job, "interval", hours=2, id="changed", replace_existing=True)
    second_adapter = get_adapter(second)
    assert second_adapter is not None
    with patch(
        "vercel.integrations.apscheduler._backends.redis.RedisJobCoordinator",
        InMemoryJobCoordinator,
    ):
        second_adapter._bind_runtime()
    second_adapter._driver = driver  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
    second_adapter.coordinator.driver = driver
    # The new deployment holds its own driver handle on the shared hash.
    driver.deployment = "dpl_two"
    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg",
    ):
        second.start()

    assert set(store.jobs) == {"keep", "changed", "dynamic"}
    assert store.jobs["keep"].next_run_time == kept_run_time
    assert store.jobs["changed"].next_run_time != changed_run_time
    assert store.jobs["changed"].trigger.interval == timedelta(hours=2)
    assert driver.reconciled == "dpl_two"


def test_implicit_activation_respects_chain_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Deployment-URL traffic never adopts a chain; alias traffic does."""
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_123")
    _scheduler, adapter, driver = scheduler_with_driver()
    driver.owner_deployment_value = "dpl_other"

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg",
    ) as send:
        adapter.auto_activate()

    send.assert_not_called()
    assert driver.owner_deployment_value == "dpl_other"

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg",
    ) as send:
        adapter.auto_activate(takeover_allowed=True)

    send.assert_called_once()
    assert driver.owner_deployment_value == "dpl_test"
    assert driver.reconciled == "dpl_test"


def test_stale_deployment_touches_are_inert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A demoted deployment must not write into the shared namespace.

    Its cold start must not resurrect declarations another version deleted,
    its wake publishes must stay unsent, and its mutation APIs must refuse
    loudly instead of racing the owner.
    """
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_123")

    owner_scheduler, _owner_adapter, driver = scheduler_with_driver()
    owner_scheduler.add_job(
        durable_noop_job,
        "interval",
        hours=1,
        id="kept",
        replace_existing=True,
    )
    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg",
    ):
        owner_scheduler.start()
    store: Any = owner_scheduler._jobstores["default"]
    assert set(store.jobs) == {"kept"}

    # A demoted deployment cold-starts against the shared store: same code
    # age, different declarations ("legacy" was deleted by the owner's code).
    _clear_scheduler_registrations()
    monkeypatch.setenv("VERCEL_DEPLOYMENT_ID", "dpl_stale")
    stale = BlockingScheduler(timezone=UTC, jobstores={"default": store})
    stale.add_job(durable_noop_job, "interval", hours=1, id="legacy", replace_existing=True)
    stale_adapter = get_adapter(stale)
    assert stale_adapter is not None
    with patch(
        "vercel.integrations.apscheduler._backends.redis.RedisJobCoordinator",
        InMemoryJobCoordinator,
    ):
        stale_adapter._bind_runtime()
    stale_adapter._driver = driver  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
    stale_adapter.coordinator.driver = driver
    driver.deployment = "dpl_stale"
    driver.owner_deployment_value = "dpl_test"  # the promoted deployment

    stale_adapter.ensure_local_started()

    assert set(store.jobs) == {"kept"}  # no resurrection of "legacy"
    assert stale_adapter.publish_pending_wakeup() is None
    stale_adapter._lifecycle_called = True
    with pytest.raises(APSchedulerConfigurationError, match="no longer drives"):
        stale_adapter.prepare_runtime_mutation()


def test_two_schedulers_sharing_a_store_key_collide_loudly() -> None:
    """Distinct durable schedulers require distinct jobs_key values.

    Depending on whether the runtime registers queue handlers at
    construction, the collision surfaces at ``__init__`` (failing the build
    during introspection) or at the first identity use; both are loud.
    """
    scheduler_with_driver()

    def build_second_scheduler_and_use_its_identity() -> None:
        second = BlockingScheduler(
            timezone=UTC,
            jobstores={"default": InMemoryRedisJobStore()},
        )
        second_adapter = get_adapter(second)
        assert second_adapter is not None
        _ = second_adapter.identity

    with pytest.raises(APSchedulerConfigurationError, match="distinct durable identity"):
        build_second_scheduler_and_use_its_identity()


def test_deliveries_reach_each_scheduler_by_intrinsic_identity() -> None:
    """Two schedulers in one module each serve their own subscriptions."""
    first_scheduler, _first_adapter, first_driver = scheduler_with_driver(
        jobs_key="reports",
    )
    second_scheduler, _second_adapter, second_driver = scheduler_with_driver(
        jobs_key="cleanup",
    )
    register_scheduler(first_scheduler)
    register_scheduler(second_scheduler)

    topics = {subscription.topic for subscription in get_subscriptions()}
    assert topics == {
        "__aps_reports_start",
        "__aps_reports_wakeup",
        "__aps_cleanup_start",
        "__aps_cleanup_wakeup",
    }

    # The web Function durably started the second scheduler and published this.
    second_driver.start(datetime.now(UTC))
    start_payload = StartPayload(scheduler_id="cleanup", generation=1).to_payload()
    start_subscription = next(
        subscription
        for subscription in get_subscriptions()
        if subscription.topic == "__aps_cleanup_start"
    )
    start_subscription.func(
        message(
            start_payload,
            message_id="start",
            topic=start_subscription.topic,
            consumer_group=start_subscription.consumer_group,
        )
    )

    assert second_driver.start_status == "active"
    assert first_driver.generation == 0
    assert first_driver.start_status is None


def test_subscriber_runtime_ignores_web_lifecycle_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler, _, driver = scheduler_with_driver()
    monkeypatch.setenv("VERCEL_PYTHON_SUBSCRIBER_ID", TEST_SCHEDULER_ID)

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_start",
    ) as send:
        scheduler.start()
        scheduler.pause()
        scheduler.resume()

    send.assert_not_called()
    assert driver.state == "paused"
    assert driver.generation == 0
    assert scheduler.state == 0


def test_repeated_start_publishes_one_generation() -> None:
    scheduler, _, driver = scheduler_with_driver()

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_start",
    ) as send:
        for _ in range(100):
            scheduler.start()

    send.assert_called_once()
    assert driver.state == "running"
    assert driver.generation == 1
    assert StartPayload.from_payload(send.call_args.args[1]).generation == 1


def test_automatic_activation_starts_once_and_respects_explicit_pause() -> None:
    scheduler, adapter, driver = scheduler_with_driver()

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_start",
    ) as send:
        adapter.auto_activate()
        adapter.auto_activate()

        send.assert_called_once()
        assert driver.state == "running"
        assert driver.generation == 1

        scheduler.pause()
        adapter.auto_activate()

    send.assert_called_once()
    assert driver.state == "paused"
    assert driver.generation == 1
    assert scheduler.state == STATE_PAUSED


def test_start_paused_waits_for_resume_to_publish() -> None:
    scheduler, _, driver = scheduler_with_driver()

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_start",
    ) as send:
        scheduler.start(paused=True)
        send.assert_not_called()
        assert driver.state == "paused"
        assert driver.generation == 0
        assert scheduler.state == STATE_PAUSED

        scheduler.resume()

    send.assert_called_once()
    assert driver.state == "running"
    assert driver.generation == 1
    assert scheduler.state == STATE_RUNNING


def test_start_with_no_jobs_becomes_dormant_without_wakeup() -> None:
    scheduler, adapter, driver = scheduler_with_driver()
    register_scheduler(scheduler)
    start_subscription = get_subscriptions()[0]

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_start",
    ) as send:
        scheduler.start()
        start_payload = send.call_args.args[1]

    with patch.object(adapter, "publish_wakeup") as publish:
        start_subscription.func(
            message(
                start_payload,
                message_id="start",
                topic=start_subscription.topic,
                consumer_group=start_subscription.consumer_group,
            )
        )

    publish.assert_not_called()
    assert driver.state == "running"
    assert driver.start_status == "active"
    assert driver.current is None


def test_runtime_add_rearms_dormant_chain_with_monotonic_sequence() -> None:
    scheduler, _adapter, driver = scheduler_with_driver()
    register_scheduler(scheduler)
    start_subscription = get_subscriptions()[0]

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_start",
    ) as send:
        scheduler.start()
        start_payload = send.call_args.args[1]
    start_subscription.func(
        message(
            start_payload,
            message_id="start",
            topic=start_subscription.topic,
            consumer_group=start_subscription.consumer_group,
        )
    )

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="wake-1",
    ) as send:
        scheduler.add_job(
            lambda: None,
            "date",
            run_date=datetime.now(UTC) + timedelta(minutes=5),
            id="first",
        )

    first = driver.current
    assert first is not None
    assert first.sequence == 1
    assert send.call_count == 1

    assert driver.claim_wake(first, "owner", datetime.now(UTC)).state == "claimed"
    terminal = driver.finish_wake(first, "owner", None, datetime.now(UTC))
    assert terminal.state == "advanced"
    assert terminal.wake is None

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="wake-2",
    ) as send:
        scheduler.add_job(
            lambda: None,
            "date",
            run_date=datetime.now(UTC) + timedelta(minutes=10),
            id="second",
        )

    second = driver.current
    assert second is not None
    assert second.sequence == 2
    assert send.call_count == 1


def test_failed_runtime_mutation_retry_repairs_pending_wake() -> None:
    scheduler, _adapter, driver = scheduler_with_driver()
    register_scheduler(scheduler)
    start_subscription = get_subscriptions()[0]

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_start",
    ) as send:
        scheduler.start()
        start_payload = send.call_args.args[1]
    start_subscription.func(
        message(
            start_payload,
            message_id="start",
            topic=start_subscription.topic,
            consumer_group=start_subscription.consumer_group,
        )
    )

    # The add commits durably and arms a wake, but every publish fails.
    run_date = datetime.now(UTC) + timedelta(minutes=5)
    with (
        patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            side_effect=ConnectionError("queue unavailable"),
        ),
        pytest.raises(ConnectionError),
    ):
        scheduler.add_job(
            lambda: None,
            "date",
            run_date=run_date,
            id="repair",
        )
    armed = driver.current
    assert armed is not None
    assert armed.status == "pending"

    # Retrying the same add fails on the conflicting id, but the retry must
    # still publish the wake armed by the interrupted first attempt.
    with (
        patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="wake-repaired",
        ) as send,
        pytest.raises(APSchedulerConfigurationError, match="already exists"),
    ):
        scheduler.add_job(
            lambda: None,
            "date",
            run_date=run_date,
            id="repair",
        )
    assert send.call_count == 1
    repaired = driver.current
    assert repaired is not None
    assert repaired.status == "published"


def test_is_scheduler_subscriber_classifies_declared_objects() -> None:
    scheduler, _adapter, _driver = scheduler_with_driver()
    del scheduler

    assert is_scheduler_subscriber(TEST_SCHEDULER_MODULE, "scheduler")
    assert not is_scheduler_subscriber(TEST_SCHEDULER_MODULE, "missing")
    assert not is_scheduler_subscriber("not_imported_module", "scheduler")

    modules[TEST_SCHEDULER_MODULE].__dict__["plain"] = object()
    assert not is_scheduler_subscriber(TEST_SCHEDULER_MODULE, "plain")


def test_runtime_mutation_requires_lifecycle_activation() -> None:
    scheduler, adapter, _ = scheduler_with_driver()

    @scheduler.scheduled_job("interval", minutes=5, id="cleanup")
    def cleanup() -> None:
        return

    with pytest.raises(
        APSchedulerConfigurationError,
        match=r"call scheduler\.start\(\)",
    ):
        scheduler.modify_job("cleanup", name="changed")

    assert adapter.scheduler.state == 0


def test_second_durable_store_is_rejected() -> None:
    scheduler = BlockingScheduler(
        timezone=UTC,
        jobstores={
            "default": InMemoryRedisJobStore(),
            "secondary": InMemoryRedisJobStore(),
        },
    )
    bind_test_scheduler(scheduler)

    with pytest.raises(
        APSchedulerConfigurationError,
        match='durable store must be the one named "default"',
    ):
        scheduler.start()


def test_source_store_rows_dispatch_on_wakeup() -> None:
    _scheduler, adapter, _driver, source = scheduler_with_source_store()
    now = datetime.now(UTC)
    source.rows["sub-1"] = now - timedelta(seconds=5)

    result = adapter.process_wakeup(now)

    assert source.dispatched == ["sub-1"]
    assert "sub-1" in result.due_job_ids
    assert source.rows == {}
    # Nothing left in any store: no polling, the chain goes dormant.
    assert result.next_wakeup_time is None


def test_empty_source_store_lets_the_chain_go_dormant() -> None:
    _scheduler, adapter, _driver, source = scheduler_with_source_store()
    now = datetime.now(UTC)

    result = adapter.process_wakeup(now)

    assert source.dispatched == []
    assert result.next_wakeup_time is None


def test_source_store_next_run_time_is_trusted_exactly() -> None:
    _scheduler, adapter, _driver, source = scheduler_with_source_store()
    now = datetime.now(UTC)
    due = now + timedelta(days=30)
    source.rows["sub-far"] = due

    result = adapter.process_wakeup(now)

    # The successor arms at the store's exact next run time; there is no
    # poll cap pulling it closer.
    assert source.dispatched == []
    assert result.next_wakeup_time == due


def test_recurring_source_job_advances_through_update_job() -> None:
    _scheduler, adapter, _driver, source = scheduler_with_source_store()
    now = datetime.now(UTC)
    source.trigger_factory = lambda run_date: IntervalTrigger(
        minutes=5,
        start_date=run_date,
        timezone=UTC,
    )
    due = now - timedelta(seconds=5)
    source.rows["recurring"] = due

    adapter.process_wakeup(now)

    assert source.dispatched == []
    assert source.updated == ["recurring"]
    assert source.rows["recurring"] == due + timedelta(minutes=5)


def test_source_store_read_errors_bound_the_retry() -> None:
    scheduler, adapter, _driver, source = scheduler_with_source_store()
    now = datetime.now(UTC)
    source.rows["sub-1"] = now - timedelta(seconds=5)
    source.fail_reads = True

    result = adapter.process_wakeup(now)

    assert source.dispatched == []
    assert result.next_wakeup_time is not None
    # Bounded on both sides: no immediate hot retry, no unbounded stall.
    assert result.next_wakeup_time >= now + timedelta(seconds=scheduler.jobstore_retry_interval - 1)
    assert result.next_wakeup_time <= datetime.now(UTC) + timedelta(
        seconds=scheduler.jobstore_retry_interval
    )


def test_source_dispatch_failure_retries_and_preserves_the_row() -> None:
    scheduler, adapter, _driver, source = scheduler_with_source_store()
    now = datetime.now(UTC)
    due = now - timedelta(seconds=5)
    source.rows["sub-1"] = due

    def failing_remove(job_id: str) -> None:
        raise ConnectionError("source database unavailable")

    source.remove_job = failing_remove  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]

    result = adapter.process_wakeup(now)

    # The failed advance leaves the row for the bounded retry; the row's
    # overdue time must not pull the successor into an immediate hot loop.
    assert source.rows == {"sub-1": due}
    assert result.next_wakeup_time is not None
    assert result.next_wakeup_time >= now + timedelta(seconds=scheduler.jobstore_retry_interval - 1)
    assert result.next_wakeup_time <= datetime.now(UTC) + timedelta(
        seconds=scheduler.jobstore_retry_interval
    )


def test_source_executor_mismatch_is_floored_not_hot_spun() -> None:
    scheduler, adapter, _driver, source = scheduler_with_source_store()
    now = datetime.now(UTC)
    source.executor_alias = "missing"
    source.rows["sub-1"] = now - timedelta(seconds=5)

    result = adapter.process_wakeup(now)

    # The job is skipped, stays due, and cannot drive an immediate wake.
    assert source.dispatched == []
    assert source.rows
    assert result.next_wakeup_time is not None
    assert result.next_wakeup_time >= now + timedelta(seconds=scheduler.jobstore_retry_interval - 1)


def test_malformed_source_job_is_stepped_over() -> None:
    scheduler, adapter, _driver, source = scheduler_with_source_store()
    now = datetime.now(UTC)
    broken_date = now - timedelta(seconds=10)

    class BrokenTrigger(DateTrigger):
        def get_next_fire_time(self, previous_fire_time: Any, now: Any) -> Any:
            raise RuntimeError("malformed job")

    def factory(run_date: datetime) -> Any:
        trigger_type = BrokenTrigger if run_date == broken_date else DateTrigger
        return trigger_type(run_date, UTC)

    source.trigger_factory = factory
    source.rows["sub-broken"] = broken_date
    source.rows["sub-good"] = now - timedelta(seconds=5)

    result = adapter.process_wakeup(now)

    # The broken job is skipped without stalling the wake or the good job.
    assert source.dispatched == ["sub-good"]
    assert result.next_wakeup_time is not None
    assert result.next_wakeup_time >= now + timedelta(seconds=scheduler.jobstore_retry_interval - 1)


def test_mutations_do_not_fall_through_to_source_stores() -> None:
    scheduler, _adapter, _driver, source = scheduler_with_source_store()
    now = datetime.now(UTC)
    due = now + timedelta(hours=1)
    source.rows["sub-1"] = due

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg",
    ):
        scheduler.start()

        # jobstore=None is pinned to the default store instead of falling
        # through to the source store's dispatch-on-remove.
        with pytest.raises(JobLookupError):
            scheduler.remove_job("sub-1")

        with pytest.raises(
            APSchedulerConfigurationError,
            match='may only target the durable "default"',
        ):
            scheduler.remove_job("sub-1", jobstore="subscription")

        with pytest.raises(
            APSchedulerConfigurationError,
            match='may only target the durable "default"',
        ):
            scheduler.pause_job("sub-1", "subscription")

        scheduler.remove_all_jobs()

    assert source.dispatched == []
    assert source.updated == []
    assert source.rows == {"sub-1": due}


def test_paused_interval_does_not_skip_source_rows() -> None:
    scheduler, adapter, _driver, source = scheduler_with_source_store()
    del scheduler
    now = datetime.now(UTC)
    source.rows["sub-overdue"] = now - timedelta(hours=1)

    adapter.activate_generation(now)
    result = adapter.process_wakeup(now)

    assert source.dispatched == ["sub-overdue"]
    assert "sub-overdue" in result.due_job_ids


def test_adding_a_source_store_rearms_an_active_chain() -> None:
    scheduler, _adapter, driver = scheduler_with_driver()
    register_scheduler(scheduler)
    start_subscription = get_subscriptions()[0]

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_start",
    ) as send:
        scheduler.start()
        start_payload = send.call_args.args[1]
    start_subscription.func(
        message(
            start_payload,
            message_id="start",
            topic=start_subscription.topic,
            consumer_group=start_subscription.consumer_group,
        )
    )
    assert driver.current is None

    source = InMemorySourceJobStore()
    due = datetime.now(UTC) + timedelta(minutes=5)
    source.rows["sub-1"] = due
    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_wake",
    ):
        scheduler.add_jobstore(source, "subscription")

    armed = driver.current
    assert armed is not None
    assert armed.status == "published"
    assert armed.logical_time == due


def test_adding_an_empty_source_store_leaves_the_chain_dormant() -> None:
    scheduler, _adapter, driver = scheduler_with_driver()
    register_scheduler(scheduler)
    start_subscription = get_subscriptions()[0]

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_start",
    ) as send:
        scheduler.start()
        start_payload = send.call_args.args[1]
    start_subscription.func(
        message(
            start_payload,
            message_id="start",
            topic=start_subscription.topic,
            consumer_group=start_subscription.consumer_group,
        )
    )
    assert driver.current is None

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_wake",
    ):
        scheduler.add_jobstore(InMemorySourceJobStore(), "subscription")

    # Nothing scheduled anywhere: no polling wake is armed.
    assert driver.current is None


def test_start_rearms_wake_from_source_store_on_unchanged_generation() -> None:
    scheduler, _adapter, driver, source = scheduler_with_source_store()
    register_scheduler(scheduler)
    # A prior session's generation is active and dormant.
    driver.start(datetime.now(UTC))
    driver.start_status = "active"
    due = datetime.now(UTC) + timedelta(minutes=5)
    source.rows["sub-1"] = due

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_wake",
    ):
        scheduler.start()

    armed = driver.current
    assert armed is not None
    assert armed.status == "published"
    assert armed.logical_time == due


def test_explicit_wakeup_rearms_the_chain_from_source_stores() -> None:
    scheduler, _adapter, driver, source = scheduler_with_source_store()
    register_scheduler(scheduler)
    start_subscription = get_subscriptions()[0]

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_start",
    ) as send:
        scheduler.start()
        start_payload = send.call_args.args[1]
    start_subscription.func(
        message(
            start_payload,
            message_id="start",
            topic=start_subscription.topic,
            consumer_group=start_subscription.consumer_group,
        )
    )
    assert driver.current is None

    # A row appears out of band; the application signals it explicitly.
    due = datetime.now(UTC) + timedelta(minutes=5)
    source.rows["sub-1"] = due
    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_wake",
    ):
        scheduler.wakeup()

    armed = driver.current
    assert armed is not None
    assert armed.status == "published"
    assert armed.logical_time == due


def test_redis_backend_rejects_a_cache_store_under_a_source_alias() -> None:
    scheduler = BlockingScheduler(
        timezone=UTC,
        jobstores={
            "default": InMemoryRedisJobStore(),
            "secondary": CacheJobStore(),
        },
    )
    bind_test_scheduler(scheduler)

    with pytest.raises(
        APSchedulerConfigurationError,
        match='durable store must be the one named "default"',
    ):
        scheduler.start()


def test_add_job_into_a_source_store_is_rejected() -> None:
    scheduler, adapter, _driver, _source = scheduler_with_source_store()
    scheduler.add_job(
        durable_noop_job,
        "interval",
        minutes=5,
        id="stray",
        jobstore="subscription",
    )

    with pytest.raises(APSchedulerConfigurationError, match="source store"):
        adapter.ensure_local_started()


def test_start_with_only_a_source_store_arms_the_wake_at_its_due_time() -> None:
    scheduler, _adapter, driver, source = scheduler_with_source_store()
    register_scheduler(scheduler)
    start_subscription = get_subscriptions()[0]
    due = datetime.now(UTC) + timedelta(minutes=5)
    source.rows["sub-1"] = due

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_start",
    ) as send:
        scheduler.start()
        start_payload = send.call_args.args[1]

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_wake",
    ):
        start_subscription.func(
            message(
                start_payload,
                message_id="start",
                topic=start_subscription.topic,
                consumer_group=start_subscription.consumer_group,
            )
        )

    # The first wake arms at the source store's exact next due time.
    assert driver.current is not None
    assert driver.current.status == "published"
    assert driver.current.logical_time == due


def test_wake_delivery_dispatches_source_rows_and_arms_the_successor() -> None:
    scheduler, _adapter, driver, source = scheduler_with_source_store()
    register_scheduler(scheduler)
    wake_subscription = get_subscriptions()[1]
    driver.start(datetime.now(UTC))
    driver.start_status = "active"
    logical = datetime.now(UTC)
    token = WakeToken(1, 1, logical, status="published")
    driver.current = token
    source.rows["sub-1"] = logical - timedelta(seconds=5)
    later = logical + timedelta(minutes=5)
    source.rows["sub-2"] = later

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_wake",
    ):
        wake_subscription.func(
            message(
                WakeupPayload.from_token(TEST_SCHEDULER_ID, token).to_payload(),
                message_id="wake",
                topic=wake_subscription.topic,
                consumer_group=wake_subscription.consumer_group,
            )
        )

    assert source.dispatched == ["sub-1"]
    successor = driver.current
    assert successor is not None
    assert successor.sequence == 2
    assert successor.logical_time == later


def test_pause_resume_fences_old_start_messages() -> None:
    scheduler, adapter, driver = scheduler_with_driver()
    register_scheduler(scheduler)
    subscriptions = get_subscriptions()
    start_subscription = subscriptions[0]
    sent_payloads: list[dict[str, Any]] = []

    def capture_send(topic: str, payload: dict[str, Any], **kwargs: Any) -> str:
        del topic, kwargs
        sent_payloads.append(payload)
        return f"msg_{len(sent_payloads)}"

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        side_effect=capture_send,
    ):
        scheduler.start()
        scheduler.pause()
        assert scheduler.state == STATE_PAUSED
        scheduler.resume()

    assert driver.generation == 2
    assert scheduler.state == STATE_RUNNING
    assert len(sent_payloads) == 2

    with (
        patch.object(adapter, "activate_generation") as activate,
        patch.object(
            adapter,
            "get_next_wakeup_time",
            return_value=datetime.now(UTC) + timedelta(minutes=1),
        ),
        patch.object(adapter, "publish_wakeup") as publish,
    ):
        start_subscription.func(
            message(
                sent_payloads[0],
                message_id="old",
                topic=start_subscription.topic,
                consumer_group=start_subscription.consumer_group,
            )
        )
        activate.assert_not_called()
        publish.assert_not_called()

        start_subscription.func(
            message(
                sent_payloads[1],
                message_id="current",
                topic=start_subscription.topic,
                consumer_group=start_subscription.consumer_group,
            )
        )
        activate.assert_called_once()
        publish.assert_called_once()


def test_pause_racing_with_wakeup_prevents_successor() -> None:
    scheduler, adapter, driver = scheduler_with_driver()
    register_scheduler(scheduler)
    wake_subscription = get_subscriptions()[1]
    driver.start(datetime.now(UTC))
    driver.start_status = "active"
    token = WakeToken(
        generation=1,
        sequence=1,
        logical_time=datetime.now(UTC),
        status="published",
    )
    driver.current = token

    def process_and_pause(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        scheduler.pause()
        return type(
            "Result",
            (),
            {
                "next_wakeup_time": datetime.now(UTC) + timedelta(minutes=1),
            },
        )()

    with (
        patch.object(adapter, "process_wakeup", side_effect=process_and_pause),
        patch.object(adapter, "publish_wakeup") as publish,
    ):
        wake_subscription.func(
            message(
                WakeupPayload.from_token(
                    TEST_SCHEDULER_ID,
                    token,
                ).to_payload(),
                message_id="wake",
                topic=wake_subscription.topic,
                consumer_group=wake_subscription.consumer_group,
            )
        )

    publish.assert_not_called()
    assert driver.state == "paused"
    assert driver.current == token


def test_stale_wakeup_runs_no_jobs_and_publishes_no_successor() -> None:
    scheduler, adapter, driver = scheduler_with_driver()
    register_scheduler(scheduler)
    wake_subscription = get_subscriptions()[1]
    driver.start(datetime.now(UTC))
    old = WakeToken(1, 1, datetime.now(UTC))
    driver.pause(datetime.now(UTC))

    with (
        patch.object(adapter, "process_wakeup") as process,
        patch.object(adapter, "publish_wakeup") as publish,
    ):
        wake_subscription.func(
            message(
                WakeupPayload.from_token(
                    TEST_SCHEDULER_ID,
                    old,
                ).to_payload(),
                message_id="stale",
                topic=wake_subscription.topic,
                consumer_group=wake_subscription.consumer_group,
            )
        )

    process.assert_not_called()
    publish.assert_not_called()


def test_lost_wakeup_owner_retries_instead_of_acknowledging() -> None:
    scheduler, adapter, driver = scheduler_with_driver()
    register_scheduler(scheduler)
    wake_subscription = get_subscriptions()[1]
    driver.start(datetime.now(UTC))
    driver.start_status = "active"
    token = WakeToken(
        generation=1,
        sequence=1,
        logical_time=datetime.now(UTC),
        status="published",
    )
    driver.current = token

    result = type(
        "Result",
        (),
        {"next_wakeup_time": None},
    )()
    with (
        patch.object(adapter, "process_wakeup", return_value=result),
        patch.object(
            driver,
            "finish_wake",
            return_value=FinishResult("lost"),
        ),
        pytest.raises(RetryAfter),
    ):
        wake_subscription.func(
            message(
                WakeupPayload.from_token(
                    TEST_SCHEDULER_ID,
                    token,
                ).to_payload(),
                message_id="lost",
                topic=wake_subscription.topic,
                consumer_group=wake_subscription.consumer_group,
            )
        )


def test_durable_job_requires_explicit_id() -> None:
    scheduler, adapter, _ = scheduler_with_driver()
    scheduler.add_job(lambda: None, "interval", minutes=1)

    with pytest.raises(
        APSchedulerConfigurationError,
        match="explicit stable id",
    ):
        adapter.ensure_local_started()


def test_cold_start_preserves_persisted_next_run_time() -> None:
    scheduler, adapter, _ = scheduler_with_driver()
    persisted_time = datetime.now(UTC) + timedelta(hours=2)

    scheduler.add_job(
        durable_noop_job,
        "interval",
        minutes=5,
        id="cleanup",
        replace_existing=True,
    )

    store = scheduler._jobstores["default"]
    pending_job = scheduler.get_job("cleanup")
    assert pending_job is not None
    persisted_job = object.__new__(type(pending_job))
    for attribute in pending_job.__slots__:
        if attribute != "__weakref__" and hasattr(pending_job, attribute):
            setattr(persisted_job, attribute, getattr(pending_job, attribute))
    persisted_job._modify(
        name="runtime-name",
        next_run_time=persisted_time,
    )
    persisted_job._jobstore_alias = "default"
    store.jobs["cleanup"] = persisted_job

    adapter.ensure_local_started()

    materialized = store.lookup_job("cleanup")
    assert materialized.name == "runtime-name"
    assert materialized.next_run_time == persisted_time


def test_new_generation_skips_paused_interval() -> None:
    scheduler, adapter, _ = scheduler_with_driver()
    old_time = datetime.now(UTC) - timedelta(hours=1)

    @scheduler.scheduled_job("interval", minutes=5, id="cleanup")
    def cleanup() -> None:
        return

    adapter.ensure_local_started()
    store = scheduler._jobstores["default"]
    job = store.lookup_job("cleanup")
    job._modify(next_run_time=old_time)
    store.update_job(job)
    activation = datetime.now(UTC)

    adapter.activate_generation(activation)

    assert store.lookup_job("cleanup").next_run_time >= activation
