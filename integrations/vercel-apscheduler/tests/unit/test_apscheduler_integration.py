from __future__ import annotations

from typing import Any

from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from sys import modules
from threading import Lock
from types import ModuleType
from unittest.mock import patch

import pytest
from apscheduler.jobstores.base import ConflictingIdError, JobLookupError
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import STATE_PAUSED, STATE_RUNNING
from apscheduler.schedulers.blocking import BlockingScheduler

from vercel.integrations.apscheduler import (
    APSchedulerConfigurationError,
    install_vercel_apscheduler_integration,
)
from vercel.integrations.apscheduler._adapter import get_adapter
from vercel.integrations.apscheduler._driver import (
    ClaimResult,
    DriverSnapshot,
    FinishResult,
    StartDecision,
    WakeToken,
)
from vercel.integrations.apscheduler._jobstore import RedisJobCoordinator
from vercel.integrations.apscheduler._payload import StartPayload, WakeupPayload
from vercel.integrations.apscheduler._subscriber import register_scheduler
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
TEST_SCHEDULER_ID = "scheduler_scheduler"


def durable_noop_job() -> None:
    return


class InMemoryRedisJobStore(RedisJobStore):
    def __init__(self) -> None:
        self.jobs_key = "apscheduler.jobs"
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


class FakeDriver:
    def __init__(self) -> None:
        self.lock = Lock()
        self.state = "paused"
        self.generation = 0
        self.start_status: str | None = None
        self.activation_time: datetime | None = None
        self.current: WakeToken | None = None
        self.last_sequence = 0
        self.owner: str | None = None

    def start(
        self,
        now: datetime,
        *,
        idle_timeout_seconds: int | None = None,
    ) -> StartDecision:
        del now, idle_timeout_seconds
        with self.lock:
            changed = self.state != "running"
            if changed:
                self.state = "running"
                self.generation += 1
                self.start_status = "pending"
                self.activation_time = None
                self.current = None
                self.last_sequence = 0
            return StartDecision(
                generation=self.generation,
                changed=changed,
                start_status=self.start_status or "",
                current_wake=self.current,
            )

    def pause(self, now: datetime) -> bool:
        del now
        with self.lock:
            changed = self.state == "running"
            self.state = "paused"
            return changed

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
    clear_subscriptions()
    install_vercel_apscheduler_integration(register_queues=False)
    yield
    clear_subscriptions()


def bind_test_scheduler(scheduler: BlockingScheduler) -> None:
    modules[TEST_SCHEDULER_MODULE].__dict__["scheduler"] = scheduler


class InMemoryJobCoordinator(RedisJobCoordinator):
    """Mirrors the coordinator's Lua semantics against the in-memory store."""

    def __init__(self, store: Any, driver: Any, adapter: Any) -> None:
        super().__init__(store, driver, adapter)
        self._original_add_job = store.add_job
        self._original_update_job = store.update_job
        self._original_remove_job = store.remove_job
        self._original_remove_all_jobs = store.remove_all_jobs
        self._versions: dict[str, int] = {}
        self._revision = 0

    def add_job(self, job: Any) -> None:
        try:
            self._original_add_job(job)
        except ConflictingIdError:
            if self.adapter.is_runtime_mutation or self.adapter.is_wake_mutation:
                raise
            return
        self._versions[job.id] = self._next_revision()
        self._rearm(job, rearm=True)

    def update_job(self, job: Any) -> None:
        self._original_update_job(job)
        self._versions[job.id] = self._next_revision()
        self._rearm(job, rearm=self.adapter.is_runtime_mutation)

    def remove_job(self, job_id: str) -> None:
        self._original_remove_job(job_id)
        self._next_revision()
        self._versions.pop(job_id, None)

    def remove_all_jobs(self) -> None:
        self._original_remove_all_jobs()
        self._next_revision()
        self._versions.clear()

    def get_due_jobs_with_revisions(self, now: datetime) -> list[tuple[Any, int]]:
        return [(job, self._versions.get(job.id, 0)) for job in self.store.get_due_jobs(now)]

    def get_all_jobs_with_revisions(self) -> list[tuple[Any, int]]:
        return [(job, self._versions.get(job.id, 0)) for job in self.store.get_all_jobs()]

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
        return True

    def _next_revision(self) -> int:
        self._revision += 1
        return self._revision

    def _rearm(self, job: Any, *, rearm: bool) -> None:
        if not rearm:
            return
        next_run_time = getattr(job, "next_run_time", None)
        if next_run_time is None:
            return
        driver: Any = self.driver
        driver.rearm(
            self.adapter.canonical_wakeup_time(next_run_time),
            datetime.now(UTC),
        )


def scheduler_with_driver() -> tuple[BlockingScheduler, Any, FakeDriver]:
    scheduler = BlockingScheduler(
        timezone=UTC,
        jobstores={"default": InMemoryRedisJobStore()},
    )
    bind_test_scheduler(scheduler)
    adapter = get_adapter(scheduler)
    assert adapter is not None
    with patch(
        "vercel.integrations.apscheduler._adapter.RedisJobCoordinator",
        InMemoryJobCoordinator,
    ):
        adapter._bind_runtime()
    driver = FakeDriver()
    adapter._driver = driver  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
    adapter.coordinator.driver = driver  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
    return scheduler, adapter, driver


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


def test_memory_job_store_is_rejected() -> None:
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


def test_runtime_registry_binds_the_loaded_subscriber_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VERCEL_PYTHON_SUBSCRIBER_ID", raising=False)
    monkeypatch.setenv(
        "VERCEL_APSCHEDULER_SUBSCRIBERS",
        (
            '[{"id":"other_scheduler","entrypoint":"not_loaded:scheduler"},'
            '{"id":"scheduler_scheduler","entrypoint":"test_scheduler:scheduler"}]'
        ),
    )
    scheduler = BlockingScheduler(
        timezone=UTC,
        jobstores={"default": InMemoryRedisJobStore()},
    )
    module = ModuleType("test_scheduler")
    module.__dict__["scheduler"] = scheduler
    monkeypatch.setitem(modules, module.__name__, module)
    adapter = get_adapter(scheduler)
    assert adapter is not None

    adapter._bind_runtime()

    assert adapter.identity.scheduler_id == "scheduler_scheduler"


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


def test_multiple_job_stores_are_rejected() -> None:
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
        match="exactly one job store",
    ):
        scheduler.start()


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
                    "scheduler_scheduler",
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
                    "scheduler_scheduler",
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
                    "scheduler_scheduler",
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
