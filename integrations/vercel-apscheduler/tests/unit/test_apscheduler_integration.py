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
from apscheduler.schedulers.blocking import BlockingScheduler

from vercel.integrations.apscheduler import (
    APSchedulerConfigurationError,
    install_vercel_apscheduler_integration,
)
from vercel.integrations.apscheduler._adapter import get_adapter
from vercel.integrations.apscheduler._driver import (
    ClaimResult,
    DriverSnapshot,
    StartDecision,
    WakeToken,
)
from vercel.integrations.apscheduler._payload import StartPayload, WakeupPayload
from vercel.integrations.apscheduler._subscriber import register_scheduler
from vercel.queue import Message, MessageMetadata, SanitizedName, get_subscriptions
from vercel.queue.testing import clear_subscriptions

UTC = timezone.utc


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
        self.owner: str | None = None

    def start(self, now: datetime) -> StartDecision:
        del now
        with self.lock:
            changed = self.state != "running"
            if changed:
                self.state = "running"
                self.generation += 1
                self.start_status = "pending"
                self.activation_time = None
                self.current = None
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
        next_logical_time: datetime,
        now: datetime,
    ) -> WakeToken | None:
        del now
        with self.lock:
            if self.owner != owner or self.state != "running" or generation != self.generation:
                if self.owner == owner:
                    self.owner = None
                return None
            self.owner = None
            self.start_status = "active"
            self.current = WakeToken(generation, 1, next_logical_time)
            return self.current

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
        next_logical_time: datetime,
        now: datetime,
    ) -> WakeToken | None:
        del now
        with self.lock:
            if (
                self.owner != owner
                or self.state != "running"
                or token.generation != self.generation
                or self.current is None
                or token.sequence != self.current.sequence
            ):
                if self.owner == owner:
                    self.owner = None
                return None
            self.owner = None
            self.current = WakeToken(
                self.generation,
                token.sequence + 1,
                next_logical_time,
            )
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
    monkeypatch.setenv("VERCEL_PYTHON_SUBSCRIBER_ID", "scheduler_scheduler")
    monkeypatch.delenv("VERCEL_APSCHEDULER_DISCOVERY", raising=False)
    monkeypatch.delenv("VERCEL_SERVICE_TYPE", raising=False)
    monkeypatch.delenv("VERCEL_SERVICE_TRIGGER", raising=False)
    monkeypatch.delenv("VERCEL_DEV_QUEUE_SERVING", raising=False)
    clear_subscriptions()
    install_vercel_apscheduler_integration(register_queues=False)
    yield
    clear_subscriptions()


def scheduler_with_driver() -> tuple[BlockingScheduler, Any, FakeDriver]:
    scheduler = BlockingScheduler(
        timezone=UTC,
        jobstores={"default": InMemoryRedisJobStore()},
    )
    adapter = get_adapter(scheduler)
    assert adapter is not None
    adapter._bind_runtime()
    driver = FakeDriver()
    adapter._driver = driver  # ty: ignore[invalid-assignment]
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


def test_payloads_round_trip_generation_and_sequence() -> None:
    start = StartPayload("scheduler_scheduler", 3)
    wake = WakeupPayload(
        "scheduler_scheduler",
        3,
        7,
        datetime(2026, 7, 30, 12, 0, tzinfo=UTC),
    )

    assert StartPayload.from_payload(start.to_payload()) == start
    assert WakeupPayload.from_payload(wake.to_payload()) == wake


def test_memory_job_store_is_rejected() -> None:
    scheduler = BlockingScheduler(timezone=UTC)

    with pytest.raises(
        APSchedulerConfigurationError,
        match="default RedisJobStore",
    ):
        scheduler.start()


def test_runtime_registry_binds_the_loaded_subscriber_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VERCEL_PYTHON_SUBSCRIBER_ID")
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

        scheduler.resume()

    send.assert_called_once()
    assert driver.state == "running"
    assert driver.generation == 1


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
        scheduler.resume()

    assert driver.generation == 2
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

    @scheduler.scheduled_job("interval", minutes=5, id="cleanup")
    def cleanup() -> None:
        return

    store = scheduler._jobstores["default"]
    pending_job = scheduler.get_job("cleanup")
    assert pending_job is not None
    pending_job._modify(next_run_time=persisted_time)
    pending_job._jobstore_alias = "default"
    store.jobs["cleanup"] = pending_job

    adapter.ensure_local_started()

    assert store.lookup_job("cleanup").next_run_time == persisted_time


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
