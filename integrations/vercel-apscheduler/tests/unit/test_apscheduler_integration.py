from __future__ import annotations

from typing import Any

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pytest

pytest.importorskip("apscheduler")

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.base import BaseJobStore
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from vercel.integrations.apscheduler import (
    MemoryCursor,
    VercelAPSchedulerOptions,
    WakeupPayload,
    _adapter as adapter_module,
    adopt_scheduler,
    install_vercel_apscheduler_integration,
)
from vercel.integrations.apscheduler._adapter import SchedulerAdapter
from vercel.integrations.apscheduler._payload import CursorEntry
from vercel.integrations.apscheduler._watchdog import APSchedulerWatchdogAsgiApp
from vercel.queue import (
    DuplicateIdempotencyKeyError,
    Message,
    MessageMetadata,
    SanitizedName,
    get_subscriptions,
)
from vercel.queue.testing import clear_subscriptions

UTC = timezone.utc


class DurableLikeJobStore(BaseJobStore):
    def __init__(self, next_run_time: datetime | None) -> None:
        super().__init__()
        self.next_run_time = next_run_time

    def lookup_job(self, job_id: str) -> Any:
        del job_id
        return None

    def get_due_jobs(self, now: datetime) -> list[Any]:
        del now
        return []

    def get_next_run_time(self) -> datetime | None:
        return self.next_run_time

    def get_all_jobs(self) -> list[Any]:
        return []

    def add_job(self, job: Any) -> None:
        del job

    def update_job(self, job: Any) -> None:
        del job

    def remove_job(self, job_id: str) -> None:
        del job_id

    def remove_all_jobs(self) -> None:
        return None

    def shutdown(self) -> None:
        return None


class RetainedJobStore(BaseJobStore):
    def __init__(self) -> None:
        super().__init__()
        self.jobs: dict[str, Any] = {}

    def lookup_job(self, job_id: str) -> Any:
        return self.jobs.get(job_id)

    def get_due_jobs(self, now: datetime) -> list[Any]:
        return [
            job
            for job in self.jobs.values()
            if job.next_run_time is not None and job.next_run_time <= now
        ]

    def get_next_run_time(self) -> datetime | None:
        return min(
            (job.next_run_time for job in self.jobs.values() if job.next_run_time is not None),
            default=None,
        )

    def get_all_jobs(self) -> list[Any]:
        return list(self.jobs.values())

    def add_job(self, job: Any) -> None:
        self.jobs[job.id] = job

    def update_job(self, job: Any) -> None:
        self.jobs[job.id] = job

    def remove_job(self, job_id: str) -> None:
        del self.jobs[job_id]

    def remove_all_jobs(self) -> None:
        self.jobs.clear()

    def shutdown(self) -> None:
        return None


def _options() -> VercelAPSchedulerOptions:
    return VercelAPSchedulerOptions(
        max_delay_seconds=23 * 60 * 60,
        retention_seconds=24 * 60 * 60,
    )


def _scheduler() -> tuple[BlockingScheduler, SchedulerAdapter]:
    install_vercel_apscheduler_integration(options=_options())
    scheduler = BlockingScheduler(timezone=UTC)
    return scheduler, adopt_scheduler(scheduler, _options())


@pytest.fixture(autouse=True)
def disable_vercel_runtime_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VERCEL", raising=False)
    monkeypatch.setenv("VERCEL_PYTHON_SUBSCRIBER_ID", "scheduler-a")


class TestWakeupPayload:
    def test_round_trip_normalizes_logical_time_to_utc(self) -> None:
        logical_time = datetime.fromisoformat("2026-04-09T08:00:00-04:00")
        cursor = MemoryCursor(
            jobs={
                "id:cleanup": CursorEntry(
                    job_id="cleanup",
                    fingerprint="sha256:test",
                    state="scheduled",
                    next_run_time=datetime(2026, 4, 10, 4, 0, tzinfo=UTC),
                    nominal_run_time=datetime(2026, 4, 10, 3, 59, 45, tzinfo=UTC),
                )
            }
        )

        restored = WakeupPayload.from_payload(
            WakeupPayload("scheduler-a", logical_time, cursor=cursor).to_payload()
        )

        assert restored.scheduler_id == "scheduler-a"
        assert restored.logical_time == datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        assert restored.cursor.jobs["id:cleanup"].next_run_time == datetime(
            2026,
            4,
            10,
            4,
            0,
            tzinfo=UTC,
        )
        assert restored.cursor.jobs["id:cleanup"].nominal_run_time == datetime(
            2026,
            4,
            10,
            3,
            59,
            45,
            tzinfo=UTC,
        )

    @pytest.mark.parametrize(
        ("payload", "message"),
        [
            ("not-an-object", "Invalid wakeup payload: expected object"),
            (
                {
                    "vercel": {"kind": "not-apscheduler", "version": 2},
                    "scheduler_id": "scheduler-a",
                    "logical_time": "2026-04-09T12:00:00+00:00",
                },
                "Invalid wakeup payload: not an APScheduler wakeup envelope",
            ),
            (
                {
                    "vercel": {"kind": "apscheduler.wakeup", "version": 1},
                    "scheduler_id": "scheduler-a",
                    "logical_time": "2026-04-09T12:00:00+00:00",
                },
                "Invalid wakeup payload: unsupported version",
            ),
            (
                {
                    "vercel": {"kind": "apscheduler.wakeup", "version": 2},
                    "scheduler_id": "scheduler-a",
                    "logical_time": "not-a-time",
                },
                "Invalid wakeup payload: logical_time must be ISO-8601",
            ),
            (
                {
                    "vercel": {"kind": "apscheduler.wakeup", "version": 2},
                    "scheduler_id": "scheduler-a",
                    "logical_time": "2026-04-09T12:00:00",
                },
                "logical_time must be timezone-aware",
            ),
        ],
    )
    def test_from_payload_rejects_invalid_payloads(
        self,
        payload: object,
        message: str,
    ) -> None:
        with pytest.raises(ValueError, match=message):
            WakeupPayload.from_payload(payload)


class TestQueueRegistration:
    def test_queue_identity_comes_from_builder_subscriber_id(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VERCEL_PYTHON_SUBSCRIBER_ID", "jobs_scheduler")

        options = VercelAPSchedulerOptions()
        adapter = adopt_scheduler(BlockingScheduler(timezone=UTC), options)

        assert adapter.identity.scheduler_id == "jobs_scheduler"
        assert adapter.identity.start_topic == "__aps_jobs_scheduler_start"
        assert adapter.identity.wakeup_topic == "__aps_jobs_scheduler_wakeup"
        assert adapter.identity.consumer_group == "apscheduler-jobs_scheduler"
        with pytest.raises(TypeError, match="unknown APScheduler integration option"):
            VercelAPSchedulerOptions.from_value({"scheduler_id": "user-chosen"})

    def test_publish_only_activation_does_not_register_topics(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VERCEL", "1")
        monkeypatch.delenv("VERCEL_DEV_QUEUE_SERVING", raising=False)
        monkeypatch.delenv("VERCEL_SERVICE_TYPE", raising=False)
        monkeypatch.delenv("VERCEL_SERVICE_TRIGGER", raising=False)
        monkeypatch.setattr(adapter_module._PATCH_STATE, "register_queues", False)
        clear_subscriptions()

        install_vercel_apscheduler_integration(register_queues=False)
        BlockingScheduler(timezone=UTC)

        assert get_subscriptions() == ()

    def test_publish_only_activation_defers_control_entrypoint_import(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("VERCEL_DEV_QUEUE_SERVING", raising=False)
        monkeypatch.delenv("VERCEL_SERVICE_TYPE", raising=False)
        monkeypatch.delenv("VERCEL_SERVICE_TRIGGER", raising=False)
        monkeypatch.setattr(adapter_module._PATCH_STATE, "register_queues", False)

        with patch(
            "vercel.integrations.apscheduler.control._load_control_from_env"
        ) as load_control:
            install_vercel_apscheduler_integration(register_queues=False)
            load_control.assert_not_called()

            install_vercel_apscheduler_integration(register_queues=True)
            load_control.assert_called_once_with()

    def test_dev_queue_sidecar_registers_after_publish_only_activation(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VERCEL", "1")
        monkeypatch.setenv("VERCEL_DEV_QUEUE_SERVING", "1")
        monkeypatch.setattr(adapter_module._PATCH_STATE, "register_queues", False)
        clear_subscriptions()

        install_vercel_apscheduler_integration(register_queues=False)
        BlockingScheduler(timezone=UTC)

        assert [subscription.topic for subscription in get_subscriptions()] == [
            "__aps_scheduler-a_start",
            "__aps_scheduler-a_wakeup",
        ]
        clear_subscriptions()

    def test_local_scheduler_construction_does_not_register_queue_topics(self) -> None:
        clear_subscriptions()
        install_vercel_apscheduler_integration(options=_options())

        BlockingScheduler(timezone=UTC)

        assert get_subscriptions() == ()

    def test_vercel_import_registers_start_and_wakeup_topics(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        created_at = datetime(2026, 4, 9, 11, 59, tzinfo=UTC)
        monkeypatch.setenv("VERCEL", "1")
        clear_subscriptions()
        install_vercel_apscheduler_integration(options=_options())
        scheduler = BlockingScheduler(timezone=UTC)
        adapter = adopt_scheduler(scheduler)

        subscriptions = get_subscriptions()

        assert [
            (
                subscription.topic,
                subscription.consumer_group,
                subscription.retry_after_seconds,
                subscription.max_concurrency,
                subscription.max_attempts,
            )
            for subscription in subscriptions
        ] == [
            (
                "__aps_scheduler-a_start",
                "apscheduler-scheduler-a",
                30,
                1,
                None,
            ),
            (
                "__aps_scheduler-a_wakeup",
                "apscheduler-scheduler-a",
                30,
                1,
                None,
            ),
        ]

        start_subscription = subscriptions[0]
        message: Message[dict[str, Any]] = Message(
            payload={},
            metadata=MessageMetadata(
                message_id="msg-start",
                delivery_count=1,
                created_at=created_at,
                topic=start_subscription.topic,
                consumer_group=SanitizedName(start_subscription.consumer_group),
            ),
        )
        with patch.object(adapter, "seed") as seed:
            start_subscription.func(message)

        seed.assert_called_once_with(now=created_at, kind="start")
        clear_subscriptions()


class TestControlPublishGuard:
    @patch("vercel.integrations.apscheduler._adapter.vqs_sync.send")
    def test_publish_guard_fences_successor_after_jobs_run(self, mock_send: Any) -> None:
        tick_at = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        calls: list[str] = []
        scheduler, adapter = _scheduler()
        scheduler.add_job(
            lambda: calls.append("ran"),
            "interval",
            seconds=30,
            start_date=tick_at,
            id="job-1",
        )

        result = adapter.process_wakeup(
            tick_at,
            publish_next=True,
            now=tick_at,
            epoch=7,
            publish_guard=lambda: False,
        )

        assert calls == ["ran"]
        assert result.next_wakeup_time == tick_at + timedelta(seconds=30)
        assert result.published_wakeup is None
        mock_send.assert_not_called()
        adapter.shutdown(wait=True)


class TestWarmSchedulerState:
    @patch("vercel.integrations.apscheduler._adapter.vqs_sync.send")
    def test_new_epoch_rebases_without_catching_up(
        self,
        mock_send: Any,
    ) -> None:
        anchor = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        first_start = anchor + timedelta(seconds=1)
        restart = anchor + timedelta(minutes=5, seconds=5)
        scheduler, adapter = _scheduler()
        scheduler.add_job(
            lambda: None,
            "interval",
            seconds=30,
            start_date=anchor,
            id="heartbeat",
        )

        first = adapter.seed(now=first_start, epoch=1)
        restarted = adapter.seed(now=restart, epoch=2)

        assert first is not None
        assert first.logical_time == anchor + timedelta(seconds=30)
        assert restarted is not None
        assert restarted.logical_time == anchor + timedelta(minutes=5, seconds=30)
        assert restarted.delay_seconds == 25
        assert mock_send.call_count == 2
        assert mock_send.call_args.kwargs["idempotency_key"] == (
            "aps:scheduler-a:2:2026-04-09T12:05:30+00:00"
        )
        adapter.shutdown(wait=True)

    @patch("vercel.integrations.apscheduler._adapter.vqs_sync.send")
    def test_new_epoch_skips_expired_durable_date_job(
        self,
        mock_send: Any,
    ) -> None:
        run_at = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        restart = run_at + timedelta(minutes=5)
        scheduler, adapter = _scheduler()
        jobstore = RetainedJobStore()
        scheduler.add_jobstore(jobstore, alias="durable")
        scheduler.add_job(
            lambda: None,
            trigger=DateTrigger(run_date=run_at, timezone=UTC),
            id="once",
            jobstore="durable",
        )

        first = adapter.seed(now=run_at - timedelta(minutes=1), epoch=1)
        restarted = adapter.seed(now=restart, epoch=2)

        assert first is not None
        assert first.logical_time == run_at
        assert jobstore.lookup_job("once").next_run_time is None
        assert restarted is not None
        assert restarted.logical_time == restart + timedelta(seconds=60)
        assert mock_send.call_count == 2
        adapter.shutdown(wait=True)

    def test_message_cursor_is_restored_before_running_jobs(self) -> None:
        anchor = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        handoff_time = anchor + timedelta(minutes=5)
        calls: list[str] = []
        scheduler, adapter = _scheduler()
        scheduler.add_job(
            lambda: calls.append("ran"),
            "interval",
            seconds=30,
            start_date=anchor,
            id="heartbeat",
            coalesce=False,
        )
        adapter.ensure_started(pending_jobs_reference_time=anchor)
        initial_entry = adapter._memory_cursor().jobs["id:heartbeat"]
        cursor = MemoryCursor(
            jobs={
                "id:heartbeat": CursorEntry(
                    job_id="heartbeat",
                    fingerprint=initial_entry.fingerprint,
                    state="scheduled",
                    next_run_time=handoff_time,
                    nominal_run_time=handoff_time,
                )
            }
        )

        result = adapter.process_wakeup(
            handoff_time,
            cursor=cursor,
            publish_next=False,
            now=handoff_time,
            epoch=2,
        )

        assert calls == ["ran"]
        assert result.due_job_ids == ("heartbeat",)
        assert result.next_wakeup_time == handoff_time + timedelta(seconds=30)
        adapter.shutdown(wait=True)


class TestStockSchedulerAdapter:
    def test_install_adopts_new_stock_scheduler_with_builder_identity(self) -> None:
        install_vercel_apscheduler_integration(options=_options())
        scheduler = BlockingScheduler(timezone=UTC)

        adapter = adopt_scheduler(scheduler)

        assert adapter.identity.scheduler_id == "scheduler-a"
        assert adapter.identity.wakeup_topic == "__aps_scheduler-a_wakeup"
        assert adapter.identity.start_topic == "__aps_scheduler-a_start"
        assert adapter.identity.consumer_group == "apscheduler-scheduler-a"

    @patch("vercel.integrations.apscheduler._adapter.vqs_sync.send")
    def test_process_wakeup_runs_due_job_and_publishes_successor(self, mock_send: Any) -> None:
        tick_at = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        calls: list[str] = []
        mock_send.return_value = "message-2"
        scheduler, adapter = _scheduler()

        def task() -> None:
            calls.append("ran")

        scheduler.add_job(
            task,
            "interval",
            seconds=30,
            start_date=tick_at,
            id="job-1",
        )

        result = adapter.process_wakeup(tick_at, publish_next=True, now=tick_at)

        assert calls == ["ran"]
        assert result.due_job_ids == ("job-1",)
        assert result.next_wakeup_time == tick_at + timedelta(seconds=30)
        assert result.published_wakeup is not None
        assert result.published_wakeup.message_id == "message-2"
        assert mock_send.call_args.kwargs["idempotency_key"] == (
            "aps:scheduler-a:2026-04-09T12:00:30+00:00"
        )
        adapter.shutdown(wait=True)

    @patch("vercel.integrations.apscheduler._adapter.vqs_sync.send")
    def test_successor_publish_failure_fails_the_delivery(self, mock_send: Any) -> None:
        tick_at = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        mock_send.side_effect = RuntimeError("queue unavailable")
        scheduler, adapter = _scheduler()
        scheduler.add_job(
            lambda: None,
            "interval",
            seconds=30,
            start_date=tick_at,
            id="job-1",
        )

        with pytest.raises(RuntimeError, match="queue unavailable"):
            adapter.process_wakeup(tick_at, publish_next=True, now=tick_at)

        adapter.shutdown(wait=True)

    @patch("vercel.integrations.apscheduler._adapter.vqs_sync.send")
    def test_duplicate_successor_is_safe_to_acknowledge(self, mock_send: Any) -> None:
        tick_at = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        mock_send.side_effect = DuplicateIdempotencyKeyError("already exists")
        scheduler, adapter = _scheduler()
        scheduler.add_job(
            lambda: None,
            "interval",
            seconds=30,
            start_date=tick_at,
            id="job-1",
        )

        result = adapter.process_wakeup(tick_at, publish_next=True, now=tick_at)

        assert result.published_wakeup is not None
        assert result.published_wakeup.message_id is None
        adapter.shutdown(wait=True)

    @patch("vercel.integrations.apscheduler._adapter.vqs_sync.send")
    def test_durable_jobstore_caps_far_future_wakeup_for_polling(
        self,
        mock_send: Any,
    ) -> None:
        now = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        scheduler, adapter = _scheduler()
        scheduler.add_jobstore(
            DurableLikeJobStore(now + timedelta(hours=4)),
            alias="durable",
        )

        result = adapter.seed(now=now)

        assert result is not None
        assert result.logical_time == now + timedelta(seconds=60)
        assert mock_send.call_args.kwargs["idempotency_key"] == (
            "aps:scheduler-a:2026-04-09T12:01:00+00:00"
        )
        adapter.shutdown(wait=True)

    @patch("vercel.integrations.apscheduler._adapter.vqs_sync.send")
    def test_durable_jobstore_with_no_jobs_still_polls(
        self,
        mock_send: Any,
    ) -> None:
        now = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        scheduler, adapter = _scheduler()
        scheduler.add_jobstore(DurableLikeJobStore(None), alias="durable")

        result = adapter.seed(now=now)

        assert result is not None
        assert result.logical_time == now + timedelta(seconds=60)
        assert mock_send.called
        adapter.shutdown(wait=True)

    def test_cursor_carries_memory_next_run_time_across_cold_start(self) -> None:
        tick_at = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        calls: list[str] = []

        first_scheduler, first_adapter = _scheduler()

        def task() -> None:
            calls.append("ran")

        first_scheduler.add_job(
            task,
            "interval",
            seconds=30,
            start_date=tick_at,
            id="job-1",
        )
        first_result = first_adapter.process_wakeup(tick_at, publish_next=False, now=tick_at)
        first_cursor = first_adapter._memory_cursor()
        first_adapter.shutdown(wait=True)

        second_scheduler, second_adapter = _scheduler()
        second_scheduler.add_job(
            task,
            "interval",
            seconds=30,
            start_date=tick_at,
            id="job-1",
        )
        second_result = second_adapter.process_wakeup(
            tick_at + timedelta(seconds=30),
            cursor=first_cursor,
            publish_next=False,
            now=tick_at + timedelta(seconds=30),
        )

        assert first_result.next_wakeup_time == tick_at + timedelta(seconds=30)
        assert second_result.due_job_ids == ("job-1",)
        assert calls == ["ran", "ran"]
        second_adapter.shutdown(wait=True)

    def test_deleted_memory_job_is_ignored_from_old_cursor(self) -> None:
        tick_at = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        old_cursor = MemoryCursor(
            jobs={
                "id:cleanup": CursorEntry(
                    job_id="cleanup",
                    fingerprint="sha256:old",
                    state="scheduled",
                    next_run_time=tick_at,
                )
            }
        )

        _second_scheduler, second_adapter = _scheduler()
        result = second_adapter.process_wakeup(
            tick_at,
            cursor=old_cursor,
            publish_next=False,
            now=tick_at,
        )

        assert result.due_job_ids == ()
        assert result.next_wakeup_time is None
        second_adapter.shutdown(wait=True)

    def test_new_memory_job_not_in_old_cursor_is_calculated_from_current_code(self) -> None:
        tick_at = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        calls: list[str] = []
        first_scheduler, first_adapter = _scheduler()
        first_scheduler.add_job(
            lambda: None,
            "cron",
            hour=13,
            timezone=UTC,
            id="cleanup",
        )
        first_adapter.ensure_started(pending_jobs_reference_time=tick_at)
        old_cursor = first_adapter._memory_cursor()
        first_adapter.shutdown(wait=True)

        second_scheduler, second_adapter = _scheduler()

        def heartbeat() -> None:
            calls.append("heartbeat")

        second_scheduler.add_job(
            heartbeat,
            "interval",
            seconds=30,
            start_date=tick_at,
            id="heartbeat",
        )
        result = second_adapter.process_wakeup(
            tick_at,
            cursor=old_cursor,
            publish_next=False,
            now=tick_at,
        )

        assert result.due_job_ids == ("heartbeat",)
        assert calls == ["heartbeat"]
        second_adapter.shutdown(wait=True)

    def test_same_id_reschedule_ignores_old_cursor_fingerprint(self) -> None:
        reference = datetime(2026, 4, 9, 11, 0, tzinfo=UTC)
        new_run = datetime(2026, 4, 9, 13, 0, tzinfo=UTC)
        calls: list[str] = []
        first_scheduler, first_adapter = _scheduler()

        first_scheduler.add_job(
            lambda: None,
            "cron",
            hour=12,
            timezone=UTC,
            id="sync",
        )
        first_adapter.ensure_started(pending_jobs_reference_time=reference)
        old_cursor = first_adapter._memory_cursor()
        first_adapter.shutdown(wait=True)

        second_scheduler, second_adapter = _scheduler()

        def new_task() -> None:
            calls.append("new")

        second_scheduler.add_job(
            new_task,
            "cron",
            hour=13,
            timezone=UTC,
            id="sync",
        )
        result = second_adapter.process_wakeup(
            new_run,
            cursor=old_cursor,
            publish_next=False,
            now=new_run,
        )

        assert result.due_job_ids == ("sync",)
        assert calls == ["new"]
        second_adapter.shutdown(wait=True)

    def test_memory_interval_without_start_date_is_rejected(self) -> None:
        tick_at = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        scheduler, adapter = _scheduler()
        scheduler.add_job(lambda: None, "interval", seconds=30, id="interval")

        with pytest.raises(RuntimeError, match="without an explicit start_date"):
            adapter.process_wakeup(tick_at, publish_next=False, now=tick_at)

    def test_memory_interval_trigger_object_is_rejected(self) -> None:
        tick_at = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        scheduler, adapter = _scheduler()
        scheduler.add_job(
            lambda: None,
            trigger=IntervalTrigger(seconds=30, start_date=tick_at, timezone=UTC),
            id="interval",
        )

        with pytest.raises(RuntimeError, match="Pre-built IntervalTrigger objects"):
            adapter.process_wakeup(tick_at, publish_next=False, now=tick_at)

    def test_memory_jobs_require_explicit_ids(self) -> None:
        tick_at = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        scheduler, adapter = _scheduler()
        scheduler.add_job(lambda: None, "cron", minute="*", timezone=UTC)

        with pytest.raises(TypeError, match="require an explicit stable id"):
            adapter.process_wakeup(tick_at, publish_next=False, now=tick_at)

    def test_memory_date_trigger_is_rejected(self) -> None:
        tick_at = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        scheduler, adapter = _scheduler()
        scheduler.add_job(
            lambda: None,
            trigger=DateTrigger(run_date=tick_at, timezone=UTC),
            id="once",
        )

        with pytest.raises(TypeError, match="finite DateTrigger"):
            adapter.process_wakeup(tick_at, publish_next=False, now=tick_at)

    def test_memory_jitter_is_deterministic_across_cold_starts(self) -> None:
        tick_at = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        next_run_times: list[datetime] = []
        nominal_run_times: list[datetime | None] = []

        def task() -> None:
            return None

        for _ in range(2):
            scheduler, adapter = _scheduler()
            scheduler.add_job(
                task,
                "cron",
                minute="*",
                jitter=30,
                timezone=UTC,
                id="jittered",
            )
            adapter.ensure_started(pending_jobs_reference_time=tick_at)
            cursor = adapter._memory_cursor()
            entry = cursor.jobs["id:jittered"]
            assert entry.next_run_time is not None
            next_run_times.append(entry.next_run_time)
            nominal_run_times.append(entry.nominal_run_time)
            adapter.shutdown(wait=True)

        assert next_run_times[0] == next_run_times[1]
        assert nominal_run_times == [tick_at, tick_at]
        assert tick_at <= next_run_times[0] <= tick_at + timedelta(seconds=30)

    def test_deploy_seed_preserves_pending_jittered_occurrence(self) -> None:
        nominal_time = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)

        def task() -> None:
            return None

        first_scheduler, first_adapter = _scheduler()
        first_scheduler.add_job(
            task,
            "cron",
            minute="*",
            jitter=30,
            timezone=UTC,
            id="jittered",
        )
        first_adapter.ensure_started(pending_jobs_reference_time=nominal_time)
        first_entry = first_adapter._memory_cursor().jobs["id:jittered"]
        first_adapter.shutdown(wait=True)

        assert first_entry.next_run_time is not None
        assert first_entry.next_run_time > nominal_time
        redeploy_time = nominal_time + (first_entry.next_run_time - nominal_time) / 2
        second_scheduler, second_adapter = _scheduler()
        second_scheduler.add_job(
            task,
            "cron",
            minute="*",
            jitter=30,
            timezone=UTC,
            id="jittered",
        )
        second_adapter.ensure_started(pending_jobs_reference_time=redeploy_time)
        second_entry = second_adapter._memory_cursor().jobs["id:jittered"]

        assert second_entry.nominal_run_time == nominal_time
        assert second_entry.next_run_time == first_entry.next_run_time
        second_adapter.shutdown(wait=True)

    def test_jittered_interval_keeps_anchored_nominal_cadence(self) -> None:
        anchor = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        calls: list[str] = []
        scheduler, adapter = _scheduler()
        scheduler.add_job(
            lambda: calls.append("ran"),
            "interval",
            seconds=30,
            start_date=anchor,
            jitter=10,
            id="jittered-interval",
        )
        adapter.ensure_started(pending_jobs_reference_time=anchor)
        first_cursor = adapter._memory_cursor()
        first_actual = first_cursor.jobs["id:jittered-interval"].next_run_time
        assert first_actual is not None

        result = adapter.process_wakeup(
            first_actual,
            publish_next=False,
            now=first_actual,
        )
        next_cursor = adapter._memory_cursor()
        next_entry = next_cursor.jobs["id:jittered-interval"]

        assert calls == ["ran"]
        assert result.due_job_ids == ("jittered-interval",)
        next_nominal = next_entry.nominal_run_time
        next_actual = next_entry.next_run_time
        assert next_nominal is not None
        assert next_nominal == anchor + timedelta(seconds=30)
        assert next_actual is not None
        assert next_nominal <= next_actual
        assert next_actual <= next_nominal + timedelta(seconds=10)
        adapter.shutdown(wait=True)

    def test_jobs_sharing_nominal_time_keep_distinct_jittered_wakes(self) -> None:
        nominal_time = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        calls: list[str] = []
        scheduler, adapter = _scheduler()
        for job_id in ("first", "second"):
            scheduler.add_job(
                lambda value=job_id: calls.append(value),
                "cron",
                minute="*",
                jitter=30,
                timezone=UTC,
                id=job_id,
            )
        adapter.ensure_started(pending_jobs_reference_time=nominal_time)
        cursor = adapter._memory_cursor()
        entries = [cursor.jobs["id:first"], cursor.jobs["id:second"]]
        first_actual = entries[0].next_run_time
        second_actual = entries[1].next_run_time
        assert first_actual is not None
        assert second_actual is not None
        actual_times = sorted((first_actual, second_actual))

        assert {entry.nominal_run_time for entry in entries} == {nominal_time}
        assert actual_times[0] != actual_times[1]

        result = adapter.process_wakeup(
            actual_times[0],
            publish_next=False,
            now=actual_times[0],
        )

        assert len(calls) == 1
        assert result.next_wakeup_time == actual_times[1]
        adapter.shutdown(wait=True)

    def test_jitter_is_capped_before_following_nominal_occurrence(self) -> None:
        anchor = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        scheduler, adapter = _scheduler()
        scheduler.add_job(
            lambda: None,
            "interval",
            seconds=5,
            start_date=anchor,
            jitter=30,
            id="frequent",
        )
        adapter.ensure_started(pending_jobs_reference_time=anchor)
        entry = adapter._memory_cursor().jobs["id:frequent"]

        assert entry.nominal_run_time == anchor
        assert entry.next_run_time is not None
        assert anchor <= entry.next_run_time < anchor + timedelta(seconds=5)
        adapter.shutdown(wait=True)

    def test_custom_executor_is_rejected_before_acknowledge_is_possible(self) -> None:
        tick_at = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        scheduler = BlockingScheduler(
            timezone=UTC,
            executors={"default": ThreadPoolExecutor(1)},
        )
        adapter = adopt_scheduler(scheduler, _options())
        scheduler.add_job(
            lambda: None,
            "cron",
            minute="*",
            timezone=UTC,
            id="threaded",
        )

        with pytest.raises(TypeError, match="requires the inline default executor"):
            adapter.process_wakeup(tick_at, publish_next=False, now=tick_at)


class TestWatchdog:
    @pytest.mark.asyncio
    @patch("vercel.integrations.apscheduler._adapter.vqs_sync.send")
    async def test_get_seeds_current_scheduler_chain(self, mock_send: Any) -> None:
        scheduler, adapter = _scheduler()
        scheduler.add_job(
            lambda: None,
            "cron",
            minute="*",
            timezone=UTC,
            id="cleanup",
        )
        responses: list[dict[str, Any]] = []

        async def send(message: dict[str, Any]) -> None:
            responses.append(message)

        app = APSchedulerWatchdogAsgiApp(adapter)
        await app(
            {"type": "http", "method": "GET", "headers": []},
            None,
            send,
        )

        assert responses[0]["status"] == 204
        assert mock_send.call_args.args[1]["kind"] == "watchdog"

    @pytest.mark.asyncio
    async def test_get_requires_cron_secret_when_configured(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("CRON_SECRET", "secret")
        adapter = Mock()
        responses: list[dict[str, Any]] = []

        async def send(message: dict[str, Any]) -> None:
            responses.append(message)

        app = APSchedulerWatchdogAsgiApp(adapter)
        await app(
            {"type": "http", "method": "GET", "headers": []},
            None,
            send,
        )

        assert responses[0]["status"] == 401
        adapter.seed.assert_not_called()

    @pytest.mark.asyncio
    async def test_rejects_non_get_requests(self) -> None:
        adapter = Mock()
        responses: list[dict[str, Any]] = []

        async def send(message: dict[str, Any]) -> None:
            responses.append(message)

        app = APSchedulerWatchdogAsgiApp(adapter)
        await app(
            {"type": "http", "method": "POST", "headers": []},
            None,
            send,
        )

        assert responses[0]["status"] == 405
        adapter.seed.assert_not_called()
