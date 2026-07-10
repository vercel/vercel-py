from __future__ import annotations

from typing import Any

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

pytest.importorskip("apscheduler")

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from vercel.integrations.apscheduler import (
    MemoryCursor,
    VercelAPSchedulerOptions,
    WakeupPayload,
    adopt_scheduler,
    install_vercel_apscheduler_integration,
)
from vercel.integrations.apscheduler._adapter import SchedulerAdapter
from vercel.integrations.apscheduler._payload import CursorEntry
from vercel.integrations.apscheduler._seed import main as seed_main


def _options() -> VercelAPSchedulerOptions:
    return VercelAPSchedulerOptions(
        scheduler_id="scheduler-a",
        wakeup_topic="__aps_scheduler_a",
        consumer_group="apscheduler",
        max_delay_seconds=23 * 60 * 60,
        retention_seconds=24 * 60 * 60,
    )


def _scheduler() -> tuple[BlockingScheduler, SchedulerAdapter]:
    scheduler = BlockingScheduler(timezone=UTC)
    return scheduler, adopt_scheduler(scheduler, _options())


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


class TestStockSchedulerAdapter:
    def test_install_attaches_queue_subscription_to_stock_scheduler(self) -> None:
        install_vercel_apscheduler_integration(options=_options())
        scheduler = BlockingScheduler(timezone=UTC)

        subscriptions = scheduler.get_queue_subscriptions()

        assert subscriptions == [
            {
                "topic": "__aps_scheduler_a",
                "retry_after_seconds": 30,
                "max_concurrency": 1,
            }
        ]

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
            trigger=IntervalTrigger(seconds=30, start_date=tick_at, timezone=UTC),
            id="job-1",
        )

        result = adapter.process_wakeup(tick_at, publish_next=True, now=tick_at)

        assert calls == ["ran"]
        assert result.due_job_ids == ("job-1",)
        assert result.next_wakeup_time == tick_at + timedelta(seconds=30)
        assert result.published_wakeup is not None
        assert result.published_wakeup.message_id == "message-2"
        assert mock_send.call_args.kwargs["idempotency_key"] == (
            "aps:v1:scheduler-a:2026-04-09T12:00:30+00:00"
        )
        adapter.shutdown(wait=True)

    def test_cursor_carries_memory_next_run_time_across_cold_start(self) -> None:
        tick_at = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        calls: list[str] = []

        first_scheduler, first_adapter = _scheduler()

        def task() -> None:
            calls.append("ran")

        first_scheduler.add_job(
            task,
            trigger=IntervalTrigger(seconds=30, start_date=tick_at, timezone=UTC),
            id="job-1",
        )
        first_result = first_adapter.process_wakeup(tick_at, publish_next=False, now=tick_at)
        first_cursor = first_adapter._memory_cursor()
        first_adapter.shutdown(wait=True)

        second_scheduler, second_adapter = _scheduler()
        second_scheduler.add_job(
            task,
            trigger=IntervalTrigger(seconds=30, start_date=tick_at, timezone=UTC),
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
        first_scheduler, first_adapter = _scheduler()
        first_scheduler.add_job(
            lambda: None,
            trigger=DateTrigger(run_date=tick_at, timezone=UTC),
            id="cleanup",
        )
        first_adapter.ensure_started(pending_jobs_reference_time=tick_at)
        old_cursor = first_adapter._memory_cursor()
        first_adapter.shutdown(wait=True)

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

    def test_same_id_reschedule_ignores_old_cursor_fingerprint(self) -> None:
        old_run = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
        new_run = datetime(2026, 4, 9, 12, 30, tzinfo=UTC)
        calls: list[str] = []
        first_scheduler, first_adapter = _scheduler()

        first_scheduler.add_job(
            lambda: None,
            trigger=DateTrigger(run_date=old_run, timezone=UTC),
            id="sync",
        )
        first_adapter.ensure_started(pending_jobs_reference_time=old_run)
        old_cursor = first_adapter._memory_cursor()
        first_adapter.shutdown(wait=True)

        second_scheduler, second_adapter = _scheduler()

        def new_task() -> None:
            calls.append("new")

        second_scheduler.add_job(
            new_task,
            trigger=DateTrigger(run_date=new_run, timezone=UTC),
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


class TestSeedCli:
    @patch("vercel.integrations.apscheduler._adapter.vqs_sync.send")
    def test_seed_cli_imports_scheduler_and_sends_first_wakeup(
        self,
        mock_send: Any,
        tmp_path: Any,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        mock_send.return_value = "message-1"
        schedule = tmp_path / "schedule.py"
        schedule.write_text(
            """
from datetime import UTC, datetime
from apscheduler.schedulers.blocking import BlockingScheduler

scheduler = BlockingScheduler(timezone=UTC)

def task(): pass

scheduler.add_job(
    task,
    'date',
    run_date=datetime(2026, 4, 9, 12, 0, tzinfo=UTC),
    id='cleanup',
)
""".lstrip(),
            encoding="utf-8",
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        monkeypatch.setenv("VERCEL_APSCHEDULER_SUBSCRIBER_NAME", "schedule_scheduler")
        monkeypatch.setenv("VERCEL_APSCHEDULER_TOPIC", "__aps_schedule_scheduler")
        monkeypatch.setenv("VERCEL_APSCHEDULER_CONSUMER", "consumer")

        exit_code = seed_main([
            "--entrypoint",
            "schedule:scheduler",
            "--now",
            "2026-04-09T11:59:00+00:00",
        ])

        assert exit_code == 0
        assert mock_send.call_args.args[0] == "__aps_schedule_scheduler"
        assert mock_send.call_args.kwargs["idempotency_key"] == (
            "aps:v1:schedule_scheduler:2026-04-09T12:00:00+00:00"
        )
