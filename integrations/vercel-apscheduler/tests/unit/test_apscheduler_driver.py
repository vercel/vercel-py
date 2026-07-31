from __future__ import annotations

from datetime import datetime, timedelta, timezone

from vercel.integrations.apscheduler._payload import StartPayload, WakeupPayload
from vercel.integrations.apscheduler._time import canonical_scheduled_logical_time

UTC = timezone.utc


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


def test_far_future_wakes_bridge_without_an_idle_poll() -> None:
    now = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
    due = now + timedelta(seconds=70)

    first = canonical_scheduled_logical_time(
        due,
        now=now,
        max_delay_seconds=30,
    )
    second = canonical_scheduled_logical_time(
        due,
        now=first,
        max_delay_seconds=30,
    )
    final = canonical_scheduled_logical_time(
        due,
        now=second,
        max_delay_seconds=30,
    )

    assert (first, second, final) == (
        now + timedelta(seconds=10),
        now + timedelta(seconds=40),
        due,
    )
