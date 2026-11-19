from __future__ import annotations

from typing import Any

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from apscheduler.triggers.calendarinterval import CalendarIntervalTrigger
from apscheduler.triggers.combining import AndTrigger, OrTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from vercel.integrations.apscheduler._triggers import (
    MINUTE,
    CronCover,
    cover,
    run_time_in_minute,
    runs_more_than_once_a_minute,
    without_jitter,
)

NEW_YORK = ZoneInfo("America/New_York")
START = datetime(2026, 1, 1, tzinfo=UTC)
_BOUNDS = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 6)]


def _allowed(cron: str) -> list[set[int]]:
    fields = []
    for expression, (low, high) in zip(cron.split(), _BOUNDS, strict=True):
        values: set[int] = set()
        for item in expression.split(","):
            body, _, step = item.partition("/")
            if body == "*":
                first, last = low, high
            elif "-" in body:
                first, last = map(int, body.split("-"))
            else:
                first = last = int(body)
            values.update(range(first, last + 1, int(step) if step else 1))
        fields.append(values)
    return fields


def _fires(covered: CronCover, minute: datetime, allowed: list[set[int]]) -> bool:
    local = minute.astimezone(ZoneInfo(covered.timezone))
    actual = (local.minute, local.hour, local.day, local.month, (local.weekday() + 1) % 7)
    return all(value in values for value, values in zip(actual, allowed, strict=True))


def _stock_run_times(trigger: Any, end: datetime) -> Iterator[datetime]:
    fire = trigger.get_next_fire_time(None, START)
    while fire is not None and fire < end:
        if fire >= START:
            yield fire.astimezone(UTC)
        # An AND trigger given its previous fire time never advances when its
        # triggers disagree, so ask it for the next time after this one.
        previous = None if isinstance(trigger, AndTrigger) else fire
        fire = trigger.get_next_fire_time(previous, fire + timedelta(microseconds=1))


EQUIVALENCE_CASES = [
    pytest.param(CronTrigger(hour=4, timezone=UTC), 30, "0 4 * * *", id="daily"),
    pytest.param(
        CronTrigger(minute="*/15", hour="1-3", timezone=NEW_YORK),
        80,
        "*/15 1-3 * * *",
        id="every-15-minutes-across-dst",
    ),
    pytest.param(
        CronTrigger(day_of_week="mon-fri", hour=9, minute=30, timezone=NEW_YORK),
        30,
        "30 9 * * 1-5",
        id="weekdays",
    ),
    pytest.param(
        CronTrigger(day_of_week="*/2", hour=6, timezone=UTC),
        30,
        "0 6 * * 0-1,3,5",
        id="weekday-step-as-list",
    ),
    pytest.param(
        CronTrigger(day="last", hour=23, timezone=UTC), 120, "0 23 28-31 * *", id="last-day"
    ),
    pytest.param(
        CronTrigger(day="3rd fri", hour=12, timezone=UTC),
        60,
        "0 12 15-21 * *",
        id="nth-weekday",
    ),
    pytest.param(
        CronTrigger(day="last sun", hour=12, timezone=UTC),
        60,
        "0 12 22-31 * *",
        id="last-weekday",
    ),
    pytest.param(
        CronTrigger(day=13, day_of_week="fri", timezone=UTC),
        75,
        "0 0 13 * *",
        id="day-and-weekday",
    ),
    pytest.param(
        CronTrigger(week="1-10", day_of_week="mon", hour=8, timezone=UTC),
        90,
        "0 8 * * 1",
        id="week",
    ),
    pytest.param(CronTrigger(year=2026, month=1, day=1, timezone=UTC), 400, "0 0 1 1 *", id="year"),
    pytest.param(
        CronTrigger(hour="*", start_date="2026-01-10", end_date="2026-01-20", timezone=UTC),
        30,
        "0 * * * *",
        id="start-and-end-dates",
    ),
    pytest.param(
        CronTrigger(month="jan-mar", day=1, timezone=UTC), 120, "0 0 1 1-3 *", id="months"
    ),
    pytest.param(
        IntervalTrigger(minutes=5, start_date=datetime(2026, 1, 1, 0, 2, tzinfo=UTC)),
        2,
        "2-59/5 * * * *",
        id="interval-dividing-the-hour",
    ),
    pytest.param(
        IntervalTrigger(minutes=5, start_date=datetime(2026, 1, 1, 0, 2, 30, tzinfo=UTC)),
        2,
        "2-59/5 * * * *",
        id="interval-with-seconds",
    ),
    pytest.param(
        IntervalTrigger(minutes=7, start_date=START), 2, "* * * * *", id="interval-7-minutes"
    ),
    pytest.param(
        IntervalTrigger(seconds=90, start_date=START), 2, "* * * * *", id="interval-90-seconds"
    ),
    pytest.param(
        IntervalTrigger(minutes=90, start_date=datetime(2026, 1, 1, 0, 15, tzinfo=UTC)),
        5,
        "15,45 * * * *",
        id="interval-90-minutes",
    ),
    pytest.param(
        IntervalTrigger(hours=6, start_date=datetime(2026, 1, 1, 1, 30, tzinfo=UTC)),
        30,
        "30 1-23/6 * * *",
        id="interval-dividing-the-day",
    ),
    pytest.param(
        IntervalTrigger(hours=5, start_date=datetime(2026, 1, 1, 1, 30, tzinfo=UTC)),
        30,
        "30 * * * *",
        id="interval-5-hours",
    ),
    pytest.param(
        IntervalTrigger(days=1, start_date=datetime(2026, 1, 1, 9, tzinfo=NEW_YORK)),
        120,
        "0 14 * * *",
        id="interval-days-in-utc",
    ),
    pytest.param(
        IntervalTrigger(weeks=2, start_date=datetime(2026, 1, 3, 9, tzinfo=UTC)),
        120,
        "0 9 * * 6",
        id="interval-weeks",
    ),
    pytest.param(
        CalendarIntervalTrigger(
            months=1, hour=3, minute=15, start_date="2026-01-31", timezone=NEW_YORK
        ),
        120,
        "15 3 * * *",
        id="calendar-interval",
    ),
    pytest.param(
        DateTrigger(datetime(2026, 3, 5, 9, 30, tzinfo=NEW_YORK)),
        70,
        "30 14 5 3 *",
        id="date",
    ),
    pytest.param(
        OrTrigger([
            CronTrigger(hour=3, timezone=UTC),
            CronTrigger(hour=15, minute=30, timezone=UTC),
        ]),
        10,
        "0,30 3,15 * * *",
        id="or",
    ),
    pytest.param(
        AndTrigger([
            CronTrigger(hour="*/2", timezone=UTC),
            CronTrigger(day_of_week="sat", hour="*", timezone=UTC),
        ]),
        15,
        "0 */2 * * 6",
        id="and",
    ),
]


@pytest.mark.parametrize(("trigger", "days", "cron"), EQUIVALENCE_CASES)
def test_firings_run_exactly_the_stock_run_times(trigger: Any, days: int, cron: str) -> None:
    covered = cover(trigger)
    assert covered.cron == cron
    allowed = _allowed(covered.cron)
    end = START + timedelta(days=days)
    stock = list(_stock_run_times(trigger, end))
    assert stock

    # Every stock run falls in a minute the deployed cron fires in.
    for run_time in stock:
        assert _fires(covered, run_time.replace(second=0, microsecond=0), allowed), run_time

    # Asked minute by minute, the firings run exactly the stock run times.
    runs = []
    minute = START
    while minute < end:
        if _fires(covered, minute, allowed):
            due = run_time_in_minute(trigger, minute)
            if due is not None:
                runs.append(due.astimezone(UTC))
        minute += MINUTE
    assert runs == stock


def test_uses_the_trigger_timezone_name() -> None:
    assert cover(CronTrigger(hour=9, timezone=NEW_YORK)) == CronCover(
        "0 9 * * *", "America/New_York"
    )


def test_a_custom_trigger_fires_every_minute() -> None:
    class EveryOtherMinute(IntervalTrigger):
        pass

    assert cover(EveryOtherMinute(minutes=2, start_date=START)) == CronCover("* * * * *", "UTC")


def test_a_cron_in_a_timezone_without_a_name_fires_every_minute() -> None:
    offset = timezone(timedelta(hours=2))
    assert cover(CronTrigger(hour=9, timezone=offset)) == CronCover("* * * * *", "UTC")


def test_combined_triggers_in_two_timezones_fire_every_minute() -> None:
    trigger = OrTrigger([CronTrigger(hour=3, timezone=UTC), CronTrigger(hour=3, timezone=NEW_YORK)])
    assert cover(trigger) == CronCover("* * * * *", "UTC")


@pytest.mark.parametrize(
    ("trigger", "expected"),
    [
        (CronTrigger(second="*/30", timezone=UTC), True),
        (CronTrigger(timezone=UTC), True),
        (CronTrigger(second=30, timezone=UTC), False),
        (CronTrigger(minute="*", timezone=UTC), False),
        (IntervalTrigger(seconds=30, start_date=START), True),
        (IntervalTrigger(seconds=60, start_date=START), False),
        (OrTrigger([CronTrigger(hour=1), IntervalTrigger(seconds=1)]), True),
        (AndTrigger([CronTrigger(hour=1), IntervalTrigger(seconds=1)]), False),
    ],
)
def test_detects_triggers_that_run_more_than_once_a_minute(trigger: Any, *, expected: bool) -> None:
    assert runs_more_than_once_a_minute(trigger) is expected


def test_without_jitter_strips_jitter_from_a_copy() -> None:
    child = CronTrigger(hour=3, jitter=30, timezone=UTC)
    trigger = OrTrigger([child], jitter=10)

    stripped = without_jitter(trigger)

    assert stripped.jitter is None
    assert stripped.triggers[0].jitter is None
    assert trigger.jitter == 10
    assert child.jitter == 30


def test_a_minute_without_a_run_time_has_none() -> None:
    trigger = CronTrigger(hour=3, timezone=UTC)
    assert run_time_in_minute(trigger, datetime(2026, 1, 1, 3, 1, tzinfo=UTC)) is None
    assert run_time_in_minute(trigger, datetime(2026, 1, 1, 3, tzinfo=UTC)) == datetime(
        2026, 1, 1, 3, tzinfo=UTC
    )


def test_a_past_date_never_runs() -> None:
    trigger = DateTrigger(datetime(2025, 3, 5, 9, 30, tzinfo=UTC))
    assert run_time_in_minute(trigger, datetime(2026, 3, 5, 9, 30, tzinfo=UTC)) is None
