"""Deploy APScheduler triggers as Vercel cron schedules.

A Vercel schedule fires at most once a minute, on a five-field cron. Each job
deploys as the narrowest cron that fires in every minute its trigger can run
in, and each firing asks the trigger whether it runs in that minute. The
answer depends only on the trigger and the minute, so every function instance
agrees on it.
"""

from __future__ import annotations

from typing import Any

import copy
import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, tzinfo

from ._imports import (
    AllExpression,
    AndTrigger,
    CalendarIntervalTrigger,
    CronTrigger,
    DateTrigger,
    IntervalTrigger,
    LastDayOfMonthExpression,
    OrTrigger,
    RangeExpression,
    WeekdayPositionExpression,
)

MINUTE = timedelta(minutes=1)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Crontab order, with the bounds Vercel accepts. Day of week counts from
# Sunday, as in crontab.
_FIELDS = ("minute", "hour", "day", "month", "day_of_week")
_BOUNDS = {
    "minute": (0, 59),
    "hour": (0, 23),
    "day": (1, 31),
    "month": (1, 12),
    "day_of_week": (0, 6),
}

__all__ = [
    "CronCover",
    "cover",
    "run_time_in_minute",
    "runs_more_than_once_a_minute",
    "without_jitter",
]


@dataclass(frozen=True)
class CronCover:
    """A cron that fires in every minute a trigger can run in.

    Attributes:
        cron: Five-field crontab expression.
        timezone: IANA timezone `cron` is evaluated in.

    """

    cron: str
    timezone: str


@dataclass(frozen=True)
class _Fields:
    """The values a cron allows in each field, keyed like `_FIELDS`."""

    timezone: str
    values: dict[str, frozenset[int]]


def _every_minute(timezone: str = "UTC") -> _Fields:
    return _Fields(
        timezone,
        {name: frozenset(range(low, high + 1)) for name, (low, high) in _BOUNDS.items()},
    )


def cover(trigger: Any) -> CronCover:
    """Return the narrowest cron that fires in every minute `trigger` can run in.

    A trigger whose times can't be bounded by a cron, such as a custom trigger
    class, deploys as every minute.
    """
    return _render(_cover(trigger) or _every_minute())


def runs_more_than_once_a_minute(trigger: Any) -> bool:
    """Whether `trigger` can run twice in one minute, which no schedule can fire."""
    kind = type(trigger)
    if kind is CronTrigger:
        second = next(field for field in trigger.fields if field.name == "second")
        return len(_expression_values(second, 0, 59)) > 1
    if kind is IntervalTrigger:
        return trigger.interval_length < MINUTE.total_seconds()
    if kind is OrTrigger:
        return any(runs_more_than_once_a_minute(child) for child in trigger.triggers)
    if kind is AndTrigger:
        return all(runs_more_than_once_a_minute(child) for child in trigger.triggers)
    return False


def without_jitter(trigger: Any) -> Any:
    """Copy `trigger` so that its fire times are not randomized.

    Jitter makes a trigger's next fire time random, so two instances would
    disagree on whether a firing's minute runs the job.
    """
    try:
        stripped = copy.deepcopy(trigger)
    except Exception:  # noqa: BLE001 - a custom trigger may not copy
        return trigger
    _strip_jitter(stripped)
    return stripped


def _strip_jitter(trigger: Any) -> None:
    if getattr(trigger, "jitter", None) is not None:
        trigger.jitter = None
    for child in getattr(trigger, "triggers", ()):
        _strip_jitter(child)


def run_time_in_minute(trigger: Any, minute: datetime) -> datetime | None:
    """Return the first time in `[minute, minute + 1 minute)` that `trigger` fires at."""
    end = minute + MINUTE
    fire = trigger.get_next_fire_time(None, minute)
    # A calendar interval and a date start from their start date, however long ago.
    while fire is not None and fire < minute:
        following = trigger.get_next_fire_time(fire, minute)
        if following is None or following <= fire:
            return None
        fire = following
    if fire is not None and fire < end:
        return fire
    return None


def _cover(trigger: Any) -> _Fields | None:
    kind = type(trigger)
    if kind is CronTrigger:
        return _cover_cron(trigger)
    if kind is IntervalTrigger:
        return _cover_interval(trigger)
    if kind is DateTrigger:
        return _cover_date(trigger)
    if CalendarIntervalTrigger is not None and kind is CalendarIntervalTrigger:
        return _cover_calendar_interval(trigger)
    if kind is OrTrigger:
        covers = [_cover(child) for child in trigger.triggers]
        if any(cover is None for cover in covers):
            return None
        return _combine([cover for cover in covers if cover is not None], union=True)
    if kind is AndTrigger:
        # A child that can't be bounded allows every minute, so it doesn't
        # narrow the others.
        return _combine(
            [cover for child in trigger.triggers if (cover := _cover(child)) is not None],
            union=False,
        )
    return None


def _combine(covers: list[_Fields], *, union: bool) -> _Fields | None:
    """Fields allowing the union, or the intersection, of `covers`' minutes.

    Combining field by field gives a superset of the combined minutes, so it
    is still a cover.
    """
    if not covers or any(cover.timezone != covers[0].timezone for cover in covers):
        return None
    values: dict[str, frozenset[int]] = {}
    for name in _FIELDS:
        sets = [cover.values[name] for cover in covers]
        values[name] = frozenset.union(*sets) if union else frozenset.intersection(*sets)
        if not values[name]:
            return None
    return _Fields(covers[0].timezone, values)


def _cover_cron(trigger: Any) -> _Fields | None:
    zone = _zone_name(trigger.timezone)
    if zone is None:
        return None
    fields = {field.name: field for field in trigger.fields}
    values = {name: _expression_values(fields[name], *_BOUNDS[name]) for name in _FIELDS}
    # APScheduler counts weekdays from Monday.
    values["day_of_week"] = frozenset((value + 1) % 7 for value in values["day_of_week"])
    return _Fields(zone, values)


def _expression_values(field: Any, low: int, high: int) -> frozenset[int]:
    """Every value `field` can take, or a superset of them."""
    values: set[int] = set()
    for expression in field.expressions:
        if isinstance(expression, LastDayOfMonthExpression):
            values.update(range(28, 32))
        elif isinstance(expression, WeekdayPositionExpression):
            # Positions 0 to 4 are the 1st to the 5th weekday of a month, which
            # fall in that week of the month. 5 is the last one, which falls on
            # day 22 or later.
            if expression.option_num == 5:
                values.update(range(22, 32))
            else:
                first = expression.option_num * 7 + 1
                values.update(range(first, first + 7))
        elif isinstance(expression, RangeExpression):
            last = high if expression.last is None else expression.last
            values.update(range(expression.first, last + 1, expression.step or 1))
        elif type(expression) is AllExpression:
            values.update(range(low, high + 1, expression.step or 1))
        else:
            return frozenset(range(low, high + 1))
    return frozenset(value for value in values if low <= value <= high)


def _cover_interval(trigger: Any) -> _Fields | None:
    seconds = trigger.interval_length
    if seconds % 60:
        return None
    minutes = int(seconds // 60)
    anchor = trigger.start_date.astimezone(UTC)
    anchor_minute = math.floor((anchor - EPOCH).total_seconds() / 60)
    fields = _every_minute()
    values = dict(fields.values)
    values["minute"] = _residues(anchor_minute, math.gcd(minutes, 60), 60)
    if minutes % 60 == 0:
        hours = minutes // 60
        values["hour"] = _residues(anchor_minute // 60, math.gcd(hours, 24), 24)
        if hours % (24 * 7) == 0:
            values["day_of_week"] = frozenset({(anchor.weekday() + 1) % 7})
    return _Fields("UTC", values)


def _residues(anchor: int, step: int, size: int) -> frozenset[int]:
    return frozenset(range(anchor % step, size, step))


def _cover_date(trigger: Any) -> _Fields:
    run_date = trigger.run_date.astimezone(UTC)
    fields = _every_minute()
    return _Fields(
        "UTC",
        {
            **fields.values,
            "minute": frozenset({run_date.minute}),
            "hour": frozenset({run_date.hour}),
            "day": frozenset({run_date.day}),
            "month": frozenset({run_date.month}),
        },
    )


def _cover_calendar_interval(trigger: Any) -> _Fields | None:
    zone = _zone_name(trigger.timezone)
    if zone is None:
        return None
    run_time = trigger._time  # noqa: SLF001 - the time of day every run uses
    fields = _every_minute(zone)
    return _Fields(
        zone,
        {
            **fields.values,
            "minute": frozenset({run_time.minute}),
            "hour": frozenset({run_time.hour}),
        },
    )


def _zone_name(zone: tzinfo | None) -> str | None:
    """Return the IANA name of `zone`, which a Vercel schedule needs."""
    if zone is None:
        return None
    # `zoneinfo` names its zones `key`, and pytz, which APScheduler 3.10 uses, `zone`.
    name = getattr(zone, "key", None) or getattr(zone, "zone", None)
    if isinstance(name, str):
        return name
    if zone.utcoffset(None) == timedelta(0) and zone.dst(None) in {None, timedelta(0)}:
        return "UTC"
    return None


def _render(fields: _Fields) -> CronCover:
    values = dict(fields.values)
    day_restricted = len(values["day"]) < 31
    weekday_restricted = len(values["day_of_week"]) < 7
    # Vercel rejects a cron that restricts both, so keep the one that fires on
    # fewer days, and let each firing check the other.
    if day_restricted and weekday_restricted:
        if len(values["day"]) / 31 <= len(values["day_of_week"]) / 7:
            values["day_of_week"] = frozenset(range(7))
        else:
            values["day"] = frozenset(range(1, 32))
    cron = " ".join(
        # Vercel translates a step in the day of week wrongly, so it gets a list.
        _render_field(values[name], *_BOUNDS[name], steps=name != "day_of_week")
        for name in _FIELDS
    )
    return CronCover(cron, fields.timezone)


def _render_field(values: Iterable[int], low: int, high: int, *, steps: bool) -> str:
    ordered = sorted(values)
    if ordered == list(range(low, high + 1)):
        return "*"
    if steps and len(ordered) > 2:
        step = ordered[1] - ordered[0]
        if step > 1 and ordered == list(range(ordered[0], high + 1, step)):
            return f"*/{step}" if ordered[0] == low else f"{ordered[0]}-{high}/{step}"
    parts: list[str] = []
    start = previous = ordered[0]
    for value in [*ordered[1:], None]:
        if value is not None and value == previous + 1:
            previous = value
            continue
        parts.append(str(start) if start == previous else f"{start}-{previous}")
        if value is not None:
            start = previous = value
    return ",".join(parts)
