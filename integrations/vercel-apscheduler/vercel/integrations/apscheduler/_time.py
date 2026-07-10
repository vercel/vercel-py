from __future__ import annotations

from datetime import UTC, datetime, timedelta

__all__ = [
    "as_utc",
    "canonical_scheduled_logical_time",
    "earliest",
    "require_aware_datetime",
]


def require_aware_datetime(value: datetime, *, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value


def as_utc(value: datetime, *, name: str = "value") -> datetime:
    return require_aware_datetime(value, name=name).astimezone(UTC)


def earliest(current: datetime | None, candidate: datetime | None) -> datetime | None:
    if candidate is None:
        return current
    if current is None or candidate < current:
        return candidate
    return current


def canonical_scheduled_logical_time(
    logical_time: datetime,
    *,
    now: datetime,
    max_delay_seconds: int,
) -> datetime:
    logical_time_utc = as_utc(logical_time)
    now_utc = as_utc(now, name="now")
    max_delay = timedelta(seconds=max_delay_seconds)
    latest_publish_time = now_utc + max_delay
    if logical_time_utc <= latest_publish_time:
        return logical_time_utc

    remaining_hops, remainder = divmod(logical_time_utc - now_utc, max_delay)
    if remainder != timedelta(0):
        remaining_hops += 1

    bridge_hops = int(remaining_hops) - 1
    return logical_time_utc - bridge_hops * max_delay
