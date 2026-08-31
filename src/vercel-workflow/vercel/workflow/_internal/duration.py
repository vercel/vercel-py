"""Duration parsing shared by `sleep()` and `RetryableError`."""

from __future__ import annotations

import re
from datetime import datetime, timedelta

from vercel._internal.core.polyfills import UTC

duration_re = re.compile(
    r"(-?\d+(?:\.\d+)?)\s*(ms|s|seconds?|m|minutes?|h|hours?|d|days?|w|weeks?)",
    re.IGNORECASE,
)
duration_units = {
    "ms": 1,
    "s": 1_000,
    "second": 1_000,
    "seconds": 1_000,
    "m": 60 * 1_000,
    "minute": 60 * 1_000,
    "minutes": 60 * 1_000,
    "h": 60 * 60 * 1_000,
    "hour": 60 * 60 * 1_000,
    "hours": 60 * 60 * 1_000,
    "d": 24 * 60 * 60 * 1_000,
    "day": 24 * 60 * 60 * 1_000,
    "days": 24 * 60 * 60 * 1_000,
    "w": 7 * 24 * 60 * 60 * 1_000,
    "week": 7 * 24 * 60 * 60 * 1_000,
    "weeks": 7 * 24 * 60 * 60 * 1_000,
}


DurationParam = int | float | timedelta | datetime | str


def parse_duration_to_date(param: DurationParam) -> datetime:
    if isinstance(param, str):
        items = [float(v) * duration_units[u] for v, u in duration_re.findall(param)]
        if not items:
            raise RuntimeError(f"Invalid duration parameter: {param}")
        ms = sum(items)
        if ms < 0:
            raise RuntimeError(f"Duration parameter must be non-negative: {param}")
        return datetime.now(UTC) + timedelta(milliseconds=ms)

    elif isinstance(param, (int, float)):
        if param < 0:
            raise RuntimeError(f"Duration parameter must be non-negative: {param}")
        return datetime.now(UTC) + timedelta(seconds=param)

    elif isinstance(param, timedelta):
        if param < timedelta(0):
            raise RuntimeError(f"Duration parameter must be non-negative: {param}")
        return datetime.now(UTC) + param

    elif isinstance(param, datetime):
        if param.tzinfo is None:
            raise RuntimeError("Duration parameter must have tzinfo")
        return param

    else:
        raise RuntimeError(f"Invalid duration parameter: {param}")
