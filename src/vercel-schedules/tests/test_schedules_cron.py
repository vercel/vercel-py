from typing import Any

import pytest

from vercel.schedules import Cron, SchedulesValidationError


@pytest.mark.parametrize(
    ("cron", "expression"),
    [
        (Cron(), "* * * * *"),
        (Cron(minute=30), "30 * * * *"),
        # Fields less significant than the ones set default to their minimum,
        # so this fires once at 03:00 rather than every minute of that hour.
        (Cron(hour=3), "0 3 * * *"),
        (Cron(hour="9-17", minute="*/15"), "*/15 9-17 * * *"),
        (Cron(day=1), "0 0 1 * *"),
        (Cron(month=6), "0 0 1 6 *"),
        # `day_of_week` never defaults to its minimum.
        (Cron(day_of_week="1-5", hour=9), "0 9 * * 1-5"),
        (Cron(day_of_week=0), "0 0 * * 0"),
    ],
)
def test_omitted_fields_default_like_apscheduler(cron: Cron, expression: str) -> None:
    assert str(cron) == expression


def test_from_crontab_keeps_every_field() -> None:
    cron = Cron.from_crontab("15 */2 1,15 * *")

    assert (cron.minute, cron.hour, cron.day, cron.month, cron.day_of_week) == (
        "15",
        "*/2",
        "1,15",
        "*",
        "*",
    )
    assert cron == Cron(minute=15, hour="*/2", day="1,15")
    assert Cron.from_crontab(str(cron)) == cron


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"minute": 60}, "minute must be between 0 and 59"),
        ({"hour": -1}, "hour must be between 0 and 23"),
        ({"day": 0}, "day must be between 1 and 31"),
        ({"day_of_week": 7}, "day_of_week must be between 0 and 6"),
        ({"day_of_week": "MON"}, "day_of_week must hold only numbers"),
        ({"minute": ""}, "minute must hold only numbers"),
        ({"minute": True}, "minute must be an int or a string"),
    ],
)
def test_rejects_invalid_fields(kwargs: dict[str, Any], match: str) -> None:
    with pytest.raises(SchedulesValidationError, match=match):
        Cron(**kwargs)
