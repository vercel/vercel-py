"""Public value types for the Schedules SDK."""

import re
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Literal, TypeAlias

from pydantic import ValidationInfo, field_validator

from vercel.schedules._internal.base import SchedulesModel
from vercel.schedules._internal.errors import SchedulesValidationError

# VSS stores and validates jitter as the AWS EventBridge Scheduler flexible
# time window: an integer number of minutes.
JITTER_UNIT = timedelta(minutes=1)

MIN_JITTER = timedelta(minutes=1)
"""Smallest jitter the service accepts."""

MAX_JITTER = timedelta(minutes=15)
"""Largest jitter the service accepts."""


class ScheduleState(StrEnum):
    """Whether a schedule currently fires."""

    ACTIVE = "active"
    INACTIVE = "inactive"


class ScheduleSource(StrEnum):
    """How a schedule was defined."""

    STATIC = "static"
    """Declared in project configuration and synced on deploy."""

    DYNAMIC = "dynamic"
    """Created at runtime through this API."""


CronField: TypeAlias = int | str
"""One cron field: a number, or a crontab field such as `"*/15"` or `"1-5"`."""

# Crontab order, which is also the order `Cron` renders in.
_CRON_FIELDS = ("minute", "hour", "day", "month", "day_of_week")

# Most to least significant. A field less significant than every field the
# caller sets defaults to its minimum rather than `*`, so `Cron(hour=3)` fires
# once at 03:00 instead of every minute of that hour. Like APScheduler's
# `CronTrigger`, `day_of_week` always defaults to `*`.
_SIGNIFICANCE = ("month", "day", "day_of_week", "hour", "minute")
_MINIMUMS = {"month": "1", "day": "1", "hour": "0", "minute": "0"}

_BOUNDS = {
    "minute": (0, 59),
    "hour": (0, 23),
    "day": (1, 31),
    "month": (1, 12),
    "day_of_week": (0, 6),
}

# The service accepts only numbers, `*`, and the `,` `-` `/` operators; month and
# weekday names are rejected.
_CRON_FIELD_PATTERN = re.compile(r"[0-9*,/-]+")

# One comma-separated item: `*`, `N`, or `N-M`, with an optional `/step`.
_CRON_ITEM_PATTERN = re.compile(r"(?:\*|(\d+)(?:-(\d+))?)(?:/(\d+))?")


def _check_cron_item(item: str, low: int, high: int) -> None:
    match = _CRON_ITEM_PATTERN.fullmatch(item)
    if match is None:
        raise SchedulesValidationError(f"has a malformed item {item!r}")
    start, end, step = match.groups()
    for number in (start, end):
        if number is not None and not low <= int(number) <= high:
            raise SchedulesValidationError(f"must be between {low} and {high}, got {item!r}")
    if start is not None and end is not None and int(start) > int(end):
        raise SchedulesValidationError(f"has a range that ends before it starts: {item!r}")
    if step is not None and int(step) == 0:
        raise SchedulesValidationError(f"has a step of zero: {item!r}")


def _cron_defaults(fields: dict[str, CronField | None]) -> dict[str, CronField]:
    explicit = [name for name in _SIGNIFICANCE if fields[name] is not None]
    least = _SIGNIFICANCE.index(explicit[-1]) if explicit else len(_SIGNIFICANCE)
    resolved: dict[str, CronField] = {}
    for position, name in enumerate(_SIGNIFICANCE):
        value = fields[name]
        if value is not None:
            resolved[name] = value
        elif position > least and name in _MINIMUMS:
            resolved[name] = _MINIMUMS[name]
        else:
            resolved[name] = "*"
    return resolved


class Cron(SchedulesModel):
    """A recurring schedule, as the five fields of a crontab expression.

    Fields you omit default to `*`, except fields less significant than every
    field you set, which default to their minimum. `Cron(hour=3)` fires every
    day at 03:00, and `Cron(day=1)` at midnight on the first of each month.
    `day_of_week` always defaults to `*`.

    `str(cron)` renders the crontab expression, and `Cron.from_crontab` parses
    one. The expression is evaluated in the schedule's `timezone`.

    Attributes:
        minute: 0-59.
        hour: 0-23.
        day: Day of the month, 1-31.
        month: 1-12.
        day_of_week: 0-6, where 0 is Sunday.
    """

    minute: str
    hour: str
    day: str
    month: str
    day_of_week: str

    def __init__(
        self,
        *,
        minute: CronField | None = None,
        hour: CronField | None = None,
        day: CronField | None = None,
        month: CronField | None = None,
        day_of_week: CronField | None = None,
    ) -> None:
        super().__init__(
            **_cron_defaults(
                {
                    "minute": minute,
                    "hour": hour,
                    "day": day,
                    "month": month,
                    "day_of_week": day_of_week,
                }
            )
        )

    @classmethod
    def from_crontab(cls, expression: str) -> "Cron":
        """Parse a five-field crontab expression, such as `"0 3 * * *"`.

        Raises:
            SchedulesValidationError: If the expression does not have five
                fields, a field holds anything but numbers, `*`, `,`, `-`, and
                `/`, or a field has a number out of range, a backwards range, or
                a zero step.
        """
        parts = expression.split()
        if len(parts) != len(_CRON_FIELDS):
            raise SchedulesValidationError(
                f"cron expression {expression!r} must have {len(_CRON_FIELDS)} fields"
            )
        return cls(**dict(zip(_CRON_FIELDS, parts, strict=True)))

    @field_validator(*_CRON_FIELDS, mode="before")
    @classmethod
    def _check_field(cls, value: object, info: ValidationInfo) -> str:
        name = info.field_name or ""
        if isinstance(value, bool) or not isinstance(value, int | str):
            raise SchedulesValidationError(f"must be an int or a string, got {value!r}")
        low, high = _BOUNDS[name]
        if isinstance(value, int):
            if not low <= value <= high:
                raise SchedulesValidationError(f"must be between {low} and {high}, got {value}")
            return str(value)
        if not _CRON_FIELD_PATTERN.fullmatch(value):
            raise SchedulesValidationError(
                f"must hold only numbers, '*', ',', '-' and '/', got {value!r}"
            )
        for item in value.split(","):
            _check_cron_item(item, low, high)
        return value

    def __str__(self) -> str:
        return " ".join(getattr(self, name) for name in _CRON_FIELDS)


class OneOffExpression(SchedulesModel):
    """A schedule that fires once, at a fixed instant.

    Attributes:
        at: Wall-clock time of the firing, as stored by the service. It has no
            offset and reads in the schedule's `timezone`.
    """

    at: datetime


ScheduleExpression: TypeAlias = Cron | OneOffExpression
"""When a schedule fires: on a cron cadence or once at a fixed instant."""


class QueueTarget(SchedulesModel):
    """Where a schedule dispatches: a Vercel Queue topic."""

    topic: str


class FunctionTarget(SchedulesModel):
    """Where a schedule dispatches: a function build output.

    Attributes:
        function: Output path of the function.
        service: Service the function belongs to, when `function` is a path
            within that service.
    """

    function: str
    service: str | None = None


class PathTarget(SchedulesModel):
    """Where a schedule dispatches: a public path of the deployment.

    Attributes:
        path: Deployment path, starting with `/`.
        request_method: HTTP method the path is requested with. The service
            uses `POST` when this is `None`.
    """

    path: str
    request_method: Literal["GET", "POST"] | None = None


ScheduleTarget: TypeAlias = QueueTarget | FunctionTarget | PathTarget
"""Where each schedule firing is dispatched."""


class Schedule(SchedulesModel):
    """A schedule as stored by the Schedules service.

    A schedule is identified by its `name` within a `namespace`; `schedule_id`
    is the service's internal identifier and is informational.

    Attributes:
        schedule_id: Unique identifier assigned by the service.
        owner_id: Team that owns the schedule.
        project_id: Project the schedule belongs to.
        name: Schedule name, unique within its project and namespace.
        namespace: Namespace the schedule belongs to.
        expression: When the schedule fires.
        timezone: IANA timezone the expression is evaluated in.
        jitter: Random delay added to each firing, if configured.
        target: Where each firing is dispatched.
        state: Whether the schedule fires.
        active_until: When the schedule is disabled again, if it was enabled
            with `until`. Set only while the schedule is active and that time
            is in the future.
        source: Whether the schedule is static or dynamic.
        created_at: When the schedule was created.
        updated_at: When the schedule was last changed.
    """

    schedule_id: str
    owner_id: str
    project_id: str
    name: str
    namespace: str
    expression: ScheduleExpression
    timezone: str
    jitter: timedelta | None = None
    target: ScheduleTarget
    state: ScheduleState
    active_until: datetime | None = None
    source: ScheduleSource
    created_at: datetime
    updated_at: datetime

    @property
    def is_active(self) -> bool:
        """Whether the schedule fires."""
        return self.state is ScheduleState.ACTIVE


JSONValue: TypeAlias = Any
"""Any JSON-serializable value."""


__all__ = [
    "JITTER_UNIT",
    "MAX_JITTER",
    "MIN_JITTER",
    "Cron",
    "CronField",
    "FunctionTarget",
    "JSONValue",
    "OneOffExpression",
    "PathTarget",
    "QueueTarget",
    "Schedule",
    "ScheduleExpression",
    "ScheduleSource",
    "ScheduleState",
    "ScheduleTarget",
]
