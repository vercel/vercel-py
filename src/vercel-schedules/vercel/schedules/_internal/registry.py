"""Schedules declared in code with `@schedule`, in a process-wide registry."""

import functools
import inspect
import re
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, TypeVar

from anyio import to_thread

from vercel.schedules._internal.errors import SchedulesValidationError
from vercel.schedules._internal.metadata import ScheduleMetadata
from vercel.schedules._internal.models import Cron
from vercel.schedules._internal.service import jitter_minutes, zone_info

F = TypeVar("F", bound=Callable[..., Any])

# The names the Schedules API and `vercel.json` accept.
_NAME_PATTERN = re.compile(r"[0-9A-Za-z][0-9A-Za-z._-]{0,255}")

_POSITIONAL_KINDS = (
    inspect.Parameter.POSITIONAL_ONLY,
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
)


def _check_max_duration(max_duration: timedelta) -> None:
    if max_duration <= timedelta(0):
        raise SchedulesValidationError(f"max_duration must be positive, got {max_duration}")
    if max_duration % timedelta(seconds=1):
        raise SchedulesValidationError("max_duration must be a whole number of seconds")


def _check_regions(regions: Sequence[str]) -> tuple[str, ...]:
    # A string is a sequence of characters, so `regions="fra1"` would
    # otherwise become ("f", "r", "a", "1").
    if isinstance(regions, str):
        raise SchedulesValidationError(
            f"regions must be a list of region IDs, such as [{regions!r}]"
        )
    checked = tuple(regions)
    if not checked:
        raise SchedulesValidationError("regions must name at least one region")
    for region in checked:
        if not isinstance(region, str) or not region.strip():
            raise SchedulesValidationError(
                f"regions must contain region IDs such as 'iad1', got {region!r}"
            )
    return checked


def _qualified_name(func: Callable[..., Any]) -> str:
    return f"{func.__module__}.{func.__qualname__}"


def _takes_metadata(func: Callable[..., Any]) -> bool:
    """Whether the handler takes the `ScheduleMetadata`, which must be all it requires."""
    parameters = inspect.signature(func).parameters.values()
    positional = [p for p in parameters if p.kind in _POSITIONAL_KINDS]
    # The metadata would land in a parameter with a default, such as
    # `full: bool = False`, which the signature can't tell from an optional
    # `metadata` parameter.
    for p in positional:
        if p.default is not inspect.Parameter.empty:
            raise TypeError(
                f"schedule handler {_qualified_name(func)} would receive the "
                f"ScheduleMetadata in {p.name!r}, which has a default; make it required "
                "to take the metadata, or keyword-only"
            )
    if len(positional) > 1 or any(
        p.kind is inspect.Parameter.KEYWORD_ONLY and p.default is inspect.Parameter.empty
        for p in parameters
    ):
        raise TypeError(
            f"schedule handler {_qualified_name(func)} must take no arguments or one "
            "ScheduleMetadata argument"
        )
    return bool(positional) or any(p.kind is inspect.Parameter.VAR_POSITIONAL for p in parameters)


@dataclass(frozen=True)
class RegisteredSchedule:
    """A schedule declared in code with `@schedule`.

    Attributes:
        name: Schedule name, unique within the process.
        cron: When the schedule fires.
        timezone: IANA timezone `cron` is evaluated in, or `None` for UTC.
        jitter: Random delay added to each firing, if configured.
        func: The handler.
        max_duration: Longest a firing may run, or `None` for the platform
            default.
        regions: Regions the schedule's function is deployed to, or `None`
            for the project's default regions.
    """

    name: str
    cron: Cron
    timezone: str | None
    jitter: timedelta | None
    func: Callable[..., Any]
    max_duration: timedelta | None = None
    regions: tuple[str, ...] | None = None
    _takes_metadata: bool = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_takes_metadata", _takes_metadata(self.func))

    async def run(self, metadata: ScheduleMetadata) -> None:
        """Run the handler for one firing of this schedule.

        A coroutine function is awaited, and a regular function runs in a
        worker thread so it does not block the event loop. Exceptions from the
        handler propagate.
        """
        call = functools.partial(self.func, metadata) if self._takes_metadata else self.func
        if inspect.iscoroutinefunction(self.func):
            await call()
        else:
            await to_thread.run_sync(call)


@dataclass(frozen=True)
class _Entry:
    schedule: RegisteredSchedule
    named: bool
    """Whether `@schedule` was passed `name=`."""


_schedules: dict[str, _Entry] = {}
_schedules_lock = threading.Lock()


def _register(registered: RegisteredSchedule, *, named: bool) -> None:
    with _schedules_lock:
        # Every schedule stacked on one function names itself, so none of
        # them takes the function's name.
        stacked = [e for e in _schedules.values() if e.schedule.func is registered.func]
        if stacked and not (named and all(e.named for e in stacked)):
            raise SchedulesValidationError(
                f"{_qualified_name(registered.func)} has more than one @schedule; "
                "pass name= to each"
            )
        existing = _schedules.get(registered.name)
        if existing is not None:
            raise SchedulesValidationError(
                f"schedule {registered.name!r} is already declared by "
                f"{_qualified_name(existing.schedule.func)}"
            )
        _schedules[registered.name] = _Entry(registered, named)


def schedule(
    cron: str | Cron,
    /,
    *,
    name: str | None = None,
    timezone: str | None = None,
    jitter: timedelta | None = None,
    max_duration: timedelta | None = None,
    regions: Sequence[str] | None = None,
) -> Callable[[F], F]:
    """Declare a function as the handler of a recurring schedule.

    Importing the module registers the schedule, and `get_registered_schedules()`
    returns every registered one. On each firing, `RegisteredSchedule.run` calls
    the function with no arguments, or with the `ScheduleMetadata` if it takes one.
    It may be a coroutine function.

    ```python
    from vercel.schedules import Cron, ScheduleMetadata, schedule

    @schedule(Cron(hour=3))
    def reindex() -> None: ...

    @schedule("*/15 * * * *", name="sync-billing", timezone="America/New_York")
    async def sync(metadata: ScheduleMetadata) -> None: ...

    @schedule(Cron(hour=4), max_duration=timedelta(minutes=15), regions=["fra1"])
    def export_reports() -> None: ...
    ```

    A deployment runs schedules with the same `max_duration` and `regions` in
    one function, and each other combination in a function of its own.

    Stack `@schedule` to run one function on several schedules, and pass each
    one a `name`:

    ```python
    @schedule("0 3 * * *", name="report-morning")
    @schedule("0 15 * * *", name="report-afternoon", regions=["fra1"])
    def report(metadata: ScheduleMetadata) -> None: ...
    ```

    Args:
        cron: When the schedule fires, as a `Cron` or a five-field crontab
            string, evaluated in `timezone`.
        name: Schedule name, unique among the schedules the entrypoint
            registers. Defaults to the function's name, unless the function
            has more than one `@schedule`. A deployment names the schedule
            `<entry>.<name>`, after its `vercel.json` entry. Renaming a
            schedule replaces it.
        timezone: IANA timezone name. Defaults to UTC.
        jitter: Random delay added to each firing, in whole minutes between one
            and fifteen inclusive.
        max_duration: Longest a firing may run, in whole seconds. Defaults to
            the platform's default for functions. The plan sets the maximum.
        regions: Regions this schedule's function is deployed to, such as
            `["fra1"]`. Each firing runs once, in one of them; the others are
            used only if it is unavailable. Pass one region to choose where the
            schedule runs. Defaults to the project's regions.

    Returns:
        A decorator that registers the function and returns it unchanged.

    Raises:
        SchedulesValidationError: If an argument is invalid, such as a
            `max_duration` that is not a positive whole number of seconds or
            empty `regions`, a schedule with this name is already declared,
            or the function has more than one `@schedule` and one of them has
            no `name`.
        TypeError: If the function requires arguments other than one
            `ScheduleMetadata`, or has a positional parameter with a default.
    """
    parsed = Cron.from_crontab(cron) if isinstance(cron, str) else cron
    if timezone is not None:
        zone_info(timezone)
    if jitter is not None:
        jitter_minutes(jitter)
    if max_duration is not None:
        _check_max_duration(max_duration)
    checked_regions = None if regions is None else _check_regions(regions)

    def decorator(func: F) -> F:
        schedule_name = func.__name__ if name is None else name
        if not _NAME_PATTERN.fullmatch(schedule_name):
            raise SchedulesValidationError(
                f"schedule name {schedule_name!r} must start with a letter or number and "
                "contain only letters, numbers, '.', '_' and '-'; pass name=..."
            )
        _register(
            RegisteredSchedule(
                name=schedule_name,
                cron=parsed,
                timezone=timezone,
                jitter=jitter,
                func=func,
                max_duration=max_duration,
                regions=checked_regions,
            ),
            named=name is not None,
        )
        return func

    return decorator


def get_registered_schedules() -> tuple[RegisteredSchedule, ...]:
    """Return the schedules declared with `@schedule` in the modules imported so far."""
    with _schedules_lock:
        return tuple(entry.schedule for entry in _schedules.values())


__all__ = ["RegisteredSchedule", "get_registered_schedules", "schedule"]
