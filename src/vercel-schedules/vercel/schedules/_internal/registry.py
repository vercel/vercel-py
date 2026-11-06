"""Schedules declared in code with `@schedule`, in a process-wide registry."""

import functools
import inspect
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, TypeVar

from anyio import to_thread

from vercel.schedules._internal.errors import ScheduleEventParseError, SchedulesValidationError
from vercel.schedules._internal.metadata import ScheduleMetadata
from vercel.schedules._internal.models import Cron
from vercel.schedules._internal.service import jitter_minutes, zone_info

F = TypeVar("F", bound=Callable[..., Any])

# The names the Schedules API and `vercel.json` accept.
_NAME_PATTERN = re.compile(r"[0-9A-Za-z][0-9A-Za-z._-]{0,255}")

_EVENT_PARAMETER_KINDS = (
    inspect.Parameter.POSITIONAL_ONLY,
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
    inspect.Parameter.VAR_POSITIONAL,
)


def _qualified_name(func: Callable[..., Any]) -> str:
    return f"{func.__module__}.{func.__qualname__}"


def _takes_metadata(func: Callable[..., Any]) -> bool:
    """Whether the handler takes the `ScheduleMetadata`, which must be all it requires."""
    parameters = inspect.signature(func).parameters.values()
    required = [
        p
        for p in parameters
        if p.default is inspect.Parameter.empty
        and p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    ]
    if len(required) > 1 or any(p.kind is inspect.Parameter.KEYWORD_ONLY for p in required):
        raise TypeError(
            f"schedule handler {_qualified_name(func)} must take no arguments or one "
            "ScheduleMetadata argument"
        )
    return any(p.kind in _EVENT_PARAMETER_KINDS for p in parameters)


@dataclass(frozen=True)
class RegisteredSchedule:
    """A schedule declared in code with `@schedule`.

    Attributes:
        name: Schedule name, unique within the project.
        cron: When the schedule fires.
        timezone: IANA timezone `cron` is evaluated in, or `None` for UTC.
        jitter: Random delay added to each firing, if configured.
        func: The handler.
    """

    name: str
    cron: Cron
    timezone: str | None
    jitter: timedelta | None
    func: Callable[..., Any]

    async def run(self, metadata: ScheduleMetadata) -> None:
        """Run the handler for one firing of this schedule.

        A coroutine function is awaited, and a regular function runs in a
        worker thread so it does not block the event loop. Exceptions from the
        handler propagate.

        Raises:
            ScheduleEventParseError: If `metadata` is a firing of another schedule.
        """
        if metadata.name != self.name:
            raise ScheduleEventParseError(
                f"firing of schedule {metadata.name!r} cannot run schedule {self.name!r}"
            )
        call = functools.partial(self.func, metadata) if _takes_metadata(self.func) else self.func
        if inspect.iscoroutinefunction(self.func):
            await call()
        else:
            await to_thread.run_sync(call)


_schedules: dict[str, RegisteredSchedule] = {}
_schedules_lock = threading.Lock()


def _register(registered: RegisteredSchedule) -> None:
    with _schedules_lock:
        existing = _schedules.get(registered.name)
        # Importing a module again, such as on reload, registers its handlers
        # again; only a different handler with the same name is a conflict.
        if existing is not None and _qualified_name(existing.func) != _qualified_name(
            registered.func
        ):
            raise SchedulesValidationError(
                f"schedule {registered.name!r} is already declared by "
                f"{_qualified_name(existing.func)}"
            )
        _schedules[registered.name] = registered


def schedule(
    cron: str | Cron,
    /,
    *,
    name: str | None = None,
    timezone: str | None = None,
    jitter: timedelta | None = None,
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
    ```

    Args:
        cron: When the schedule fires, as a `Cron` or a five-field crontab
            string, evaluated in `timezone`.
        name: Schedule name, unique within the project. Defaults to the
            function's name. Renaming a schedule replaces it.
        timezone: IANA timezone name. Defaults to UTC.
        jitter: Random delay added to each firing, in whole minutes between one
            and fifteen inclusive.

    Returns:
        A decorator that registers the function and returns it unchanged.

    Raises:
        SchedulesValidationError: If an argument is invalid, or another
            function already declares a schedule with this name.
        TypeError: If the function requires arguments other than one
            `ScheduleMetadata`.
    """
    parsed = Cron.from_crontab(cron) if isinstance(cron, str) else cron
    if timezone is not None:
        zone_info(timezone)
    if jitter is not None:
        jitter_minutes(jitter)

    def decorator(func: F) -> F:
        schedule_name = func.__name__ if name is None else name
        if not _NAME_PATTERN.fullmatch(schedule_name):
            raise SchedulesValidationError(
                f"schedule name {schedule_name!r} must start with a letter or number and "
                "contain only letters, numbers, '.', '_' and '-'; pass name=..."
            )
        _takes_metadata(func)
        _register(
            RegisteredSchedule(
                name=schedule_name, cron=parsed, timezone=timezone, jitter=jitter, func=func
            )
        )
        return func

    return decorator


def get_registered_schedules() -> tuple[RegisteredSchedule, ...]:
    """Return the schedules declared with `@schedule` in the modules imported so far."""
    with _schedules_lock:
        return tuple(_schedules.values())


__all__ = ["RegisteredSchedule", "get_registered_schedules", "schedule"]
