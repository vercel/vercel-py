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

from vercel._internal.core.typeutils import (
    TypeAnnotationResolutionError,
    args,
    is_union_type,
    resolve_annotation_with_namespace_from_call_stack,
    strip_annotated,
)
from vercel.schedules._internal.errors import SchedulesValidationError
from vercel.schedules._internal.metadata import ScheduleMetadata
from vercel.schedules._internal.models import Cron
from vercel.schedules._internal.service import check_jitter, zone_info

F = TypeVar("F", bound=Callable[..., Any])

# The names the Schedules API and `vercel.json` accept.
_NAME_PATTERN = re.compile(r"[0-9A-Za-z][0-9A-Za-z._-]{0,255}")


def _check_max_duration(max_duration: timedelta) -> None:
    if not isinstance(max_duration, timedelta):
        raise SchedulesValidationError(
            "max_duration must be a timedelta, such as timedelta(seconds=300), "
            f"got {max_duration!r}"
        )
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


def _is_metadata_annotation(annotation: Any) -> bool:
    annotation = strip_annotated(annotation)
    if is_union_type(annotation):
        members = [strip_annotated(a) for a in args(annotation) if a is not type(None)]
        return members == [ScheduleMetadata]
    return annotation is ScheduleMetadata


def _takes_metadata(func: Callable[..., Any], parameter: inspect.Parameter) -> bool:
    annotation = parameter.annotation
    if annotation is inspect.Parameter.empty:
        return False
    try:
        resolved = resolve_annotation_with_namespace_from_call_stack(
            annotation, globalns=getattr(inspect.unwrap(func), "__globals__", {})
        )
    except TypeAnnotationResolutionError as exc:
        # An annotation that names the metadata but can't be resolved, such as
        # one imported under `TYPE_CHECKING`, would otherwise receive nothing.
        if "ScheduleMetadata" in str(annotation):
            raise TypeError(
                f"schedule handler {_qualified_name(func)} annotates {parameter.name!r} "
                f"as {annotation!r}, which can't be resolved; import ScheduleMetadata "
                "at runtime"
            ) from exc.__cause__
        return False
    return _is_metadata_annotation(resolved.annotation)


def _metadata_parameter(func: Callable[..., Any]) -> inspect.Parameter | None:
    """The parameter annotated `ScheduleMetadata`, which receives the firing's metadata.

    A firing supplies nothing else, so every other parameter needs a default.
    """
    parameters = list(inspect.signature(func).parameters.values())
    found: inspect.Parameter | None = None
    for parameter in parameters:
        if parameter.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            raise TypeError(
                f"schedule handler {_qualified_name(func)} must not take *args or "
                "**kwargs; a decorator's wrapper should use functools.wraps"
            )
        if _takes_metadata(func, parameter):
            if found is not None:
                raise TypeError(
                    f"schedule handler {_qualified_name(func)} takes the ScheduleMetadata "
                    f"twice, in {found.name!r} and {parameter.name!r}"
                )
            found = parameter
        elif parameter.default is inspect.Parameter.empty:
            raise TypeError(
                f"schedule handler {_qualified_name(func)} requires {parameter.name!r}, "
                "which a firing can't supply; annotate it as ScheduleMetadata to take "
                "the firing's metadata, or give it a default"
            )
    # A positional-only parameter can't be passed by name.
    if (
        found is not None
        and found.kind is inspect.Parameter.POSITIONAL_ONLY
        and found is not parameters[0]
    ):
        raise TypeError(
            f"schedule handler {_qualified_name(func)} takes the ScheduleMetadata in the "
            f"positional-only {found.name!r}, which must be its first parameter"
        )
    return found


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
    _metadata_parameter: inspect.Parameter | None = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_metadata_parameter", _metadata_parameter(self.func))

    async def run(self, metadata: ScheduleMetadata) -> None:
        """Run the handler for one firing of this schedule.

        A coroutine function is awaited, and a regular function runs in a
        worker thread so it does not block the event loop. An awaitable that a
        regular function returns is awaited too. The metadata is passed to the
        parameter annotated `ScheduleMetadata`, if there is one. Exceptions
        from the handler propagate.
        """
        parameter = self._metadata_parameter
        if parameter is None:
            call = self.func
        elif parameter.kind is inspect.Parameter.POSITIONAL_ONLY:
            call = functools.partial(self.func, metadata)
        else:
            call = functools.partial(self.func, **{parameter.name: metadata})
        if inspect.iscoroutinefunction(self.func):
            await call()
        else:
            # A sync wrapper around a coroutine function, such as a decorator
            # that uses `functools.wraps`, returns the coroutine.
            result = await to_thread.run_sync(call)
            if inspect.isawaitable(result):
                await result


@dataclass(frozen=True)
class _Entry:
    schedule: RegisteredSchedule
    named: bool
    """Whether `@schedule` was passed `name=`."""


_schedules: dict[str, _Entry] = {}
_schedules_lock = threading.Lock()


def _source_file(func: Callable[..., Any]) -> str | None:
    code = getattr(inspect.unwrap(func), "__code__", None)
    return None if code is None else code.co_filename


def _is_rerun(existing: Callable[..., Any], new: Callable[..., Any]) -> bool:
    """Whether `new` is `existing`'s definition executed again.

    A reload executes the module again under the same name and file. A file
    run as `__main__` after being imported keeps its file, and an edited
    notebook cell keeps its module.
    """
    if existing is new or existing.__qualname__ != new.__qualname__:
        return False
    if existing.__module__ == new.__module__:
        return True
    source_file = _source_file(existing)
    return source_file is not None and source_file == _source_file(new)


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
        if existing is not None and not _is_rerun(existing.schedule.func, registered.func):
            raise SchedulesValidationError(
                f"schedule {registered.name!r} is already declared by "
                f"{_qualified_name(existing.schedule.func)}"
            )
        _schedules[registered.name] = _Entry(registered, named)


def schedule(
    *,
    cron: str | Cron,
    name: str | None = None,
    timezone: str | None = None,
    jitter: timedelta | None = None,
    max_duration: timedelta | None = None,
    regions: Sequence[str] | None = None,
) -> Callable[[F], F]:
    """Declare a function as the handler of a recurring schedule.

    Importing the module registers the schedule, and `get_registered_schedules()`
    returns every registered one. On each firing, `RegisteredSchedule.run` calls
    the function, which may be a coroutine function. A parameter annotated
    `ScheduleMetadata` receives the firing's metadata, under any name. A firing
    supplies nothing else, so every other parameter needs a default.

    ```python
    from vercel.schedules import Cron, ScheduleMetadata, schedule

    @schedule(cron=Cron(hour=3))
    def reindex() -> None: ...

    @schedule(cron="*/15 * * * *", name="sync-billing", timezone="America/New_York")
    async def sync(metadata: ScheduleMetadata) -> None: ...

    @schedule(cron=Cron(hour=4), max_duration=timedelta(minutes=15), regions=["fra1"])
    def export_reports() -> None: ...
    ```

    A deployment runs schedules with the same `max_duration` and `regions` in
    one function, and each other combination in a function of its own.

    Stack `@schedule` to run one function on several schedules, and pass each
    one a `name`:

    ```python
    @schedule(cron="0 3 * * *", name="report-morning")
    @schedule(cron="0 15 * * *", name="report-afternoon", regions=["fra1"])
    def report(metadata: ScheduleMetadata) -> None: ...
    ```

    Args:
        cron: When the schedule fires, as a `Cron` or a five-field crontab
            string, evaluated in `timezone`.
        name: Schedule name, unique among the schedules the entrypoint
            registers. Defaults to the function's name, unless the function
            has more than one `@schedule`. A deployment names the schedule
            `<entry>.<name>`, after its `vercel.json` entry. Renaming a
            schedule replaces it. Running the same definition again, such as
            in a module reload, replaces its registration.
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
            `max_duration` that is not a `timedelta` of a positive whole number
            of seconds or empty `regions`, another function already declares
            a schedule with this name, or the function has more than one
            `@schedule` and one of them has no `name`.
        TypeError: If the function has a parameter without a default that is
            not annotated `ScheduleMetadata`, more than one parameter that is,
            or `*args` or `**kwargs`.
    """
    parsed = Cron.from_crontab(cron) if isinstance(cron, str) else cron
    if timezone is not None:
        zone_info(timezone)
    if jitter is not None:
        check_jitter(jitter)
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
