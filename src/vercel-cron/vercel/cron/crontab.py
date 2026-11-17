from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar, overload

_F = TypeVar("_F", bound=Callable[..., Any])
_Field = int | str


class CronTabError(Exception):
    pass


@dataclass(frozen=True)
class CronSchedule:
    minute: _Field = "*"
    hour: _Field = "*"
    day: _Field = "*"
    month: _Field = "*"
    day_of_week: _Field = "*"

    @classmethod
    def from_str(cls, s: str) -> CronSchedule:
        parts = s.split()
        if len(parts) != 5:
            raise CronTabError(f"Expected 5 cron fields, got {len(parts)}: {s!r}")
        return cls(*parts)

    def __str__(self) -> str:
        return f"{self.minute} {self.hour} {self.day} {self.month} {self.day_of_week}"


def _resolve(fn: Callable[..., Any]) -> None:
    module_name = fn.__module__
    name = fn.__name__
    qualname = fn.__qualname__

    if qualname != name:
        raise CronTabError(
            f"Cannot register {qualname!r}: only module-level functions are supported"
        )

    module = sys.modules.get(module_name)
    if module is None:
        raise CronTabError(
            f"Cannot register {name!r}: module {module_name!r} not found in sys.modules"
        )

    obj = getattr(module, name, None)
    if obj is not fn:
        raise CronTabError(f"Cannot register {name!r}: could not resolve {module_name}:{name}")


def _make_schedule(schedule: str | CronSchedule | None, **kwargs: _Field) -> CronSchedule:
    if schedule is not None:
        if isinstance(schedule, str):
            return CronSchedule.from_str(schedule)
        return schedule
    return CronSchedule(**kwargs)


class CronTab:
    def __init__(self) -> None:
        self._jobs: list[tuple[Callable[..., Any], CronSchedule]] = []

    @overload
    def register(self, schedule: str | CronSchedule, /) -> Callable[[_F], _F]: ...

    @overload
    def register(
        self,
        *,
        minute: _Field = "*",
        hour: _Field = "*",
        day: _Field = "*",
        month: _Field = "*",
        day_of_week: _Field = "*",
    ) -> Callable[[_F], _F]: ...

    def register(
        self, schedule: str | CronSchedule | None = None, **kwargs: _Field
    ) -> Callable[[_F], _F]:
        sched = _make_schedule(schedule, **kwargs)

        def decorator(fn: _F) -> _F:
            self._jobs.append((fn, sched))
            return fn

        return decorator

    def get_crons(self) -> list[tuple[str, str]]:
        result = []
        for fn, sched in self._jobs:
            _resolve(fn)
            module = fn.__module__
            name = fn.__name__
            result.append((f"{module}:{name}", str(sched)))
        return result


@overload
def cron(schedule: str | CronSchedule, /) -> Callable[[_F], _F]: ...


@overload
def cron(
    *,
    minute: _Field = "*",
    hour: _Field = "*",
    day: _Field = "*",
    month: _Field = "*",
    day_of_week: _Field = "*",
) -> Callable[[_F], _F]: ...


def cron(schedule: str | CronSchedule | None = None, **kwargs: _Field) -> Callable[[_F], _F]:
    sched = _make_schedule(schedule, **kwargs)

    def decorator(fn: _F) -> _F:
        def get_crons() -> list[tuple[str, str]]:
            _resolve(fn)
            return [(f"{fn.__module__}:{fn.__name__}", str(sched))]

        fn.get_crons = get_crons  # type: ignore[attr-defined]
        return fn

    return decorator
