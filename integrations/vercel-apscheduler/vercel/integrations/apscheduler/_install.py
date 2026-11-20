"""Patch APScheduler so a module's jobs deploy as Vercel schedules."""

from __future__ import annotations

from typing import Any

import functools
import inspect
import weakref
from collections.abc import Callable

from ._errors import APSchedulerConfigurationError
from ._imports import (
    BaseScheduler,
    CalendarIntervalTrigger,
    DateTrigger,
    IntervalTrigger,
    MemoryJobStore,
    undefined,
)
from ._jobs import declare, in_firing
from ._triggers import EPOCH

__all__ = ["install_vercel_apscheduler_integration"]

_ADD_JOB = inspect.signature(BaseScheduler.add_job)
_IMMUTABLE = ("modify_job", "remove_job", "remove_all_jobs")

_originals: dict[tuple[type, str], Any] = {}
_started: weakref.WeakSet[BaseScheduler] = weakref.WeakSet()


def install_vercel_apscheduler_integration() -> None:
    """Run the APScheduler jobs a module declares as Vercel schedules.

    The Vercel Python builder calls this before importing a schedule module,
    and the runtime before importing the code that serves a deployment. After
    it, each job a scheduler adds deploys as a schedule named after the job's
    id, and runs when that schedule fires. The scheduler itself never starts:
    `start()` and `shutdown()` do nothing, and jobs can't change at runtime.
    """
    if _originals:
        return
    _patch(BaseScheduler, "__init__", _init)
    _patch(BaseScheduler, "_configure", _configure)
    _patch(BaseScheduler, "add_job", _add_job)
    _patch(BaseScheduler, "add_jobstore", _add_jobstore)
    for name in _IMMUTABLE:
        _patch(BaseScheduler, name, _immutable(name))
    _patch(IntervalTrigger, "__init__", _interval_init)
    _patch(DateTrigger, "__init__", _date_init)
    if CalendarIntervalTrigger is not None:
        _patch(CalendarIntervalTrigger, "__init__", _calendar_interval_init)


def _patch(owner: type, name: str, make: Callable[[Any], Any]) -> None:
    original = owner.__dict__[name]
    _originals[owner, name] = original
    setattr(owner, name, functools.wraps(original)(make(original)))


def _uninstall() -> None:
    """Restore APScheduler. For tests."""
    for (owner, name), original in _originals.items():
        setattr(owner, name, original)
    _originals.clear()


def _init(original: Any) -> Any:
    def init(self: BaseScheduler, *args: Any, **kwargs: Any) -> None:
        original(self, *args, **kwargs)
        # Each scheduler class starts its own way, such as in a thread or by
        # blocking, so the instance overrides whichever its class has.
        self.start = functools.partial(_start, self)  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
        self.shutdown = functools.partial(_shutdown, self)  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]

    return init


def _start(scheduler: BaseScheduler, paused: bool = False) -> None:  # noqa: FBT001, FBT002
    # The deployment's schedules run the jobs, so a running scheduler would
    # run them twice. Starting ends declaration: later jobs could never deploy.
    del paused
    _started.add(scheduler)


def _shutdown(scheduler: BaseScheduler, wait: bool = True) -> None:  # noqa: FBT001, FBT002
    del scheduler, wait


def _configure(original: Any) -> Any:
    def _configure(self: BaseScheduler, config: dict[str, Any]) -> None:
        original(self, config)
        # A firing arrives up to about a minute after its time, so stock
        # APScheduler's one-second grace would skip most runs. Without a grace
        # set, a job runs however late its firing is.
        if "misfire_grace_time" not in (config.get("job_defaults") or {}):
            self._job_defaults["misfire_grace_time"] = None

    return _configure


def _add_job(original: Any) -> Any:
    def add_job(self: BaseScheduler, *args: Any, **kwargs: Any) -> Any:
        arguments = _ADD_JOB.bind(self, *args, **kwargs).arguments
        func = arguments.get("func")
        label = getattr(func, "__qualname__", func)
        if self in _started or in_firing():
            raise APSchedulerConfigurationError(
                f"can't add a job for {label} at runtime: each job deploys as a Vercel "
                "schedule, so declare it when the module is imported, before start()"
            )
        if arguments.get("id") is None:
            raise APSchedulerConfigurationError(
                f"the job for {label} needs an id: it names the job's Vercel schedule, so "
                "it must stay the same in every deployment"
            )
        if arguments.get("next_run_time", undefined) is not undefined:
            raise APSchedulerConfigurationError(
                f"job {arguments['id']!r} sets next_run_time, but a Vercel schedule runs a "
                "job only when its trigger fires"
            )
        job = original(self, *args, **kwargs)
        try:
            declare(self, job, replace_existing=bool(arguments.get("replace_existing")))
        except BaseException:
            self._pending_jobs[:] = [
                pending for pending in self._pending_jobs if pending[0] is not job
            ]
            raise
        return job

    return add_job


def _add_jobstore(original: Any) -> Any:
    def add_jobstore(
        self: BaseScheduler, jobstore: Any, alias: str = "default", **jobstore_opts: Any
    ) -> None:
        original(self, jobstore, alias, **jobstore_opts)
        store = self._jobstores[alias]
        if not isinstance(store, MemoryJobStore):
            del self._jobstores[alias]
            raise APSchedulerConfigurationError(
                f'job store "{alias}" is a {type(store).__name__}, but Vercel deploys the '
                "jobs a module declares in code; remove the job store"
            )

    return add_jobstore


def _immutable(name: str) -> Callable[[Any], Any]:
    def make(original: Any) -> Any:
        del original

        def method(self: BaseScheduler, *args: Any, **kwargs: Any) -> Any:
            del self, args, kwargs
            raise APSchedulerConfigurationError(
                f"{name}() can't change a job: each job deploys as a Vercel schedule, so "
                "change its declaration and deploy"
            )

        return method

    return make


def _interval_init(original: Any) -> Any:
    signature = inspect.signature(original)

    def init(self: Any, *args: Any, **kwargs: Any) -> None:
        original(self, *args, **kwargs)
        if signature.bind(self, *args, **kwargs).arguments.get("start_date") is None:
            # Without a start date, an interval counts from when its process
            # started, so every function instance would run it at other times.
            # Counting from the Unix epoch runs `minutes=5` at :00, :05 and so on.
            self.start_date = EPOCH.astimezone(self.timezone)

    return init


def _date_init(original: Any) -> Any:
    signature = inspect.signature(original)

    def init(self: Any, *args: Any, **kwargs: Any) -> None:
        if signature.bind(self, *args, **kwargs).arguments.get("run_date") is None:
            raise APSchedulerConfigurationError(
                "a date trigger needs run_date: without it, the job runs once when the "
                "scheduler starts, and a deployment never starts one"
            )
        original(self, *args, **kwargs)

    return init


def _calendar_interval_init(original: Any) -> Any:
    def init(self: Any, **kwargs: Any) -> None:
        longer_than_a_day = any(kwargs.get(unit) for unit in ("years", "months", "weeks")) or (
            kwargs.get("days", 0) > 1
        )
        if kwargs.get("start_date") is None and longer_than_a_day:
            raise APSchedulerConfigurationError(
                "a calendar interval longer than a day needs start_date: without it, the "
                "interval counts from the day its process started, so every function "
                "instance would run it on other days"
            )
        original(self, **kwargs)

    return init
