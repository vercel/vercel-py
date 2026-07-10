from __future__ import annotations

from typing import Any, cast

import json
import logging
import math
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from types import MethodType

import vercel.queue as vqs
import vercel.queue.sync as vqs_sync

from ._executor import VercelInlineExecutor
from ._imports import (
    EVENT_JOB_MAX_INSTANCES,
    EVENT_JOB_SUBMITTED,
    STATE_PAUSED,
    STATE_RUNNING,
    STATE_STOPPED,
    BaseScheduler,
    JobSubmissionEvent,
    MaxInstancesReachedError,
    MemoryJobStore,
)
from ._options import VercelAPSchedulerOptions, is_vercel_runtime
from ._payload import CursorEntry, MemoryCursor, WakeupPayload
from ._time import as_utc, canonical_scheduled_logical_time, earliest, require_aware_datetime

LOGGER = logging.getLogger("vercel.integrations.apscheduler")
ADAPTER_ATTR = "_vercel_apscheduler_adapter"
WAKEUP_KEY_PREFIX = "aps:v1"

__all__ = [
    "ADAPTER_ATTR",
    "PublishedWakeup",
    "SchedulerAdapter",
    "WakeupProcessingResult",
    "adopt_scheduler",
    "get_adapter",
    "install_vercel_apscheduler_integration",
    "seed_next_wakeup",
]


@dataclass(frozen=True, slots=True)
class PublishedWakeup:
    logical_time: datetime
    delay_seconds: int
    idempotency_key: str
    message_id: str | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "logical_time", as_utc(self.logical_time, name="logical_time"))


@dataclass(frozen=True, slots=True)
class WakeupProcessingResult:
    logical_time: datetime
    due_job_ids: tuple[str, ...]
    next_wakeup_time: datetime | None
    published_wakeup: PublishedWakeup | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "logical_time", as_utc(self.logical_time, name="logical_time"))
        if self.next_wakeup_time is not None:
            object.__setattr__(
                self,
                "next_wakeup_time",
                as_utc(self.next_wakeup_time, name="next_wakeup_time"),
            )


@dataclass(slots=True)
class _DueJobPlan:
    job: Any
    jobstore_alias: str
    run_times: list[datetime]
    next_run_time: datetime | None


@dataclass(frozen=True, slots=True)
class _JobDefinition:
    schedule_key: str
    fingerprint: str
    trigger_kind: str
    unanchored_interval: bool


@dataclass(slots=True)
class _PatchState:
    installed: bool = False
    default_options: VercelAPSchedulerOptions | None = None
    original_init: Callable[..., Any] | None = None
    original_add_job: Callable[..., Any] | None = None
    original_real_add_job: Callable[..., Any] | None = None
    original_blocking_start: Callable[..., Any] | None = None
    original_background_start: Callable[..., Any] | None = None
    original_asyncio_start: Callable[..., Any] | None = None


_PATCH_STATE = _PatchState()


def _stable_repr(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple)):
        return [_stable_repr(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _stable_repr(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, datetime):
        return value.isoformat()
    module = getattr(value, "__module__", None)
    qualname = getattr(value, "__qualname__", None)
    if isinstance(module, str) and isinstance(qualname, str):
        return f"{module}.{qualname}"
    return repr(value)


def _job_func_name(func: Any) -> str:
    module = getattr(func, "__module__", "")
    qualname = getattr(func, "__qualname__", repr(func))
    return f"{module}.{qualname}" if module else qualname


def _json_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return "sha256:" + sha256(encoded.encode("utf-8")).hexdigest()


def _known_add_job_kwargs() -> set[str]:
    return {
        "func",
        "trigger",
        "args",
        "kwargs",
        "id",
        "name",
        "misfire_grace_time",
        "coalesce",
        "max_instances",
        "next_run_time",
        "jobstore",
        "executor",
        "replace_existing",
    }


def _safe_arg(args: tuple[Any, ...], index: int, default: Any = None) -> Any:
    return args[index] if len(args) > index else default


def _build_definition(
    job: Any,
    add_args: tuple[Any, ...],
    add_kwargs: dict[str, Any],
) -> _JobDefinition:
    trigger_arg = add_kwargs.get("trigger", _safe_arg(add_args, 1))
    trigger_kwargs = {
        key: value for key, value in add_kwargs.items() if key not in _known_add_job_kwargs()
    }
    job_id = str(job.id)
    explicit_id = add_kwargs.get("id", _safe_arg(add_args, 4))
    schedule_key = f"id:{job_id}" if explicit_id else f"auto:{job_id}"

    if isinstance(trigger_arg, str):
        trigger_kind = trigger_arg
    else:
        trigger_kind = type(getattr(job, "trigger", trigger_arg)).__name__

    unanchored_interval = trigger_arg == "interval" and "start_date" not in trigger_kwargs

    fingerprint_payload = {
        "func": _job_func_name(job.func),
        "args": _stable_repr(job.args),
        "kwargs": _stable_repr(job.kwargs),
        "id": explicit_id or job_id,
        "trigger": _stable_repr(trigger_arg),
        "trigger_args": _stable_repr(trigger_kwargs),
        "misfire_grace_time": _stable_repr(getattr(job, "misfire_grace_time", None)),
        "coalesce": _stable_repr(getattr(job, "coalesce", None)),
        "max_instances": _stable_repr(getattr(job, "max_instances", None)),
    }
    return _JobDefinition(
        schedule_key=schedule_key,
        fingerprint=_json_hash(fingerprint_payload),
        trigger_kind=str(trigger_kind),
        unanchored_interval=unanchored_interval,
    )


def get_adapter(scheduler: Any) -> SchedulerAdapter | None:
    return cast("SchedulerAdapter | None", getattr(scheduler, ADAPTER_ATTR, None))


class SchedulerAdapter:
    def __init__(
        self,
        scheduler: BaseScheduler,
        options: VercelAPSchedulerOptions,
    ) -> None:
        self.scheduler = scheduler
        self.options = options
        self._logger = logging.getLogger(f"{LOGGER.name}.{options.scheduler_id}")
        self._pending_jobs_reference_time: datetime | None = None
        self._pending_cursor: MemoryCursor = MemoryCursor.empty()
        self._job_definitions: dict[str, _JobDefinition] = {}
        self._suppress_wakeup = False
        self._adopt_instance_methods()

    def _adopt_instance_methods(self) -> None:
        self.scheduler.get_queue_subscriptions = MethodType(  # type: ignore[attr-defined]
            lambda sched: self.get_queue_subscriptions(),
            self.scheduler,
        )
        self.scheduler.wakeup = MethodType(  # type: ignore[method-assign]
            lambda sched: self.wakeup(),
            self.scheduler,
        )

    def capture_job_definition(
        self,
        job: Any,
        add_args: tuple[Any, ...],
        add_kwargs: dict[str, Any],
    ) -> None:
        try:
            definition = _build_definition(job, add_args, add_kwargs)
        except Exception:
            self._logger.debug("failed to capture APScheduler job definition", exc_info=True)
            return
        self._job_definitions[str(job.id)] = definition

    def definition_for_job(self, job: Any) -> _JobDefinition:
        existing = self._job_definitions.get(str(job.id))
        if existing is not None:
            return existing
        fallback = _JobDefinition(
            schedule_key=f"id:{job.id}",
            fingerprint=_json_hash({
                "func": _job_func_name(job.func),
                "id": str(job.id),
                "trigger": repr(job.trigger),
            }),
            trigger_kind=type(job.trigger).__name__,
            unanchored_interval=False,
        )
        self._job_definitions[str(job.id)] = fallback
        return fallback

    def get_queue_subscriptions(self) -> list[dict[str, Any]]:
        entry: dict[str, Any] = {
            "topic": self.options.wakeup_topic,
            "retry_after_seconds": self.options.retry_after_seconds,
            "max_concurrency": self.options.max_concurrency,
        }
        if self.options.max_attempts is not None:
            entry["max_deliveries"] = self.options.max_attempts
        return [entry]

    def build_wakeup_idempotency_key(self, logical_time: datetime) -> str:
        logical_time_utc = as_utc(logical_time, name="logical_time")
        return f"{WAKEUP_KEY_PREFIX}:{self.options.scheduler_id}:{logical_time_utc.isoformat()}"

    def wakeup(self) -> None:
        if self._suppress_wakeup or self.scheduler.state != STATE_RUNNING:
            return
        self.seed()

    def ensure_started(
        self,
        *,
        pending_jobs_reference_time: datetime | None = None,
        cursor: MemoryCursor | None = None,
    ) -> None:
        reference = (
            require_aware_datetime(
                pending_jobs_reference_time,
                name="pending_jobs_reference_time",
            ).astimezone(self.scheduler.timezone)
            if pending_jobs_reference_time is not None
            else None
        )
        self._pending_jobs_reference_time = reference
        self._pending_cursor = cursor or MemoryCursor.empty()
        self._suppress_wakeup = True
        try:
            if self.scheduler.state == STATE_STOPPED:
                self._inject_default_executor()
                BaseScheduler.start(self.scheduler, paused=False)
        finally:
            self._pending_jobs_reference_time = None
            self._pending_cursor = MemoryCursor.empty()
            self._suppress_wakeup = False

    def _inject_default_executor(self) -> None:
        executors = self.scheduler._executors
        if "default" in executors:
            return
        self.scheduler.add_executor(VercelInlineExecutor(), "default")

    def materialize_pending_job(self, job: Any) -> None:
        reference = self._pending_jobs_reference_time
        if reference is None or hasattr(job, "next_run_time"):
            return

        definition = self.definition_for_job(job)
        cursor_entry = self._pending_cursor.jobs.get(definition.schedule_key)
        if cursor_entry is not None and cursor_entry.fingerprint == definition.fingerprint:
            if cursor_entry.state == "scheduled":
                job._modify(next_run_time=cursor_entry.next_run_time)
            else:
                job._modify(next_run_time=None)
            return

        if definition.unanchored_interval:
            raise RuntimeError(
                f'APScheduler job "{job.id}" uses an interval trigger without an explicit '
                "start_date. This is not supported with MemoryJobStore on Vercel because "
                "APScheduler anchors it to import time on every cold start."
            )

        job._modify(
            next_run_time=job.trigger.get_next_fire_time(
                None,
                reference,
            )
        )

    def _memory_cursor(self) -> MemoryCursor:
        jobs: dict[str, CursorEntry] = {}
        with self.scheduler._jobstores_lock:
            for jobstore in self.scheduler._jobstores.values():
                if not isinstance(jobstore, MemoryJobStore):
                    continue
                for job in jobstore.get_all_jobs():
                    definition = self.definition_for_job(job)
                    next_run_time = getattr(job, "next_run_time", None)
                    if next_run_time is None:
                        jobs[definition.schedule_key] = CursorEntry(
                            job_id=str(job.id),
                            fingerprint=definition.fingerprint,
                            state="paused",
                        )
                    else:
                        jobs[definition.schedule_key] = CursorEntry(
                            job_id=str(job.id),
                            fingerprint=definition.fingerprint,
                            state="scheduled",
                            next_run_time=next_run_time,
                        )
        return MemoryCursor(jobs=jobs)

    def _get_next_wakeup_time_unchecked(self) -> datetime | None:
        next_wakeup_time: datetime | None = None
        with self.scheduler._jobstores_lock:
            for jobstore in self.scheduler._jobstores.values():
                next_run_time = jobstore.get_next_run_time()
                if next_run_time is not None:
                    next_wakeup_time = earliest(
                        next_wakeup_time,
                        next_run_time.astimezone(self.scheduler.timezone),
                    )
        return next_wakeup_time

    def get_next_wakeup_time(self) -> datetime | None:
        self.ensure_started(pending_jobs_reference_time=datetime.now(UTC))
        return self._get_next_wakeup_time_unchecked()

    def seed(
        self,
        *,
        now: datetime | None = None,
        kind: str = "seed",
    ) -> PublishedWakeup | None:
        now_utc = as_utc(now or datetime.now(UTC), name="now")
        self.ensure_started(pending_jobs_reference_time=now_utc)
        if self.scheduler.state not in {STATE_RUNNING, STATE_PAUSED}:
            return None

        next_wakeup_time = self._get_next_wakeup_time_unchecked()
        if next_wakeup_time is None:
            return None
        return self.publish_wakeup(
            next_wakeup_time,
            cursor=self._memory_cursor(),
            now=now_utc,
            kind=kind,
        )

    def publish_wakeup(
        self,
        logical_time: datetime,
        *,
        cursor: MemoryCursor,
        now: datetime | None = None,
        kind: str = "tick",
    ) -> PublishedWakeup:
        now_utc = as_utc(now or datetime.now(UTC), name="now")
        scheduled_logical_time = canonical_scheduled_logical_time(
            logical_time,
            now=now_utc,
            max_delay_seconds=self.options.max_delay_seconds,
        )
        delay_seconds = max(0, math.ceil((scheduled_logical_time - now_utc).total_seconds()))
        idempotency_key = self.build_wakeup_idempotency_key(scheduled_logical_time)
        payload = WakeupPayload(
            scheduler_id=self.options.scheduler_id,
            logical_time=scheduled_logical_time,
            cursor=cursor,
            kind=kind,
        ).to_payload()
        try:
            message_id = vqs_sync.send(
                self.options.wakeup_topic,
                payload,
                idempotency_key=idempotency_key,
                retention=self.options.retention_seconds,
                delay=delay_seconds,
            )
        except vqs.DuplicateIdempotencyKeyError:
            self._logger.info(
                'Wakeup "%s" is already scheduled via idempotency key "%s"',
                scheduled_logical_time,
                idempotency_key,
            )
            message_id = None
        return PublishedWakeup(
            logical_time=scheduled_logical_time,
            delay_seconds=delay_seconds,
            idempotency_key=idempotency_key,
            message_id=message_id,
        )

    def process_payload(
        self,
        payload: WakeupPayload,
        *,
        publish_next: bool = True,
        now: datetime | None = None,
    ) -> WakeupProcessingResult:
        if payload.scheduler_id != self.options.scheduler_id:
            raise ValueError(
                f"Wakeup payload targeted scheduler {payload.scheduler_id!r}, "
                f"expected {self.options.scheduler_id!r}"
            )
        return self.process_wakeup(
            payload.logical_time,
            cursor=payload.cursor,
            publish_next=publish_next,
            now=now,
        )

    def process_wakeup(
        self,
        logical_time: datetime,
        *,
        cursor: MemoryCursor | None = None,
        publish_next: bool = True,
        now: datetime | None = None,
    ) -> WakeupProcessingResult:
        effective_logical_time = require_aware_datetime(
            logical_time,
            name="logical_time",
        ).astimezone(self.scheduler.timezone)
        self.ensure_started(
            pending_jobs_reference_time=effective_logical_time,
            cursor=cursor,
        )
        if self.scheduler.state != STATE_RUNNING:
            return WakeupProcessingResult(
                logical_time=effective_logical_time,
                due_job_ids=(),
                next_wakeup_time=self._get_next_wakeup_time_unchecked(),
                published_wakeup=None,
            )

        due_jobs, retry_wakeup_time = self._plan_due_jobs(effective_logical_time)
        self._submit_due_jobs(due_jobs, logical_time=effective_logical_time)
        next_wakeup_time = earliest(
            retry_wakeup_time,
            self._get_next_wakeup_time_unchecked(),
        )
        published_wakeup = (
            self.publish_wakeup(
                next_wakeup_time,
                cursor=self._memory_cursor(),
                now=now,
                kind="tick",
            )
            if publish_next and next_wakeup_time is not None
            else None
        )
        return WakeupProcessingResult(
            logical_time=effective_logical_time,
            due_job_ids=tuple(plan.job.id for plan in due_jobs),
            next_wakeup_time=next_wakeup_time,
            published_wakeup=published_wakeup,
        )

    def _plan_due_jobs(
        self,
        logical_time: datetime,
    ) -> tuple[list[_DueJobPlan], datetime | None]:
        due_jobs: list[_DueJobPlan] = []
        retry_wakeup_time: datetime | None = None
        with self.scheduler._jobstores_lock:
            for jobstore_alias, jobstore in self.scheduler._jobstores.items():
                try:
                    due_store_jobs = jobstore.get_due_jobs(logical_time)
                except Exception as exc:
                    self._logger.warning(
                        'Error getting due jobs from job store "%s": %s',
                        jobstore_alias,
                        exc,
                    )
                    retry_wakeup_time = earliest(
                        retry_wakeup_time,
                        logical_time + timedelta(seconds=self.scheduler.jobstore_retry_interval),
                    )
                    continue

                for job in due_store_jobs:
                    run_times = job._get_run_times(logical_time)
                    if run_times and job.coalesce:
                        run_times = run_times[-1:]

                    if not run_times:
                        continue

                    next_run_time = job.trigger.get_next_fire_time(run_times[-1], logical_time)
                    due_jobs.append(
                        _DueJobPlan(
                            job=job,
                            jobstore_alias=jobstore_alias,
                            run_times=list(run_times),
                            next_run_time=next_run_time,
                        )
                    )

        return due_jobs, retry_wakeup_time

    def _submit_due_jobs(
        self,
        due_jobs: list[_DueJobPlan],
        *,
        logical_time: datetime,
    ) -> None:
        events = []
        with self.scheduler._jobstores_lock:
            for plan in due_jobs:
                try:
                    executor = self.scheduler._lookup_executor(plan.job.executor)
                except BaseException:
                    self._logger.error(
                        'Executor lookup ("%s") failed for job "%s" -- removing it from '
                        "the job store",
                        plan.job.executor,
                        plan.job,
                    )
                    self.scheduler.remove_job(plan.job.id, plan.jobstore_alias)
                    continue

                try:
                    if hasattr(executor, "set_reference_time"):
                        executor.set_reference_time(logical_time)
                    executor.submit_job(plan.job, plan.run_times)
                except MaxInstancesReachedError:
                    self._logger.warning(
                        'Execution of job "%s" skipped: maximum number of running '
                        "instances reached (%d)",
                        plan.job,
                        plan.job.max_instances,
                    )
                    events.append(
                        JobSubmissionEvent(
                            EVENT_JOB_MAX_INSTANCES,
                            plan.job.id,
                            plan.jobstore_alias,
                            plan.run_times,
                        )
                    )
                except BaseException:
                    self._logger.exception(
                        'Error submitting job "%s" to executor "%s"',
                        plan.job,
                        plan.job.executor,
                    )
                else:
                    events.append(
                        JobSubmissionEvent(
                            EVENT_JOB_SUBMITTED,
                            plan.job.id,
                            plan.jobstore_alias,
                            plan.run_times,
                        )
                    )

                if plan.next_run_time is not None:
                    plan.job._modify(next_run_time=plan.next_run_time)
                    self.scheduler._lookup_jobstore(plan.jobstore_alias).update_job(plan.job)
                else:
                    self.scheduler.remove_job(plan.job.id, plan.jobstore_alias)

        for event in events:
            self.scheduler._dispatch_event(event)

    def shutdown(self, *, wait: bool = True) -> None:
        if self.scheduler.state != STATE_STOPPED:
            BaseScheduler.shutdown(self.scheduler, wait=wait)


def adopt_scheduler(
    scheduler: BaseScheduler,
    options: VercelAPSchedulerOptions | dict[str, Any] | None = None,
) -> SchedulerAdapter:
    install_vercel_apscheduler_integration(options=options)
    existing = get_adapter(scheduler)
    if existing is not None:
        return existing
    resolved_options = VercelAPSchedulerOptions.from_value(options or _PATCH_STATE.default_options)
    adapter = SchedulerAdapter(scheduler, resolved_options)
    setattr(scheduler, ADAPTER_ATTR, adapter)
    return adapter


def seed_next_wakeup(
    scheduler: BaseScheduler,
    *,
    now: datetime | None = None,
    options: VercelAPSchedulerOptions | dict[str, Any] | None = None,
) -> PublishedWakeup | None:
    adapter = adopt_scheduler(scheduler, options)
    try:
        return adapter.seed(now=now)
    finally:
        adapter.shutdown(wait=True)


def _patched_init(self: BaseScheduler, *args: Any, **kwargs: Any) -> Any:
    original_init = _PATCH_STATE.original_init
    if original_init is None:
        raise RuntimeError("APScheduler integration patch is not initialized")
    result = original_init(self, *args, **kwargs)
    if get_adapter(self) is None:
        options = _PATCH_STATE.default_options or VercelAPSchedulerOptions.from_env()
        setattr(self, ADAPTER_ATTR, SchedulerAdapter(self, options))
    return result


def _patched_add_job(self: BaseScheduler, *args: Any, **kwargs: Any) -> Any:
    original_add_job = _PATCH_STATE.original_add_job
    if original_add_job is None:
        raise RuntimeError("APScheduler integration patch is not initialized")
    job = original_add_job(self, *args, **kwargs)
    adapter = get_adapter(self)
    if adapter is not None:
        adapter.capture_job_definition(job, args, dict(kwargs))
    return job


def _patched_real_add_job(
    self: BaseScheduler,
    job: Any,
    jobstore_alias: str,
    replace_existing: bool,
) -> Any:
    adapter = get_adapter(self)
    if adapter is not None:
        adapter.materialize_pending_job(job)
    original_real_add_job = _PATCH_STATE.original_real_add_job
    if original_real_add_job is None:
        raise RuntimeError("APScheduler integration patch is not initialized")
    return original_real_add_job(self, job, jobstore_alias, replace_existing)


def _defused_start(self: BaseScheduler, paused: bool = False) -> None:
    adapter = get_adapter(self)
    if adapter is None or not is_vercel_runtime():
        original = _original_start_for_instance(self)
        original(self, paused=paused)
        return
    adapter.ensure_started(pending_jobs_reference_time=datetime.now(UTC))


def _original_start_for_instance(instance: BaseScheduler) -> Callable[..., Any]:
    class_name = type(instance).__name__
    module_name = type(instance).__module__
    if class_name == "BlockingScheduler" and _PATCH_STATE.original_blocking_start is not None:
        return _PATCH_STATE.original_blocking_start
    if class_name == "BackgroundScheduler" and _PATCH_STATE.original_background_start is not None:
        return _PATCH_STATE.original_background_start
    if module_name.endswith(".asyncio") and _PATCH_STATE.original_asyncio_start is not None:
        return _PATCH_STATE.original_asyncio_start
    return BaseScheduler.start


def _patch_scheduler_start_methods() -> None:
    try:
        from apscheduler.schedulers.blocking import (
            BlockingScheduler,  # type: ignore[import-untyped]
        )
    except ImportError:
        BlockingScheduler = None  # type: ignore[assignment]
    if BlockingScheduler is not None and _PATCH_STATE.original_blocking_start is None:
        _PATCH_STATE.original_blocking_start = BlockingScheduler.start
        BlockingScheduler.start = _defused_start  # type: ignore[method-assign]

    try:
        from apscheduler.schedulers.background import (
            BackgroundScheduler,  # type: ignore[import-untyped]
        )
    except ImportError:
        BackgroundScheduler = None  # type: ignore[assignment]
    if BackgroundScheduler is not None and _PATCH_STATE.original_background_start is None:
        _PATCH_STATE.original_background_start = BackgroundScheduler.start
        BackgroundScheduler.start = _defused_start  # type: ignore[method-assign]

    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler  # type: ignore[import-untyped]
    except ImportError:
        AsyncIOScheduler = None  # type: ignore[assignment]
    if AsyncIOScheduler is not None and _PATCH_STATE.original_asyncio_start is None:
        _PATCH_STATE.original_asyncio_start = AsyncIOScheduler.start
        AsyncIOScheduler.start = _defused_start  # type: ignore[method-assign]


def install_vercel_apscheduler_integration(
    *,
    options: VercelAPSchedulerOptions | dict[str, Any] | None = None,
) -> None:
    resolved_options = VercelAPSchedulerOptions.from_value(options)
    _PATCH_STATE.default_options = resolved_options
    if _PATCH_STATE.installed:
        return

    _PATCH_STATE.original_init = BaseScheduler.__init__
    _PATCH_STATE.original_add_job = BaseScheduler.add_job
    _PATCH_STATE.original_real_add_job = BaseScheduler._real_add_job

    BaseScheduler.__init__ = _patched_init  # type: ignore[method-assign]
    BaseScheduler.add_job = _patched_add_job  # type: ignore[method-assign]
    BaseScheduler._real_add_job = _patched_real_add_job  # type: ignore[method-assign]
    _patch_scheduler_start_methods()
    _PATCH_STATE.installed = True


def maybe_auto_install_from_env() -> None:
    value = os.environ.get("VERCEL_PYTHON_AUTO_APSCHEDULER")
    if value and value.strip().casefold() in {"1", "true", "yes", "on"}:
        install_vercel_apscheduler_integration()
