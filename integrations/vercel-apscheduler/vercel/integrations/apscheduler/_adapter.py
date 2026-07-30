from __future__ import annotations

from typing import Any, cast

import json
import logging
import math
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from os import environ
from sys import modules
from types import MethodType

import vercel.queue as vqs
import vercel.queue.sync as vqs_sync

from ._driver import (
    APSchedulerConfigurationError,
    RedisDriver,
    StartDecision,
    WakeToken,
)
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
    RedisJobStore,
)
from ._jobstore import RedisJobCoordinator
from ._options import (
    SUBSCRIBER_ID_ENV,
    VercelAPSchedulerOptions,
    _SchedulerIdentity,
    is_discovery_runtime,
    is_queue_serving_runtime,
    is_vercel_runtime,
)
from ._payload import StartPayload, WakeupPayload
from ._time import as_utc, canonical_scheduled_logical_time, earliest

LOGGER = logging.getLogger("vercel.integrations.apscheduler")
UTC = timezone.utc
ADAPTER_ATTR = "_vercel_apscheduler_adapter"
DEPLOYMENT_ENV = "VERCEL_DEPLOYMENT_ID"
SUBSCRIBERS_ENV = "VERCEL_APSCHEDULER_SUBSCRIBERS"
WAKEUP_KEY_PREFIX = "aps"

__all__ = [
    "ADAPTER_ATTR",
    "PublishedWakeup",
    "SchedulerAdapter",
    "WakeupProcessingResult",
    "adopt_scheduler",
    "get_adapter",
    "install_vercel_apscheduler_integration",
]


@dataclass(frozen=True, slots=True)
class PublishedWakeup:
    logical_time: datetime
    delay_seconds: int
    idempotency_key: str
    message_id: str | None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "logical_time",
            as_utc(self.logical_time, name="logical_time"),
        )


@dataclass(frozen=True, slots=True)
class WakeupProcessingResult:
    logical_time: datetime
    due_job_ids: tuple[str, ...]
    next_wakeup_time: datetime | None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "logical_time",
            as_utc(self.logical_time, name="logical_time"),
        )
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
    expected_revision: int


@dataclass(frozen=True, slots=True)
class _JobDefinition:
    explicit_id: bool


@dataclass(slots=True)
class _PatchState:
    installed: bool = False
    register_queues: bool = False
    default_options: VercelAPSchedulerOptions | None = None
    original_init: Callable[..., Any] | None = None
    original_add_job: Callable[..., Any] | None = None
    original_real_add_job: Callable[..., Any] | None = None
    original_modify_job: Callable[..., Any] | None = None
    original_remove_job: Callable[..., Any] | None = None
    original_remove_all_jobs: Callable[..., Any] | None = None
    original_resume_job: Callable[..., Any] | None = None
    original_base_start: Callable[..., Any] | None = None
    original_blocking_start: Callable[..., Any] | None = None
    original_background_start: Callable[..., Any] | None = None
    original_asyncio_start: Callable[..., Any] | None = None
    original_pause: Callable[..., Any] | None = None
    original_resume: Callable[..., Any] | None = None


_PATCH_STATE = _PatchState()


def get_adapter(scheduler: Any) -> SchedulerAdapter | None:
    return cast("SchedulerAdapter | None", getattr(scheduler, ADAPTER_ATTR, None))


class SchedulerAdapter:
    """Turns one durable APScheduler instance into one fenced Queue driver."""

    def __init__(
        self,
        scheduler: BaseScheduler,
        options: VercelAPSchedulerOptions,
    ) -> None:
        self.scheduler = scheduler
        self.options = options
        self.identity = _SchedulerIdentity.from_env()
        self._identity_bound = environ.get(SUBSCRIBER_ID_ENV) is not None
        self._deployment: str | None = None
        self._driver: RedisDriver | None = None
        self._coordinator: RedisJobCoordinator | None = None
        self._job_definitions: dict[str, _JobDefinition] = {}
        self._current_add_explicit = False
        self._runtime_mutation_depth = 0
        self._lifecycle_called = False
        self._queue_lifecycle_warning_emitted = False
        self._suppress_wakeup = False
        self._native_wakeup = scheduler.wakeup
        self._adopt_instance_methods()

    @property
    def deployment(self) -> str:
        self._bind_runtime()
        return cast("str", self._deployment)

    @property
    def driver(self) -> RedisDriver:
        self._bind_runtime()
        return cast("RedisDriver", self._driver)

    @property
    def coordinator(self) -> RedisJobCoordinator:
        self._bind_runtime()
        return cast("RedisJobCoordinator", self._coordinator)

    @property
    def is_runtime_mutation(self) -> bool:
        return self._runtime_mutation_depth > 0 and not self._suppress_wakeup

    def _adopt_instance_methods(self) -> None:
        self.scheduler.wakeup = MethodType(  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
            lambda scheduler: self.wakeup(),
            self.scheduler,
        )

    def capture_job_definition(
        self,
        job: Any,
        add_args: tuple[Any, ...],
        add_kwargs: dict[str, Any],
    ) -> None:
        explicit_id = add_kwargs.get("id")
        if explicit_id is None and len(add_args) > 4:
            explicit_id = add_args[4]
        self._job_definitions[str(job.id)] = _JobDefinition(
            explicit_id=explicit_id is not None,
        )

    def start(self, *, paused: bool = False) -> None:
        """Durably start this deployment's scheduler exactly once."""
        self._bind_runtime()
        self._validate_durable_configuration()
        self._lifecycle_called = True
        self.ensure_local_started()
        now = datetime.now(UTC)
        if paused:
            self.driver.pause(now)
            self._pause_local()
            return
        decision = self.driver.start(now)
        self._publish_start_if_needed(decision, now=now)
        if (
            not decision.changed
            and decision.start_status == "active"
            and decision.current_wake is not None
            and decision.current_wake.status == "pending"
        ):
            self.publish_wakeup(decision.current_wake, now=now)
        self._resume_local_if_paused()

    def auto_activate(
        self,
        *,
        idle_timeout_seconds: int | None = None,
    ) -> None:
        """Activate on request activity without overriding an explicit pause."""
        self._bind_runtime()
        self._validate_durable_configuration()
        self._lifecycle_called = True
        self.ensure_local_started()
        now = datetime.now(UTC)
        decision = self.driver.auto_activate(
            now,
            idle_timeout_seconds=idle_timeout_seconds,
        )
        if decision.state != "running":
            self._pause_local()
            return
        self._publish_start_if_needed(decision, now=now)
        if (
            not decision.changed
            and decision.start_status == "active"
            and decision.current_wake is not None
            and decision.current_wake.status == "pending"
        ):
            self.publish_wakeup(decision.current_wake, now=now)
        self._resume_local_if_paused()

    def pause(self) -> None:
        """Durably fence the current generation."""
        self._bind_runtime()
        self._validate_durable_configuration()
        self._lifecycle_called = True
        self.ensure_local_started()
        self.driver.pause(datetime.now(UTC))
        self._pause_local()

    def resume(self) -> None:
        """Resume by creating one new durable generation."""
        self.start()

    def _publish_start_if_needed(
        self,
        decision: StartDecision,
        *,
        now: datetime,
    ) -> None:
        if decision.start_status != "pending":
            return
        payload = StartPayload(
            scheduler_id=self.identity.scheduler_id,
            generation=decision.generation,
        ).to_payload()
        idempotency_key = (
            f"{WAKEUP_KEY_PREFIX}:start:{self.deployment}:"
            f"{self.identity.scheduler_id}:{decision.generation}"
        )
        try:
            vqs_sync.send(
                self.identity.start_topic,
                payload,
                deployment=self.deployment,
                idempotency_key=idempotency_key,
                retention=self.options.retention_seconds,
            )
        except vqs.DuplicateIdempotencyKeyError:
            pass
        self.driver.mark_start_published(decision.generation, now)

    def publish_wakeup(
        self,
        token: WakeToken,
        *,
        now: datetime | None = None,
    ) -> PublishedWakeup:
        """Publish the driver's current wake token with a stable Queue identity."""
        now_utc = as_utc(now or datetime.now(UTC), name="now")
        delay_seconds = max(
            0,
            math.ceil((token.logical_time - now_utc).total_seconds()),
        )
        idempotency_key = (
            f"{WAKEUP_KEY_PREFIX}:wake:{self.deployment}:"
            f"{self.identity.scheduler_id}:{token.generation}:{token.sequence}"
        )
        payload = WakeupPayload.from_token(
            self.identity.scheduler_id,
            token,
        ).to_payload()
        try:
            message_id = vqs_sync.send(
                self.identity.wakeup_topic,
                payload,
                deployment=self.deployment,
                idempotency_key=idempotency_key,
                retention=self.options.retention_seconds,
                delay=delay_seconds,
            )
        except vqs.DuplicateIdempotencyKeyError:
            message_id = None
        self.driver.mark_wake_published(
            token.generation,
            token.sequence,
            now_utc,
        )
        return PublishedWakeup(
            logical_time=token.logical_time,
            delay_seconds=delay_seconds,
            idempotency_key=idempotency_key,
            message_id=message_id,
        )

    def publish_pending_wakeup(self) -> PublishedWakeup | None:
        """Repair a wake reserved in Redis before a failed Queue send."""
        snapshot = self.driver.snapshot()
        wake = snapshot.current_wake
        if snapshot.state != "running" or wake is None or wake.status != "pending":
            return None
        return self.publish_wakeup(wake)

    def canonical_wakeup_time(
        self,
        logical_time: datetime,
        *,
        now: datetime | None = None,
    ) -> datetime:
        return canonical_scheduled_logical_time(
            logical_time,
            now=as_utc(now or datetime.now(UTC), name="now"),
            max_delay_seconds=self.options.max_delay_seconds,
        )

    @contextmanager
    def runtime_mutation(self, *, explicit_id: bool | None = None) -> Iterator[None]:
        self.prepare_runtime_mutation()
        prior_explicit = self._current_add_explicit
        if explicit_id is not None:
            self._current_add_explicit = explicit_id
        self._runtime_mutation_depth += 1
        try:
            yield
        finally:
            self._runtime_mutation_depth -= 1
            self._current_add_explicit = prior_explicit

    def prepare_runtime_mutation(self) -> None:
        if not self._lifecycle_called:
            raise APSchedulerConfigurationError(
                "call scheduler.start() before mutating durable jobs in this Function"
            )
        self.ensure_local_started()

    def ensure_local_started(self) -> None:
        """Start APScheduler internals without starting a scheduler thread."""
        self._bind_runtime()
        self._validate_durable_configuration()
        if self.scheduler.state != STATE_STOPPED:
            return
        self._inject_inline_executor()
        previous = self._suppress_wakeup
        self._suppress_wakeup = True
        try:
            original_start = cast(
                "Callable[..., Any]",
                _PATCH_STATE.original_base_start,
            )
            original_start(self.scheduler, paused=False)
            self._validate_materialized_jobs()
        except BaseException:
            if self.scheduler.state != STATE_STOPPED:
                BaseScheduler.shutdown(self.scheduler, wait=True)
            raise
        finally:
            self._suppress_wakeup = previous

    def _pause_local(self) -> None:
        if self.scheduler.state != STATE_RUNNING:
            return
        original_pause = cast(
            "Callable[[BaseScheduler], None]",
            _PATCH_STATE.original_pause,
        )
        original_pause(self.scheduler)

    def _resume_local_if_paused(self) -> None:
        if self.scheduler.state != STATE_PAUSED:
            return
        previous = self._suppress_wakeup
        self._suppress_wakeup = True
        try:
            original_resume = cast(
                "Callable[[BaseScheduler], None]",
                _PATCH_STATE.original_resume,
            )
            original_resume(self.scheduler)
        finally:
            self._suppress_wakeup = previous

    def activate_generation(self, activation_time: datetime) -> None:
        """Start locally and skip occurrences from the paused interval."""
        self.ensure_local_started()
        self._rebase_before(
            as_utc(activation_time, name="activation_time").astimezone(self.scheduler.timezone)
        )

    def get_next_wakeup_time(self, reference_time: datetime) -> datetime | None:
        """Return the exact next durable due time, if any."""
        del reference_time
        with self.scheduler._jobstores_lock:
            return self.scheduler._jobstores["default"].get_next_run_time()

    def process_wakeup(
        self,
        logical_time: datetime,
        *,
        now: datetime | None = None,
    ) -> WakeupProcessingResult:
        self.ensure_local_started()
        delivery_time = as_utc(now or datetime.now(UTC), name="now").astimezone(
            self.scheduler.timezone
        )
        scheduled_time = as_utc(logical_time, name="logical_time").astimezone(
            self.scheduler.timezone
        )
        evaluation_time = max(delivery_time, scheduled_time)
        previous = self._suppress_wakeup
        self._suppress_wakeup = True
        try:
            due_jobs, retry_time = self._plan_due_jobs(evaluation_time)
            self._submit_due_jobs(due_jobs, logical_time=evaluation_time)
            next_wakeup_time = earliest(
                retry_time,
                self.get_next_wakeup_time(evaluation_time),
            )
        finally:
            self._suppress_wakeup = previous
        return WakeupProcessingResult(
            logical_time=evaluation_time.astimezone(UTC),
            due_job_ids=tuple(plan.job.id for plan in due_jobs),
            next_wakeup_time=(
                next_wakeup_time.astimezone(UTC) if next_wakeup_time is not None else None
            ),
        )

    def materialize_pending_job(
        self,
        job: Any,
        jobstore_alias: str,
        replace_existing: bool,
    ) -> bool:
        jobstore = self.scheduler._lookup_jobstore(jobstore_alias)
        if not isinstance(jobstore, RedisJobStore):
            raise APSchedulerConfigurationError(
                "vercel-apscheduler v1 supports RedisJobStore only; "
                f'job store "{jobstore_alias}" is {type(jobstore).__name__}'
            )
        definition = self._job_definitions.get(str(job.id))
        explicit_id = (
            definition.explicit_id if definition is not None else self._current_add_explicit
        )
        if not explicit_id:
            raise APSchedulerConfigurationError(
                "jobs in a durable APScheduler store require an explicit stable id"
            )
        if getattr(job, "executor", "default") != "default":
            raise APSchedulerConfigurationError(
                f'job "{job.id}" must use the default Vercel inline executor'
            )
        existing = jobstore.lookup_job(job.id)
        if existing is None:
            return True
        if not replace_existing:
            raise APSchedulerConfigurationError(
                f'job "{job.id}" already exists in Redis; declare it with replace_existing=True'
            )
        if self.is_runtime_mutation:
            return True
        job._jobstore_alias = jobstore_alias
        return False

    def wakeup(self) -> None:
        if not is_vercel_runtime():
            self._native_wakeup()
            return
        if self._suppress_wakeup or self.scheduler.state != STATE_RUNNING:
            return
        self.publish_pending_wakeup()

    def warn_ignored_queue_lifecycle(self, method: str) -> None:
        if self._queue_lifecycle_warning_emitted:
            return
        self._queue_lifecycle_warning_emitted = True
        LOGGER.warning(
            "Ignoring scheduler.%s() while serving an APScheduler queue delivery; "
            "call lifecycle methods from a web Function instead",
            method,
        )

    def _bind_runtime(self) -> None:
        if not self._identity_bound:
            self.identity = _resolve_identity(self.scheduler)
            self._identity_bound = True
        deployment = environ.get(DEPLOYMENT_ENV)
        if not deployment:
            raise APSchedulerConfigurationError(
                f"{DEPLOYMENT_ENV} is required to run an APScheduler subscriber"
            )
        if self._deployment is not None and self._deployment != deployment:
            raise APSchedulerConfigurationError(
                "one scheduler object cannot be shared across Vercel deployments"
            )
        self._deployment = deployment
        stores = self._validate_durable_configuration()
        tag = f"{{{deployment}:{self.identity.scheduler_id}}}"
        for alias, store in stores.items():
            namespace = getattr(store, "_vercel_apscheduler_namespace", None)
            expected_namespace = (deployment, self.identity.scheduler_id)
            if namespace is not None and namespace != expected_namespace:
                raise APSchedulerConfigurationError(
                    f'job store "{alias}" is already bound to another scheduler'
                )
            if namespace is None:
                if any(character in store.jobs_key + store.run_times_key for character in "{}"):
                    raise APSchedulerConfigurationError(
                        "custom Redis job-store keys cannot contain Redis hash tags"
                    )
                store.jobs_key = f"{store.jobs_key}:{tag}:jobs"
                store.run_times_key = f"{store.run_times_key}:{tag}:run_times"
                store.__dict__["_vercel_apscheduler_namespace"] = expected_namespace
        if self._driver is None:
            self._driver = RedisDriver(
                stores["default"].redis,
                deployment=deployment,
                scheduler_id=self.identity.scheduler_id,
            )
        if self._coordinator is None:
            self._coordinator = RedisJobCoordinator(
                stores["default"],
                self._driver,
                self,
            )
            self._coordinator.install()

    def _validate_durable_configuration(self) -> dict[str, RedisJobStore]:
        stores = self.scheduler._jobstores
        if "default" not in stores:
            raise APSchedulerConfigurationError(
                "vercel-apscheduler requires a configured default RedisJobStore"
            )
        if set(stores) != {"default"}:
            aliases = ", ".join(sorted(stores))
            raise APSchedulerConfigurationError(
                "vercel-apscheduler v1 supports exactly one job store named "
                f'"default"; configured: {aliases}'
            )
        if not isinstance(stores["default"], RedisJobStore):
            raise APSchedulerConfigurationError(
                "vercel-apscheduler requires a configured default RedisJobStore"
            )
        return cast("dict[str, RedisJobStore]", stores)

    def _inject_inline_executor(self) -> None:
        existing = self.scheduler._executors.get("default")
        if existing is not None and not isinstance(existing, VercelInlineExecutor):
            raise APSchedulerConfigurationError(
                "vercel-apscheduler requires the default executor; "
                "custom executors are not supported in v1"
            )
        if existing is None:
            self.scheduler.add_executor(VercelInlineExecutor(), "default")

    def _validate_materialized_jobs(self) -> None:
        with self.scheduler._jobstores_lock:
            for alias, jobstore in self.scheduler._jobstores.items():
                for job in jobstore.get_all_jobs():
                    if getattr(job, "executor", "default") != "default":
                        raise APSchedulerConfigurationError(
                            f'job "{job.id}" in "{alias}" must use the default executor'
                        )

    def _rebase_before(self, activation_time: datetime) -> None:
        with self.scheduler._jobstores_lock:
            for job, revision in self.coordinator.get_all_jobs_with_revisions():
                next_run_time = getattr(job, "next_run_time", None)
                if next_run_time is None or next_run_time >= activation_time:
                    continue
                next_run_time = job.trigger.get_next_fire_time(
                    None,
                    activation_time,
                )
                while next_run_time is not None and next_run_time < activation_time:
                    previous_run_time = next_run_time
                    next_run_time = job.trigger.get_next_fire_time(
                        previous_run_time,
                        activation_time,
                    )
                    if next_run_time is not None and next_run_time <= previous_run_time:
                        raise RuntimeError(
                            f'APScheduler trigger for job "{job.id}" did not advance while resuming'
                        )
                if next_run_time is None:
                    self.coordinator.cas_remove_job(job.id, revision)
                else:
                    job._modify(next_run_time=next_run_time)
                    self.coordinator.cas_update_job(job, revision)

    def _plan_due_jobs(
        self,
        logical_time: datetime,
    ) -> tuple[list[_DueJobPlan], datetime | None]:
        due_jobs: list[_DueJobPlan] = []
        retry_time: datetime | None = None
        with self.scheduler._jobstores_lock:
            try:
                jobs = self.coordinator.get_due_jobs_with_revisions(logical_time)
            except Exception as exc:
                self._logger.warning(
                    'Error getting due jobs from job store "default": %s',
                    exc,
                )
                retry_time = logical_time + timedelta(
                    seconds=self.scheduler.jobstore_retry_interval
                )
            else:
                for job, revision in jobs:
                    run_times = job._get_run_times(logical_time)
                    next_run_time = (
                        job.trigger.get_next_fire_time(run_times[-1], logical_time)
                        if run_times
                        else None
                    )
                    if run_times and job.coalesce:
                        run_times = run_times[-1:]
                    if run_times:
                        due_jobs.append(
                            _DueJobPlan(
                                job=job,
                                jobstore_alias="default",
                                run_times=list(run_times),
                                next_run_time=next_run_time,
                                expected_revision=revision,
                            )
                        )
        return due_jobs, retry_time

    def _submit_due_jobs(
        self,
        due_jobs: list[_DueJobPlan],
        *,
        logical_time: datetime,
    ) -> None:
        events = []
        with self.scheduler._jobstores_lock:
            for plan in due_jobs:
                executor = self.scheduler._lookup_executor(plan.job.executor)
                try:
                    if hasattr(executor, "set_reference_time"):
                        executor.set_reference_time(logical_time)
                    executor.submit_job(plan.job, plan.run_times)
                except MaxInstancesReachedError:
                    events.append(
                        JobSubmissionEvent(
                            EVENT_JOB_MAX_INSTANCES,
                            plan.job.id,
                            plan.jobstore_alias,
                            plan.run_times,
                        )
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
                if plan.next_run_time is None:
                    self.coordinator.cas_remove_job(
                        plan.job.id,
                        plan.expected_revision,
                    )
                else:
                    plan.job._modify(next_run_time=plan.next_run_time)
                    self.coordinator.cas_update_job(
                        plan.job,
                        plan.expected_revision,
                    )
        for event in events:
            self.scheduler._dispatch_event(event)

    @property
    def _logger(self) -> logging.Logger:
        return logging.getLogger(f"{LOGGER.name}.{self.identity.scheduler_id}")


def _resolve_identity(scheduler: BaseScheduler) -> _SchedulerIdentity:
    raw = environ.get(SUBSCRIBERS_ENV)
    if not raw:
        raise APSchedulerConfigurationError(
            f"{SUBSCRIBERS_ENV} is not set. Declare the scheduler in [[tool.vercel.subscribers]]."
        )
    try:
        entries = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise APSchedulerConfigurationError(f"{SUBSCRIBERS_ENV} must contain JSON") from exc
    if not isinstance(entries, list):
        raise APSchedulerConfigurationError(f"{SUBSCRIBERS_ENV} must contain a JSON array")

    matches: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        subscriber_id = entry.get("id")
        entrypoint = entry.get("entrypoint")
        if not isinstance(subscriber_id, str) or not isinstance(entrypoint, str):
            continue
        module_name, separator, variable_name = entrypoint.partition(":")
        if not separator:
            continue
        module = modules.get(module_name)
        if module is not None and getattr(module, variable_name, None) is scheduler:
            matches.append(subscriber_id)
    if len(matches) != 1:
        raise APSchedulerConfigurationError(
            "scheduler.start()/pause()/resume() must be called on exactly one "
            "object declared in [[tool.vercel.subscribers]]"
        )
    return _SchedulerIdentity.from_subscriber_id(matches[0])


def adopt_scheduler(
    scheduler: BaseScheduler,
    options: VercelAPSchedulerOptions | dict[str, Any] | None = None,
) -> SchedulerAdapter:
    existing = get_adapter(scheduler)
    if existing is not None:
        return existing
    install_vercel_apscheduler_integration(options=options)
    resolved = VercelAPSchedulerOptions.from_value(options or _PATCH_STATE.default_options)
    adapter = SchedulerAdapter(scheduler, resolved)
    setattr(scheduler, ADAPTER_ATTR, adapter)
    return adapter


def _patched_init(self: BaseScheduler, *args: Any, **kwargs: Any) -> Any:
    original = _PATCH_STATE.original_init
    if original is None:
        raise RuntimeError("APScheduler integration patch is not initialized")
    result = original(self, *args, **kwargs)
    options = VercelAPSchedulerOptions.from_value(_PATCH_STATE.default_options)
    adapter = SchedulerAdapter(self, options)
    setattr(self, ADAPTER_ATTR, adapter)
    if is_vercel_runtime() and (_PATCH_STATE.register_queues or is_queue_serving_runtime()):
        from ._subscriber import register_scheduler

        register_scheduler(self, options=options)
    return result


def _patched_add_job(self: BaseScheduler, *args: Any, **kwargs: Any) -> Any:
    original = _PATCH_STATE.original_add_job
    if original is None:
        raise RuntimeError("APScheduler integration patch is not initialized")
    adapter = get_adapter(self)
    explicit_id = kwargs.get("id")
    if explicit_id is None and len(args) > 4:
        explicit_id = args[4]
    if (
        adapter is not None
        and is_vercel_runtime()
        and not is_discovery_runtime()
        and not is_queue_serving_runtime()
        and adapter._lifecycle_called
    ):
        with adapter.runtime_mutation(explicit_id=explicit_id is not None):
            job = original(self, *args, **kwargs)
    else:
        job = original(self, *args, **kwargs)
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
    if adapter is not None and is_vercel_runtime():
        should_write = adapter.materialize_pending_job(
            job,
            jobstore_alias,
            replace_existing,
        )
        if not should_write:
            return None
    original = _PATCH_STATE.original_real_add_job
    if original is None:
        raise RuntimeError("APScheduler integration patch is not initialized")
    return original(self, job, jobstore_alias, replace_existing)


def _patched_modify_job(
    self: BaseScheduler,
    job_id: str,
    jobstore: str | None = None,
    **changes: Any,
) -> Any:
    original = _PATCH_STATE.original_modify_job
    if original is None:
        raise RuntimeError("APScheduler integration patch is not initialized")
    adapter = get_adapter(self)
    if (
        adapter is None
        or not is_vercel_runtime()
        or is_discovery_runtime()
        or is_queue_serving_runtime()
    ):
        return original(self, job_id, jobstore, **changes)
    with adapter.runtime_mutation():
        return original(self, job_id, jobstore, **changes)


def _patched_remove_job(
    self: BaseScheduler,
    job_id: str,
    jobstore: str | None = None,
) -> None:
    original = _PATCH_STATE.original_remove_job
    if original is None:
        raise RuntimeError("APScheduler integration patch is not initialized")
    adapter = get_adapter(self)
    if (
        adapter is None
        or not is_vercel_runtime()
        or is_discovery_runtime()
        or is_queue_serving_runtime()
    ):
        original(self, job_id, jobstore)
        return
    with adapter.runtime_mutation():
        original(self, job_id, jobstore)


def _patched_remove_all_jobs(
    self: BaseScheduler,
    jobstore: str | None = None,
) -> None:
    original = _PATCH_STATE.original_remove_all_jobs
    if original is None:
        raise RuntimeError("APScheduler integration patch is not initialized")
    adapter = get_adapter(self)
    if (
        adapter is None
        or not is_vercel_runtime()
        or is_discovery_runtime()
        or is_queue_serving_runtime()
    ):
        original(self, jobstore)
        return
    with adapter.runtime_mutation():
        original(self, jobstore)


def _patched_resume_job(
    self: BaseScheduler,
    job_id: str,
    jobstore: str | None = None,
) -> Any:
    original = _PATCH_STATE.original_resume_job
    if original is None:
        raise RuntimeError("APScheduler integration patch is not initialized")
    adapter = get_adapter(self)
    if (
        adapter is None
        or not is_vercel_runtime()
        or is_discovery_runtime()
        or is_queue_serving_runtime()
    ):
        return original(self, job_id, jobstore)
    adapter.prepare_runtime_mutation()
    return original(self, job_id, jobstore)


def _patched_start(self: BaseScheduler, paused: bool = False) -> None:
    adapter = get_adapter(self)
    if adapter is None or not is_vercel_runtime():
        _original_start_for_instance(self)(self, paused=paused)
        return
    if is_discovery_runtime():
        return
    if is_queue_serving_runtime():
        adapter.warn_ignored_queue_lifecycle("start")
        return
    adapter.start(paused=paused)


def _patched_base_start(self: BaseScheduler, paused: bool = False) -> None:
    adapter = get_adapter(self)
    if adapter is None or not is_vercel_runtime():
        original = cast(
            "Callable[..., None]",
            _PATCH_STATE.original_base_start,
        )
        original(self, paused=paused)
        return
    _patched_start(self, paused=paused)


def _patched_pause(self: BaseScheduler) -> None:
    adapter = get_adapter(self)
    if adapter is None or not is_vercel_runtime():
        original = cast("Callable[[BaseScheduler], None]", _PATCH_STATE.original_pause)
        original(self)
        return
    if is_discovery_runtime():
        return
    if is_queue_serving_runtime():
        adapter.warn_ignored_queue_lifecycle("pause")
        return
    adapter.pause()


def _patched_resume(self: BaseScheduler) -> None:
    adapter = get_adapter(self)
    if adapter is None or not is_vercel_runtime():
        original = cast("Callable[[BaseScheduler], None]", _PATCH_STATE.original_resume)
        original(self)
        return
    if is_discovery_runtime():
        return
    if is_queue_serving_runtime():
        adapter.warn_ignored_queue_lifecycle("resume")
        return
    adapter.resume()


def _original_start_for_instance(
    instance: BaseScheduler,
) -> Callable[..., Any]:
    scheduler_types = {
        (scheduler_type.__module__, scheduler_type.__name__)
        for scheduler_type in type(instance).__mro__
    }
    if (
        "apscheduler.schedulers.background",
        "BackgroundScheduler",
    ) in scheduler_types and _PATCH_STATE.original_background_start is not None:
        return _PATCH_STATE.original_background_start
    if (
        "apscheduler.schedulers.blocking",
        "BlockingScheduler",
    ) in scheduler_types and _PATCH_STATE.original_blocking_start is not None:
        return _PATCH_STATE.original_blocking_start
    if (
        "apscheduler.schedulers.asyncio",
        "AsyncIOScheduler",
    ) in scheduler_types and _PATCH_STATE.original_asyncio_start is not None:
        return _PATCH_STATE.original_asyncio_start
    return cast("Callable[..., Any]", _PATCH_STATE.original_base_start)


def _patch_start_methods() -> None:
    from apscheduler.schedulers.asyncio import (  # type: ignore[import-untyped]
        AsyncIOScheduler,
    )
    from apscheduler.schedulers.background import (  # type: ignore[import-untyped]
        BackgroundScheduler,
    )
    from apscheduler.schedulers.blocking import (  # type: ignore[import-untyped]
        BlockingScheduler,
    )

    _PATCH_STATE.original_blocking_start = BlockingScheduler.start
    _PATCH_STATE.original_background_start = BackgroundScheduler.start
    _PATCH_STATE.original_asyncio_start = AsyncIOScheduler.start
    BlockingScheduler.start = _patched_start  # type: ignore[method-assign]
    BackgroundScheduler.start = _patched_start  # type: ignore[method-assign]
    AsyncIOScheduler.start = _patched_start  # type: ignore[method-assign]


def install_vercel_apscheduler_integration(
    *,
    options: VercelAPSchedulerOptions | dict[str, Any] | None = None,
    register_queues: bool = True,
) -> None:
    if options is not None:
        _PATCH_STATE.default_options = VercelAPSchedulerOptions.from_value(options)
    _PATCH_STATE.register_queues = _PATCH_STATE.register_queues or register_queues
    if _PATCH_STATE.installed:
        from ._automatic import register_automatic_activation

        register_automatic_activation()
        return

    _PATCH_STATE.original_init = BaseScheduler.__init__
    _PATCH_STATE.original_add_job = BaseScheduler.add_job
    _PATCH_STATE.original_real_add_job = BaseScheduler._real_add_job
    _PATCH_STATE.original_modify_job = BaseScheduler.modify_job
    _PATCH_STATE.original_remove_job = BaseScheduler.remove_job
    _PATCH_STATE.original_remove_all_jobs = BaseScheduler.remove_all_jobs
    _PATCH_STATE.original_resume_job = BaseScheduler.resume_job
    _PATCH_STATE.original_base_start = BaseScheduler.start
    _PATCH_STATE.original_pause = BaseScheduler.pause
    _PATCH_STATE.original_resume = BaseScheduler.resume

    BaseScheduler.__init__ = _patched_init  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
    BaseScheduler.add_job = _patched_add_job  # type: ignore[method-assign]
    BaseScheduler._real_add_job = _patched_real_add_job  # type: ignore[method-assign]
    BaseScheduler.modify_job = _patched_modify_job  # type: ignore[method-assign]
    BaseScheduler.remove_job = _patched_remove_job  # type: ignore[method-assign]
    BaseScheduler.remove_all_jobs = _patched_remove_all_jobs  # type: ignore[method-assign]
    BaseScheduler.resume_job = _patched_resume_job  # type: ignore[method-assign]
    BaseScheduler.start = _patched_base_start  # type: ignore[method-assign]
    BaseScheduler.pause = _patched_pause  # type: ignore[method-assign]
    BaseScheduler.resume = _patched_resume  # type: ignore[method-assign]
    _patch_start_methods()
    _PATCH_STATE.installed = True
    from ._automatic import register_automatic_activation

    register_automatic_activation()
