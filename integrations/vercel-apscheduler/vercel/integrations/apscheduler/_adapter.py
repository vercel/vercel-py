from __future__ import annotations

from typing import Any, cast

import logging
import math
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from os import environ
from sys import modules
from types import MethodType
from weakref import WeakValueDictionary

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
    IntervalTrigger,
    JobSubmissionEvent,
    MaxInstancesReachedError,
    RedisJobStore,
)
from ._jobstore import PROVENANCE_DECLARED, RedisJobCoordinator
from ._options import (
    VercelAPSchedulerOptions,
    _SchedulerIdentity,
    is_discovery_runtime,
    is_queue_serving_runtime,
    is_vercel_runtime,
    resolve_state_scope,
)
from ._payload import StartPayload, WakeupPayload
from ._time import as_utc, canonical_scheduled_logical_time, earliest

LOGGER = logging.getLogger("vercel.integrations.apscheduler")
UTC = timezone.utc
ADAPTER_ATTR = "_vercel_apscheduler_adapter"
DEPLOYMENT_ENV = "VERCEL_DEPLOYMENT_ID"
SUBSCRIBERS_ENV = "VERCEL_APSCHEDULER_SUBSCRIBERS"
WAKEUP_KEY_PREFIX = "aps"
RAW_STORE_KEY_ATTR = "_vercel_apscheduler_raw_jobs_key"

# One live adapter per durable identity. Two schedulers whose stores collapse
# to the same identity would interleave one namespace, so the second claim
# fails loudly instead.
_ACTIVE_IDENTITIES: WeakValueDictionary[str, SchedulerAdapter] = WeakValueDictionary()

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
    originals: dict[str, Callable[..., Any]] = field(default_factory=dict)


_PATCH_STATE = _PatchState()


def _original(name: str) -> Callable[..., Any]:
    original = _PATCH_STATE.originals.get(name)
    if original is None:
        raise RuntimeError("APScheduler integration patch is not initialized")
    return original


def get_adapter(scheduler: Any) -> SchedulerAdapter | None:
    return cast("SchedulerAdapter | None", getattr(scheduler, ADAPTER_ATTR, None))


def _trigger_fingerprint(trigger: Any) -> tuple[str, str, str]:
    """Digest a trigger into its user-declared, comparable schedule.

    ``IntervalTrigger`` without an explicit ``start_date`` auto-anchors at
    declaration time, so that field would look changed on every deployment
    and re-anchor unchanged schedules; it is excluded from the digest.
    """
    state: Any = trigger.__getstate__()
    if isinstance(state, dict) and type(trigger) is IntervalTrigger:
        state = {key: value for key, value in state.items() if key != "start_date"}
    return (
        type(trigger).__module__,
        type(trigger).__qualname__,
        repr(sorted(state.items(), key=lambda item: str(item[0])))
        if isinstance(state, dict)
        else repr(state),
    )


class SchedulerAdapter:
    """Turns one durable APScheduler instance into one fenced Queue driver."""

    def __init__(
        self,
        scheduler: BaseScheduler,
        options: VercelAPSchedulerOptions,
    ) -> None:
        self.scheduler = scheduler
        self.options = options
        self._identity: _SchedulerIdentity | None = None
        self._deployment: str | None = None
        self._scope: str | None = None
        self._registration_deferred = False
        self._declared_jobs: dict[str, Any] = {}
        self._reconciled = False
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
    def identity(self) -> _SchedulerIdentity:
        """The scheduler's durable identity, derived from its job store.

        The configured ``jobs_key`` is refactor-stable and exactly as durable
        as the state it names, so renaming the scheduler's variable or moving
        its module never orphans the Redis namespace or the queue topics. The
        ``scheduler_id`` option pins the identity explicitly instead.
        """
        if self._identity is None:
            self._identity = self._claim_identity(self._derive_identity())
        return self._identity

    def _derive_identity(self) -> _SchedulerIdentity:
        if self.options.scheduler_id is not None:
            try:
                return _SchedulerIdentity.from_scheduler_id(self.options.scheduler_id)
            except ValueError as exc:
                raise APSchedulerConfigurationError(str(exc)) from exc
        store = self.scheduler._jobstores.get("default")
        if not isinstance(store, RedisJobStore):
            raise APSchedulerConfigurationError(
                "vercel-apscheduler requires a configured default RedisJobStore"
            )
        raw_key = store.__dict__.setdefault(RAW_STORE_KEY_ATTR, store.jobs_key)
        try:
            return _SchedulerIdentity.from_store_key(raw_key)
        except ValueError as exc:
            raise APSchedulerConfigurationError(str(exc)) from exc

    def _claim_identity(self, identity: _SchedulerIdentity) -> _SchedulerIdentity:
        existing = _ACTIVE_IDENTITIES.get(identity.scheduler_id)
        if existing is not None and existing is not self:
            raise APSchedulerConfigurationError(
                f'two schedulers derive the durable identity "{identity.scheduler_id}"; '
                "give each scheduler's RedisJobStore a distinct jobs_key"
            )
        _ACTIVE_IDENTITIES[identity.scheduler_id] = self
        return identity

    @property
    def deployment(self) -> str:
        self._bind_runtime()
        return cast("str", self._deployment)

    @property
    def scope(self) -> str:
        """Durable state namespace.

        Named environments share one namespace across deployments; previews
        and development are deployment-scoped.
        """
        self._bind_runtime()
        return cast("str", self._scope)

    @property
    def driver(self) -> RedisDriver:
        self._bind_runtime()
        return cast("RedisDriver", self._driver)

    @property
    def coordinator(self) -> RedisJobCoordinator:
        self._bind_runtime()
        return cast("RedisJobCoordinator", self._coordinator)

    def _owns_namespace(self) -> bool:
        """Whether this deployment currently drives the shared chain.

        Deployment-scoped namespaces are trivially owned. In a shared
        namespace, only the owner may write declared jobs, publish wakes, or
        mutate jobs; a stale deployment's touches must be inert so a demoted
        code version can never resurrect or rewrite production state.
        """
        if not self._scope_outlives_deployments:
            return True
        return self.driver.owner_deployment() == self.deployment

    @property
    def _scope_outlives_deployments(self) -> bool:
        """Whether this namespace is shared by successive deployments.

        True for named environments; previews and development get a fresh
        namespace per deployment, so takeover reconciliation never applies.
        """
        return self._scope != self._deployment

    @property
    def is_runtime_mutation(self) -> bool:
        return self._runtime_mutation_depth > 0 and not self._suppress_wakeup

    @property
    def is_wake_mutation(self) -> bool:
        """Whether an executing job is mutating the store from its own wake.

        Cold-start declarations also run with wakeups suppressed, but they
        dispatch while the scheduler is still stopped; only wake processing
        runs suppressed in a locally running scheduler.
        """
        return self._suppress_wakeup and self.scheduler.state == STATE_RUNNING

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
        self._lifecycle_called = True
        self.ensure_local_started()
        now = datetime.now(UTC)
        if paused:
            self.driver.pause(now)
            self._pause_local()
            return
        decision = self.driver.start(now)
        self._reconcile_takeover(now)
        self._publish_start_if_needed(decision, now=now)
        if not decision.changed and decision.start_status == "active":
            self.repair_wakeup(now=now)
        self._resume_local_if_paused()

    def auto_activate(self) -> None:
        """Activate on request activity without overriding an explicit pause."""
        self._bind_runtime()
        self._validate_durable_configuration()
        self._lifecycle_called = True
        self.ensure_local_started()
        now = datetime.now(UTC)
        decision = self.driver.auto_activate(now)
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
            f"{WAKEUP_KEY_PREFIX}:start:{self.scope}:"
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
            f"{WAKEUP_KEY_PREFIX}:wake:{self.scope}:"
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
        if not self._owns_namespace():
            return None
        snapshot = self.driver.snapshot()
        wake = snapshot.current_wake
        if snapshot.state != "running" or wake is None or wake.status != "pending":
            return None
        return self.publish_wakeup(wake)

    def repair_wakeup(self, *, now: datetime | None = None) -> PublishedWakeup | None:
        """Publish a reserved-but-unsent wake, resurrecting an overdue one first.

        A ``published`` wake whose message died (a rollback strands it in a
        queue with no forward alias; alias or retention expiry drops it) is
        demoted back to ``pending`` and republished into the current
        deployment's queue. Loud on purpose: a repair firing means a message
        actually died, which an operator may want to correlate with a
        rollback or an incident.
        """
        now_utc = as_utc(now or datetime.now(UTC), name="now")
        if self.driver.repair_overdue_wake(now_utc):
            self._logger.warning(
                "Republishing the current wake for scheduler %r: its queue "
                "message is overdue and presumed lost",
                self.identity.scheduler_id,
            )
        return self.publish_pending_wakeup()

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
        except BaseException:
            # A retried mutation can fail (for example with a conflicting job
            # id) after an interrupted earlier attempt armed a wake it never
            # published. Repair that pending wake so retrying a mutation is
            # always safe, even when the retry itself errors.
            if self._runtime_mutation_depth == 1:
                try:
                    self.publish_pending_wakeup()
                except Exception:
                    self._logger.exception(
                        "Could not repair the pending wake after a failed runtime mutation"
                    )
            raise
        finally:
            self._runtime_mutation_depth -= 1
            self._current_add_explicit = prior_explicit

    def prepare_runtime_mutation(self) -> None:
        if not self._lifecycle_called:
            raise APSchedulerConfigurationError(
                "call scheduler.start() before mutating durable jobs in this Function"
            )
        self.ensure_local_started()
        if not self._owns_namespace():
            raise APSchedulerConfigurationError(
                f'deployment "{self.deployment}" no longer drives scheduler '
                f'"{self.identity.scheduler_id}"; mutate jobs through the '
                "promoted deployment"
            )

    def ensure_local_started(self) -> None:
        """Start APScheduler internals without starting a scheduler thread."""
        self._bind_runtime()
        self._validate_durable_configuration()
        if self.scheduler.state != STATE_STOPPED:
            self._reconcile_takeover(datetime.now(UTC))
            return
        self._inject_inline_executor()
        previous = self._suppress_wakeup
        self._suppress_wakeup = True
        try:
            _original("base_start")(self.scheduler, paused=False)
            self._validate_materialized_jobs()
        except BaseException:
            if self.scheduler.state != STATE_STOPPED:
                BaseScheduler.shutdown(self.scheduler, wait=True)
            raise
        finally:
            self._suppress_wakeup = previous
        self._reconcile_takeover(datetime.now(UTC))

    def _pause_local(self) -> None:
        if self.scheduler.state != STATE_RUNNING:
            return
        _original("pause")(self.scheduler)

    def _resume_local_if_paused(self) -> None:
        if self.scheduler.state != STATE_PAUSED:
            return
        previous = self._suppress_wakeup
        self._suppress_wakeup = True
        try:
            _original("resume")(self.scheduler)
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
        if not (self.is_runtime_mutation or self.is_wake_mutation):
            # Cold-start declarations are the reconciliation input on takeover.
            self._declared_jobs[str(job.id)] = job
        if job.executor != "default":
            raise APSchedulerConfigurationError(
                f'job "{job.id}" must use the default Vercel inline executor'
            )
        if not (self.is_runtime_mutation or self.is_wake_mutation) and not self._owns_namespace():
            # A stale deployment's cold start must not write declarations into
            # a namespace another deployment drives; taking ownership runs the
            # reconciliation that writes them instead.
            self._fill_declaration_defaults(job, jobstore_alias)
            return False
        existing = jobstore.lookup_job(job.id)
        if existing is None:
            return True
        if not replace_existing:
            raise APSchedulerConfigurationError(
                f'job "{job.id}" already exists in Redis; declare it with replace_existing=True'
            )
        if self.is_runtime_mutation or self.is_wake_mutation:
            return True
        self._fill_declaration_defaults(job, jobstore_alias)
        return False

    def _fill_declaration_defaults(self, job: Any, jobstore_alias: str) -> None:
        """Complete a declared job whose store write was skipped.

        The skipped write leaves the object without the defaults upstream
        ``_real_add_job`` would fill; reconciliation may persist it later.
        """
        replacements: dict[str, Any] = {
            key: value
            for key, value in self.scheduler._job_defaults.items()
            if not hasattr(job, key)
        }
        if not hasattr(job, "next_run_time"):
            replacements["next_run_time"] = job.trigger.get_next_fire_time(
                None,
                datetime.now(self.scheduler.timezone),
            )
        job._modify(**replacements)
        job._jobstore_alias = jobstore_alias

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
        try:
            scope = resolve_state_scope(deployment)
        except ValueError as exc:
            raise APSchedulerConfigurationError(str(exc)) from exc
        self._scope = scope
        tag = f"{{{scope}:{self.identity.scheduler_id}}}"
        for alias, store in stores.items():
            namespace = getattr(store, "_vercel_apscheduler_namespace", None)
            expected_namespace = (scope, self.identity.scheduler_id)
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
                scope=scope,
                scheduler_id=self.identity.scheduler_id,
                deployment=deployment,
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
                    if job.executor != "default":
                        raise APSchedulerConfigurationError(
                            f'job "{job.id}" in "{alias}" must use the default executor'
                        )

    def _reconcile_takeover(self, now: datetime) -> None:
        """Sync code-declared jobs into a namespace another deployment wrote.

        Environment-scoped stores outlive deployments, so on the first touch
        by a new deployment the code's declarations win for ``declared`` jobs:
        removed declarations are deleted before any due planning, changed
        triggers restart their schedule, unchanged jobs keep their progress,
        and a declared record that no longer loads is rewritten from its
        declaration. ``runtime`` jobs belong to the store and are never
        touched; an unloadable one is quarantined.
        """
        if self._reconciled or not self._scope_outlives_deployments:
            return
        if not self._owns_namespace():
            return
        if self.driver.reconciled_deployment() == self.deployment:
            self._reconciled = True
            return
        missing = dict(self._declared_jobs)
        with self.scheduler._jobstores_lock:
            jobs, undecodable = self.coordinator.get_all_jobs_with_revisions()
            self._reconcile_undecodable(undecodable, missing, now)
            for job, revision, provenance in jobs:
                missing.pop(str(job.id), None)
                if provenance != PROVENANCE_DECLARED:
                    continue
                declared = self._declared_jobs.get(str(job.id))
                if declared is None:
                    synced = self.coordinator.cas_remove_job(job.id, revision)
                    if synced:
                        self._logger.info(
                            'Removed job "%s": this deployment no longer declares it',
                            job.id,
                        )
                    continue
                if _trigger_fingerprint(declared.trigger) == _trigger_fingerprint(job.trigger):
                    next_run_time = job.next_run_time
                else:
                    next_run_time = declared.trigger.get_next_fire_time(
                        None,
                        now.astimezone(self.scheduler.timezone),
                    )
                declared._modify(next_run_time=next_run_time)
                synced = self.coordinator.cas_update_job(declared, revision)
                if not synced:
                    # A concurrent handler moved the revision; the next sync
                    # pass sees the current value.
                    self._logger.debug(
                        'Job "%s" changed while reconciling; leaving the newer revision',
                        job.id,
                    )
            for declared in missing.values():
                # Declared before this deployment owned the namespace, so the
                # cold-start materialization deliberately did not write it.
                self.coordinator.add_job(declared)
        self.driver.mark_reconciled(self.deployment, now)
        self._reconciled = True

    def _reconcile_undecodable(
        self,
        undecodable: list[tuple[str, int, str]],
        missing: dict[str, Any],
        now: datetime,
    ) -> None:
        """Repair or sideline records this deployment's code cannot load."""
        for job_id, revision, provenance in undecodable:
            declared = missing.pop(job_id, None)
            if provenance != PROVENANCE_DECLARED:
                # The store owns runtime jobs, even unloadable ones.
                self.coordinator.quarantine_job(job_id)
                continue
            if declared is None:
                if self.coordinator.cas_remove_job(job_id, revision):
                    self._logger.info(
                        'Removed job "%s": this deployment no longer declares it',
                        job_id,
                    )
                continue
            # The code still declares this job but its persisted record no
            # longer loads (typically its function moved). The declaration is
            # authoritative; restart its schedule under the new code.
            declared._modify(
                next_run_time=declared.trigger.get_next_fire_time(
                    None,
                    now.astimezone(self.scheduler.timezone),
                )
            )
            if self.coordinator.cas_update_job(declared, revision):
                self._logger.info(
                    'Rewrote job "%s" from its declaration: the persisted '
                    "definition no longer loads under this deployment",
                    job_id,
                )

    def _rebase_before(self, activation_time: datetime) -> None:
        with self.scheduler._jobstores_lock:
            jobs, _undecodable = self.coordinator.get_all_jobs_with_revisions()
            for job, revision, _provenance in jobs:
                next_run_time = job.next_run_time
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
                    advanced = self.coordinator.cas_remove_job(
                        plan.job.id,
                        plan.expected_revision,
                    )
                else:
                    plan.job._modify(next_run_time=plan.next_run_time)
                    advanced = self.coordinator.cas_update_job(
                        plan.job,
                        plan.expected_revision,
                    )
                if not advanced:
                    # A concurrent mutation moved the revision; its value wins
                    # and its dirty marker keeps the chain covered.
                    self._logger.debug(
                        'Job "%s" was mutated during its run; keeping the newer definition',
                        plan.job.id,
                    )
        for event in events:
            self.scheduler._dispatch_event(event)

    @property
    def _logger(self) -> logging.Logger:
        return logging.getLogger(f"{LOGGER.name}.{self.identity.scheduler_id}")


def is_scheduler_subscriber(module_name: str, variable_name: str) -> bool:
    """Whether the imported module declares a scheduler adopted by this integration.

    The Vercel Python builder calls this during subscriber introspection to
    classify declared entrypoints without depending on internal topic names.
    """
    module = modules.get(module_name)
    candidate = getattr(module, variable_name, None) if module is not None else None
    return get_adapter(candidate) is not None


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
    result = _original("init")(self, *args, **kwargs)
    options = VercelAPSchedulerOptions.from_value(_PATCH_STATE.default_options)
    adapter = SchedulerAdapter(self, options)
    setattr(self, ADAPTER_ATTR, adapter)
    _register_queues_when_ready(self, adapter)
    return result


def _register_queues_when_ready(
    scheduler: BaseScheduler,
    adapter: SchedulerAdapter,
) -> None:
    """Register queue handlers once the durable identity is derivable.

    Identity comes from the default job store, which may be configured after
    construction (``add_jobstore``); registration is deferred until it
    appears so both configuration styles register during module import.
    """
    if not (is_vercel_runtime() and (_PATCH_STATE.register_queues or is_queue_serving_runtime())):
        return
    store = scheduler._jobstores.get("default")
    if adapter.options.scheduler_id is None and not isinstance(store, RedisJobStore):
        adapter._registration_deferred = True
        return
    from ._subscriber import register_scheduler

    register_scheduler(scheduler, options=adapter.options)
    adapter._registration_deferred = False


def _patched_add_jobstore(self: BaseScheduler, *args: Any, **kwargs: Any) -> Any:
    result = _original("add_jobstore")(self, *args, **kwargs)
    adapter = get_adapter(self)
    if adapter is not None and adapter._registration_deferred:
        _register_queues_when_ready(self, adapter)
    return result


def _patched_add_job(self: BaseScheduler, *args: Any, **kwargs: Any) -> Any:
    original = _original("add_job")
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
    return _original("real_add_job")(self, job, jobstore_alias, replace_existing)


def _patched_mutation(name: str) -> Callable[..., Any]:
    """Runtime job mutations run inside one repair-aware mutation scope."""

    def patched(self: BaseScheduler, *args: Any, **kwargs: Any) -> Any:
        original = _original(name)
        adapter = get_adapter(self)
        if (
            adapter is None
            or not is_vercel_runtime()
            or is_discovery_runtime()
            or is_queue_serving_runtime()
        ):
            return original(self, *args, **kwargs)
        with adapter.runtime_mutation():
            return original(self, *args, **kwargs)

    return patched


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
        _original("base_start")(self, paused=paused)
        return
    _patched_start(self, paused=paused)


def _patched_lifecycle(name: str) -> Callable[..., Any]:
    def patched(self: BaseScheduler) -> None:
        adapter = get_adapter(self)
        if adapter is None or not is_vercel_runtime():
            _original(name)(self)
            return
        if is_discovery_runtime():
            return
        if is_queue_serving_runtime():
            adapter.warn_ignored_queue_lifecycle(name)
            return
        getattr(adapter, name)()

    return patched


def _original_start_for_instance(
    instance: BaseScheduler,
) -> Callable[..., Any]:
    scheduler_types = {
        (scheduler_type.__module__, scheduler_type.__name__)
        for scheduler_type in type(instance).__mro__
    }
    for module_name, class_name, original_name in (
        ("apscheduler.schedulers.background", "BackgroundScheduler", "background_start"),
        ("apscheduler.schedulers.blocking", "BlockingScheduler", "blocking_start"),
        ("apscheduler.schedulers.asyncio", "AsyncIOScheduler", "asyncio_start"),
    ):
        if (module_name, class_name) in scheduler_types:
            original = _PATCH_STATE.originals.get(original_name)
            if original is not None:
                return original
    return _original("base_start")


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

    _PATCH_STATE.originals["blocking_start"] = BlockingScheduler.start
    _PATCH_STATE.originals["background_start"] = BackgroundScheduler.start
    _PATCH_STATE.originals["asyncio_start"] = AsyncIOScheduler.start
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

    _PATCH_STATE.originals = {
        "init": BaseScheduler.__init__,
        "add_job": BaseScheduler.add_job,
        "add_jobstore": BaseScheduler.add_jobstore,
        "real_add_job": BaseScheduler._real_add_job,
        "modify_job": BaseScheduler.modify_job,
        "remove_job": BaseScheduler.remove_job,
        "remove_all_jobs": BaseScheduler.remove_all_jobs,
        "resume_job": BaseScheduler.resume_job,
        "base_start": BaseScheduler.start,
        "pause": BaseScheduler.pause,
        "resume": BaseScheduler.resume,
    }
    BaseScheduler.__init__ = _patched_init  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
    BaseScheduler.add_jobstore = _patched_add_jobstore  # type: ignore[method-assign]
    BaseScheduler.add_job = _patched_add_job  # type: ignore[method-assign]
    BaseScheduler._real_add_job = _patched_real_add_job  # type: ignore[method-assign]
    BaseScheduler.modify_job = _patched_mutation("modify_job")  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
    BaseScheduler.remove_job = _patched_mutation("remove_job")  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
    BaseScheduler.remove_all_jobs = _patched_mutation("remove_all_jobs")  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
    BaseScheduler.resume_job = _patched_mutation("resume_job")  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
    BaseScheduler.start = _patched_base_start  # type: ignore[method-assign]
    BaseScheduler.pause = _patched_lifecycle("pause")  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
    BaseScheduler.resume = _patched_lifecycle("resume")  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
    _patch_start_methods()
    _PATCH_STATE.installed = True
    from ._automatic import register_automatic_activation

    register_automatic_activation()
