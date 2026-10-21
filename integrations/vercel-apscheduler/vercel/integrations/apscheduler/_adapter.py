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

from ._backends import Backend, Driver, JobCoordinator, resolve_backend
from ._control import LifecyclePayload
from ._executor import VercelInlineExecutor
from ._imports import (
    EVENT_JOB_MAX_INSTANCES,
    EVENT_JOB_REMOVED,
    EVENT_JOB_SUBMITTED,
    STATE_PAUSED,
    STATE_RUNNING,
    STATE_STOPPED,
    BaseScheduler,
    JobEvent,
    JobSubmissionEvent,
    MaxInstancesReachedError,
)
from ._options import (
    VercelAPSchedulerOptions,
    _SchedulerIdentity,
    development_deployment_id,
    is_discovery_runtime,
    is_queue_serving_runtime,
    is_vercel_runtime,
    resolve_environment,
    resolve_state_scope,
)
from ._payload import StartPayload, WakeupPayload
from ._time import as_utc, canonical_scheduled_logical_time, earliest
from ._types import (
    APSchedulerConfigurationError,
    NamespaceFencedError,
    StartDecision,
    WakeToken,
)

LOGGER = logging.getLogger("vercel.integrations.apscheduler")
UTC = timezone.utc
ADAPTER_ATTR = "_vercel_apscheduler_adapter"
DEPLOYMENT_ENV = "VERCEL_DEPLOYMENT_ID"
WAKEUP_KEY_PREFIX = "aps"
# Queue delivery rounds wake delays up to whole seconds and adds dispatch
# latency, so a grace below this cannot be met and skips occurrences.
MIN_QUEUE_MISFIRE_GRACE_SECONDS = 5
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
        self._driver: Driver | None = None
        self._coordinator: JobCoordinator | None = None
        self._backend: Backend | None = None
        self._job_definitions: dict[str, _JobDefinition] = {}
        self._current_add_explicit = False
        self._runtime_mutation_depth = 0
        self._lifecycle_called = False
        self._queue_lifecycle_warning_emitted = False
        self._short_grace_warned: set[str] = set()
        self._suppress_wakeup = False
        self._native_wakeup = scheduler.wakeup
        self._adopt_instance_methods()

    @property
    def identity(self) -> _SchedulerIdentity:
        """The scheduler's durable identity.

        Derived from the builder-assigned subscriber id, which is
        refactor-stable, so renaming the scheduler's variable or moving its
        module never orphans the durable namespace or the queue topics. The
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
        return cast("_SchedulerIdentity", self.backend.derive_identity(self.scheduler))

    def _claim_identity(self, identity: _SchedulerIdentity) -> _SchedulerIdentity:
        existing = _ACTIVE_IDENTITIES.get(identity.scheduler_id)
        if existing is not None and existing is not self:
            raise APSchedulerConfigurationError(
                f'two schedulers derive the durable identity "{identity.scheduler_id}"; '
                "give each scheduler a distinct durable identity"
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
    def driver(self) -> Driver:
        self._bind_runtime()
        return cast("Driver", self._driver)

    @property
    def coordinator(self) -> JobCoordinator:
        self._bind_runtime()
        return cast("JobCoordinator", self._coordinator)

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
        """Whether the namespace can outlive this code's view of it.

        Always true on the cache backend: its records are evictable in
        every scope, so read-repair-from-code is the durability story
        regardless of scoping. A future durable backend would return
        ``self._scope != self._deployment`` here.
        """
        return True

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
            self._publish_lifecycle_control("pause", now)
            self._pause_local()
            return
        decision = self.driver.start(
            now,
            idle_timeout_seconds=self._preview_idle_timeout_seconds(),
        )
        self._publish_start_if_needed(decision, now=now)
        if not decision.changed and decision.start_status == "active":
            self._rearm_wake_from_stores(now)
            self.repair_wakeup(now=now)
        self._resume_local_if_paused()

    def auto_activate(
        self,
        *,
        idle_timeout_seconds: int | None = None,
        takeover_allowed: bool = False,
    ) -> bool:
        """Activate on request activity without overriding an explicit pause.

        ``takeover_allowed`` is true only when the triggering request arrived
        through an environment alias, which routes exclusively to the
        currently promoted deployment. Traffic to a deployment's own URL
        proves nothing about promotion, so it may drive an owned chain but
        never adopt someone else's.

        Returns whether this deployment is settled: it drives the chain (an
        explicit pause counts), so periodic sweeps suffice. False means a
        takeover is still owed to a future alias-routed request.
        """
        self._lifecycle_called = True
        self.ensure_local_started()
        now = datetime.now(UTC)
        decision = self.driver.auto_activate(
            now,
            idle_timeout_seconds=idle_timeout_seconds,
            takeover_allowed=takeover_allowed,
        )
        if not decision.owned:
            return False
        if decision.state != "running":
            self._pause_local()
            return True
        self._publish_start_if_needed(decision, now=now)
        if not decision.changed and decision.start_status == "active":
            self._rearm_wake_from_stores(now)
            self.repair_wakeup(now=now)
        self._resume_local_if_paused()
        return True

    def pause(self) -> None:
        """Durably fence the current generation."""
        self._lifecycle_called = True
        self.ensure_local_started()
        now = datetime.now(UTC)
        self.driver.pause(now)
        self._publish_lifecycle_control("pause", now)
        self._pause_local()

    def _publish_lifecycle_control(self, action: str, now: datetime) -> None:
        """Carry a lifecycle flag over the queue where the cache cannot.

        Cache documents are per-process under ``vercel dev`` and per-region
        in deployments, so the flag also rides the start topic to whichever
        process serves the chain.
        """
        payload = LifecyclePayload(
            scheduler_id=self.identity.scheduler_id,
            action=action,
            issued_at=now,
            generation=self.driver.snapshot().generation,
        ).to_payload()
        try:
            vqs_sync.send(
                self.identity.start_topic,
                payload,
                deployment=self.deployment,
                retention=self.options.retention_seconds,
            )
        except Exception:
            LOGGER.exception(
                "Failed to publish the %s control message; the flag is "
                "recorded in the cache document only",
                action,
            )

    def resume(self) -> None:
        """Resume by creating one new durable generation."""
        self.start()

    def _preview_idle_timeout_seconds(self) -> int | None:
        # _automatic imports this module for the adapter registry; resolving
        # at call time keeps the modules from importing each other at load.
        from ._automatic import _preview_idle_timeout

        return _preview_idle_timeout()

    def _idle_bounded(self) -> bool:
        """Whether this chain is subject to a preview idle deadline.

        Configuration-derived, so racing publishers stamp identical payloads.
        """
        return self._preview_idle_timeout_seconds() is not None

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
            idle_bounded=self._idle_bounded(),
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
            idle_bounded=self._idle_bounded(),
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
        """Repair a wake reserved durably before a failed Queue send."""
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

    def _rearm_wake_from_stores(self, now: datetime | None = None) -> None:
        """Pull the current wake in to the stores' exact next due time.

        Source-store schedules change without a job write to ride on, so a
        dormant or far-armed chain only learns about them here: at explicit
        ``wakeup()`` calls, at a runtime ``add_jobstore()``, and at
        activation against an already-active generation.
        """
        if not self.source_jobstores:
            return
        if not self._owns_namespace():
            return
        now_utc = as_utc(now or datetime.now(UTC), name="now")
        next_time = self.get_next_wakeup_time(now_utc)
        if next_time is None:
            return
        self.driver.rearm_wake(self.canonical_wakeup_time(next_time, now=now_utc), now_utc)
        self.publish_pending_wakeup()

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
        """Mark a post-boundary ``add_job`` so the store classifies it.

        The runtime flag is what routes the write into the coordinator's
        declared-only rejection instead of the declaration path.
        """
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
        """Start locally and skip occurrences from the paused interval.

        Only the durable store is rebased: source entries that came due
        while paused must still dispatch on the next evaluation.
        """
        self.ensure_local_started()
        self._rebase_before(
            as_utc(activation_time, name="activation_time").astimezone(self.scheduler.timezone)
        )

    @property
    def source_jobstores(self) -> dict[str, Any]:
        """Non-default job stores, whose schedules an external system owns.

        Read at each wake and at explicit ``wakeup()`` calls; never
        declared into, reconciled, or rebased.
        """
        return {
            alias: store for alias, store in self.scheduler._jobstores.items() if alias != "default"
        }

    def get_next_wakeup_time(
        self,
        reference_time: datetime,
        *,
        source_floors: dict[str, datetime] | None = None,
    ) -> datetime | None:
        """Return the exact next due time across the durable and source stores.

        Source stores change out of band, and there is no polling: an entry
        that appears after this wake is noticed at the next chain-scheduled
        wake or at an explicit ``scheduler.wakeup()`` call. ``source_floors``
        names stores that kept an entry due (a failed or skipped advance);
        their overdue times are floored to the bounded retry so a persistent
        failure cannot hot-spin the chain with immediate wakes.
        """
        reference = as_utc(reference_time, name="reference_time")
        retry_delay = timedelta(seconds=self.scheduler.jobstore_retry_interval)
        floors = source_floors or {}
        with self.scheduler._jobstores_lock:
            next_time = self.scheduler._jobstores["default"].get_next_run_time()
            for alias, store in self.source_jobstores.items():
                try:
                    candidate = store.get_next_run_time()
                    if candidate is not None:
                        candidate = as_utc(candidate, name=f'job store "{alias}" next run time')
                except Exception as exc:
                    self._logger.warning(
                        'Error getting the next run time from job store "%s": %s',
                        alias,
                        exc,
                    )
                    candidate = reference + retry_delay
                floor = floors.get(alias)
                if floor is not None:
                    candidate = floor if candidate is None else max(candidate, floor)
                next_time = earliest(next_time, candidate)
        return next_time

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
            source_job_ids, source_floors = self._dispatch_source_jobs(evaluation_time)
            next_wakeup_time = earliest(
                retry_time,
                self.get_next_wakeup_time(evaluation_time, source_floors=source_floors),
            )
        finally:
            self._suppress_wakeup = previous
        return WakeupProcessingResult(
            logical_time=evaluation_time.astimezone(UTC),
            due_job_ids=tuple(plan.job.id for plan in due_jobs) + tuple(source_job_ids),
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
        if jobstore_alias != "default":
            raise APSchedulerConfigurationError(
                f'job store "{jobstore_alias}" is a source store owned by its '
                "external system; scheduler-managed jobs may only target the "
                'durable "default" store'
            )
        if not self.backend.supports_store(jobstore):
            raise APSchedulerConfigurationError(
                f"the {self.backend.name} backend does not support job store "
                f'"{jobstore_alias}" ({type(jobstore).__name__})'
            )
        definition = self._job_definitions.get(str(job.id))
        explicit_id = (
            definition.explicit_id if definition is not None else self._current_add_explicit
        )
        if not explicit_id:
            raise APSchedulerConfigurationError(
                "jobs in a durable APScheduler store require an explicit stable id"
            )
        # Before defaults fill, the attribute exists only when the job chose
        # its own grace; the scheduler-level default is checked at configure.
        grace = getattr(job, "misfire_grace_time", None)
        if (
            grace is not None
            and grace < MIN_QUEUE_MISFIRE_GRACE_SECONDS
            and str(job.id) not in self._short_grace_warned
        ):
            self._short_grace_warned.add(str(job.id))
            self._logger.warning(
                'Job "%s" sets misfire_grace_time=%s; queue delivery cannot '
                "meet a grace below %s seconds, so occurrences may be "
                "skipped as misfires",
                job.id,
                grace,
                MIN_QUEUE_MISFIRE_GRACE_SECONDS,
            )
        if not (self.is_runtime_mutation or self.is_wake_mutation):
            # Cold-start declarations are the store's read-repair index, and
            # repair may serialize them at any read, so complete the defaults
            # upstream _real_add_job would otherwise fill later.
            self._fill_declaration_defaults(job, jobstore_alias)
            self._declared_jobs[str(job.id)] = job
        if job.executor != "default":
            raise APSchedulerConfigurationError(
                f'job "{job.id}" must use the default Vercel inline executor'
            )
        if not (self.is_runtime_mutation or self.is_wake_mutation) and not self._owns_namespace():
            # A stale deployment's cold start must not write declarations into
            # a namespace another deployment drives; read-repair writes them
            # once ownership arrives.
            return False
        existing = jobstore.lookup_job(job.id)
        if existing is None:
            return True
        if not replace_existing:
            raise APSchedulerConfigurationError(
                f'job "{job.id}" already exists in the durable store; '
                "declare it with replace_existing=True"
            )
        return self.is_runtime_mutation or self.is_wake_mutation

    def _fill_declaration_defaults(self, job: Any, jobstore_alias: str) -> None:
        """Complete a declared job the way upstream ``_real_add_job`` would.

        Declarations serve as the store's read-repair input, which can
        serialize them before upstream fills their defaults.
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
        # An explicit wakeup is the signal that a source store changed out
        # of band: recompute the exact next due time across every store and
        # pull the chain's wake in to it. Durable-store writes rearm
        # transactionally, so without source stores publishing the pending
        # wake is all that is left to do.
        self._rearm_wake_from_stores()
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
        if not deployment and resolve_environment().casefold() == "development":
            # `vercel dev` does not set a deployment id (SDKs read its mere
            # presence as "deployed"); development derives a stable synthetic
            # one instead.
            deployment = development_deployment_id()
        if not deployment:
            raise APSchedulerConfigurationError(
                f"{DEPLOYMENT_ENV} is required to run an APScheduler subscriber"
            )
        if self._deployment is not None and self._deployment != deployment:
            raise APSchedulerConfigurationError(
                "one scheduler object cannot be shared across Vercel deployments"
            )
        self._deployment = deployment
        backend = self.backend
        self._validate_durable_configuration()
        try:
            scope = resolve_state_scope(deployment)
        except ValueError as exc:
            raise APSchedulerConfigurationError(str(exc)) from exc
        self._scope = scope
        if self._driver is None or self._coordinator is None:
            bound = backend.bind(self, scope=scope, deployment=deployment)
            self._driver = bound.driver
            self._coordinator = bound.coordinator
            for alias, store in self.source_jobstores.items():
                self._logger.info(
                    'Job store "%s" (%s) is a source store: its due jobs run '
                    "at each wake, and out-of-band schedule changes are "
                    "noticed at the next chain-scheduled wake or at an "
                    "explicit scheduler.wakeup() call",
                    alias,
                    type(store).__name__,
                )

    @property
    def backend(self) -> Backend:
        if self._backend is None:
            self._backend = resolve_backend(self.scheduler)
        return self._backend

    def _validate_durable_configuration(self) -> dict[str, Any]:
        return self.backend.validate_configuration(self.scheduler)

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
        # Enumerating a source store can be a full table scan; its jobs are
        # validated at dispatch instead.
        with self.scheduler._jobstores_lock:
            for job in self.scheduler._jobstores["default"].get_all_jobs():
                if job.executor != "default":
                    raise APSchedulerConfigurationError(
                        f'job "{job.id}" in "default" must use the default executor'
                    )

    def _rebase_before(self, activation_time: datetime) -> None:
        with self.scheduler._jobstores_lock:
            jobs = self.coordinator.get_all_jobs_with_revisions()
            for job, revision in jobs:
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

    def _dispatch_source_jobs(
        self,
        logical_time: datetime,
    ) -> tuple[list[str], dict[str, datetime]]:
        """Run due source-store jobs with stock ``_process_jobs`` semantics.

        No revisions and no ownership fencing: delivery is at least once,
        and a source store dedupes its own dispatch (see SCHEDULER.md).
        Returns the submitted job ids and a retry floor for every store
        that kept an entry due, so the successor cannot hot-spin on it.
        """
        submitted: list[str] = []
        floors: dict[str, datetime] = {}
        events: list[Any] = []
        retry_time = logical_time + timedelta(seconds=self.scheduler.jobstore_retry_interval)
        with self.scheduler._jobstores_lock:
            for alias, store in self.source_jobstores.items():
                try:
                    due_jobs = store.get_due_jobs(logical_time)
                except Exception as exc:
                    self._logger.warning(
                        'Error getting due jobs from job store "%s": %s',
                        alias,
                        exc,
                    )
                    floors[alias] = retry_time
                    continue
                store_submitted, needs_retry = self._run_due_source_jobs(
                    alias,
                    store,
                    due_jobs,
                    logical_time,
                    events,
                )
                submitted.extend(store_submitted)
                if needs_retry:
                    floors[alias] = retry_time
        for event in events:
            self.scheduler._dispatch_event(event)
        return submitted, floors

    def _run_due_source_jobs(
        self,
        alias: str,
        store: Any,
        due_jobs: list[Any],
        logical_time: datetime,
        events: list[Any],
    ) -> tuple[list[str], bool]:
        """Submit one store's due jobs and advance them in the store.

        A True result means at least one entry stayed due: a failed store
        write abandons the store's remaining jobs, a malformed or skipped
        job is stepped over, and either way the caller floors the store's
        next wake to the bounded retry.
        """
        submitted: list[str] = []
        needs_retry = False
        for job in due_jobs:
            try:
                outcome, submitted_id = self._run_source_job(
                    alias, store, job, logical_time, events
                )
            except Exception:
                # A user-written store can materialize a malformed job;
                # step over it instead of stalling the wake.
                self._logger.exception('Error running a job from source store "%s"', alias)
                needs_retry = True
                continue
            if submitted_id is not None:
                submitted.append(submitted_id)
            if outcome == "abort":
                return submitted, True
            if outcome == "skipped":
                needs_retry = True
        return submitted, needs_retry

    def _run_source_job(
        self,
        alias: str,
        store: Any,
        job: Any,
        logical_time: datetime,
        events: list[Any],
    ) -> tuple[str, str | None]:
        """Run one due source job and advance it through its store.

        Returns "ok" when the job advanced, "skipped" when it stayed due,
        and "abort" when the store write failed.
        """
        try:
            executor = self.scheduler._lookup_executor(job.executor)
        except Exception:
            # Upstream removes the job here, but a source store's remove
            # can carry dispatch semantics; skip instead.
            self._logger.error(
                'Executor lookup ("%s") failed for job "%s" in source store "%s"; skipping it',
                getattr(job, "executor", None),
                job.id,
                alias,
            )
            return "skipped", None
        run_times = job._get_run_times(logical_time)
        if run_times and job.coalesce:
            run_times = run_times[-1:]
        if not run_times:
            return "ok", None
        submitted_id: str | None = None
        try:
            if hasattr(executor, "set_reference_time"):
                executor.set_reference_time(logical_time)
            executor.submit_job(job, run_times)
        except MaxInstancesReachedError:
            events.append(JobSubmissionEvent(EVENT_JOB_MAX_INSTANCES, job.id, alias, run_times))
        except Exception:
            self._logger.exception(
                'Error submitting job "%s" to executor "%s"',
                job,
                job.executor,
            )
        else:
            events.append(JobSubmissionEvent(EVENT_JOB_SUBMITTED, job.id, alias, run_times))
            submitted_id = str(job.id)
        next_run_time = job.trigger.get_next_fire_time(run_times[-1], logical_time)
        if next_run_time is not None:
            job._modify(next_run_time=next_run_time)
        try:
            if next_run_time is not None:
                store.update_job(job)
            else:
                store.remove_job(job.id)
                events.append(JobEvent(EVENT_JOB_REMOVED, job.id, alias))
        except Exception:
            self._logger.exception(
                'Error advancing job "%s" in source store "%s"',
                job.id,
                alias,
            )
            return "abort", submitted_id
        return "ok", submitted_id

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
                try:
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
                except NamespaceFencedError:
                    # Another deployment took the chain mid-run. Work already
                    # executed stays executed (delivery is at least once);
                    # remaining due jobs belong to the new owner.
                    self._logger.info(
                        'Deployment "%s" no longer drives this scheduler; '
                        "leaving the remaining due jobs to the new owner",
                        self.deployment,
                    )
                    break
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


def _grace_explicitly_configured(
    gconfig: dict[str, Any],
    prefix: str,
    options: dict[str, Any],
) -> bool:
    """Whether this configure call chooses a misfire grace itself."""
    job_defaults = options.get("job_defaults")
    if isinstance(job_defaults, str):
        # An opaque textual reference; assume it is a deliberate choice.
        return True
    if isinstance(job_defaults, dict) and "misfire_grace_time" in job_defaults:
        return True
    prefix_length = len(prefix) if prefix else 0
    for key, value in gconfig.items():
        name = str(key)
        if prefix:
            if not name.startswith(prefix):
                continue
            name = name[prefix_length:]
        if name == "job_defaults.misfire_grace_time":
            return True
        if name == "job_defaults" and (
            isinstance(value, str) or (isinstance(value, dict) and "misfire_grace_time" in value)
        ):
            return True
    return False


def _patched_configure(
    self: BaseScheduler,
    gconfig: dict[str, Any] | None = None,
    prefix: str = "apscheduler.",
    **options: Any,
) -> Any:
    resolved_gconfig: dict[str, Any] = {} if gconfig is None else gconfig
    result = _original("configure")(self, resolved_gconfig, prefix, **options)
    if not is_vercel_runtime():
        return result
    if _grace_explicitly_configured(resolved_gconfig, prefix, options):
        value = self._job_defaults.get("misfire_grace_time")
        if value is not None and value < MIN_QUEUE_MISFIRE_GRACE_SECONDS:
            LOGGER.warning(
                "job_defaults sets misfire_grace_time=%s; queue delivery "
                "cannot meet a grace below %s seconds, so occurrences may "
                "be skipped as misfires",
                value,
                MIN_QUEUE_MISFIRE_GRACE_SECONDS,
            )
    else:
        # Stock APScheduler calibrates its one-second default grace to
        # in-process wakeup precision, which queue delivery cannot meet:
        # keeping it would skip occurrences on routine dispatch jitter.
        # Unset grace on Vercel means occurrences run whenever their wake
        # arrives; a finite grace is the explicit opt-in for skip-if-late.
        self._job_defaults["misfire_grace_time"] = None
    return result


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
    if adapter.options.scheduler_id is None and not adapter.backend.identity_ready(scheduler):
        # The backend cannot derive a durable identity yet (the declared
        # subscriber mapping resolves only once the declaring module finishes
        # importing); registration re-runs when it can.
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
    alias = kwargs.get("alias", args[1] if len(args) > 1 else "default")
    if adapter is None or alias == "default" or not adapter._lifecycle_called:
        return result
    if is_vercel_runtime() and not is_discovery_runtime() and not is_queue_serving_runtime():
        # A source store added after activation gets no wake from any other
        # path; pull the chain in to its reported next due time.
        adapter._rearm_wake_from_stores()
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


# Positional index of the jobstore alias in each patched mutation's
# arguments (after self).
_MUTATION_JOBSTORE_INDEX = {
    "modify_job": 1,
    "remove_job": 1,
    "resume_job": 1,
    "remove_all_jobs": 0,
}


def _pin_mutation_to_default(
    name: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Pin a durable mutation to the default store.

    Stock APScheduler lets ``jobstore=None`` fall through to every store,
    but a source store's ``remove_job`` can carry dispatch semantics, so an
    app-level mutation must never reach one.
    """
    index = _MUTATION_JOBSTORE_INDEX[name]
    if len(args) > index:
        alias = args[index]
        if alias is None:
            args = (*args[:index], "default", *args[index + 1 :])
            alias = "default"
    else:
        alias = kwargs.get("jobstore")
        if alias is None:
            kwargs = {**kwargs, "jobstore": "default"}
            alias = "default"
    if alias != "default":
        raise APSchedulerConfigurationError(
            f'scheduler-managed jobs may only target the durable "default" '
            f'job store, not "{alias}"; source stores are owned by their '
            "external system"
        )
    return args, kwargs


def _patched_mutation(name: str) -> Callable[..., Any]:
    """Job mutations are rejected on Vercel: the store is immutable at runtime.

    Queue-serving processes pass through so an executing job's in-wake call
    reaches the coordinator, whose gate rejects it with the same contract.
    """
    subject = name.replace("_", " ")

    def patched(self: BaseScheduler, *args: Any, **kwargs: Any) -> Any:
        original = _original(name)
        adapter = get_adapter(self)
        if adapter is None or not is_vercel_runtime() or is_discovery_runtime():
            return original(self, *args, **kwargs)
        args, kwargs = _pin_mutation_to_default(name, args, kwargs)
        if is_queue_serving_runtime():
            return original(self, *args, **kwargs)
        raise APSchedulerConfigurationError(
            f"cannot {subject} on Vercel: the managed job store is "
            "immutable at runtime; change the declaration and deploy, or "
            "gate the job's work with state your application owns"
        )

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
        "configure": BaseScheduler.configure,
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
    BaseScheduler.configure = _patched_configure  # type: ignore[method-assign]
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
