"""Cache job store and coordinator: one record per declared job.

The declarations are the index. Every read enumerates the code-declared job
ids, so a record nothing declares is unreachable (it ages out by TTL), and a
record that is missing, unreadable, or whose declared schedule changed is
rebuilt from its declaration at the point of use (read-repair). Writes are
owner-fenced best-effort; a demoted deployment's reads degrade to a
declaration-derived view without writing.
"""

from __future__ import annotations

from typing import Any

import base64
import logging
import pickle
import random
import time
from datetime import datetime, timezone
from itertools import starmap

from apscheduler.job import Job  # type: ignore[import-untyped]
from apscheduler.jobstores.base import (  # type: ignore[import-untyped]
    BaseJobStore,
    ConflictingIdError,
    JobLookupError,
)
from apscheduler.util import (  # type: ignore[import-untyped]
    datetime_to_utc_timestamp,
)
from vercel.cache import get_cache

from ..._imports import IntervalTrigger
from ..._types import APSchedulerConfigurationError, NamespaceFencedError
from ._doc import _WRITE_ATTEMPTS, DOC_TTL_SECONDS
from ._driver import CacheDriver

LOGGER = logging.getLogger("vercel.integrations.apscheduler")
UTC = timezone.utc

__all__ = ["CacheJobCoordinator", "CacheJobStore", "trigger_fingerprint"]


def trigger_fingerprint(trigger: Any) -> str:
    """Digest a trigger into its user-declared, comparable schedule.

    ``IntervalTrigger`` without an explicit ``start_date`` auto-anchors at
    declaration time, so that field would look changed on every deployment
    and re-anchor unchanged schedules; it is excluded from the digest.
    """
    state: Any = trigger.__getstate__()
    if isinstance(state, dict) and type(trigger) is IntervalTrigger:
        state = {key: value for key, value in state.items() if key != "start_date"}
    detail = (
        repr(sorted(state.items(), key=lambda item: str(item[0])))
        if isinstance(state, dict)
        else repr(state)
    )
    return f"{type(trigger).__module__}:{type(trigger).__qualname__}:{detail}"


class CacheJobStore(BaseJobStore):  # type: ignore[misc]
    """APScheduler job store over one Runtime Cache record per declared job.

    Reads enumerate the declared ids through the coordinator, so eviction,
    takeover, and schedule changes heal per job instead of per population;
    the write methods are replaced by the coordinator at bind time.
    """

    def __init__(self) -> None:
        super().__init__()
        self.pickle_protocol = pickle.HIGHEST_PROTOCOL
        self.key_prefix: str | None = None
        self.tag: str | None = None

    def bind_namespace(self, *, scope: str, scheduler_id: str) -> None:
        expected = (scope, scheduler_id)
        namespace = getattr(self, "_vercel_apscheduler_namespace", None)
        if namespace is not None and namespace != expected:
            raise APSchedulerConfigurationError("job store is already bound to another scheduler")
        self.key_prefix = f"aps:{scope}:{scheduler_id}:job:"
        self.tag = f"aps:{scope}:{scheduler_id}"
        self._vercel_apscheduler_namespace = expected

    @property
    def _coordinator(self) -> CacheJobCoordinator:
        coordinator = self.__dict__.get("_vercel_apscheduler_coordinator")
        if coordinator is None:
            raise APSchedulerConfigurationError("cache job store is not bound yet")
        return coordinator  # type: ignore[no-any-return]

    def _record_key(self, job_id: str) -> str:
        if self.key_prefix is None:
            raise APSchedulerConfigurationError("cache job store is not bound yet")
        return f"{self.key_prefix}{job_id}"

    def _load_record(self, job_id: str) -> dict[str, Any] | None:
        record = get_cache().get(self._record_key(job_id))
        return record if isinstance(record, dict) else None

    def _store_record(self, job_id: str, record: dict[str, Any]) -> None:
        get_cache().set(
            self._record_key(job_id),
            record,
            {"ttl": DOC_TTL_SECONDS, "tags": [self.tag]},
        )

    def _delete_record(self, job_id: str) -> None:
        get_cache().delete(self._record_key(job_id))

    def _reconstitute_job(self, job_state: dict[str, Any]) -> Any:
        """Rebuild a Job exactly as upstream APScheduler stores do."""
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        return job

    def _decode(self, record: dict[str, Any]) -> Any | None:
        try:
            state = pickle.loads(base64.b64decode(record["state"]))
            return self._reconstitute_job(state)
        except Exception:  # noqa: BLE001 - any unpickling failure repairs
            return None

    def lookup_job(self, job_id: str) -> Any | None:
        entry = self._coordinator.entry(str(job_id))
        return entry[0] if entry is not None else None

    def get_due_jobs(self, now: datetime) -> list[Any]:
        return [job for job, _revision in self._coordinator.get_due_jobs_with_revisions(now)]

    def get_next_run_time(self) -> datetime | None:
        run_times = [
            job.next_run_time
            for job, _revision in self._coordinator.get_all_jobs_with_revisions()
            if job.next_run_time is not None
        ]
        return min(run_times, default=None)

    def get_all_jobs(self) -> list[Any]:
        return [job for job, _revision in self._coordinator.get_all_jobs_with_revisions()]

    def add_job(self, job: Any) -> None:  # pragma: no cover - replaced by install()
        raise APSchedulerConfigurationError("cache job store used before binding")

    def update_job(self, job: Any) -> None:  # pragma: no cover - replaced by install()
        raise APSchedulerConfigurationError("cache job store used before binding")

    def remove_job(self, job_id: str) -> None:  # pragma: no cover - replaced by install()
        raise APSchedulerConfigurationError("cache job store used before binding")

    def remove_all_jobs(self) -> None:  # pragma: no cover - replaced by install()
        raise APSchedulerConfigurationError("cache job store used before binding")


class CacheJobCoordinator:
    """Couples per-job records to the driver: declared-only, read-repair.

    The store is immutable at runtime: every record is a code declaration
    plus execution progress, so eviction can never lose state that code and
    the in-flight messages cannot restate. Revision checks are
    read-merge-write rather than atomic, which shrinks but cannot eliminate
    lost updates under concurrency — and a race now only ever involves one
    job's record, never its neighbors.
    """

    def __init__(self, store: CacheJobStore, driver: CacheDriver, adapter: Any) -> None:
        self.store = store
        self.driver = driver
        self.adapter = adapter

    def install(self) -> None:
        self.store.add_job = self.add_job  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
        self.store.update_job = self.update_job  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
        self.store.remove_job = self.remove_job  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
        self.store.remove_all_jobs = self.remove_all_jobs  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
        self.store.__dict__["_vercel_apscheduler_coordinator"] = self

    # --- record plumbing -------------------------------------------------

    def _declared(self) -> dict[str, Any]:
        return dict(self.adapter._declared_jobs)

    def _owner_allows_writes(self) -> bool:
        owner = self.driver.owner_deployment()
        return owner is None or owner == self.driver.deployment

    def _check_fence(self, subject: str) -> None:
        if not self._owner_allows_writes():
            raise NamespaceFencedError(
                f'deployment "{self.driver.deployment}" no longer drives this '
                f"scheduler; the write to {subject} was fenced"
            )

    def _record(self, job: Any, revision: int) -> dict[str, Any]:
        state = pickle.dumps(job.__getstate__(), self.store.pickle_protocol)
        next_run_time = getattr(job, "next_run_time", None)
        return {
            "state": base64.b64encode(state).decode(),
            "next_run_time_ts": (
                datetime_to_utc_timestamp(next_run_time) if next_run_time is not None else None
            ),
            "revision": revision,
            "fingerprint": trigger_fingerprint(job.trigger),
        }

    def _persist(self, job_id: str, record: dict[str, Any]) -> None:
        """Write one record with bounded retries against transient failures."""
        last_error: Exception | None = None
        for attempt in range(_WRITE_ATTEMPTS):
            try:
                self.store._store_record(job_id, record)
            except Exception as exc:  # noqa: BLE001 - cache I/O is best-effort
                last_error = exc
                time.sleep(random.uniform(0.02, 0.1) * (attempt + 1))
            else:
                return
        raise RuntimeError("cache job store write failed") from last_error

    def _erase(self, job_id: str) -> None:
        last_error: Exception | None = None
        for attempt in range(_WRITE_ATTEMPTS):
            try:
                self.store._delete_record(job_id)
            except Exception as exc:  # noqa: BLE001 - cache I/O is best-effort
                last_error = exc
                time.sleep(random.uniform(0.02, 0.1) * (attempt + 1))
            else:
                return
        raise RuntimeError("cache job store write failed") from last_error

    # --- enumeration with read-repair ------------------------------------

    def entry(self, job_id: str) -> tuple[Any, int] | None:
        """Return ``(job, revision)`` for one declared id, repairing as needed."""
        declared = self._declared().get(str(job_id))
        if declared is None:
            return None
        return self._entry_for(str(job_id), declared)

    def _entry_for(self, job_id: str, declared: Any) -> tuple[Any, int]:
        record = self.store._load_record(job_id)
        if record is not None:
            job = self.store._decode(record)
            if job is not None and record.get("fingerprint") == trigger_fingerprint(
                declared.trigger
            ):
                job.id = job_id
                return job, int(record.get("revision") or 0)
        return self._repair(job_id, declared, record)

    def _repair(
        self,
        job_id: str,
        declared: Any,
        stale_record: dict[str, Any] | None,
    ) -> tuple[Any, int]:
        """Rebuild one record from its declaration at the point of use.

        Restarting the schedule from now is deliberate: the record's own
        progress is gone or belongs to a different declared trigger, and
        recomputing from now skips the unobserved interval instead of
        replaying it (a past-due date declaration does not re-fire).

        The write is owner-fenced and best-effort: a demoted deployment
        still gets a declaration-derived view for local reads, but writes
        nothing into the namespace it no longer drives.
        """
        scheduler = getattr(self.store, "_scheduler", None)
        now = datetime.now(scheduler.timezone if scheduler is not None else UTC)
        state = declared.__getstate__()
        state["next_run_time"] = declared.trigger.get_next_fire_time(None, now)
        job = self.store._reconstitute_job(state)
        job.id = job_id
        revision = int((stale_record or {}).get("revision") or 0) + 1
        if self._owner_allows_writes():
            try:
                self._persist(job_id, self._record(job, revision))
            except RuntimeError:
                LOGGER.exception(
                    'Could not persist the rebuilt record for job "%s"; '
                    "serving the declaration-derived view",
                    job_id,
                )
            else:
                LOGGER.warning(
                    'Rebuilt job "%s" from its declaration: its record was %s',
                    job_id,
                    (
                        "missing (possible cache eviction)"
                        if stale_record is None
                        else "written for a different declared schedule"
                    ),
                )
        return job, revision

    def get_due_jobs_with_revisions(self, now: datetime) -> list[tuple[Any, int]]:
        timestamp = datetime_to_utc_timestamp(now)
        return [
            (job, revision)
            for job, revision in self.get_all_jobs_with_revisions()
            if job.next_run_time is not None
            and datetime_to_utc_timestamp(job.next_run_time) <= timestamp
        ]

    def get_all_jobs_with_revisions(self) -> list[tuple[Any, int]]:
        entries = list(starmap(self._entry_for, self._declared().items()))
        entries.sort(
            key=lambda entry: (
                entry[0].next_run_time is None,
                entry[0].next_run_time or datetime.max.replace(tzinfo=UTC),
            ),
        )
        return entries

    # --- writes -----------------------------------------------------------

    def _reject_runtime_mutation(self, subject: str) -> None:
        """Refuse a runtime write: durable inputs are code and time.

        Every record must stay reconstructable from the declarations, so
        runtime state changes have nowhere durable to live. A job is changed
        by shipping its changed declaration; a job that must not run is
        stopped by removing or gating its declaration in code, or by a flag
        in application-owned storage that the job checks.
        """
        if self.adapter.is_runtime_mutation or self.adapter.is_wake_mutation:
            raise APSchedulerConfigurationError(
                f"cannot {subject} at runtime: the managed job store is "
                "immutable at runtime; change the declaration and deploy, or "
                "gate the job's work with state your application owns"
            )

    def _rearm(self, job: Any, *, always: bool = False) -> None:
        # Adds rearm unconditionally: a declaration restored onto a dormant
        # chain must mint the wake nothing else will. rearm_wake's own guards
        # make cold-start adds a no-op.
        if not (always or self.adapter.is_runtime_mutation):
            return
        next_run_time = getattr(job, "next_run_time", None)
        if next_run_time is None:
            return
        candidate = self.adapter.canonical_wakeup_time(next_run_time)
        self.driver.rearm_wake(candidate, datetime.now(UTC))

    def add_job(self, job: Any) -> None:
        runtime = self.adapter.is_runtime_mutation or self.adapter.is_wake_mutation
        job_id = str(job.id)
        if self.store._load_record(job_id) is not None:
            # Declarations are insert-if-absent; a runtime add of an existing
            # id surfaces the conflict for upstream's replace_existing path,
            # whose update is then rejected as a runtime mutation.
            if runtime:
                raise ConflictingIdError(job.id)
            return
        if runtime:
            # The store's contents must be reconstructable from code: an
            # evictable, per-region cache cannot durably hold the only copy
            # of a job nothing declares.
            raise APSchedulerConfigurationError(
                f'cannot create job "{job.id}" at runtime: the managed job '
                "store holds only code-declared jobs; keep dynamic schedules "
                "in your own database, or publish a delayed queue message "
                "for one-shot work"
            )
        self._check_fence(f'job "{job_id}"')
        self._persist(job_id, self._record(job, 1))
        self._rearm(job, always=True)

    def update_job(self, job: Any) -> None:
        self._reject_runtime_mutation(f'update job "{job.id}"')
        job_id = str(job.id)
        record = self.store._load_record(job_id)
        if record is None:
            raise JobLookupError(job.id)
        self._check_fence(f'job "{job_id}"')
        self._persist(job_id, self._record(job, int(record.get("revision") or 0) + 1))
        self._rearm(job)

    def remove_job(self, job_id: str) -> None:
        self._reject_runtime_mutation(f'remove job "{job_id}"')
        if self.store._load_record(str(job_id)) is None:
            raise JobLookupError(job_id)
        self._check_fence(f'job "{job_id}"')
        self._erase(str(job_id))

    def remove_all_jobs(self) -> None:
        self._reject_runtime_mutation("remove jobs")
        self._check_fence("the job store")
        for job_id in self._declared():
            self._erase(job_id)

    def cas_update_job(self, job: Any, expected_revision: int) -> bool:
        self._check_fence(f'job "{job.id}"')
        job_id = str(job.id)
        record = self.store._load_record(job_id)
        if record is None or int(record.get("revision") or 0) != expected_revision:
            return False
        self._persist(job_id, self._record(job, expected_revision + 1))
        return True

    def cas_remove_job(self, job_id: str, expected_revision: int) -> bool:
        self._check_fence(f'job "{job_id}"')
        record = self.store._load_record(str(job_id))
        if record is None or int(record.get("revision") or 0) != expected_revision:
            return False
        self._erase(str(job_id))
        return True
