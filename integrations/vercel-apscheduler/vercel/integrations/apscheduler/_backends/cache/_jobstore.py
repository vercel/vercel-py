"""Cache job store and coordinator: best-effort, provenance-tagged."""

from __future__ import annotations

from typing import Any

import base64
import logging
import operator
import pickle
import random
import time
from datetime import datetime, timezone

from apscheduler.job import Job  # type: ignore[import-untyped]
from apscheduler.jobstores.base import (  # type: ignore[import-untyped]
    BaseJobStore,
    ConflictingIdError,
    JobLookupError,
)
from apscheduler.util import (  # type: ignore[import-untyped]
    datetime_to_utc_timestamp,
    utc_timestamp_to_datetime,
)
from vercel.cache import get_cache

from ..._types import (
    PROVENANCE_DECLARED,
    PROVENANCE_RUNTIME,
    APSchedulerConfigurationError,
    NamespaceFencedError,
)
from ._doc import _INDEX_MERGE_ATTEMPTS, DOC_TTL_SECONDS
from ._driver import CacheDriver

LOGGER = logging.getLogger("vercel.integrations.apscheduler")
UTC = timezone.utc

_UNCHANGED = object()

__all__ = ["CacheJobCoordinator", "CacheJobStore"]


class CacheJobStore(BaseJobStore):  # type: ignore[misc]
    """APScheduler job store over one Runtime Cache document.

    All jobs live in a single JSON document so every mutation is one
    read-merge-write; the write methods are replaced by the coordinator at
    bind time. Scaling note: this bounds the practical job count by the
    cache's value-size limit; sharding is a follow-up if real projects hit
    it.
    """

    def __init__(self) -> None:
        super().__init__()
        self.pickle_protocol = pickle.HIGHEST_PROTOCOL
        self.doc_key: str | None = None
        self.tag: str | None = None

    def bind_namespace(self, *, scope: str, scheduler_id: str) -> None:
        expected = (scope, scheduler_id)
        namespace = getattr(self, "_vercel_apscheduler_namespace", None)
        if namespace is not None and namespace != expected:
            raise APSchedulerConfigurationError("job store is already bound to another scheduler")
        self.doc_key = f"aps:{scope}:{scheduler_id}:jobs"
        self.tag = f"aps:{scope}:{scheduler_id}"
        self._vercel_apscheduler_namespace = expected

    def _load(self) -> dict[str, Any]:
        if self.doc_key is None:
            raise APSchedulerConfigurationError("cache job store is not bound yet")
        doc = get_cache().get(self.doc_key)
        if not isinstance(doc, dict) or not isinstance(doc.get("jobs"), dict):
            return {"revision_counter": 0, "jobs": {}}
        normalized = {
            "revision_counter": int(doc.get("revision_counter") or 0),
            "jobs": dict(doc["jobs"]),
        }
        # The reconcile marker shares this document (and its eviction fate).
        if isinstance(doc.get("reconciled_deployment"), str):
            normalized["reconciled_deployment"] = doc["reconciled_deployment"]
        return normalized

    def _store(self, doc: dict[str, Any]) -> None:
        if self.doc_key is None:
            raise APSchedulerConfigurationError("cache job store is not bound yet")
        get_cache().set(self.doc_key, doc, {"ttl": DOC_TTL_SECONDS, "tags": [self.tag]})

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
        except Exception:  # noqa: BLE001 - any unpickling failure quarantines
            return None

    @staticmethod
    def _run_time(record: dict[str, Any]) -> float | None:
        value = record.get("next_run_time_ts")
        return float(value) if value is not None else None

    def lookup_job(self, job_id: str) -> Any | None:
        record = self._load()["jobs"].get(str(job_id))
        if record is None:
            return None
        job = self._decode(record)
        if job is not None:
            job.id = str(job_id)
        return job

    def get_due_jobs(self, now: datetime) -> list[Any]:
        timestamp = datetime_to_utc_timestamp(now)
        jobs = []
        for record in self._load()["jobs"].values():
            run_time = self._run_time(record)
            if record.get("quarantined") or run_time is None or run_time > timestamp:
                continue
            job = self._decode(record)
            if job is not None:
                jobs.append((run_time, job))
        return [job for _, job in sorted(jobs, key=operator.itemgetter(0))]

    def get_next_run_time(self) -> datetime | None:
        run_times = [
            run_time
            for record in self._load()["jobs"].values()
            if not record.get("quarantined") and (run_time := self._run_time(record)) is not None
        ]
        if not run_times:
            return None
        return utc_timestamp_to_datetime(min(run_times))

    def get_all_jobs(self) -> list[Any]:
        jobs = []
        for record in self._load()["jobs"].values():
            job = self._decode(record)
            if job is not None:
                jobs.append(job)
        jobs.sort(
            key=lambda job: (
                job.next_run_time is None,
                job.next_run_time or datetime.max.replace(tzinfo=UTC),
            )
        )
        return jobs

    def add_job(self, job: Any) -> None:  # pragma: no cover - replaced by install()
        raise APSchedulerConfigurationError("cache job store used before binding")

    def update_job(self, job: Any) -> None:  # pragma: no cover - replaced by install()
        raise APSchedulerConfigurationError("cache job store used before binding")

    def remove_job(self, job_id: str) -> None:  # pragma: no cover - replaced by install()
        raise APSchedulerConfigurationError("cache job store used before binding")

    def remove_all_jobs(self) -> None:  # pragma: no cover - replaced by install()
        raise APSchedulerConfigurationError("cache job store used before binding")


class CacheJobCoordinator:
    """Couples the cache job store to its driver, best-effort.

    The revision counter and CAS checks are read-merge-write rather than
    atomic, which shrinks but cannot eliminate lost updates under
    concurrency. Declared jobs are protected by reconciliation-from-code;
    runtime jobs accept the documented risk.
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

    def _record(self, job: Any, revision: int, provenance: str) -> dict[str, Any]:
        state = pickle.dumps(job.__getstate__(), self.store.pickle_protocol)
        next_run_time = getattr(job, "next_run_time", None)
        return {
            "state": base64.b64encode(state).decode(),
            "next_run_time_ts": (
                datetime_to_utc_timestamp(next_run_time) if next_run_time is not None else None
            ),
            "revision": revision,
            "provenance": provenance,
            "quarantined": False,
        }

    def _provenance(self) -> str:
        if self.adapter.is_runtime_mutation or self.adapter.is_wake_mutation:
            return PROVENANCE_RUNTIME
        return PROVENANCE_DECLARED

    def _mutate(self, apply: Any, *, fenced: bool = True) -> Any:
        """Read-merge-write with bounded retries against transient failures.

        The owner fence is best-effort (checked against the driver document,
        not atomically with the write), but it keeps the adapter's
        ``NamespaceFencedError`` paths live: a demoted deployment's stale
        pass aborts instead of resurrecting old declarations.
        """
        last_error: Exception | None = None
        for attempt in range(_INDEX_MERGE_ATTEMPTS):
            if fenced:
                owner = self.driver.owner_deployment()
                if owner is not None and owner != self.driver.deployment:
                    raise NamespaceFencedError(
                        f'deployment "{self.driver.deployment}" no longer drives '
                        "this scheduler; the job-store write was fenced"
                    )
            try:
                doc = self.store._load()
                result = apply(doc)
                if result is not _UNCHANGED:
                    self.store._store(doc)
            except APSchedulerConfigurationError:
                raise
            except Exception as exc:  # noqa: BLE001 - cache I/O is best-effort
                last_error = exc
                time.sleep(random.uniform(0.02, 0.1) * (attempt + 1))
            else:
                return result
        raise RuntimeError("cache job store write failed") from last_error

    def _rearm(self, job: Any, *, always: bool = False) -> None:
        # Adds rearm unconditionally: a declaration restored onto a dormant
        # chain (reconcile after jobs-doc eviction) must mint the wake
        # nothing else will. rearm_wake's own guards make cold-start adds a
        # no-op.
        if not (always or self.adapter.is_runtime_mutation):
            return
        next_run_time = getattr(job, "next_run_time", None)
        if next_run_time is None:
            return
        candidate = self.adapter.canonical_wakeup_time(next_run_time)
        self.driver.rearm_wake(candidate, datetime.now(UTC))

    def add_job(self, job: Any) -> None:
        provenance = self._provenance()

        def apply(doc: dict[str, Any]) -> Any:
            if str(job.id) in doc["jobs"]:
                return "conflict"
            doc["revision_counter"] += 1
            doc["jobs"][str(job.id)] = self._record(job, doc["revision_counter"], provenance)
            return None

        if self._mutate(apply) == "conflict":
            # Declarations are insert-if-absent; runtime adds surface the
            # conflict so replace_existing can route through update_job.
            if self.adapter.is_runtime_mutation or self.adapter.is_wake_mutation:
                raise ConflictingIdError(job.id)
            return
        self._rearm(job, always=True)

    def update_job(self, job: Any) -> None:
        def apply(doc: dict[str, Any]) -> Any:
            record = doc["jobs"].get(str(job.id))
            if record is None:
                return "missing"
            doc["revision_counter"] += 1
            updated = self._record(job, doc["revision_counter"], record.get("provenance") or "")
            doc["jobs"][str(job.id)] = updated
            return None

        if self._mutate(apply) == "missing":
            raise JobLookupError(job.id)
        self._rearm(job)

    def remove_job(self, job_id: str) -> None:
        def apply(doc: dict[str, Any]) -> Any:
            if doc["jobs"].pop(str(job_id), None) is None:
                return "missing"
            doc["revision_counter"] += 1
            return None

        if self._mutate(apply) == "missing":
            raise JobLookupError(job_id)

    def remove_all_jobs(self) -> None:
        def apply(doc: dict[str, Any]) -> Any:
            doc["jobs"].clear()
            doc["revision_counter"] += 1
            return None

        self._mutate(apply)

    def get_due_jobs_with_revisions(self, now: datetime) -> list[tuple[Any, int]]:
        timestamp = datetime_to_utc_timestamp(now)
        due: list[tuple[float, Any, int]] = []
        for job_id, record in self.store._load()["jobs"].items():
            run_time = CacheJobStore._run_time(record)
            if record.get("quarantined") or run_time is None or run_time > timestamp:
                continue
            job = self.store._decode(record)
            if job is None:
                self.quarantine_job(job_id)
                continue
            due.append((run_time, job, int(record.get("revision") or 0)))
        due.sort(key=operator.itemgetter(0))
        return [(job, revision) for _, job, revision in due]

    def get_all_jobs_with_revisions(
        self,
    ) -> tuple[list[tuple[Any, int, str]], list[tuple[str, int, str]]]:
        jobs: list[tuple[Any, int, str]] = []
        undecodable: list[tuple[str, int, str]] = []
        for job_id, record in self.store._load()["jobs"].items():
            revision = int(record.get("revision") or 0)
            provenance = str(record.get("provenance") or "")
            job = self.store._decode(record)
            if job is None:
                undecodable.append((job_id, revision, provenance))
                continue
            jobs.append((job, revision, provenance))
        jobs.sort(
            key=lambda item: (
                item[0].next_run_time is None,
                item[0].next_run_time or datetime.max.replace(tzinfo=UTC),
            ),
        )
        return jobs, undecodable

    def cas_update_job(self, job: Any, expected_revision: int) -> bool:
        def apply(doc: dict[str, Any]) -> Any:
            record = doc["jobs"].get(str(job.id))
            if record is None or int(record.get("revision") or 0) != expected_revision:
                return False
            doc["revision_counter"] += 1
            doc["jobs"][str(job.id)] = self._record(
                job,
                doc["revision_counter"],
                record.get("provenance") or "",
            )
            return True

        return bool(self._mutate(apply) is True)

    def cas_remove_job(self, job_id: str, expected_revision: int) -> bool:
        def apply(doc: dict[str, Any]) -> Any:
            record = doc["jobs"].get(str(job_id))
            if record is None or int(record.get("revision") or 0) != expected_revision:
                return False
            del doc["jobs"][str(job_id)]
            doc["revision_counter"] += 1
            return True

        return bool(self._mutate(apply) is True)

    def quarantine_job(self, job_id: str) -> None:
        def apply(doc: dict[str, Any]) -> Any:
            record = doc["jobs"].get(str(job_id))
            if record is None or record.get("quarantined"):
                return _UNCHANGED
            record["quarantined"] = True
            return None

        self._mutate(apply, fenced=False)
        LOGGER.error(
            'Quarantined APScheduler job "%s": its persisted definition can '
            "no longer be loaded by this deployment's code",
            job_id,
        )
