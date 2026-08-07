"""Atomic coordination between one APScheduler Redis store and its driver."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import logging
import pickle
from collections.abc import Iterator
from datetime import datetime, timezone

from apscheduler.jobstores.base import (  # type: ignore[import-untyped]
    ConflictingIdError,
    JobLookupError,
)
from apscheduler.util import (  # type: ignore[import-untyped]
    datetime_to_utc_timestamp,
)

from ..._imports import RedisJobStore
from ..._types import (
    PROVENANCE_DECLARED,
    PROVENANCE_RUNTIME,
    NamespaceFencedError,
)

LOGGER = logging.getLogger("vercel.integrations.apscheduler")

if TYPE_CHECKING:
    from ..._adapter import SchedulerAdapter
    from ._driver import RedisDriver

UTC = timezone.utc

__all__ = ["RedisJobCoordinator"]


# Shared head of the job write scripts: refuse the whole write when another
# deployment owns the chain. A missing owner passes, so a fresh scope and a
# preview's first materialization (which run before the driver records an
# owner) still write. ARGV[8] is the writing deployment.
_OWNER_FENCE_FRAGMENT = """
local owner = redis.call("HGET", KEYS[4], "owner_deployment")
if owner and owner ~= ARGV[8] then
  return {"fenced", ""}
end
"""


# Shared tail of the job write scripts: bump the store revision, persist the
# job, and rearm the wake token in the same atomic transaction. ARGV[4] is
# "1" when the write may rearm, ARGV[5] the job's canonical wake candidate,
# ARGV[6] the current time, ARGV[7] the provenance recorded on insert, and
# ARGV[8] the writing deployment: only the chain's owner may rearm it.
_WRITE_JOB_FRAGMENT = """
local revision = redis.call("HINCRBY", KEYS[4], "job_revision", 1)
redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
redis.call("HSET", KEYS[3], ARGV[1], tostring(revision))
if ARGV[3] ~= "" then
  redis.call("ZADD", KEYS[2], ARGV[3], ARGV[1])
else
  redis.call("ZREM", KEYS[2], ARGV[1])
end

if ARGV[4] == "1" and ARGV[5] ~= ""
  and redis.call("HGET", KEYS[4], "state") == "running"
  and redis.call("HGET", KEYS[4], "owner_deployment") == ARGV[8]
then
  local active_owner = redis.call("HGET", KEYS[4], "active_owner")
  if active_owner then
    local dirty = redis.call("HGET", KEYS[4], "dirty_logical_time")
    if not dirty or ARGV[5] < dirty then
      redis.call("HSET", KEYS[4], "dirty_logical_time", ARGV[5])
    end
  elseif redis.call("HGET", KEYS[4], "start_status") == "active" then
    local current = redis.call("HGET", KEYS[4], "current_logical_time")
    if not current or ARGV[5] < current then
      local sequence = tonumber(redis.call("HGET", KEYS[4], "current_sequence") or "0") + 1
      redis.call(
        "HSET",
        KEYS[4],
        "current_sequence", tostring(sequence),
        "current_logical_time", ARGV[5],
        "current_status", "pending",
        "updated_at", ARGV[6]
      )
    end
  end
end
return {"ok", tostring(revision)}
"""


_ADD_JOB_SCRIPT = f"""
-- vercel-apscheduler-v1:add-job
{_OWNER_FENCE_FRAGMENT}
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 1 then
  return {{"conflict", ""}}
end
redis.call("HSET", KEYS[5], ARGV[1], ARGV[7])
{_WRITE_JOB_FRAGMENT}
"""


_UPDATE_JOB_SCRIPT = f"""
-- vercel-apscheduler-v1:update-job
{_OWNER_FENCE_FRAGMENT}
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0 then
  return {{"missing", ""}}
end
{_WRITE_JOB_FRAGMENT}
"""


_REMOVE_JOB_SCRIPT = """
-- vercel-apscheduler-v1:remove-job
local owner = redis.call("HGET", KEYS[4], "owner_deployment")
if owner and owner ~= ARGV[2] then
  return "fenced"
end
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0 then
  return "missing"
end
redis.call("HINCRBY", KEYS[4], "job_revision", 1)
redis.call("HDEL", KEYS[1], ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("HDEL", KEYS[3], ARGV[1])
redis.call("HDEL", KEYS[5], ARGV[1])
return "ok"
"""


_REMOVE_ALL_JOBS_SCRIPT = """
-- vercel-apscheduler-v1:remove-all-jobs
local owner = redis.call("HGET", KEYS[4], "owner_deployment")
if owner and owner ~= ARGV[1] then
  return "fenced"
end
redis.call("HINCRBY", KEYS[4], "job_revision", 1)
redis.call("DEL", KEYS[1], KEYS[2], KEYS[3], KEYS[5])
return "ok"
"""


_GET_DUE_WITH_REVISIONS_SCRIPT = """
-- vercel-apscheduler-v1:get-due-with-revisions
local ids = redis.call("ZRANGEBYSCORE", KEYS[2], 0, ARGV[1])
local result = {}
for _, id in ipairs(ids) do
  local state = redis.call("HGET", KEYS[1], id)
  if state then
    table.insert(result, id)
    table.insert(result, state)
    table.insert(result, redis.call("HGET", KEYS[3], id) or "0")
  end
end
return result
"""


_GET_ALL_WITH_REVISIONS_SCRIPT = """
-- vercel-apscheduler-v1:get-all-with-revisions
local ids = redis.call("HKEYS", KEYS[1])
local result = {}
for _, id in ipairs(ids) do
  local state = redis.call("HGET", KEYS[1], id)
  if state then
    table.insert(result, id)
    table.insert(result, state)
    table.insert(result, redis.call("HGET", KEYS[2], id) or "0")
    table.insert(result, redis.call("HGET", KEYS[3], id) or "")
  end
end
return result
"""


_CAS_UPDATE_JOB_SCRIPT = """
-- vercel-apscheduler-v1:cas-update-job
local owner = redis.call("HGET", KEYS[4], "owner_deployment")
if owner and owner ~= ARGV[5] then
  return -1
end
local current = tonumber(redis.call("HGET", KEYS[3], ARGV[1]) or "0")
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0 or current ~= tonumber(ARGV[2]) then
  return 0
end
local revision = redis.call("HINCRBY", KEYS[4], "job_revision", 1)
redis.call("HSET", KEYS[1], ARGV[1], ARGV[3])
redis.call("HSET", KEYS[3], ARGV[1], tostring(revision))
if ARGV[4] ~= "" then
  redis.call("ZADD", KEYS[2], ARGV[4], ARGV[1])
else
  redis.call("ZREM", KEYS[2], ARGV[1])
end
return 1
"""


_CAS_REMOVE_JOB_SCRIPT = """
-- vercel-apscheduler-v1:cas-remove-job
local owner = redis.call("HGET", KEYS[4], "owner_deployment")
if owner and owner ~= ARGV[3] then
  return -1
end
local current = tonumber(redis.call("HGET", KEYS[3], ARGV[1]) or "0")
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0 or current ~= tonumber(ARGV[2]) then
  return 0
end
redis.call("HINCRBY", KEYS[4], "job_revision", 1)
redis.call("HDEL", KEYS[1], ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("HDEL", KEYS[3], ARGV[1])
redis.call("HDEL", KEYS[5], ARGV[1])
return 1
"""


class RedisJobCoordinator:
    """Atomically couples one APScheduler Redis store to its driver."""

    def __init__(
        self,
        store: RedisJobStore,
        driver: RedisDriver,
        adapter: SchedulerAdapter,
    ) -> None:
        self.store = store
        self.redis: Any = store.redis
        self.driver = driver
        self.adapter = adapter
        self.versions_key = f"{store.jobs_key}:versions"
        self.provenance_key = f"{store.jobs_key}:provenance"

    @property
    def keys(self) -> tuple[str, str, str, str, str]:
        return (
            self.store.jobs_key,
            self.store.run_times_key,
            self.versions_key,
            self.driver.key,
            self.provenance_key,
        )

    def install(self) -> None:
        self.store.add_job = self.add_job  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
        self.store.update_job = self.update_job  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
        self.store.remove_job = self.remove_job  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
        self.store.remove_all_jobs = self.remove_all_jobs  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
        self.store.__dict__["_vercel_apscheduler_coordinator"] = self

    def add_job(self, job: Any) -> None:
        result = self._write(_ADD_JOB_SCRIPT, job, rearm=True)
        state = _text(result[0])
        if state == "fenced":
            raise self._fenced(job.id)
        # Code declarations are materialized insert-if-absent. Concurrent cold
        # starts must retain the first durable value instead of replacing a
        # runtime mutation with a stale declaration. Runtime and in-wake adds
        # raise instead, so APScheduler's replace_existing fallback can update
        # the persisted job.
        if state == "conflict" and (
            self.adapter.is_runtime_mutation or self.adapter.is_wake_mutation
        ):
            raise ConflictingIdError(job.id)

    def update_job(self, job: Any) -> None:
        result = self._write(
            _UPDATE_JOB_SCRIPT,
            job,
            rearm=self.adapter.is_runtime_mutation,
        )
        state = _text(result[0])
        if state == "fenced":
            raise self._fenced(job.id)
        if state == "missing":
            raise JobLookupError(job.id)

    def remove_job(self, job_id: str) -> None:
        result = _text(
            self.redis.eval(
                _REMOVE_JOB_SCRIPT,
                len(self.keys),
                *self.keys,
                job_id,
                self.driver.deployment,
            )
        )
        if result == "fenced":
            raise self._fenced(job_id)
        if result == "missing":
            raise JobLookupError(job_id)

    def remove_all_jobs(self) -> None:
        result = _text(
            self.redis.eval(
                _REMOVE_ALL_JOBS_SCRIPT,
                len(self.keys),
                *self.keys,
                self.driver.deployment,
            )
        )
        if result == "fenced":
            raise self._fenced(None)

    def get_due_jobs_with_revisions(
        self,
        now: datetime,
    ) -> list[tuple[Any, int]]:
        raw = self.redis.eval(
            _GET_DUE_WITH_REVISIONS_SCRIPT,
            3,
            self.store.jobs_key,
            self.store.run_times_key,
            self.versions_key,
            str(datetime_to_utc_timestamp(now)),
        )
        due: list[tuple[Any, int]] = []
        for job_id, job, revision, _provenance in self._decode_records(raw, stride=3):
            if job is None:
                self.quarantine_job(job_id)
                continue
            due.append((job, revision))
        return due

    def get_all_jobs_with_revisions(
        self,
    ) -> tuple[list[tuple[Any, int, str]], list[tuple[str, int, str]]]:
        """Return decodable jobs plus records this code can no longer load.

        Undecodable records are returned as ``(job_id, revision, provenance)``
        so takeover reconciliation can atomically restore a declaration whose
        persisted definition no longer loads, instead of stranding it.
        """
        raw = self.redis.eval(
            _GET_ALL_WITH_REVISIONS_SCRIPT,
            3,
            self.store.jobs_key,
            self.versions_key,
            self.provenance_key,
        )
        jobs: list[tuple[Any, int, str]] = []
        undecodable: list[tuple[str, int, str]] = []
        for job_id, job, revision, provenance in self._decode_records(raw, stride=4):
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
        state, score = self._serialized_job(job)
        result = int(
            self.redis.eval(
                _CAS_UPDATE_JOB_SCRIPT,
                len(self.keys),
                *self.keys,
                job.id,
                str(expected_revision),
                state,
                score,
                self.driver.deployment,
            )
        )
        if result < 0:
            raise self._fenced(job.id)
        return bool(result)

    def cas_remove_job(self, job_id: str, expected_revision: int) -> bool:
        result = int(
            self.redis.eval(
                _CAS_REMOVE_JOB_SCRIPT,
                len(self.keys),
                *self.keys,
                job_id,
                str(expected_revision),
                self.driver.deployment,
            )
        )
        if result < 0:
            raise self._fenced(job_id)
        return bool(result)

    def _fenced(self, job_id: str | None) -> NamespaceFencedError:
        subject = f'job "{job_id}"' if job_id is not None else "the job store"
        return NamespaceFencedError(
            f'deployment "{self.driver.deployment}" no longer drives this '
            f"scheduler; the write to {subject} was fenced"
        )

    def _write(self, script: str, job: Any, *, rearm: bool) -> Any:
        state, score = self._serialized_job(job)
        next_run_time = getattr(job, "next_run_time", None)
        candidate = (
            self.adapter.canonical_wakeup_time(next_run_time).isoformat()
            if next_run_time is not None
            else ""
        )
        provenance = (
            PROVENANCE_RUNTIME
            if self.adapter.is_runtime_mutation or self.adapter.is_wake_mutation
            else PROVENANCE_DECLARED
        )
        now = datetime.now(UTC)
        return self.redis.eval(
            script,
            len(self.keys),
            *self.keys,
            job.id,
            state,
            score,
            "1" if rearm else "0",
            candidate,
            now.isoformat(),
            provenance,
            self.driver.deployment,
        )

    def _serialized_job(self, job: Any) -> tuple[bytes, str]:
        state = pickle.dumps(job.__getstate__(), self.store.pickle_protocol)
        next_run_time = getattr(job, "next_run_time", None)
        score = str(datetime_to_utc_timestamp(next_run_time)) if next_run_time is not None else ""
        return state, score

    def _decode_records(
        self,
        raw: Any,
        *,
        stride: int,
    ) -> Iterator[tuple[str, Any | None, int, str]]:
        """Yield ``(job_id, job, revision, provenance)`` from a script reply.

        ``stride`` is the script's fields per record; provenance is empty when
        the script does not return it. ``job`` is ``None`` when the persisted
        state no longer reconstitutes under this deployment's code; the caller
        decides whether to quarantine or repair the record.
        """
        if not isinstance(raw, (list, tuple)) or len(raw) % stride:
            raise RuntimeError("Redis returned invalid APScheduler versioned jobs")
        for index in range(0, len(raw), stride):
            job_id = _text(raw[index])
            state = raw[index + 1]
            revision = int(_text(raw[index + 2]))
            provenance = _text(raw[index + 3]) if stride > 3 else ""
            try:
                job = self.store._reconstitute_job(state)
            except Exception:  # noqa: BLE001 - any unpickling failure
                job = None
            yield job_id, job, revision, provenance

    def quarantine_job(self, job_id: str) -> None:
        """Sideline a job whose persisted state no longer reconstitutes.

        Typically a dynamic job whose function was removed by a later
        deployment. Removing it from the due index keeps the chain alive;
        the record and its revision stay for an operator to fix or delete.
        """
        self.redis.zrem(self.store.run_times_key, job_id)
        LOGGER.error(
            'Quarantined APScheduler job "%s": its persisted definition can '
            "no longer be loaded by this deployment's code",
            job_id,
        )


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode()
    return str(value)
