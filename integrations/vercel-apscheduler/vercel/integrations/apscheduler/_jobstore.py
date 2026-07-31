"""Atomic coordination between one APScheduler Redis store and its driver."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pickle
from datetime import datetime, timezone

from apscheduler.jobstores.base import (  # type: ignore[import-untyped]
    ConflictingIdError,
    JobLookupError,
)
from apscheduler.util import (  # type: ignore[import-untyped]
    datetime_to_utc_timestamp,
)

from ._imports import RedisJobStore

if TYPE_CHECKING:
    from ._adapter import SchedulerAdapter
    from ._driver import RedisDriver

UTC = timezone.utc

__all__ = ["RedisJobCoordinator"]


# Shared tail of the job write scripts: bump the store revision, persist the
# job, and rearm the wake token in the same atomic transaction. ARGV[4] is
# "1" when the write may rearm, ARGV[5] the job's canonical wake candidate,
# ARGV[6] the current time.
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
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 1 then
  return {{"conflict", ""}}
end
{_WRITE_JOB_FRAGMENT}
"""


_UPDATE_JOB_SCRIPT = f"""
-- vercel-apscheduler-v1:update-job
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0 then
  return {{"missing", ""}}
end
{_WRITE_JOB_FRAGMENT}
"""


_REMOVE_JOB_SCRIPT = """
-- vercel-apscheduler-v1:remove-job
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0 then
  return "missing"
end
redis.call("HINCRBY", KEYS[4], "job_revision", 1)
redis.call("HDEL", KEYS[1], ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("HDEL", KEYS[3], ARGV[1])
return "ok"
"""


_REMOVE_ALL_JOBS_SCRIPT = """
-- vercel-apscheduler-v1:remove-all-jobs
redis.call("HINCRBY", KEYS[4], "job_revision", 1)
redis.call("DEL", KEYS[1], KEYS[2], KEYS[3])
return 1
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
  end
end
return result
"""


_CAS_UPDATE_JOB_SCRIPT = """
-- vercel-apscheduler-v1:cas-update-job
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
local current = tonumber(redis.call("HGET", KEYS[3], ARGV[1]) or "0")
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0 or current ~= tonumber(ARGV[2]) then
  return 0
end
redis.call("HINCRBY", KEYS[4], "job_revision", 1)
redis.call("HDEL", KEYS[1], ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("HDEL", KEYS[3], ARGV[1])
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

    @property
    def keys(self) -> tuple[str, str, str, str]:
        return (
            self.store.jobs_key,
            self.store.run_times_key,
            self.versions_key,
            self.driver.key,
        )

    def install(self) -> None:
        self.store.add_job = self.add_job  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
        self.store.update_job = self.update_job  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
        self.store.remove_job = self.remove_job  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
        self.store.remove_all_jobs = self.remove_all_jobs  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
        self.store.__dict__["_vercel_apscheduler_coordinator"] = self

    def add_job(self, job: Any) -> None:
        result = self._write(_ADD_JOB_SCRIPT, job, rearm=True)
        # Code declarations are materialized insert-if-absent. Concurrent cold
        # starts must retain the first durable value instead of replacing a
        # runtime mutation with a stale declaration. Runtime and in-wake adds
        # raise instead, so APScheduler's replace_existing fallback can update
        # the persisted job.
        if _text(result[0]) == "conflict" and (
            self.adapter.is_runtime_mutation or self.adapter.is_wake_mutation
        ):
            raise ConflictingIdError(job.id)

    def update_job(self, job: Any) -> None:
        result = self._write(
            _UPDATE_JOB_SCRIPT,
            job,
            rearm=self.adapter.is_runtime_mutation,
        )
        if _text(result[0]) == "missing":
            raise JobLookupError(job.id)

    def remove_job(self, job_id: str) -> None:
        result = _text(
            self.redis.eval(
                _REMOVE_JOB_SCRIPT,
                len(self.keys),
                *self.keys,
                job_id,
            )
        )
        if result == "missing":
            raise JobLookupError(job_id)

    def remove_all_jobs(self) -> None:
        self.redis.eval(
            _REMOVE_ALL_JOBS_SCRIPT,
            len(self.keys),
            *self.keys,
        )

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
        return self._decode_versioned_jobs(raw)

    def get_all_jobs_with_revisions(self) -> list[tuple[Any, int]]:
        raw = self.redis.eval(
            _GET_ALL_WITH_REVISIONS_SCRIPT,
            2,
            self.store.jobs_key,
            self.versions_key,
        )
        jobs = self._decode_versioned_jobs(raw)
        return sorted(
            jobs,
            key=lambda item: (
                item[0].next_run_time is None,
                item[0].next_run_time or datetime.max.replace(tzinfo=UTC),
            ),
        )

    def cas_update_job(self, job: Any, expected_revision: int) -> bool:
        state, score = self._serialized_job(job)
        return bool(
            int(
                self.redis.eval(
                    _CAS_UPDATE_JOB_SCRIPT,
                    len(self.keys),
                    *self.keys,
                    job.id,
                    str(expected_revision),
                    state,
                    score,
                )
            )
        )

    def cas_remove_job(self, job_id: str, expected_revision: int) -> bool:
        return bool(
            int(
                self.redis.eval(
                    _CAS_REMOVE_JOB_SCRIPT,
                    len(self.keys),
                    *self.keys,
                    job_id,
                    str(expected_revision),
                )
            )
        )

    def _write(self, script: str, job: Any, *, rearm: bool) -> Any:
        state, score = self._serialized_job(job)
        next_run_time = getattr(job, "next_run_time", None)
        candidate = (
            self.adapter.canonical_wakeup_time(next_run_time).isoformat()
            if next_run_time is not None
            else ""
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
        )

    def _serialized_job(self, job: Any) -> tuple[bytes, str]:
        state = pickle.dumps(job.__getstate__(), self.store.pickle_protocol)
        next_run_time = getattr(job, "next_run_time", None)
        score = str(datetime_to_utc_timestamp(next_run_time)) if next_run_time is not None else ""
        return state, score

    def _decode_versioned_jobs(self, raw: Any) -> list[tuple[Any, int]]:
        if not isinstance(raw, (list, tuple)) or len(raw) % 3:
            raise RuntimeError("Redis returned invalid APScheduler versioned jobs")
        jobs: list[tuple[Any, int]] = []
        for index in range(0, len(raw), 3):
            state = raw[index + 1]
            revision = int(_text(raw[index + 2]))
            jobs.append((self.store._reconstitute_job(state), revision))
        return jobs


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode()
    return str(value)
