"""Redis-backed lifecycle and single-driver coordination."""

from __future__ import annotations

from typing import Any, cast

import re
import threading
from contextlib import AbstractContextManager
from datetime import datetime, timedelta, timezone

from ..._time import as_utc
from ..._types import (
    WAKE_REPAIR_GRACE_SECONDS,
    ClaimResult,
    ClaimState,
    DriverSnapshot,
    FinishResult,
    FinishState,
    LifecycleState,
    StartDecision,
    WakeToken,
)

UTC = timezone.utc
DRIVER_LEASE_SECONDS = 15 * 60
DRIVER_RENEW_INTERVAL_SECONDS = 60
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
# Scopes are either a deployment id or "<project>:<environment>".
_SCOPE_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]+$")

__all__ = ["RedisDriver"]


def _idle_demotion_block(*, now_ts: str, updated_at: str, clear_owner: bool, result: str) -> str:
    """Lua that atomically demotes a running driver whose idle deadline lapsed."""
    owner_fields = (
        """,
    "active_owner",
    "active_kind",
    "active_generation",
    "active_sequence",
    "active_lease_until\""""
        if clear_owner
        else ""
    )
    return f"""local idle_expiry = tonumber(redis.call("HGET", KEYS[1], "idle_expires_at") or "")
if redis.call("HGET", KEYS[1], "state") == "running"
  and idle_expiry
  and idle_expiry <= tonumber({now_ts})
then
  redis.call(
    "HSET",
    KEYS[1],
    "state", "inactive",
    "current_sequence", "0",
    "updated_at", {updated_at}
  )
  redis.call(
    "HDEL",
    KEYS[1],
    "start_status",
    "activation_time",
    "current_logical_time",
    "current_status",
    "dirty_logical_time"{owner_fields}
  )
  return {result}
end"""


_ACTIVATE_SCRIPT = """
-- vercel-apscheduler-v1:activate
-- ARGV[1] now (ISO-8601), ARGV[2] "1" = manual start, ARGV[3] this
-- deployment, ARGV[4] "1" = may take the chain from another deployment,
-- ARGV[5] now (unix timestamp), ARGV[6] idle deadline (unix ts, "" = none).
-- A manual start also activates a paused driver and always may take over.
-- Taking over opens one new generation, fencing every message and handler
-- of the previous owner, exactly like resuming from a pause. An automatic
-- activation never overrides an explicit pause, not even during takeover.
local state = redis.call("HGET", KEYS[1], "state")
local owner = redis.call("HGET", KEYS[1], "owner_deployment")
local generation = tonumber(redis.call("HGET", KEYS[1], "generation") or "0")
local changed = 0
local foreign = owner and owner ~= ARGV[3]

local function report(owned)
  return {
    tostring(changed),
    tostring(generation),
    state or "",
    redis.call("HGET", KEYS[1], "start_status") or "",
    redis.call("HGET", KEYS[1], "current_sequence") or "",
    redis.call("HGET", KEYS[1], "current_logical_time") or "",
    redis.call("HGET", KEYS[1], "current_status") or "",
    owned
  }
end

if foreign and ARGV[4] ~= "1" then
  return report("")
end
if foreign and ARGV[2] ~= "1" and state == "paused" then
  return report("")
end

if ARGV[6] ~= "" then
  local prior_expiry = tonumber(redis.call("HGET", KEYS[1], "idle_expires_at") or "")
  if state == "running" and prior_expiry and prior_expiry <= tonumber(ARGV[5]) then
    state = "inactive"
    redis.call(
      "HDEL",
      KEYS[1],
      "start_status",
      "activation_time",
      "current_logical_time",
      "current_status",
      "dirty_logical_time"
    )
    redis.call("HSET", KEYS[1], "current_sequence", "0")
  end
  redis.call("HSET", KEYS[1], "idle_expires_at", ARGV[6])
else
  redis.call("HDEL", KEYS[1], "idle_expires_at")
end

if not state or state == "inactive" or foreign
  or (ARGV[2] == "1" and state ~= "running")
then
  generation = generation + 1
  changed = 1
  state = "running"
  redis.call(
    "HSET",
    KEYS[1],
    "state", state,
    "generation", tostring(generation),
    "start_status", "pending",
    "current_sequence", "0",
    "owner_deployment", ARGV[3]
  )
  redis.call(
    "HDEL",
    KEYS[1],
    "activation_time",
    "current_logical_time",
    "current_status",
    "dirty_logical_time"
  )
elseif not owner then
  redis.call("HSET", KEYS[1], "owner_deployment", ARGV[3])
end

redis.call("HSET", KEYS[1], "updated_at", ARGV[1])
return report("1")
"""

_PAUSE_SCRIPT = """
-- vercel-apscheduler-v1:pause
local changed = 0
if redis.call("HGET", KEYS[1], "state") == "running" then
  changed = 1
end
redis.call(
  "HSET",
  KEYS[1],
  "state", "paused",
  "updated_at", ARGV[1]
)
return changed
"""

_MARK_START_PUBLISHED_SCRIPT = """
-- vercel-apscheduler-v1:mark-start-published
if redis.call("HGET", KEYS[1], "state") ~= "running" then
  return 0
end
if tonumber(redis.call("HGET", KEYS[1], "generation") or "-1") ~= tonumber(ARGV[1]) then
  return 0
end
local status = redis.call("HGET", KEYS[1], "start_status")
if status == "pending" then
  redis.call(
    "HSET",
    KEYS[1],
    "start_status", "published",
    "updated_at", ARGV[2]
  )
end
return 1
"""

_CLAIM_START_SCRIPT = (
    """
-- vercel-apscheduler-v1:claim-start
"""
    + _idle_demotion_block(
        now_ts="ARGV[3]",
        updated_at="ARGV[5]",
        clear_owner=False,
        result='{"stale", ""}',
    )
    + """
if redis.call("HGET", KEYS[1], "state") ~= "running" then
  return {"stale", ""}
end
if redis.call("HGET", KEYS[1], "owner_deployment") ~= ARGV[6] then
  return {"stale", ""}
end
if tonumber(redis.call("HGET", KEYS[1], "generation") or "-1") ~= tonumber(ARGV[1]) then
  return {"stale", ""}
end
if redis.call("HGET", KEYS[1], "start_status") == "active" then
  return {"stale", ""}
end

local active_owner = redis.call("HGET", KEYS[1], "active_owner")
local lease_until = tonumber(redis.call("HGET", KEYS[1], "active_lease_until") or "0")
if active_owner and active_owner ~= ARGV[2] and lease_until > tonumber(ARGV[3]) then
  return {"busy", ""}
end

local activation_time = redis.call("HGET", KEYS[1], "activation_time")
if not activation_time then
  activation_time = ARGV[5]
end
redis.call(
  "HSET",
  KEYS[1],
  "start_status", "processing",
  "activation_time", activation_time,
  "active_owner", ARGV[2],
  "active_kind", "start",
  "active_generation", ARGV[1],
  "active_lease_until", ARGV[4],
  "updated_at", ARGV[5]
)
return {"claimed", activation_time}
"""
)

_FINISH_START_SCRIPT = (
    """
-- vercel-apscheduler-v1:finish-start
local owner = redis.call("HGET", KEYS[1], "active_owner")
if owner ~= ARGV[2] then
  if redis.call("HGET", KEYS[1], "state") == "running"
    and tonumber(redis.call("HGET", KEYS[1], "generation") or "-1") == tonumber(ARGV[1])
    and redis.call("HGET", KEYS[1], "start_status") == "processing"
  then
    return {"lost", "", ""}
  end
  return {"fenced", "", ""}
end

"""
    + _idle_demotion_block(
        now_ts="ARGV[5]",
        updated_at="ARGV[4]",
        clear_owner=True,
        result='{"fenced", "", ""}',
    )
    + """
if redis.call("HGET", KEYS[1], "state") ~= "running"
  or tonumber(redis.call("HGET", KEYS[1], "generation") or "-1") ~= tonumber(ARGV[1])
then
  redis.call(
    "HDEL",
    KEYS[1],
    "active_owner",
    "active_kind",
    "active_generation",
    "active_lease_until"
  )
  return {"fenced", "", ""}
end

local logical_time = ARGV[3]
local dirty_time = redis.call("HGET", KEYS[1], "dirty_logical_time")
if dirty_time and (logical_time == "" or dirty_time < logical_time) then
  logical_time = dirty_time
end
redis.call(
  "HSET",
  KEYS[1],
  "start_status", "active",
  "updated_at", ARGV[4]
)
if logical_time ~= "" then
  redis.call(
    "HSET",
    KEYS[1],
    "current_sequence", "1",
    "current_logical_time", logical_time,
    "current_status", "pending"
  )
else
  redis.call("HSET", KEYS[1], "current_sequence", "0")
  redis.call("HDEL", KEYS[1], "current_logical_time", "current_status")
end
redis.call(
  "HDEL",
  KEYS[1],
  "active_owner",
  "active_kind",
  "active_generation",
  "active_lease_until",
  "dirty_logical_time"
)
return {"advanced", logical_time ~= "" and "1" or "", logical_time}
"""
)

_MARK_WAKE_PUBLISHED_SCRIPT = """
-- vercel-apscheduler-v1:mark-wake-published
if redis.call("HGET", KEYS[1], "state") ~= "running" then
  return 0
end
if tonumber(redis.call("HGET", KEYS[1], "generation") or "-1") ~= tonumber(ARGV[1]) then
  return 0
end
if tonumber(redis.call("HGET", KEYS[1], "current_sequence") or "-1") ~= tonumber(ARGV[2]) then
  return 0
end
if redis.call("HGET", KEYS[1], "current_status") == "pending" then
  redis.call(
    "HSET",
    KEYS[1],
    "current_status", "published",
    "updated_at", ARGV[3]
  )
end
return 1
"""

_CLAIM_WAKE_SCRIPT = (
    """
-- vercel-apscheduler-v1:claim-wake
"""
    + _idle_demotion_block(
        now_ts="ARGV[5]",
        updated_at="ARGV[7]",
        clear_owner=False,
        result='"stale"',
    )
    + """
if redis.call("HGET", KEYS[1], "state") ~= "running" then
  return "stale"
end
if redis.call("HGET", KEYS[1], "owner_deployment") ~= ARGV[8] then
  return "stale"
end
if tonumber(redis.call("HGET", KEYS[1], "generation") or "-1") ~= tonumber(ARGV[1]) then
  return "stale"
end
if tonumber(redis.call("HGET", KEYS[1], "current_sequence") or "-1") ~= tonumber(ARGV[2]) then
  return "stale"
end
if redis.call("HGET", KEYS[1], "current_logical_time") ~= ARGV[3] then
  return "stale"
end

local active_owner = redis.call("HGET", KEYS[1], "active_owner")
local lease_until = tonumber(redis.call("HGET", KEYS[1], "active_lease_until") or "0")
if active_owner and active_owner ~= ARGV[4] and lease_until > tonumber(ARGV[5]) then
  return "busy"
end

redis.call(
  "HSET",
  KEYS[1],
  "current_status", "processing",
  "active_owner", ARGV[4],
  "active_kind", "wake",
  "active_generation", ARGV[1],
  "active_sequence", ARGV[2],
  "active_lease_until", ARGV[6],
  "updated_at", ARGV[7]
)
return "claimed"
"""
)

_FINISH_WAKE_SCRIPT = (
    """
-- vercel-apscheduler-v1:finish-wake
if redis.call("HGET", KEYS[1], "active_owner") ~= ARGV[3] then
  if redis.call("HGET", KEYS[1], "state") == "running"
    and tonumber(redis.call("HGET", KEYS[1], "generation") or "-1") == tonumber(ARGV[1])
    and tonumber(redis.call("HGET", KEYS[1], "current_sequence") or "-1") == tonumber(ARGV[2])
    and redis.call("HGET", KEYS[1], "current_logical_time") == ARGV[4]
  then
    return {"lost", "", ""}
  end
  return {"fenced", "", ""}
end

"""
    + _idle_demotion_block(
        now_ts="ARGV[7]",
        updated_at="ARGV[6]",
        clear_owner=True,
        result='{"fenced", "", ""}',
    )
    + """
if redis.call("HGET", KEYS[1], "state") ~= "running"
  or tonumber(redis.call("HGET", KEYS[1], "generation") or "-1") ~= tonumber(ARGV[1])
  or tonumber(redis.call("HGET", KEYS[1], "current_sequence") or "-1") ~= tonumber(ARGV[2])
  or redis.call("HGET", KEYS[1], "current_logical_time") ~= ARGV[4]
then
  redis.call(
    "HDEL",
    KEYS[1],
    "active_owner",
    "active_kind",
    "active_generation",
    "active_sequence",
    "active_lease_until"
  )
  return {"fenced", "", ""}
end

local logical_time = ARGV[5]
local dirty_time = redis.call("HGET", KEYS[1], "dirty_logical_time")
if dirty_time and (logical_time == "" or dirty_time < logical_time) then
  logical_time = dirty_time
end
local next_sequence = tonumber(ARGV[2])
if logical_time ~= "" then
  next_sequence = next_sequence + 1
end
redis.call(
  "HSET",
  KEYS[1],
  "current_sequence", tostring(next_sequence),
  "updated_at", ARGV[6]
)
if logical_time ~= "" then
  redis.call(
    "HSET",
    KEYS[1],
    "current_logical_time", logical_time,
    "current_status", "pending"
  )
else
  redis.call("HDEL", KEYS[1], "current_logical_time", "current_status")
end
redis.call(
  "HDEL",
  KEYS[1],
  "active_owner",
  "active_kind",
  "active_generation",
  "active_sequence",
  "active_lease_until",
  "dirty_logical_time"
)
return {
  "advanced",
  logical_time ~= "" and tostring(next_sequence) or "",
  logical_time
}
"""
)

_REPAIR_OVERDUE_WAKE_SCRIPT = """
-- vercel-apscheduler-v1:repair-overdue-wake
if redis.call("HGET", KEYS[1], "state") ~= "running" then
  return 0
end
if redis.call("HGET", KEYS[1], "owner_deployment") ~= ARGV[4] then
  return 0
end
local status = redis.call("HGET", KEYS[1], "current_status")
if status ~= "published" and status ~= "processing" then
  return 0
end
local logical_time = redis.call("HGET", KEYS[1], "current_logical_time")
if not logical_time or logical_time >= ARGV[1] then
  return 0
end
local lease_until = tonumber(redis.call("HGET", KEYS[1], "active_lease_until") or "0")
if redis.call("HGET", KEYS[1], "active_owner") and lease_until > tonumber(ARGV[2]) then
  return 0
end
redis.call(
  "HSET",
  KEYS[1],
  "current_status", "pending",
  "updated_at", ARGV[3]
)
return 1
"""

_RENEW_SCRIPT = """
-- vercel-apscheduler-v1:renew
if redis.call("HGET", KEYS[1], "active_owner") ~= ARGV[1] then
  return 0
end
redis.call(
  "HSET",
  KEYS[1],
  "active_lease_until", ARGV[2],
  "updated_at", ARGV[3]
)
return 1
"""

_RELEASE_SCRIPT = """
-- vercel-apscheduler-v1:release
if redis.call("HGET", KEYS[1], "active_owner") ~= ARGV[1] then
  return 0
end
redis.call(
  "HDEL",
  KEYS[1],
  "active_owner",
  "active_kind",
  "active_generation",
  "active_sequence",
  "active_lease_until"
)
return 1
"""

_MARK_RECONCILED_SCRIPT = """
-- vercel-apscheduler-v1:mark-reconciled
if redis.call("HGET", KEYS[1], "owner_deployment") ~= ARGV[1] then
  return 0
end
redis.call(
  "HSET",
  KEYS[1],
  "reconciled_deployment", ARGV[1],
  "updated_at", ARGV[2]
)
return 1
"""


class RedisDriver:
    """Atomic driver state stored beside an APScheduler Redis job store."""

    def __init__(
        self,
        client: Any,
        *,
        scope: str,
        scheduler_id: str,
        deployment: str,
    ) -> None:
        if not _SCOPE_PATTERN.fullmatch(scope):
            raise ValueError(
                "state scope must contain only letters, digits, dots, colons, "
                "underscores, and hyphens"
            )
        if not _IDENTIFIER_PATTERN.fullmatch(scheduler_id):
            raise ValueError(
                "scheduler identity must contain only letters, digits, underscores, and hyphens"
            )
        if not _IDENTIFIER_PATTERN.fullmatch(deployment):
            raise ValueError(
                "deployment id must contain only letters, digits, underscores, and hyphens"
            )
        self.client = client
        self.scope = scope
        self.scheduler_id = scheduler_id
        self.deployment = deployment
        self.key = f"vercel:apscheduler:{{{scope}:{scheduler_id}}}:driver"

    def start(
        self,
        now: datetime,
        *,
        idle_timeout_seconds: int | None = None,
    ) -> StartDecision:
        """Atomically start, resume, or take over one durable generation.

        A manual start also renews the preview idle deadline; otherwise a
        deadline that lapsed before the start could make the new generation
        immediately stale.
        """
        return self._activate(
            now,
            idle_timeout_seconds=idle_timeout_seconds,
            manual=True,
            takeover_allowed=True,
        )

    def auto_activate(
        self,
        now: datetime,
        *,
        idle_timeout_seconds: int | None = None,
        takeover_allowed: bool = False,
    ) -> StartDecision:
        """Renew activity and start unless the scheduler was explicitly paused."""
        return self._activate(
            now,
            idle_timeout_seconds=idle_timeout_seconds,
            manual=False,
            takeover_allowed=takeover_allowed,
        )

    def _activate(
        self,
        now: datetime,
        *,
        idle_timeout_seconds: int | None,
        manual: bool,
        takeover_allowed: bool,
    ) -> StartDecision:
        if idle_timeout_seconds is not None and idle_timeout_seconds <= 0:
            raise ValueError("idle_timeout_seconds must be a positive integer")
        now_utc = as_utc(now, name="now")
        expires_at = (
            now_utc.timestamp() + idle_timeout_seconds if idle_timeout_seconds is not None else None
        )
        result = self._eval(
            _ACTIVATE_SCRIPT,
            now_utc.isoformat(),
            "1" if manual else "",
            self.deployment,
            "1" if takeover_allowed else "",
            str(now_utc.timestamp()),
            str(expires_at) if expires_at is not None else "",
        )
        if not isinstance(result, (list, tuple)) or len(result) != 8:
            raise RuntimeError("Redis returned an invalid APScheduler activation result")
        generation = int(_text(result[1]))
        state_raw = _text(result[2])
        if state_raw not in {"running", "paused", "inactive"}:
            raise RuntimeError("Redis returned an unknown APScheduler lifecycle state")
        return StartDecision(
            generation=generation,
            changed=bool(int(_text(result[0]))),
            state=cast("LifecycleState", state_raw),
            start_status=_text(result[3]),
            current_wake=_wake_from_values(
                generation,
                result[4],
                result[5],
                result[6],
            ),
            owned=bool(_text(result[7])),
        )

    def pause(self, now: datetime) -> bool:
        """Fence the running generation without canceling its active owner."""
        result = self._eval(
            _PAUSE_SCRIPT,
            as_utc(now, name="now").isoformat(),
        )
        return bool(int(result))

    def mark_start_published(self, generation: int, now: datetime) -> None:
        self._eval(
            _MARK_START_PUBLISHED_SCRIPT,
            str(generation),
            as_utc(now, name="now").isoformat(),
        )

    def claim_start(
        self,
        generation: int,
        owner: str,
        now: datetime,
    ) -> ClaimResult:
        now_utc = as_utc(now, name="now")
        lease_until = now_utc + timedelta(seconds=DRIVER_LEASE_SECONDS)
        result = self._eval(
            _CLAIM_START_SCRIPT,
            str(generation),
            owner,
            str(now_utc.timestamp()),
            str(lease_until.timestamp()),
            now_utc.isoformat(),
            self.deployment,
        )
        if not isinstance(result, (list, tuple)) or len(result) != 2:
            raise RuntimeError("Redis returned an invalid APScheduler start claim")
        state = _text(result[0])
        if state not in {"claimed", "busy", "stale"}:
            raise RuntimeError("Redis returned an unknown APScheduler start claim")
        claim_state = cast("ClaimState", state)
        activation_raw = _text(result[1])
        return ClaimResult(
            state=claim_state,
            activation_time=(
                datetime.fromisoformat(activation_raw)
                if state == "claimed" and activation_raw
                else None
            ),
        )

    def finish_start(
        self,
        generation: int,
        owner: str,
        next_logical_time: datetime | None,
        now: datetime,
    ) -> FinishResult:
        logical_time = (
            as_utc(next_logical_time, name="next_logical_time")
            if next_logical_time is not None
            else None
        )
        now_utc = as_utc(now, name="now")
        result = self._eval(
            _FINISH_START_SCRIPT,
            str(generation),
            owner,
            logical_time.isoformat() if logical_time is not None else "",
            now_utc.isoformat(),
            str(now_utc.timestamp()),
        )
        return _finish_result(
            result,
            generation=generation,
        )

    def mark_wake_published(
        self,
        generation: int,
        sequence: int,
        now: datetime,
    ) -> None:
        self._eval(
            _MARK_WAKE_PUBLISHED_SCRIPT,
            str(generation),
            str(sequence),
            as_utc(now, name="now").isoformat(),
        )

    def claim_wake(
        self,
        token: WakeToken,
        owner: str,
        now: datetime,
    ) -> ClaimResult:
        now_utc = as_utc(now, name="now")
        lease_until = now_utc + timedelta(seconds=DRIVER_LEASE_SECONDS)
        result = _text(
            self._eval(
                _CLAIM_WAKE_SCRIPT,
                str(token.generation),
                str(token.sequence),
                token.logical_time.isoformat(),
                owner,
                str(now_utc.timestamp()),
                str(lease_until.timestamp()),
                now_utc.isoformat(),
                self.deployment,
            )
        )
        if result not in {"claimed", "busy", "stale"}:
            raise RuntimeError("Redis returned an unknown APScheduler wake claim")
        return ClaimResult(state=cast("ClaimState", result))

    def finish_wake(
        self,
        token: WakeToken,
        owner: str,
        next_logical_time: datetime | None,
        now: datetime,
    ) -> FinishResult:
        logical_time = (
            as_utc(next_logical_time, name="next_logical_time")
            if next_logical_time is not None
            else None
        )
        now_utc = as_utc(now, name="now")
        result = self._eval(
            _FINISH_WAKE_SCRIPT,
            str(token.generation),
            str(token.sequence),
            owner,
            token.logical_time.isoformat(),
            logical_time.isoformat() if logical_time is not None else "",
            now_utc.isoformat(),
            str(now_utc.timestamp()),
        )
        return _finish_result(
            result,
            generation=token.generation,
        )

    def snapshot(self) -> DriverSnapshot:
        values = self.client.hmget(
            self.key,
            "state",
            "generation",
            "start_status",
            "current_sequence",
            "current_logical_time",
            "current_status",
        )
        if not isinstance(values, (list, tuple)) or len(values) != 6:
            raise RuntimeError("Redis returned an invalid APScheduler driver state")
        state_raw = _text(values[0])
        state: LifecycleState
        if state_raw in {"running", "paused", "inactive"}:
            state = cast("LifecycleState", state_raw)
        else:
            state = "paused"
        generation_raw = _text(values[1])
        generation = int(generation_raw) if generation_raw else 0
        return DriverSnapshot(
            state=state,
            generation=generation,
            start_status=_optional_text(values[2]),
            current_wake=_wake_from_values(
                generation,
                values[3],
                values[4],
                values[5],
            ),
        )

    def repair_overdue_wake(
        self,
        now: datetime,
        *,
        grace_seconds: int = WAKE_REPAIR_GRACE_SECONDS,
    ) -> bool:
        """Demote a wake whose queue message is presumed lost.

        ``published`` asserts that a message exists in a queue, and
        ``processing`` that an owner is alive; a rollback strands the message
        in a queue with no forward alias, an alias or retention expiry drops
        it outright, and a crash orphans the owner. Nothing else ever
        re-checks those assertions, so the chain would sleep forever. A wake
        well past its logical time with no live owner lease is presumed dead
        and demoted to ``pending``, which the standard pending-wake repair
        then republishes. Racing a message that turns out to be alive is
        safe: the queue deduplicates the idempotency key within a queue, and
        the claim fences duplicates across queues.
        """
        now_utc = as_utc(now, name="now")
        overdue_before = now_utc - timedelta(seconds=grace_seconds)
        result = self._eval(
            _REPAIR_OVERDUE_WAKE_SCRIPT,
            overdue_before.isoformat(),
            str(now_utc.timestamp()),
            now_utc.isoformat(),
            self.deployment,
        )
        return bool(int(result))

    def owner_deployment(self) -> str | None:
        """Return the deployment currently driving this chain."""
        return _optional_text(self.client.hget(self.key, "owner_deployment"))

    def reconciled_deployment(self) -> str | None:
        """Return the deployment that last synced declarations here."""
        return _optional_text(self.client.hget(self.key, "reconciled_deployment"))

    def mark_reconciled(self, deployment: str, now: datetime) -> bool:
        """Record a completed declaration sync, only while owning the chain.

        A deployment that lost the namespace mid-reconciliation must not
        stamp it as reconciled: the marker would suppress the real owner's
        own sync. The loser leaves the marker unset and the owner
        reconciles on its next activation.
        """
        result = self._eval(
            _MARK_RECONCILED_SCRIPT,
            deployment,
            as_utc(now, name="now").isoformat(),
        )
        return bool(int(result))

    def renew(self, owner: str, now: datetime) -> bool:
        now_utc = as_utc(now, name="now")
        result = self._eval(
            _RENEW_SCRIPT,
            owner,
            str((now_utc + timedelta(seconds=DRIVER_LEASE_SECONDS)).timestamp()),
            now_utc.isoformat(),
        )
        return bool(int(result))

    def release(self, owner: str) -> None:
        self._eval(_RELEASE_SCRIPT, owner)

    def renewing(self, owner: str) -> AbstractContextManager[None]:
        return _LeaseRenewal(self, owner)

    def _eval(self, script: str, *arguments: str) -> Any:
        return self.client.eval(script, 1, self.key, *arguments)


class _LeaseRenewal(AbstractContextManager[None]):
    def __init__(self, driver: RedisDriver, owner: str) -> None:
        self.driver = driver
        self.owner = owner
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> None:
        self._thread = threading.Thread(
            target=self._run,
            name="vercel-apscheduler-driver-lease",
            daemon=True,
        )
        self._thread.start()

    def __exit__(self, *exc_info: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)

    def _run(self) -> None:
        while not self._stop.wait(DRIVER_RENEW_INTERVAL_SECONDS):
            try:
                if not self.driver.renew(self.owner, datetime.now(UTC)):
                    return
            except Exception:  # noqa: BLE001
                # The original 15-minute lease remains the safety boundary.
                # The handler will fail closed when it next touches Redis.
                return


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _optional_text(value: Any) -> str | None:
    resolved = _text(value)
    return resolved or None


def _finish_result(result: Any, *, generation: int) -> FinishResult:
    if not isinstance(result, (list, tuple)) or len(result) != 3:
        raise RuntimeError("Redis returned an invalid APScheduler finish result")
    state = _text(result[0])
    if state not in {"advanced", "fenced", "lost"}:
        raise RuntimeError("Redis returned an unknown APScheduler finish result")
    sequence_raw = _text(result[1])
    logical_time_raw = _text(result[2])
    wake = (
        WakeToken(
            generation=generation,
            sequence=int(sequence_raw),
            logical_time=datetime.fromisoformat(logical_time_raw),
        )
        if sequence_raw and logical_time_raw
        else None
    )
    return FinishResult(
        state=cast("FinishState", state),
        wake=wake,
    )


def _wake_from_values(
    generation: int,
    sequence_raw: Any,
    logical_time_raw: Any,
    status_raw: Any,
) -> WakeToken | None:
    sequence_text = _text(sequence_raw)
    logical_time_text = _text(logical_time_raw)
    if not sequence_text or not logical_time_text:
        return None
    return WakeToken(
        generation=generation,
        sequence=int(sequence_text),
        logical_time=datetime.fromisoformat(logical_time_text),
        status=_text(status_raw) or "pending",
    )
