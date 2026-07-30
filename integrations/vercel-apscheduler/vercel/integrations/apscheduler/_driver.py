"""Redis-backed lifecycle and single-driver coordination."""

from __future__ import annotations

from typing import Any, Literal, cast

import re
import threading
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from ._time import as_utc

UTC = timezone.utc
DRIVER_LEASE_SECONDS = 15 * 60
DRIVER_RENEW_INTERVAL_SECONDS = 60
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")

ClaimState = Literal["claimed", "busy", "stale"]
FinishState = Literal["advanced", "fenced", "lost"]
LifecycleState = Literal["running", "paused", "inactive"]

__all__ = [
    "APSchedulerConfigurationError",
    "ClaimResult",
    "DriverSnapshot",
    "FinishResult",
    "RedisDriver",
    "StartDecision",
    "WakeToken",
]


class APSchedulerConfigurationError(RuntimeError):
    """Raised when a scheduler cannot satisfy the Vercel runtime contract."""


@dataclass(frozen=True, slots=True)
class WakeToken:
    """The one wake message currently allowed to advance a scheduler."""

    generation: int
    sequence: int
    logical_time: datetime
    status: str = "pending"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "logical_time",
            as_utc(self.logical_time, name="logical_time"),
        )


@dataclass(frozen=True, slots=True)
class StartDecision:
    """Atomic result of starting or resuming a scheduler."""

    generation: int
    changed: bool
    start_status: str
    current_wake: WakeToken | None
    state: LifecycleState = "running"


@dataclass(frozen=True, slots=True)
class ClaimResult:
    """Result of trying to own a start or wake delivery."""

    state: ClaimState
    activation_time: datetime | None = None

    def __post_init__(self) -> None:
        if self.activation_time is not None:
            object.__setattr__(
                self,
                "activation_time",
                as_utc(self.activation_time, name="activation_time"),
            )


@dataclass(frozen=True, slots=True)
class FinishResult:
    """Atomic outcome of completing a claimed start or wake."""

    state: FinishState
    wake: WakeToken | None = None


@dataclass(frozen=True, slots=True)
class DriverSnapshot:
    """Current durable driver state."""

    state: LifecycleState
    generation: int
    start_status: str | None
    current_wake: WakeToken | None


_START_SCRIPT = """
-- vercel-apscheduler-v1:start
local state = redis.call("HGET", KEYS[1], "state")
local generation = tonumber(redis.call("HGET", KEYS[1], "generation") or "0")
local changed = 0

if state ~= "running" then
  generation = generation + 1
  changed = 1
  redis.call(
    "HSET",
    KEYS[1],
    "state", "running",
    "generation", tostring(generation),
    "start_status", "pending",
    "updated_at", ARGV[1]
  )
  redis.call(
    "HDEL",
    KEYS[1],
    "activation_time",
    "current_logical_time",
    "current_status",
    "dirty_logical_time"
  )
  redis.call("HSET", KEYS[1], "current_sequence", "0")
end

return {
  tostring(changed),
  tostring(generation),
  redis.call("HGET", KEYS[1], "start_status") or "",
  redis.call("HGET", KEYS[1], "current_sequence") or "",
  redis.call("HGET", KEYS[1], "current_logical_time") or "",
  redis.call("HGET", KEYS[1], "current_status") or ""
}
"""

_AUTO_ACTIVATE_SCRIPT = """
-- vercel-apscheduler-v1:auto-activate
local state = redis.call("HGET", KEYS[1], "state")
local generation = tonumber(redis.call("HGET", KEYS[1], "generation") or "0")
local changed = 0

if ARGV[3] ~= "" then
  local prior_expiry = tonumber(redis.call("HGET", KEYS[1], "idle_expires_at") or "")
  if state == "running" and prior_expiry and prior_expiry <= tonumber(ARGV[1]) then
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
  redis.call("HSET", KEYS[1], "idle_expires_at", ARGV[3])
else
  redis.call("HDEL", KEYS[1], "idle_expires_at")
end

if not state or state == "inactive" then
  generation = generation + 1
  changed = 1
  state = "running"
  redis.call(
    "HSET",
    KEYS[1],
    "state", state,
    "generation", tostring(generation),
    "start_status", "pending",
    "current_sequence", "0"
  )
  redis.call(
    "HDEL",
    KEYS[1],
    "activation_time",
    "current_logical_time",
    "current_status",
    "dirty_logical_time"
  )
end

redis.call("HSET", KEYS[1], "updated_at", ARGV[2])
return {
  tostring(changed),
  tostring(generation),
  state or "",
  redis.call("HGET", KEYS[1], "start_status") or "",
  redis.call("HGET", KEYS[1], "current_sequence") or "",
  redis.call("HGET", KEYS[1], "current_logical_time") or "",
  redis.call("HGET", KEYS[1], "current_status") or ""
}
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

_CLAIM_START_SCRIPT = """
-- vercel-apscheduler-v1:claim-start
local idle_expiry = tonumber(redis.call("HGET", KEYS[1], "idle_expires_at") or "")
if redis.call("HGET", KEYS[1], "state") == "running"
  and idle_expiry
  and idle_expiry <= tonumber(ARGV[3])
then
  redis.call(
    "HSET",
    KEYS[1],
    "state", "inactive",
    "current_sequence", "0",
    "updated_at", ARGV[5]
  )
  redis.call(
    "HDEL",
    KEYS[1],
    "start_status",
    "activation_time",
    "current_logical_time",
    "current_status",
    "dirty_logical_time"
  )
  return {"stale", ""}
end
if redis.call("HGET", KEYS[1], "state") ~= "running" then
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

_FINISH_START_SCRIPT = """
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

local idle_expiry = tonumber(redis.call("HGET", KEYS[1], "idle_expires_at") or "")
if redis.call("HGET", KEYS[1], "state") == "running"
  and idle_expiry
  and idle_expiry <= tonumber(ARGV[5])
then
  redis.call(
    "HSET",
    KEYS[1],
    "state", "inactive",
    "current_sequence", "0",
    "updated_at", ARGV[4]
  )
  redis.call(
    "HDEL",
    KEYS[1],
    "start_status",
    "activation_time",
    "current_logical_time",
    "current_status",
    "dirty_logical_time",
    "active_owner",
    "active_kind",
    "active_generation",
    "active_sequence",
    "active_lease_until"
  )
  return {"fenced", "", ""}
end

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

_CLAIM_WAKE_SCRIPT = """
-- vercel-apscheduler-v1:claim-wake
local idle_expiry = tonumber(redis.call("HGET", KEYS[1], "idle_expires_at") or "")
if redis.call("HGET", KEYS[1], "state") == "running"
  and idle_expiry
  and idle_expiry <= tonumber(ARGV[5])
then
  redis.call(
    "HSET",
    KEYS[1],
    "state", "inactive",
    "current_sequence", "0",
    "updated_at", ARGV[7]
  )
  redis.call(
    "HDEL",
    KEYS[1],
    "start_status",
    "activation_time",
    "current_logical_time",
    "current_status",
    "dirty_logical_time"
  )
  return "stale"
end
if redis.call("HGET", KEYS[1], "state") ~= "running" then
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

_FINISH_WAKE_SCRIPT = """
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

local idle_expiry = tonumber(redis.call("HGET", KEYS[1], "idle_expires_at") or "")
if redis.call("HGET", KEYS[1], "state") == "running"
  and idle_expiry
  and idle_expiry <= tonumber(ARGV[7])
then
  redis.call(
    "HSET",
    KEYS[1],
    "state", "inactive",
    "current_sequence", "0",
    "updated_at", ARGV[6]
  )
  redis.call(
    "HDEL",
    KEYS[1],
    "start_status",
    "activation_time",
    "current_logical_time",
    "current_status",
    "dirty_logical_time",
    "active_owner",
    "active_kind",
    "active_generation",
    "active_sequence",
    "active_lease_until"
  )
  return {"fenced", "", ""}
end

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


class RedisDriver:
    """Atomic driver state stored beside an APScheduler Redis job store."""

    def __init__(
        self,
        client: Any,
        *,
        deployment: str,
        scheduler_id: str,
    ) -> None:
        if not _IDENTIFIER_PATTERN.fullmatch(deployment):
            raise ValueError(
                "VERCEL_DEPLOYMENT_ID must contain only letters, digits, underscores, and hyphens"
            )
        if not _IDENTIFIER_PATTERN.fullmatch(scheduler_id):
            raise ValueError(
                "scheduler identity must contain only letters, digits, underscores, and hyphens"
            )
        self.client = client
        self.deployment = deployment
        self.scheduler_id = scheduler_id
        self.key = f"vercel:apscheduler:{{{deployment}:{scheduler_id}}}:driver"

    def start(self, now: datetime) -> StartDecision:
        """Atomically start or resume one durable generation."""
        now_utc = as_utc(now, name="now")
        result = self._eval(_START_SCRIPT, now_utc.isoformat())
        if not isinstance(result, (list, tuple)) or len(result) != 6:
            raise RuntimeError("Redis returned an invalid APScheduler start result")
        generation = int(_text(result[1]))
        return StartDecision(
            generation=generation,
            changed=bool(int(_text(result[0]))),
            start_status=_text(result[2]),
            current_wake=_wake_from_values(
                generation,
                result[3],
                result[4],
                result[5],
            ),
        )

    def auto_activate(
        self,
        now: datetime,
        *,
        idle_timeout_seconds: int | None = None,
    ) -> StartDecision:
        """Renew activity and start unless the scheduler was explicitly paused."""
        if idle_timeout_seconds is not None and idle_timeout_seconds <= 0:
            raise ValueError("idle_timeout_seconds must be a positive integer")
        now_utc = as_utc(now, name="now")
        expires_at = (
            now_utc.timestamp() + idle_timeout_seconds if idle_timeout_seconds is not None else None
        )
        result = self._eval(
            _AUTO_ACTIVATE_SCRIPT,
            str(now_utc.timestamp()),
            now_utc.isoformat(),
            str(expires_at) if expires_at is not None else "",
        )
        if not isinstance(result, (list, tuple)) or len(result) != 7:
            raise RuntimeError("Redis returned an invalid APScheduler auto-activation result")
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
