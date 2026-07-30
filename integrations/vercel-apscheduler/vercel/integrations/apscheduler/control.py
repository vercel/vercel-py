"""Durable start and stop controls for Vercel APScheduler subscribers."""

from __future__ import annotations

from typing import Any, Literal, Protocol

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import import_module
from os import environ

import vercel.queue as vqs
import vercel.queue.sync as vqs_sync

from ._payload import StartPayload
from ._time import as_utc

UTC = timezone.utc
CONTROL_SUBSCRIBERS_ENV = "VERCEL_APSCHEDULER_SUBSCRIBERS"
CONTROL_ENTRYPOINT_ENV = "VERCEL_APSCHEDULER_CONTROL_ENTRYPOINT"
CURRENT_DEPLOYMENT_ENV = "VERCEL_DEPLOYMENT_ID"
DEFAULT_REDIS_URL_ENV = "REDIS_URL"
DEFAULT_REDIS_KEY_PREFIX = "vercel:apscheduler"
START_MESSAGE_RETENTION_SECONDS = 7 * 24 * 60 * 60
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")

ControlStateValue = Literal["running", "stopped"]

__all__ = [
    "Control",
    "ControlBackendConfigurationError",
    "ControlConfigurationError",
    "ControlResult",
    "ControlStatus",
    "RedisControlBackend",
]


class ControlConfigurationError(RuntimeError):
    """Raised when APScheduler control is not configured for this deployment."""


class ControlBackendConfigurationError(ControlConfigurationError):
    """Raised when a configured control backend cannot be constructed."""


@dataclass(frozen=True, slots=True)
class ControlResult:
    """Result of a control state transition."""

    deployment: str
    state: ControlStateValue
    changed: bool


@dataclass(frozen=True, slots=True)
class ControlStatus:
    """Current durable scheduler state for a deployment."""

    deployment: str
    state: ControlStateValue


@dataclass(frozen=True, slots=True)
class _StartDecision:
    epoch: int
    reference_time: datetime
    changed: bool
    pending_subscribers: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "reference_time",
            as_utc(self.reference_time, name="reference_time"),
        )


class _ControlBackend(Protocol):
    """Atomic durable state required by :class:`Control`."""

    def begin_start(
        self,
        deployment: str,
        subscribers: tuple[str, ...],
        reference_time: datetime,
    ) -> _StartDecision:
        """Atomically begin or resume a deployment generation."""
        ...

    def mark_seed_published(self, deployment: str, epoch: int, subscriber: str) -> None:
        """Record that a subscriber's start message was accepted."""
        ...

    def can_seed(self, deployment: str, epoch: int, subscriber: str) -> bool:
        """Return whether a start message may seed this subscriber."""
        ...

    def mark_seed_active(self, deployment: str, epoch: int, subscriber: str) -> None:
        """Record that a subscriber has activated its current generation."""
        ...

    def is_running(self, deployment: str, epoch: int) -> bool:
        """Return whether an epoch is the deployment's running generation."""
        ...

    def stop(self, deployment: str, updated_at: datetime) -> bool:
        """Atomically stop a deployment, returning whether it was running."""
        ...

    def status(self, deployment: str) -> ControlStateValue:
        """Read a deployment's current scheduler state."""
        ...


class Control:
    """Start and stop every Vercel APScheduler subscriber in a deployment."""

    def __init__(
        self,
        *,
        backend: _ControlBackend,
    ) -> None:
        """Configure the durable state backend."""
        self.backend = backend

    def start(self, *, deployment: str | None = None) -> ControlResult:
        """Start all scheduler subscribers for a deployment exactly once."""
        resolved_deployment = _resolve_deployment(deployment)
        subscribers = self._subscribers()
        decision = self.backend.begin_start(
            resolved_deployment,
            subscribers,
            datetime.now(UTC),
        )
        for subscriber in decision.pending_subscribers:
            payload = StartPayload(
                epoch=decision.epoch,
                reference_time=decision.reference_time,
            ).to_payload()
            try:
                vqs_sync.send(
                    _start_topic(subscriber),
                    payload,
                    deployment=resolved_deployment,
                    idempotency_key=_start_idempotency_key(
                        resolved_deployment,
                        decision.epoch,
                        subscriber,
                    ),
                    retention=START_MESSAGE_RETENTION_SECONDS,
                )
            except vqs.DuplicateIdempotencyKeyError:
                # Another concurrent caller already published this exact seed.
                pass
            self.backend.mark_seed_published(
                resolved_deployment,
                decision.epoch,
                subscriber,
            )
        return ControlResult(
            deployment=resolved_deployment,
            state="running",
            changed=decision.changed,
        )

    def stop(self, *, deployment: str | None = None) -> ControlResult:
        """Stop the current scheduler generation for a deployment."""
        resolved_deployment = _resolve_deployment(deployment)
        changed = self.backend.stop(resolved_deployment, datetime.now(UTC))
        return ControlResult(
            deployment=resolved_deployment,
            state="stopped",
            changed=changed,
        )

    def status(self, *, deployment: str | None = None) -> ControlStatus:
        """Return the durable scheduler state for a deployment."""
        resolved_deployment = _resolve_deployment(deployment)
        return ControlStatus(
            deployment=resolved_deployment,
            state=self.backend.status(resolved_deployment),
        )

    def _subscribers(self) -> tuple[str, ...]:
        raw = environ.get(CONTROL_SUBSCRIBERS_ENV)
        if raw is None:
            raise ControlConfigurationError(
                f"{CONTROL_SUBSCRIBERS_ENV} is not set. Deploy this Control through "
                "[tool.vercel.apscheduler.control] so the Python builder can inject "
                "the APScheduler subscriber registry."
            )
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ControlConfigurationError(
                f"{CONTROL_SUBSCRIBERS_ENV} must contain a JSON array"
            ) from exc
        if not isinstance(parsed, list) or not all(isinstance(value, str) for value in parsed):
            raise ControlConfigurationError(
                f"{CONTROL_SUBSCRIBERS_ENV} must contain a JSON array of subscriber IDs"
            )
        return _validate_subscribers(tuple(parsed))

    def _can_seed(self, deployment: str, epoch: int, subscriber: str) -> bool:
        return self.backend.can_seed(deployment, epoch, subscriber)

    def _mark_seed_active(self, deployment: str, epoch: int, subscriber: str) -> None:
        self.backend.mark_seed_active(deployment, epoch, subscriber)

    def _is_running(self, deployment: str, epoch: int) -> bool:
        return self.backend.is_running(deployment, epoch)


@dataclass(slots=True)
class _ControlState:
    configured: Control | None = None


_CONTROL_STATE = _ControlState()


def _configure_control(control: Control) -> None:
    if not isinstance(control, Control):
        raise TypeError("APScheduler control entrypoint must resolve to a Control object")
    if _CONTROL_STATE.configured is not None and _CONTROL_STATE.configured is not control:
        raise ControlConfigurationError(
            "A different APScheduler Control object is already configured in this process"
        )
    _CONTROL_STATE.configured = control


def _get_configured_control() -> Control | None:
    return _CONTROL_STATE.configured


def _load_control_from_env() -> Control | None:
    entrypoint = environ.get(CONTROL_ENTRYPOINT_ENV)
    if not entrypoint:
        return _CONTROL_STATE.configured
    module_name, separator, variable_name = entrypoint.partition(":")
    if not separator or not module_name or not variable_name:
        raise ControlConfigurationError(f"{CONTROL_ENTRYPOINT_ENV} must use the form module:object")
    module = import_module(module_name)
    try:
        control = getattr(module, variable_name)
    except AttributeError as exc:
        raise ControlConfigurationError(
            f'APScheduler control entrypoint "{entrypoint}" does not exist'
        ) from exc
    _configure_control(control)
    return control


_BEGIN_START_SCRIPT = """
-- vercel-apscheduler:begin-start
local control = KEYS[1]
local state = redis.call("HGET", control, "state")
local epoch = tonumber(redis.call("HGET", control, "epoch") or "0")
local reference_time = redis.call("HGET", control, "reference_time")
local changed = 0

if state ~= "running" then
  epoch = epoch + 1
  reference_time = ARGV[1]
  changed = 1
  redis.call(
    "HSET",
    control,
    "state", "running",
    "epoch", tostring(epoch),
    "reference_time", reference_time,
    "updated_at", ARGV[1]
  )
end

for index = 2, #KEYS do
  local seed = KEYS[index]
  local seed_epoch = tonumber(redis.call("HGET", seed, "epoch") or "-1")
  if seed_epoch ~= epoch then
    redis.call(
      "HSET",
      seed,
      "epoch", tostring(epoch),
      "status", "pending",
      "reference_time", reference_time,
      "updated_at", ARGV[1]
    )
  end
end

return {tostring(changed), tostring(epoch), reference_time}
"""

_MARK_SEED_PUBLISHED_SCRIPT = """
-- vercel-apscheduler:mark-seed-published
local control = KEYS[1]
local seed = KEYS[2]
if redis.call("HGET", control, "state") ~= "running" then
  return 0
end
if tonumber(redis.call("HGET", control, "epoch") or "-1") ~= tonumber(ARGV[1]) then
  return 0
end
if tonumber(redis.call("HGET", seed, "epoch") or "-1") ~= tonumber(ARGV[1]) then
  return 0
end
if redis.call("HGET", seed, "status") == "pending" then
  redis.call("HSET", seed, "status", "published", "updated_at", ARGV[2])
end
return 1
"""

_CAN_SEED_SCRIPT = """
-- vercel-apscheduler:can-seed
if redis.call("HGET", KEYS[1], "state") ~= "running" then
  return 0
end
if tonumber(redis.call("HGET", KEYS[1], "epoch") or "-1") ~= tonumber(ARGV[1]) then
  return 0
end
if tonumber(redis.call("HGET", KEYS[2], "epoch") or "-1") ~= tonumber(ARGV[1]) then
  return 0
end
if redis.call("HGET", KEYS[2], "status") == "active" then
  return 0
end
return 1
"""

_MARK_SEED_ACTIVE_SCRIPT = """
-- vercel-apscheduler:mark-seed-active
if redis.call("HGET", KEYS[1], "state") ~= "running" then
  return 0
end
if tonumber(redis.call("HGET", KEYS[1], "epoch") or "-1") ~= tonumber(ARGV[1]) then
  return 0
end
if tonumber(redis.call("HGET", KEYS[2], "epoch") or "-1") ~= tonumber(ARGV[1]) then
  return 0
end
redis.call("HSET", KEYS[2], "status", "active", "updated_at", ARGV[2])
return 1
"""

_IS_RUNNING_SCRIPT = """
-- vercel-apscheduler:is-running
if redis.call("HGET", KEYS[1], "state") ~= "running" then
  return 0
end
if tonumber(redis.call("HGET", KEYS[1], "epoch") or "-1") ~= tonumber(ARGV[1]) then
  return 0
end
return 1
"""

_STOP_SCRIPT = """
-- vercel-apscheduler:stop
local previous = redis.call("HGET", KEYS[1], "state")
local changed = 0
if previous == "running" then
  changed = 1
end
redis.call("HSET", KEYS[1], "state", "stopped", "updated_at", ARGV[1])
return changed
"""


class RedisControlBackend:
    """Redis-backed atomic scheduler lifecycle state."""

    def __init__(
        self,
        host: str | None = None,
        *,
        port: int = 6379,
        db: int = 0,
        username: str | None = None,
        password: str | None = None,
        ssl: bool = False,
        key_prefix: str = DEFAULT_REDIS_KEY_PREFIX,
    ) -> None:
        """Configure Redis using a URL, a host, or ``REDIS_URL``."""
        normalized_key_prefix = key_prefix.rstrip(":")
        if (
            not normalized_key_prefix
            or "{" in normalized_key_prefix
            or "}" in normalized_key_prefix
        ):
            raise ValueError("key_prefix must be non-empty and cannot contain braces")
        self.host = host
        self.port = port
        self.db = db
        self.username = username
        self.password = password
        self.ssl = ssl
        self.key_prefix = normalized_key_prefix
        self._client: Any | None = None

    def begin_start(
        self,
        deployment: str,
        subscribers: tuple[str, ...],
        reference_time: datetime,
    ) -> _StartDecision:
        """Atomically create a generation and its per-subscriber seed records."""
        reference_time_utc = as_utc(reference_time, name="reference_time")
        keys = [
            self._control_key(deployment),
            *(self._seed_key(deployment, subscriber) for subscriber in subscribers),
        ]
        result = self._eval(
            _BEGIN_START_SCRIPT,
            keys,
            reference_time_utc.isoformat(),
        )
        if not isinstance(result, (list, tuple)) or len(result) != 3:
            raise RuntimeError("Redis returned an invalid APScheduler start result")
        changed = bool(int(_as_text(result[0])))
        epoch = int(_as_text(result[1]))
        stored_reference_time = datetime.fromisoformat(_as_text(result[2]))
        pending = tuple(
            subscriber
            for subscriber in subscribers
            if self._seed_status(deployment, subscriber, epoch) == "pending"
        )
        return _StartDecision(
            epoch=epoch,
            reference_time=stored_reference_time,
            changed=changed,
            pending_subscribers=pending,
        )

    def mark_seed_published(self, deployment: str, epoch: int, subscriber: str) -> None:
        """Mark an accepted start message without changing a newer generation."""
        self._eval(
            _MARK_SEED_PUBLISHED_SCRIPT,
            [self._control_key(deployment), self._seed_key(deployment, subscriber)],
            str(epoch),
            datetime.now(UTC).isoformat(),
        )

    def can_seed(self, deployment: str, epoch: int, subscriber: str) -> bool:
        """Return whether this seed belongs to the live generation."""
        result = self._eval(
            _CAN_SEED_SCRIPT,
            [self._control_key(deployment), self._seed_key(deployment, subscriber)],
            str(epoch),
        )
        return bool(int(result))

    def mark_seed_active(self, deployment: str, epoch: int, subscriber: str) -> None:
        """Mark a subscriber active without changing a newer generation."""
        self._eval(
            _MARK_SEED_ACTIVE_SCRIPT,
            [self._control_key(deployment), self._seed_key(deployment, subscriber)],
            str(epoch),
            datetime.now(UTC).isoformat(),
        )

    def is_running(self, deployment: str, epoch: int) -> bool:
        """Return whether an epoch is the deployment's running generation."""
        result = self._eval(
            _IS_RUNNING_SCRIPT,
            [self._control_key(deployment)],
            str(epoch),
        )
        return bool(int(result))

    def stop(self, deployment: str, updated_at: datetime) -> bool:
        """Atomically stop a deployment."""
        result = self._eval(
            _STOP_SCRIPT,
            [self._control_key(deployment)],
            as_utc(updated_at, name="updated_at").isoformat(),
        )
        return bool(int(result))

    def status(self, deployment: str) -> ControlStateValue:
        """Return ``running`` only for an explicitly running deployment."""
        state = self._client_instance().hget(self._control_key(deployment), "state")
        return "running" if _as_text(state) == "running" else "stopped"

    def _seed_status(self, deployment: str, subscriber: str, epoch: int) -> str | None:
        values = self._client_instance().hmget(
            self._seed_key(deployment, subscriber),
            "epoch",
            "status",
        )
        if not isinstance(values, (list, tuple)) or len(values) != 2:
            return None
        stored_epoch, status = values
        if stored_epoch is None or int(_as_text(stored_epoch)) != epoch:
            return None
        return None if status is None else _as_text(status)

    def _eval(self, script: str, keys: list[str], *arguments: str) -> Any:
        return self._client_instance().eval(script, len(keys), *keys, *arguments)

    def _client_instance(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            from redis import Redis  # ty: ignore[unresolved-import]
        except ImportError as exc:
            raise ControlBackendConfigurationError(
                'RedisControlBackend requires the "redis" package. Install "redis>=5,<7".'
            ) from exc

        if self.host is None:
            redis_url = environ.get(DEFAULT_REDIS_URL_ENV)
            if not redis_url:
                raise ControlBackendConfigurationError(
                    "RedisControlBackend requires host=... or the REDIS_URL environment variable"
                )
            self._client = Redis.from_url(redis_url, decode_responses=True)
        elif "://" in self.host:
            self._client = Redis.from_url(self.host, decode_responses=True)
        else:
            self._client = Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                username=self.username,
                password=self.password,
                ssl=self.ssl,
                decode_responses=True,
            )
        return self._client

    def _control_key(self, deployment: str) -> str:
        return f"{self.key_prefix}:{{{deployment}}}:control"

    def _seed_key(self, deployment: str, subscriber: str) -> str:
        return f"{self.key_prefix}:{{{deployment}}}:seed:{subscriber}"


def _resolve_deployment(deployment: str | None) -> str:
    resolved = deployment or environ.get(CURRENT_DEPLOYMENT_ENV)
    if not resolved:
        raise ControlConfigurationError(
            f"Could not resolve a deployment. Pass deployment=... or set {CURRENT_DEPLOYMENT_ENV}."
        )
    if not _IDENTIFIER_PATTERN.fullmatch(resolved):
        raise ValueError("deployment must contain only letters, digits, underscores, and hyphens")
    return resolved


def _validate_subscribers(subscribers: tuple[str, ...]) -> tuple[str, ...]:
    if not subscribers:
        raise ControlConfigurationError("No APScheduler subscribers are registered")
    unique = tuple(dict.fromkeys(subscribers))
    invalid = [subscriber for subscriber in unique if not _IDENTIFIER_PATTERN.fullmatch(subscriber)]
    if invalid:
        raise ControlConfigurationError(
            "Invalid APScheduler subscriber ID(s): " + ", ".join(invalid)
        )
    return unique


def _start_topic(subscriber: str) -> str:
    return f"__aps_{subscriber}_start"


def _start_idempotency_key(deployment: str, epoch: int, subscriber: str) -> str:
    return f"aps:start:{deployment}:{epoch}:{subscriber}"


def _as_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)
