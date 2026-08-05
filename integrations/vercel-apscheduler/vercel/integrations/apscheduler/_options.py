from __future__ import annotations

from typing import Any

import re
from dataclasses import dataclass
from os import environ

DEFAULT_MAX_DELAY_SECONDS = 23 * 60 * 60
DEFAULT_RETRY_AFTER_SECONDS = 30
DISCOVERY_ENV = "VERCEL_APSCHEDULER_DISCOVERY"
SUBSCRIBER_ID_ENV = "VERCEL_PYTHON_SUBSCRIBER_ID"
_SCHEDULER_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
# The identity slug maps "." to "-", so both charsets collapse into the VQS
# name alphabet. Two keys that collapse to the same slug are rejected as an
# identity collision when both are bound.
_STORE_KEY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")

__all__ = [
    "DEFAULT_MAX_DELAY_SECONDS",
    "DEFAULT_RETRY_AFTER_SECONDS",
    "VercelAPSchedulerOptions",
    "is_discovery_runtime",
    "is_queue_serving_runtime",
    "is_vercel_runtime",
    "resolve_state_scope",
]


def _truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().casefold() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int) -> int:
    value = environ.get(name)
    if value is None or not value:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


def is_vercel_runtime() -> bool:
    return _truthy(environ.get("VERCEL"))


def is_discovery_runtime() -> bool:
    return _truthy(environ.get(DISCOVERY_ENV))


def is_queue_serving_runtime() -> bool:
    if environ.get(SUBSCRIBER_ID_ENV):
        return True
    if _truthy(environ.get("VERCEL_DEV_QUEUE_SERVING")):
        return True
    service_type = (environ.get("VERCEL_SERVICE_TYPE") or "").strip().casefold()
    if service_type == "worker":
        return True
    service_trigger = (environ.get("VERCEL_SERVICE_TRIGGER") or "").strip().casefold()
    return service_type == "job" and service_trigger in {"queue", "workflow"}


def resolve_state_scope(deployment: str) -> str:
    """Return the namespace scope for a scheduler's durable Redis state.

    Named environments (production and custom environments) share one durable
    namespace across deployments, so schedules, dynamic jobs, and the wake
    chain survive promotions: queue aliasing hands the in-flight wake to the
    new deployment, and the shared generation fences the old one out.
    Previews and development stay deployment-scoped and disposable.
    """
    environment = (environ.get("VERCEL_TARGET_ENV") or environ.get("VERCEL_ENV") or "").strip()
    if not environment or environment.casefold() in {"preview", "development"}:
        return deployment
    project = environ.get("VERCEL_PROJECT_ID", "").strip()
    if not project:
        # Without the project, two projects sharing one Redis database would
        # silently interleave a namespace. Refuse rather than guess.
        raise ValueError(
            "VERCEL_PROJECT_ID is required to scope durable scheduler state "
            f'in the "{environment}" environment'
        )
    return f"{project}:{environment}"


@dataclass(frozen=True, slots=True)
class _SchedulerIdentity:
    """A scheduler's durable identity, derived from its job store.

    The configured ``jobs_key`` is the one user-controlled value that is both
    refactor-stable (module and variable renames never touch it) and exactly
    as durable as the state it names. Entrypoints stay locators only.
    """

    scheduler_id: str
    wakeup_topic: str
    start_topic: str
    consumer_group: str

    @classmethod
    def from_scheduler_id(cls, scheduler_id: str) -> _SchedulerIdentity:
        if not _SCHEDULER_ID_PATTERN.fullmatch(scheduler_id):
            raise ValueError(
                "scheduler_id must contain only ASCII letters, digits, underscores, and hyphens"
            )
        return cls(
            scheduler_id=scheduler_id,
            wakeup_topic=f"__aps_{scheduler_id}_wakeup",
            start_topic=f"__aps_{scheduler_id}_start",
            consumer_group=f"apscheduler-{scheduler_id}",
        )

    @classmethod
    def from_store_key(cls, jobs_key: str) -> _SchedulerIdentity:
        if not _STORE_KEY_PATTERN.fullmatch(jobs_key):
            raise ValueError(
                "a durable RedisJobStore jobs_key must contain only ASCII "
                "letters, digits, dots, underscores, and hyphens"
            )
        return cls.from_scheduler_id(jobs_key.replace(".", "-"))


@dataclass(frozen=True, slots=True)
class VercelAPSchedulerOptions:
    max_delay_seconds: int = DEFAULT_MAX_DELAY_SECONDS
    retention_seconds: int | None = DEFAULT_MAX_DELAY_SECONDS + 3600
    retry_after_seconds: int = DEFAULT_RETRY_AFTER_SECONDS
    max_concurrency: int = 1
    # Escape hatch: pins the durable identity independently of the job
    # store's configured key, for example across a deliberate key rename.
    scheduler_id: str | None = None

    @classmethod
    def from_env(cls) -> VercelAPSchedulerOptions:
        max_delay_seconds = _int_env(
            "VERCEL_APSCHEDULER_MAX_DELAY_SECONDS",
            DEFAULT_MAX_DELAY_SECONDS,
        )
        retention_raw = environ.get("VERCEL_APSCHEDULER_RETENTION_SECONDS")
        try:
            # Retention must outlive the longest bridged hop, so the default
            # follows a raised max delay.
            retention = int(retention_raw) if retention_raw else max_delay_seconds + 3600
        except ValueError as exc:
            raise ValueError(
                "VERCEL_APSCHEDULER_RETENTION_SECONDS must be a positive integer"
            ) from exc
        if retention <= 0:
            raise ValueError("VERCEL_APSCHEDULER_RETENTION_SECONDS must be a positive integer")
        return cls(
            max_delay_seconds=max_delay_seconds,
            retention_seconds=retention,
            retry_after_seconds=_int_env(
                "VERCEL_APSCHEDULER_RETRY_AFTER_SECONDS",
                DEFAULT_RETRY_AFTER_SECONDS,
            ),
            max_concurrency=_int_env("VERCEL_APSCHEDULER_MAX_CONCURRENCY", 1),
        )

    @classmethod
    def from_value(
        cls,
        value: VercelAPSchedulerOptions | dict[str, Any] | None,
    ) -> VercelAPSchedulerOptions:
        if value is None:
            return cls.from_env()
        if isinstance(value, VercelAPSchedulerOptions):
            return value
        allowed = set(cls.__dataclass_fields__)
        unknown = sorted(set(value) - allowed)
        if unknown:
            joined = ", ".join(unknown)
            raise TypeError(f"unknown APScheduler integration option(s): {joined}")
        return cls(**value)
