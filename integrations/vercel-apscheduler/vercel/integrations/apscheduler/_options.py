from __future__ import annotations

from typing import Any

import re
from dataclasses import dataclass
from os import environ

DEFAULT_MAX_DELAY_SECONDS = 23 * 60 * 60
DEFAULT_RETRY_AFTER_SECONDS = 30
DEFAULT_DURABLE_POLL_INTERVAL_SECONDS = 60
DEFAULT_SUBSCRIBER_ID = "default"
DISCOVERY_ENV = "VERCEL_APSCHEDULER_DISCOVERY"
SUBSCRIBER_ID_ENV = "VERCEL_PYTHON_SUBSCRIBER_ID"
_SUBSCRIBER_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")

__all__ = [
    "DEFAULT_DURABLE_POLL_INTERVAL_SECONDS",
    "DEFAULT_MAX_DELAY_SECONDS",
    "DEFAULT_RETRY_AFTER_SECONDS",
    "VercelAPSchedulerOptions",
    "is_discovery_runtime",
    "is_queue_serving_runtime",
    "is_vercel_runtime",
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
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def is_vercel_runtime() -> bool:
    return _truthy(environ.get("VERCEL"))


def is_discovery_runtime() -> bool:
    return _truthy(environ.get(DISCOVERY_ENV))


def is_queue_serving_runtime() -> bool:
    if _truthy(environ.get("VERCEL_DEV_QUEUE_SERVING")):
        return True
    service_type = (environ.get("VERCEL_SERVICE_TYPE") or "").strip().casefold()
    if service_type == "worker":
        return True
    service_trigger = (environ.get("VERCEL_SERVICE_TRIGGER") or "").strip().casefold()
    return service_type == "job" and service_trigger in {"queue", "workflow"}


@dataclass(frozen=True, slots=True)
class _SchedulerIdentity:
    scheduler_id: str
    wakeup_topic: str
    start_topic: str
    consumer_group: str

    @classmethod
    def from_subscriber_id(cls, subscriber_id: str) -> _SchedulerIdentity:
        if not _SUBSCRIBER_ID_PATTERN.fullmatch(subscriber_id):
            raise ValueError(
                f"{SUBSCRIBER_ID_ENV} must contain only ASCII letters, digits, "
                "underscores, and hyphens"
            )
        return cls(
            scheduler_id=subscriber_id,
            wakeup_topic=f"__aps_{subscriber_id}_wakeup",
            start_topic=f"__aps_{subscriber_id}_start",
            consumer_group=f"apscheduler-{subscriber_id}",
        )

    @classmethod
    def from_env(cls) -> _SchedulerIdentity:
        return cls.from_subscriber_id(environ.get(SUBSCRIBER_ID_ENV) or DEFAULT_SUBSCRIBER_ID)


@dataclass(frozen=True, slots=True)
class VercelAPSchedulerOptions:
    max_delay_seconds: int = DEFAULT_MAX_DELAY_SECONDS
    retention_seconds: int | None = DEFAULT_MAX_DELAY_SECONDS + 3600
    retry_after_seconds: int = DEFAULT_RETRY_AFTER_SECONDS
    durable_poll_interval_seconds: int = DEFAULT_DURABLE_POLL_INTERVAL_SECONDS
    max_attempts: int | None = None
    max_concurrency: int = 1

    @classmethod
    def from_env(cls) -> VercelAPSchedulerOptions:
        max_attempts_raw = environ.get("VERCEL_APSCHEDULER_MAX_ATTEMPTS")
        max_attempts = int(max_attempts_raw) if max_attempts_raw else None
        retention_raw = environ.get("VERCEL_APSCHEDULER_RETENTION_SECONDS")
        retention = int(retention_raw) if retention_raw else DEFAULT_MAX_DELAY_SECONDS + 3600
        return cls(
            max_delay_seconds=_int_env(
                "VERCEL_APSCHEDULER_MAX_DELAY_SECONDS",
                DEFAULT_MAX_DELAY_SECONDS,
            ),
            retention_seconds=retention,
            retry_after_seconds=_int_env(
                "VERCEL_APSCHEDULER_RETRY_AFTER_SECONDS",
                DEFAULT_RETRY_AFTER_SECONDS,
            ),
            durable_poll_interval_seconds=_int_env(
                "VERCEL_APSCHEDULER_DURABLE_POLL_INTERVAL_SECONDS",
                DEFAULT_DURABLE_POLL_INTERVAL_SECONDS,
            ),
            max_attempts=max_attempts,
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
