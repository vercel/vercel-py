from __future__ import annotations

from typing import Any

from dataclasses import dataclass
from os import environ

DEFAULT_MAX_DELAY_SECONDS = 23 * 60 * 60
DEFAULT_RETRY_AFTER_SECONDS = 30

__all__ = [
    "DEFAULT_MAX_DELAY_SECONDS",
    "DEFAULT_RETRY_AFTER_SECONDS",
    "VercelAPSchedulerOptions",
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


@dataclass(frozen=True, slots=True)
class VercelAPSchedulerOptions:
    scheduler_id: str = "default"
    wakeup_topic: str = "__aps_default"
    consumer_group: str = "apscheduler"
    max_delay_seconds: int = DEFAULT_MAX_DELAY_SECONDS
    retention_seconds: int | None = DEFAULT_MAX_DELAY_SECONDS + 3600
    retry_after_seconds: int = DEFAULT_RETRY_AFTER_SECONDS
    max_attempts: int | None = None
    max_concurrency: int = 1

    @classmethod
    def from_env(cls) -> VercelAPSchedulerOptions:
        subscriber_name = environ.get("VERCEL_APSCHEDULER_SUBSCRIBER_NAME") or "default"
        topic = environ.get("VERCEL_APSCHEDULER_TOPIC") or f"__aps_{subscriber_name}"
        consumer = environ.get("VERCEL_APSCHEDULER_CONSUMER") or "apscheduler"
        max_attempts_raw = environ.get("VERCEL_APSCHEDULER_MAX_ATTEMPTS")
        max_attempts = int(max_attempts_raw) if max_attempts_raw else None
        retention_raw = environ.get("VERCEL_APSCHEDULER_RETENTION_SECONDS")
        retention = int(retention_raw) if retention_raw else DEFAULT_MAX_DELAY_SECONDS + 3600
        return cls(
            scheduler_id=environ.get("VERCEL_APSCHEDULER_SCHEDULER_ID") or subscriber_name,
            wakeup_topic=topic,
            consumer_group=consumer,
            max_delay_seconds=_int_env(
                "VERCEL_APSCHEDULER_MAX_DELAY_SECONDS",
                DEFAULT_MAX_DELAY_SECONDS,
            ),
            retention_seconds=retention,
            retry_after_seconds=_int_env(
                "VERCEL_APSCHEDULER_RETRY_AFTER_SECONDS",
                DEFAULT_RETRY_AFTER_SECONDS,
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
        if isinstance(value, cls):
            return value
        allowed = set(cls.__dataclass_fields__)
        unknown = sorted(set(value) - allowed)
        if unknown:
            joined = ", ".join(unknown)
            raise TypeError(f"unknown APScheduler integration option(s): {joined}")
        return cls(**value)
