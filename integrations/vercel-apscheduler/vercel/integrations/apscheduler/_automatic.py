"""Request-driven automatic scheduler activation."""

from __future__ import annotations

from typing import Any

import importlib
import json
from os import environ

from ._driver import APSchedulerConfigurationError
from ._imports import BaseScheduler
from ._options import (
    is_discovery_runtime,
    is_queue_serving_runtime,
    is_vercel_runtime,
)

ENVIRONMENT_ENV = "VERCEL_ENV"
PREVIEW_IDLE_TIMEOUT_ENV = "VERCEL_APSCHEDULER_PREVIEW_IDLE_TIMEOUT_SECONDS"
SUBSCRIBERS_ENV = "VERCEL_APSCHEDULER_SUBSCRIBERS"
REQUEST_TASK_NAME = "vercel-apscheduler:auto-activate"
MAX_PREVIEW_RENEW_INTERVAL_SECONDS = 5 * 60


def register_automatic_activation() -> None:
    """Buffer activation until the runtime has installed request credentials."""
    if not _automatic_environment():
        return
    if is_discovery_runtime() or is_queue_serving_runtime():
        return

    timeout = _preview_idle_timeout()
    interval = (
        min(
            float(MAX_PREVIEW_RENEW_INTERVAL_SECONDS),
            timeout / 3,
        )
        if timeout is not None
        else None
    )
    try:
        from vercel_runtime.request_tasks import (  # type: ignore[import-not-found]  # ty: ignore[unresolved-import]
            register_request_task,
        )
    except ImportError as exc:
        raise APSchedulerConfigurationError(
            "automatic APScheduler activation requires a Vercel Python Runtime "
            "with request task support"
        ) from exc

    register_request_task(
        REQUEST_TASK_NAME,
        lambda: _activate_configured_schedulers(timeout),
        min_interval_seconds=interval,
    )


def _automatic_environment() -> bool:
    if not is_vercel_runtime() or not environ.get(SUBSCRIBERS_ENV):
        return False
    environment = (environ.get(ENVIRONMENT_ENV) or "").strip().casefold()
    if environment == "production":
        return True
    return environment == "preview" and PREVIEW_IDLE_TIMEOUT_ENV in environ


def _preview_idle_timeout() -> int | None:
    environment = (environ.get(ENVIRONMENT_ENV) or "").strip().casefold()
    if environment != "preview":
        return None
    raw = environ.get(PREVIEW_IDLE_TIMEOUT_ENV)
    try:
        timeout = int(raw) if raw is not None else 0
    except ValueError as exc:
        raise APSchedulerConfigurationError(
            f"{PREVIEW_IDLE_TIMEOUT_ENV} must be a positive integer"
        ) from exc
    if timeout <= 0:
        raise APSchedulerConfigurationError(
            f"{PREVIEW_IDLE_TIMEOUT_ENV} must be a positive integer"
        )
    return timeout


def _activate_configured_schedulers(
    idle_timeout_seconds: int | None,
) -> None:
    from ._adapter import get_adapter

    for scheduler in _configured_schedulers():
        adapter = get_adapter(scheduler)
        if adapter is None:
            raise APSchedulerConfigurationError(
                "configured APScheduler subscriber was not adopted by the integration"
            )
        adapter.auto_activate(
            idle_timeout_seconds=idle_timeout_seconds,
        )


def _configured_schedulers() -> list[BaseScheduler]:
    raw = environ.get(SUBSCRIBERS_ENV)
    if not raw:
        raise APSchedulerConfigurationError(
            f"{SUBSCRIBERS_ENV} is not set; declare the scheduler in [[tool.vercel.subscribers]]"
        )
    try:
        entries = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise APSchedulerConfigurationError(f"{SUBSCRIBERS_ENV} must contain JSON") from exc
    if not isinstance(entries, list):
        raise APSchedulerConfigurationError(f"{SUBSCRIBERS_ENV} must contain a JSON array")

    schedulers: list[BaseScheduler] = []
    for entry in entries:
        scheduler = _scheduler_from_entry(entry)
        if scheduler is not None:
            schedulers.append(scheduler)
    if not schedulers:
        raise APSchedulerConfigurationError(
            f"{SUBSCRIBERS_ENV} does not contain an APScheduler entrypoint"
        )
    return schedulers


def _scheduler_from_entry(entry: Any) -> BaseScheduler | None:
    if not isinstance(entry, dict):
        return None
    entrypoint = entry.get("entrypoint")
    if not isinstance(entrypoint, str):
        return None
    module_name, separator, variable_name = entrypoint.partition(":")
    if not separator or not module_name or not variable_name:
        return None
    module = importlib.import_module(module_name)
    scheduler = getattr(module, variable_name, None)
    return scheduler if isinstance(scheduler, BaseScheduler) else None
