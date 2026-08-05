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
SUBSCRIBERS_ENV = "VERCEL_APSCHEDULER_SUBSCRIBERS"
ACTIVATION_HOOK_NAME = "vercel-apscheduler:auto-activate"
# Activation is idempotent; the periodic re-run is what notices and heals a
# wake whose queue message died (for example stranded by a rollback).
HEAL_SWEEP_INTERVAL_SECONDS = 5 * 60


def register_automatic_activation() -> None:
    """Buffer activation until the runtime has installed request credentials."""
    if not _automatic_environment():
        return
    if is_discovery_runtime() or is_queue_serving_runtime():
        return

    try:
        from vercel_runtime.invocation_hooks import (  # type: ignore[import-not-found]  # ty: ignore[unresolved-import]
            run_on_next_invocation,
        )
    except ImportError as exc:
        raise APSchedulerConfigurationError(
            "automatic APScheduler activation requires a Vercel Python Runtime "
            "with invocation hook support"
        ) from exc

    run_on_next_invocation(
        ACTIVATION_HOOK_NAME,
        _automatic_activation_hook,
        repeat_after_seconds=HEAL_SWEEP_INTERVAL_SECONDS,
    )


def _automatic_activation_hook() -> None:
    _activate_configured_schedulers()


def _automatic_environment() -> bool:
    if not is_vercel_runtime() or not environ.get(SUBSCRIBERS_ENV):
        return False
    environment = (environ.get(ENVIRONMENT_ENV) or "").strip().casefold()
    return environment == "production"


def _activate_configured_schedulers() -> None:
    from ._adapter import get_adapter

    for scheduler in _configured_schedulers():
        adapter = get_adapter(scheduler)
        if adapter is None:
            raise APSchedulerConfigurationError(
                "configured APScheduler subscriber was not adopted by the integration"
            )
        adapter.auto_activate()


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

    if not entries:
        raise APSchedulerConfigurationError(
            f"{SUBSCRIBERS_ENV} does not contain an APScheduler entrypoint"
        )
    return [_scheduler_from_entry(entry) for entry in entries]


def _scheduler_from_entry(entry: Any) -> BaseScheduler:
    """Resolve one builder-written subscriber entry to its scheduler.

    The builder owns this environment variable, so a malformed entry or an
    entrypoint that does not name a scheduler is a build regression; failing
    loudly here beats silently skipping a scheduler that should activate.
    """
    if not isinstance(entry, dict) or not isinstance(entry.get("entrypoint"), str):
        raise APSchedulerConfigurationError(
            f"{SUBSCRIBERS_ENV} contains a malformed subscriber entry"
        )
    entrypoint = entry["entrypoint"]
    module_name, separator, variable_name = entrypoint.partition(":")
    if not separator or not module_name or not variable_name:
        raise APSchedulerConfigurationError(
            f'{SUBSCRIBERS_ENV} entrypoint "{entrypoint}" is not "module:variable"'
        )
    module = importlib.import_module(module_name)
    scheduler = getattr(module, variable_name, None)
    if not isinstance(scheduler, BaseScheduler):
        raise APSchedulerConfigurationError(
            f'{SUBSCRIBERS_ENV} entrypoint "{entrypoint}" does not name an APScheduler scheduler'
        )
    return scheduler
