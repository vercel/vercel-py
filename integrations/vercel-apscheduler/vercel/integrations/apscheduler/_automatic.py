"""Request-driven automatic scheduler activation."""

from __future__ import annotations

from typing import Any

import importlib
import json
import logging
from os import environ
from time import monotonic

from ._driver import APSchedulerConfigurationError
from ._imports import BaseScheduler
from ._options import (
    is_discovery_runtime,
    is_queue_serving_runtime,
    is_vercel_runtime,
    resolve_environment,
)

LOGGER = logging.getLogger("vercel.integrations.apscheduler")

PREVIEW_IDLE_TIMEOUT_ENV = "VERCEL_APSCHEDULER_PREVIEW_IDLE_TIMEOUT_SECONDS"
SUBSCRIBERS_ENV = "VERCEL_APSCHEDULER_SUBSCRIBERS"
ACTIVATION_HOOK_NAME = "vercel-apscheduler:auto-activate"
MAX_PREVIEW_RENEW_INTERVAL_SECONDS = 5 * 60
# Activation is idempotent; the periodic re-run renews preview deadlines and
# heals a wake whose queue message died (for example stranded by a rollback).
HEAL_SWEEP_INTERVAL_SECONDS = 5 * 60

# Whether the last sweep found a chain another deployment owns. While True,
# the hook stays eligible on every invocation so the first alias-routed
# request takes the chain over immediately, instead of waiting out a sweep
# window consumed by a request that proved nothing.
_unsettled = False
_last_sweep: float | None = None


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
        repeat_after_seconds=_sweep_interval(),
    )


def _sweep_interval() -> float:
    """Return the settled cadence: heal sweeps plus preview deadline renewal."""
    timeout = _preview_idle_timeout()
    if timeout is not None:
        return min(float(MAX_PREVIEW_RENEW_INTERVAL_SECONDS), timeout / 3)
    return float(HEAL_SWEEP_INTERVAL_SECONDS)


def _automatic_activation_hook() -> float | None:
    """Sweep the configured schedulers; stay eager while a takeover is owed.

    The return value sets the hook's next eligibility on runtimes that
    support it. While another deployment owns the chain, only an
    alias-routed request can change anything, so the hook stays eligible on
    every invocation but touches Redis only when one arrives or when the
    sweep interval lapses (the fallback that also resyncs after a manual
    ``start()``). Once settled it returns to the registered cadence.
    """
    global _last_sweep, _unsettled  # noqa: PLW0603 - process-lifetime sweep state
    now = monotonic()
    alias_routed = _request_is_alias_routed()
    recently_swept = _last_sweep is not None and now - _last_sweep < _sweep_interval()
    if _unsettled and recently_swept and not alias_routed:
        return 0.0
    settled = _activate_configured_schedulers(
        _preview_idle_timeout(),
        takeover_allowed=alias_routed,
    )
    _last_sweep = now
    _unsettled = not settled
    return 0.0 if not settled else None


def _request_is_alias_routed() -> bool:
    """Whether the current request arrived through an environment alias.

    Environment aliases (the production domain and custom domains) route
    exclusively to the currently promoted deployment, so such a request
    proves this deployment is current and may take the chain over. The
    deployment's own URL and its branch URL prove nothing: the branch URL
    tracks the newest deployment of the branch, which is exactly wrong
    during a rollback.
    """
    try:
        from vercel_runtime.invocation_hooks import (  # type: ignore[import-not-found]  # ty: ignore[unresolved-import]
            current_forwarded_host,
        )
    except ImportError:
        _warn_takeover_unavailable()
        return False
    host = (current_forwarded_host() or "").strip().casefold()
    if not host:
        return False
    own_hosts = {
        value.strip().casefold()
        for value in (environ.get("VERCEL_URL"), environ.get("VERCEL_BRANCH_URL"))
        if value
    }
    return host not in own_hosts


_takeover_warning_emitted = False


def _warn_takeover_unavailable() -> None:
    global _takeover_warning_emitted  # noqa: PLW0603 - process-lifetime warn-once
    if _takeover_warning_emitted:
        return
    _takeover_warning_emitted = True
    LOGGER.warning(
        "This Vercel Python Runtime does not expose the request host; a "
        "promoted deployment will not take over the scheduler chain "
        "automatically. Upgrade the runtime or call scheduler.start() "
        "explicitly after promoting."
    )


def _automatic_environment() -> bool:
    """Whether this deployment's environment activates schedulers on traffic.

    Named environments (production and custom environments) share one durable
    chain that must start and take over without a manual call, so they always
    activate. Previews activate only when idling is configured, so a preview
    chain cannot outlive its usefulness. The resolution must match
    ``resolve_state_scope``: a custom environment reports
    ``VERCEL_ENV=preview`` but is a named environment.
    """
    if not is_vercel_runtime() or not environ.get(SUBSCRIBERS_ENV):
        return False
    environment = resolve_environment().casefold()
    if environment in {"", "development"}:
        return False
    if environment == "preview":
        return PREVIEW_IDLE_TIMEOUT_ENV in environ
    return True


def _preview_idle_timeout() -> int | None:
    """Return the idle deadline for previews; named environments never idle."""
    if resolve_environment().casefold() != "preview":
        return None
    raw = environ.get(PREVIEW_IDLE_TIMEOUT_ENV)
    if raw is None:
        return None
    try:
        timeout = int(raw)
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
    *,
    takeover_allowed: bool,
) -> bool:
    """Sweep every configured scheduler; True when all are settled."""
    from ._adapter import get_adapter

    settled = True
    for scheduler in _configured_schedulers():
        adapter = get_adapter(scheduler)
        if adapter is None:
            raise APSchedulerConfigurationError(
                "configured APScheduler subscriber was not adopted by the integration"
            )
        if not adapter.auto_activate(
            idle_timeout_seconds=idle_timeout_seconds,
            takeover_allowed=takeover_allowed,
        ):
            settled = False
    return settled


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
