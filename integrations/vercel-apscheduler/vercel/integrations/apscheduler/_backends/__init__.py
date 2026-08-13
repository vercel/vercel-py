"""Durable-backend selection for the APScheduler integration.

There is exactly one managed backend: the Vercel Runtime Cache (which itself
falls back to per-process memory outside deployed environments, e.g. under
``vercel dev``). The ``Backend`` protocol remains the seam a future durable
substrate plugs into without changing the application-facing contract.

``VERCEL_APSCHEDULER_BACKEND`` may still be set by the builder; ``cache`` is
the only accepted value.
"""

from __future__ import annotations

from typing import Any

from os import environ

from .._types import APSchedulerConfigurationError
from ._protocols import Backend, BoundRuntime, Driver, JobCoordinator

BACKEND_ENV = "VERCEL_APSCHEDULER_BACKEND"

__all__ = [
    "BACKEND_ENV",
    "Backend",
    "BoundRuntime",
    "Driver",
    "JobCoordinator",
    "resolve_backend",
]


def resolve_backend(scheduler: Any) -> Backend:
    del scheduler
    configured = (environ.get(BACKEND_ENV) or "").strip().casefold()
    if configured == "redis":
        raise APSchedulerConfigurationError(
            "the managed Redis backend was removed: vercel-apscheduler runs "
            "on its managed job store; keep external schedules in your own "
            "database instead of a scheduler-managed Redis"
        )
    if configured and configured != "cache":
        raise APSchedulerConfigurationError(f'{BACKEND_ENV} must be "cache", not "{configured}"')
    from .cache import CacheBackend

    return CacheBackend()
