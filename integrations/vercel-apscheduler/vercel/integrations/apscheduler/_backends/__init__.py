"""Durable-backend selection for the APScheduler integration.

Selection order:

1. ``VERCEL_APSCHEDULER_BACKEND`` (``redis`` | ``cache``), injected by the
   builder once it detects the project's store configuration.
2. Autodetection: a configured default ``RedisJobStore`` selects ``redis``;
   anything else selects ``cache`` (Vercel Runtime Cache, which itself falls
   back to per-process memory outside deployed environments, e.g. under
   ``vercel dev``).
"""

from __future__ import annotations

from typing import Any

from os import environ

from .._imports import RedisJobStore
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
    configured = (environ.get(BACKEND_ENV) or "").strip().casefold()
    if configured and configured not in {"redis", "cache"}:
        raise APSchedulerConfigurationError(
            f'{BACKEND_ENV} must be "redis" or "cache", not "{configured}"'
        )
    if not configured:
        store = scheduler._jobstores.get("default")
        configured = "redis" if isinstance(store, RedisJobStore) else "cache"
    if configured == "redis":
        from .redis import RedisBackend

        return RedisBackend()
    from .cache import CacheBackend

    return CacheBackend()
