"""Redis backend: the fully-guaranteed substrate (Lua CAS, fenced ownership)."""

from __future__ import annotations

from typing import Any, cast

from dataclasses import dataclass

from ..._imports import RedisJobStore
from ..._options import _SchedulerIdentity
from ..._types import APSchedulerConfigurationError
from .._protocols import Driver, JobCoordinator
from ._driver import RedisDriver
from ._jobstore import RedisJobCoordinator

RAW_STORE_KEY_ATTR = "_vercel_apscheduler_raw_jobs_key"

__all__ = ["RAW_STORE_KEY_ATTR", "RedisBackend", "RedisDriver", "RedisJobCoordinator"]


@dataclass
class _Bound:
    driver: Driver
    coordinator: JobCoordinator


class RedisBackend:
    name = "redis"

    def validate_configuration(self, scheduler: Any) -> dict[str, Any]:
        stores = scheduler._jobstores
        if "default" not in stores:
            raise APSchedulerConfigurationError(
                "vercel-apscheduler requires a configured default RedisJobStore"
            )
        if set(stores) != {"default"}:
            aliases = ", ".join(sorted(stores))
            raise APSchedulerConfigurationError(
                "vercel-apscheduler v1 supports exactly one job store named "
                f'"default"; configured: {aliases}'
            )
        if not isinstance(stores["default"], RedisJobStore):
            raise APSchedulerConfigurationError(
                "vercel-apscheduler requires a configured default RedisJobStore"
            )
        return cast("dict[str, Any]", dict(stores))

    def supports_store(self, store: Any) -> bool:
        return isinstance(store, RedisJobStore)

    def identity_ready(self, scheduler: Any) -> bool:
        """Whether the jobs_key-derived identity is already derivable.

        It may only arrive through a later ``add_jobstore()``.
        """
        return isinstance(scheduler._jobstores.get("default"), RedisJobStore)

    def derive_identity(self, scheduler: Any) -> _SchedulerIdentity:
        store = scheduler._jobstores.get("default")
        if not isinstance(store, RedisJobStore):
            raise APSchedulerConfigurationError(
                "vercel-apscheduler requires a configured default RedisJobStore"
            )
        raw_key = store.__dict__.setdefault(RAW_STORE_KEY_ATTR, store.jobs_key)
        try:
            return _SchedulerIdentity.from_store_key(raw_key)
        except ValueError as exc:
            raise APSchedulerConfigurationError(str(exc)) from exc

    def bind(self, adapter: Any, *, scope: str, deployment: str) -> _Bound:
        stores = self.validate_configuration(adapter.scheduler)
        tag = f"{{{scope}:{adapter.identity.scheduler_id}}}"
        expected_namespace = (scope, adapter.identity.scheduler_id)
        for alias, store in stores.items():
            namespace = getattr(store, "_vercel_apscheduler_namespace", None)
            if namespace is not None and namespace != expected_namespace:
                raise APSchedulerConfigurationError(
                    f'job store "{alias}" is already bound to another scheduler'
                )
            if namespace is None:
                if any(character in store.jobs_key + store.run_times_key for character in "{}"):
                    raise APSchedulerConfigurationError(
                        "custom Redis job-store keys cannot contain Redis hash tags"
                    )
                store.jobs_key = f"{store.jobs_key}:{tag}:jobs"
                store.run_times_key = f"{store.run_times_key}:{tag}:run_times"
                store.__dict__["_vercel_apscheduler_namespace"] = expected_namespace
        driver = RedisDriver(
            stores["default"].redis,
            scope=scope,
            scheduler_id=adapter.identity.scheduler_id,
            deployment=deployment,
        )
        coordinator = RedisJobCoordinator(stores["default"], driver, adapter)
        coordinator.install()
        return _Bound(
            driver=cast("Driver", driver),
            coordinator=cast("JobCoordinator", coordinator),
        )
