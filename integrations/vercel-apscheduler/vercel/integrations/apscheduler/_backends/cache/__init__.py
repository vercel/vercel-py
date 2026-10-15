"""Runtime Cache backend: managed coordination with documented tradeoffs.

The Vercel Runtime Cache offers ``get``/``set``/``delete`` with TTLs and tags
— no compare-and-swap, no transactions, and entries may be evicted. The
backend therefore places each guarantee on something that can carry it:

- **Chain integrity** does not live here at all. The single-successor
  guarantee comes from the queue's idempotency keys: racing finishers compute
  identical successor payloads (logical times are canonical), and the queue
  accepts the publication once. Claims are best-effort filters that shrink,
  but cannot eliminate, duplicate wake *executions*; the contract is
  at-least-once.
- **Declared jobs are reconstructable, not durable.** Code is the index and
  the backup: reads enumerate the declared job ids, and a missing, unreadable,
  or schedule-changed record is rebuilt from its declaration at the point of
  use (read-repair), so eviction can only cost state that code cannot restate
  (execution progress, lifecycle flags).
- **Lifecycle flags are best-effort** by declared policy: read-merge-write
  with bounded retries; last writer wins. ``pause()`` additionally rides the
  queue as a control message.

Outside deployed environments (``vercel dev``) the cache client falls back to
per-process memory, which makes this backend the in-memory dev mode with the
queue-serving sidecar as the effective scheduler process.
"""

from __future__ import annotations

from typing import Any

from dataclasses import dataclass
from os import environ

from ..._options import (
    SUBSCRIBER_ID_ENV,
    SUBSCRIBERS_ENV,
    _SchedulerIdentity,
    resolve_declared_subscriber_id,
)
from ..._types import APSchedulerConfigurationError
from .._protocols import Driver, JobCoordinator
from ._driver import CacheDriver
from ._jobstore import CacheJobCoordinator, CacheJobStore

__all__ = ["CacheBackend", "CacheDriver", "CacheJobCoordinator", "CacheJobStore"]


@dataclass
class _Bound:
    driver: Driver
    coordinator: JobCoordinator


class CacheBackend:
    name = "cache"

    def validate_configuration(self, scheduler: Any) -> dict[str, Any]:
        stores = dict(scheduler._jobstores)
        default = stores.get("default")
        if default is not None and not isinstance(default, CacheJobStore):
            raise APSchedulerConfigurationError(
                f'job store "{type(default).__name__}" is not suitable for the '
                "cache backend, which injects its own managed store; remove "
                "the explicit default store"
            )
        for alias, store in stores.items():
            if alias != "default" and isinstance(store, CacheJobStore):
                raise APSchedulerConfigurationError(
                    f'job store "{alias}" is a {type(store).__name__}, but the '
                    'durable store must be the one named "default"; '
                    "non-default stores are source stores owned by their "
                    "external system"
                )
        return stores

    def supports_store(self, store: Any) -> bool:
        return isinstance(store, CacheJobStore)

    def identity_ready(self, scheduler: Any) -> bool:
        # Queue-serving and discovery processes are told their id outright.
        # A publishing process with a declared-subscriber mapping must wait
        # until the declaring module finishes importing, or import-time
        # registration would cache the "default" fallback and publish to
        # topics nothing serves. Without a mapping the fallback is the
        # identity everywhere, so it is always ready.
        if environ.get(SUBSCRIBER_ID_ENV):
            return True
        if not environ.get(SUBSCRIBERS_ENV):
            return True
        return resolve_declared_subscriber_id(scheduler) is not None

    def derive_identity(self, scheduler: Any) -> _SchedulerIdentity:
        # No store key to derive from; the builder-assigned subscriber id is
        # the durable identity. Sidecars receive it as an environment
        # variable; publishing processes reverse-look it up from the declared
        # {id, entrypoint} mapping; "default" covers undeclared schedulers
        # and use outside Vercel.
        subscriber_id = (
            environ.get(SUBSCRIBER_ID_ENV) or resolve_declared_subscriber_id(scheduler) or "default"
        )
        try:
            return _SchedulerIdentity.from_scheduler_id(subscriber_id)
        except ValueError as exc:
            raise APSchedulerConfigurationError(str(exc)) from exc

    def bind(self, adapter: Any, *, scope: str, deployment: str) -> _Bound:
        stores = self.validate_configuration(adapter.scheduler)
        store = stores.get("default")
        if store is None:
            store = CacheJobStore()
            with adapter.scheduler._jobstores_lock:
                adapter.scheduler._jobstores["default"] = store
                store.start(adapter.scheduler, "default")
        store.bind_namespace(scope=scope, scheduler_id=adapter.identity.scheduler_id)
        driver = CacheDriver(
            scope=scope,
            scheduler_id=adapter.identity.scheduler_id,
            deployment=deployment,
        )
        coordinator = CacheJobCoordinator(store, driver, adapter)
        coordinator.install()
        return _Bound(driver=driver, coordinator=coordinator)
