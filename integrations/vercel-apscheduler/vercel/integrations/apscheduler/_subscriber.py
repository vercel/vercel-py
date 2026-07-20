from __future__ import annotations

from typing import Any

import logging
from weakref import WeakKeyDictionary

import vercel.queue as vqs

from ._adapter import SchedulerAdapter, adopt_scheduler
from ._imports import BaseScheduler
from ._options import VercelAPSchedulerOptions
from ._payload import WakeupPayload

LOGGER = logging.getLogger("vercel.integrations.apscheduler")

_registered_schedulers: WeakKeyDictionary[BaseScheduler, SchedulerAdapter] = WeakKeyDictionary()
_registered_callbacks: list[Any] = []

__all__ = [
    "get_asgi_app",
    "register_scheduler",
]


def register_scheduler(
    scheduler: BaseScheduler,
    *,
    options: VercelAPSchedulerOptions | dict[str, Any] | None = None,
) -> SchedulerAdapter:
    existing = _registered_schedulers.get(scheduler)
    if existing is not None:
        return existing

    adapter = adopt_scheduler(scheduler, options)

    @vqs.subscribe(
        topic=adapter.options.wakeup_topic,
        consumer_group=adapter.options.consumer_group,
        retry_after=adapter.options.retry_after_seconds,
        max_concurrency=adapter.options.max_concurrency,
        max_attempts=adapter.options.max_attempts,
    )
    def _handle_wakeup(message: vqs.Message[dict[str, Any]]) -> None:
        try:
            payload = WakeupPayload.from_payload(message.payload)
        except ValueError as exc:
            LOGGER.warning(
                "Ignoring invalid APScheduler wakeup message %s on %s/%s: %s",
                message.metadata.message_id,
                message.metadata.topic,
                message.metadata.consumer_group,
                exc,
            )
            return

        if payload.scheduler_id != adapter.options.scheduler_id:
            LOGGER.warning(
                "Ignoring APScheduler wakeup message %s for scheduler %r; expected %r",
                message.metadata.message_id,
                payload.scheduler_id,
                adapter.options.scheduler_id,
            )
            return

        adapter.process_payload(payload)

    _registered_callbacks.append(_handle_wakeup)
    _registered_schedulers[scheduler] = adapter
    return adapter


def get_asgi_app(
    scheduler: BaseScheduler,
    *,
    options: VercelAPSchedulerOptions | dict[str, Any] | None = None,
) -> Any:
    register_scheduler(scheduler, options=options)
    return vqs.asgi_app()
