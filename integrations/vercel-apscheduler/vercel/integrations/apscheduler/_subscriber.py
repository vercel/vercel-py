from __future__ import annotations

from typing import Any

import logging
from datetime import datetime, timezone
from uuid import uuid4

import vercel.queue as vqs

from ._adapter import SchedulerAdapter, adopt_scheduler
from ._control import LifecyclePayload
from ._imports import BaseScheduler
from ._options import VercelAPSchedulerOptions
from ._payload import StartPayload, WakeupPayload

LOGGER = logging.getLogger("vercel.integrations.apscheduler")
UTC = timezone.utc
BUSY_RETRY_SECONDS = 5

# Registrations live for the process: the queue registry holds only weak
# references to handlers, and the handlers reference their scheduler anyway.
# Identities are intrinsic (derived from each scheduler's own job store), so
# every scheduler registers its own topics; a collision raises during the
# adapter's identity claim instead of reaching this registry.
_registered_schedulers: dict[BaseScheduler, SchedulerAdapter] = {}
_registered_callbacks: list[Any] = []

__all__ = ["register_scheduler"]


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
        topic=adapter.identity.start_topic,
        consumer_group=vqs.SanitizedName(adapter.identity.consumer_group),
        retry_after=adapter.options.retry_after_seconds,
        max_concurrency=adapter.options.max_concurrency,
        max_attempts=None,
    )
    def _handle_start(message: vqs.Message[dict[str, Any]]) -> None:
        _process_start(adapter, message)

    @vqs.subscribe(
        topic=adapter.identity.wakeup_topic,
        consumer_group=vqs.SanitizedName(adapter.identity.consumer_group),
        retry_after=adapter.options.retry_after_seconds,
        max_concurrency=adapter.options.max_concurrency,
        max_attempts=None,
    )
    def _handle_wakeup(message: vqs.Message[dict[str, Any]]) -> None:
        _process_wakeup(adapter, message)

    _registered_callbacks.extend((_handle_start, _handle_wakeup))
    _registered_schedulers[scheduler] = adapter
    return adapter


def _process_start(
    adapter: SchedulerAdapter,
    message: vqs.Message[dict[str, Any]],
) -> None:
    try:
        payload = StartPayload.from_payload(message.payload)
    except ValueError as start_error:
        try:
            control = LifecyclePayload.from_payload(message.payload)
        except ValueError:
            LOGGER.warning(
                "Ignoring invalid APScheduler start message %s: %s",
                message.metadata.message_id,
                start_error,
            )
            return
        _process_lifecycle(adapter, control, message)
        return
    if not _targets_adapter(adapter, payload.scheduler_id, message, "start"):
        return

    owner = uuid4().hex
    claim = adapter.driver.claim_start(
        payload.generation,
        owner,
        datetime.now(UTC),
    )
    if claim.state == "busy":
        raise vqs.RetryAfter(BUSY_RETRY_SECONDS)
    if claim.state == "stale":
        adapter.repair_wakeup()
        return
    activation_time = claim.activation_time
    if activation_time is None:
        raise RuntimeError("durable start claim omitted its activation time")

    try:
        with adapter.driver.renewing(owner):
            adapter.activate_generation(activation_time)
            now = datetime.now(UTC)
            next_due_time = adapter.get_next_wakeup_time(now)
            next_time = (
                adapter.canonical_wakeup_time(next_due_time, now=now)
                if next_due_time is not None
                else None
            )
            result = adapter.driver.finish_start(
                payload.generation,
                owner,
                next_time,
                now,
            )
            if result.state == "lost":
                raise vqs.RetryAfter(BUSY_RETRY_SECONDS)
            if result.wake is not None:
                adapter.publish_wakeup(result.wake, now=now)
    finally:
        adapter.driver.release(owner)


def _process_wakeup(
    adapter: SchedulerAdapter,
    message: vqs.Message[dict[str, Any]],
) -> None:
    try:
        payload = WakeupPayload.from_payload(message.payload)
    except ValueError as exc:
        LOGGER.warning(
            "Ignoring invalid APScheduler wakeup message %s: %s",
            message.metadata.message_id,
            exc,
        )
        return
    if not _targets_adapter(adapter, payload.scheduler_id, message, "wakeup"):
        return

    token = payload.to_token()
    owner = uuid4().hex
    claim = adapter.driver.claim_wake(token, owner, datetime.now(UTC))
    if claim.state == "busy":
        raise vqs.RetryAfter(BUSY_RETRY_SECONDS)
    if claim.state == "stale":
        adapter.repair_wakeup()
        return

    try:
        with adapter.driver.renewing(owner):
            now = datetime.now(UTC)
            result = adapter.process_wakeup(token.logical_time, now=now)
            next_time = (
                adapter.canonical_wakeup_time(result.next_wakeup_time, now=now)
                if result.next_wakeup_time is not None
                else None
            )
            finish = adapter.driver.finish_wake(
                token,
                owner,
                next_time,
                datetime.now(UTC),
            )
            if finish.state == "lost":
                raise vqs.RetryAfter(BUSY_RETRY_SECONDS)
            if finish.wake is not None:
                adapter.publish_wakeup(finish.wake)
    finally:
        adapter.driver.release(owner)


def _process_lifecycle(
    adapter: SchedulerAdapter,
    payload: LifecyclePayload,
    message: vqs.Message[dict[str, Any]],
) -> None:
    """Apply a queue-borne lifecycle flag to the local driver document.

    Only the cache backend publishes these; pausing an already-paused chain
    is a no-op, so redelivery needs no dedup.
    """
    if not _targets_adapter(adapter, payload.scheduler_id, message, "lifecycle"):
        return
    if payload.action == "pause":
        adapter.driver.pause(datetime.now(UTC))
        LOGGER.info(
            'Applied queue-borne pause to scheduler "%s"',
            payload.scheduler_id,
        )


def _targets_adapter(
    adapter: SchedulerAdapter,
    scheduler_id: str,
    message: vqs.Message[dict[str, Any]],
    kind: str,
) -> bool:
    if scheduler_id == adapter.identity.scheduler_id:
        return True
    LOGGER.warning(
        "Ignoring APScheduler %s message %s for scheduler %r; expected %r",
        kind,
        message.metadata.message_id,
        scheduler_id,
        adapter.identity.scheduler_id,
    )
    return False
