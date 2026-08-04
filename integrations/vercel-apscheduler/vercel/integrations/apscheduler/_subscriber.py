from __future__ import annotations

from typing import Any

import json
import logging
from datetime import datetime, timezone
from os import environ
from sys import modules
from uuid import uuid4

import vercel.queue as vqs

from ._adapter import (
    SUBSCRIBERS_ENV,
    SchedulerAdapter,
    adopt_scheduler,
    get_adapter,
)
from ._imports import BaseScheduler
from ._options import SUBSCRIBER_ID_ENV, VercelAPSchedulerOptions
from ._payload import StartPayload, WakeupPayload

LOGGER = logging.getLogger("vercel.integrations.apscheduler")
UTC = timezone.utc
BUSY_RETRY_SECONDS = 5

# Registrations live for the process: the queue registry holds only weak
# references to handlers, and the handlers reference their scheduler anyway.
_registered_schedulers: dict[BaseScheduler, SchedulerAdapter] = {}
_registered_identities: set[str] = set()
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
    # One handler pair per identity: every scheduler constructed in a
    # subscriber Function adopts that Function's identity, and the queue SDK
    # rejects duplicate (topic, consumer group) registrations. Deliveries are
    # routed to the designated scheduler in _delivery_adapter.
    if adapter.identity.scheduler_id not in _registered_identities:
        _subscribe_identity(adapter)
        _registered_identities.add(adapter.identity.scheduler_id)
    _registered_schedulers[scheduler] = adapter
    return adapter


def _subscribe_identity(adapter: SchedulerAdapter) -> None:
    @vqs.subscribe(
        topic=adapter.identity.start_topic,
        consumer_group=vqs.SanitizedName(adapter.identity.consumer_group),
        retry_after=adapter.options.retry_after_seconds,
        max_concurrency=adapter.options.max_concurrency,
        max_attempts=None,
    )
    def _handle_start(message: vqs.Message[dict[str, Any]]) -> None:
        _process_start(_delivery_adapter(adapter), message)

    @vqs.subscribe(
        topic=adapter.identity.wakeup_topic,
        consumer_group=vqs.SanitizedName(adapter.identity.consumer_group),
        retry_after=adapter.options.retry_after_seconds,
        max_concurrency=adapter.options.max_concurrency,
        max_attempts=None,
    )
    def _handle_wakeup(message: vqs.Message[dict[str, Any]]) -> None:
        _process_wakeup(_delivery_adapter(adapter), message)

    _registered_callbacks.extend((_handle_start, _handle_wakeup))


def _delivery_adapter(registered: SchedulerAdapter) -> SchedulerAdapter:
    """Return the adapter of the scheduler this Function is designated to serve.

    The handlers for an identity are registered by whichever scheduler the
    module constructs first, during ``__init__``, before the entrypoint
    variable exists. By delivery time the module is fully imported, so resolve
    the designated scheduler here. Without this, a module defining several
    schedulers would serve every subscriber Function with the one it defines
    first.
    """
    subscriber_id = environ.get(SUBSCRIBER_ID_ENV)
    raw = environ.get(SUBSCRIBERS_ENV)
    if not subscriber_id or not raw:
        return registered
    try:
        entries = json.loads(raw)
    except json.JSONDecodeError:
        return registered
    if not isinstance(entries, list):
        return registered
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("id") != subscriber_id:
            continue
        entrypoint = entry.get("entrypoint")
        if not isinstance(entrypoint, str):
            break
        module_name, separator, variable_name = entrypoint.partition(":")
        if not separator:
            break
        module = modules.get(module_name)
        candidate = getattr(module, variable_name, None) if module is not None else None
        designated = get_adapter(candidate)
        if designated is not None:
            return designated
        break
    return registered


def _process_start(
    adapter: SchedulerAdapter,
    message: vqs.Message[dict[str, Any]],
) -> None:
    try:
        payload = StartPayload.from_payload(message.payload)
    except ValueError as exc:
        LOGGER.warning(
            "Ignoring invalid APScheduler start message %s: %s",
            message.metadata.message_id,
            exc,
        )
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
        adapter.publish_pending_wakeup()
        return
    activation_time = claim.activation_time
    if activation_time is None:
        raise RuntimeError("Redis start claim omitted its activation time")

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
        adapter.publish_pending_wakeup()
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
