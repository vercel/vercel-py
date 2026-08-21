from __future__ import annotations

from typing import Any, ClassVar, cast
from typing_extensions import override

import json
import logging
import math
import os
import threading
import time
import warnings
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import timedelta
from functools import wraps
from urllib.parse import urlparse
from weakref import WeakKeyDictionary

from kombu.exceptions import ChannelError
from kombu.transport import resolve_transport, virtual
from kombu.transport.virtual.base import Empty
from kombu.utils import json as kombu_json

import vercel.queue as vqs
import vercel.queue.sync as vqs_sync
from celery import Celery, _state as celery_state
from celery.app.routes import MapRoute, prepare as prepare_task_routes

from .version import __version__

DEFAULT_CONSUMER_GROUP = "celery"
DEFAULT_REQUEUE_DELAY_SECONDS = 0
DEFAULT_PUSH_RETRY_DELAY_SECONDS = 1
# On Vercel, bouncing a push delivery with RetryAfter is expensive: redelivery
# is paced by the server and can take up to the full remaining visibility
# deadline. Prefer waiting in-request for the worker to become ready over
# bouncing, up to this budget.
DEFAULT_PUSH_HANDOFF_WAIT_SECONDS = 30.0
_PUSH_CHANNEL_WAIT_SECONDS = 5.0
_PUSH_WAIT_POLL_INTERVAL_SECONDS = 0.05
_PUSH_SETTLE_POLL_INTERVAL_SECONDS = 0.01
_EMBEDDED_WORKER_STARTUP_WAIT_SECONDS = 1.0
_QUEUE_LOGGER_NAME = "vercel.integrations.celery"
_DEBUG_ENV = "VERCEL_CELERY_DEBUG"
_DEBUG_LOGGER_NAMES = (
    _QUEUE_LOGGER_NAME,
    "celery",
    "celery.app",
    "celery.bootsteps",
    "celery.worker",
    "kombu",
    "kombu.connection",
)
TransportClass = type[Any]
CLIENT_TRANSPORT_OPTIONS = (
    "token",
    "region",
    "base_url",
    "deployment",
    "timeout",
    "headers",
)
PUBLISH_TRANSPORT_OPTIONS = (
    "retention",
    "delay",
    "use_task_id_as_idempotency_key",
)
LEASE_TRANSPORT_OPTIONS = (
    "requeue_delay_seconds",
    "push_retry_delay_seconds",
    "push_handoff_wait_seconds",
    "lease_duration",
)
CONSUMER_TRANSPORT_OPTIONS = ("consumer_group",)
QUEUE_TRANSPORT_OPTIONS = ("queue_name_prefix",)
# The current platform ceiling for function execution time.
_MAX_DURATION_MINIMUM_SECONDS = 1
_MAX_DURATION_MAXIMUM_SECONDS = 1800

# Celery apps can be short-lived in tests and app factories, so registration
# idempotency is keyed weakly by app object rather than by id(app). The values
# record only VQS-facing queue identity; when the Celery app is collected, its
# idempotency state disappears with it.
_registered_app_queues: WeakKeyDictionary[Celery, set[tuple[str, str]]] = WeakKeyDictionary()
_registered_queue_subscriptions: WeakKeyDictionary[Celery, dict[tuple[str, str], str]] = (
    WeakKeyDictionary()
)
_registered_callbacks: WeakKeyDictionary[Celery, dict[tuple[str, str], Any]] = WeakKeyDictionary()
_registration_lock = threading.RLock()


def _capped_limit_seconds(limit_seconds: int, source: str) -> int:
    if limit_seconds <= _MAX_DURATION_MAXIMUM_SECONDS:
        return limit_seconds
    warnings.warn(
        f"{source} exceeds the {_MAX_DURATION_MAXIMUM_SECONDS}s maximum "
        f"function execution time; capping max_duration at "
        f"{_MAX_DURATION_MAXIMUM_SECONDS}s",
        UserWarning,
        stacklevel=2,
    )
    return _MAX_DURATION_MAXIMUM_SECONDS


@dataclass
class _EmbeddedWorker:
    app: Celery
    worker: Any
    thread: threading.Thread


_embedded_workers: WeakKeyDictionary[Celery, _EmbeddedWorker] = WeakKeyDictionary()
_embedded_workers_lock = threading.RLock()

# Push deliveries enter through VQS subscriber callbacks, not Kombu polling. We
# keep live push channels here so a callback can hand the leased VQS message to
# whichever Kombu channel currently has a consumer and prefetch capacity.
_push_channels: list[PushChannel | AutoChannel] = []
_push_channels_lock = threading.RLock()


@dataclass
class _FinalizeHookState:
    installed: bool = False
    register_queues: bool = False
    default_broker_set_by_installer: bool = False
    connection_transport_options_hook_installed: bool = False


_finalize_hook_state = _FinalizeHookState()


def _set_default_broker_set_by_installer(*, value: bool) -> None:
    _finalize_hook_state.default_broker_set_by_installer = value


def debug_log(event: str, **fields: Any) -> None:
    if not _queue_debug_enabled():
        return
    payload = {
        "event": event,
        **{name: value for name, value in fields.items() if value is not None},
    }
    logging.getLogger(_QUEUE_LOGGER_NAME).info(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    )


def _queue_debug_enabled() -> bool:
    return os.environ.get(_DEBUG_ENV) in {"1", "true"}


def _configure_celery_debug_logging() -> None:
    if not _queue_debug_enabled():
        return
    for logger_name in _DEBUG_LOGGER_NAMES:
        logging.getLogger(logger_name).setLevel(logging.DEBUG)


class _KombuJSONDecoder(json.JSONDecoder):
    def __init__(self) -> None:
        super().__init__(object_hook=kombu_json.object_hook)


class _KombuMessageTransport(vqs.RawJsonTransport[dict[str, Any]]):
    def __init__(self) -> None:
        super().__init__(
            json_encoder=kombu_json.JSONEncoder,
            json_decoder=_KombuJSONDecoder,
        )


@dataclass
class _TrackedDelivery:
    message: vqs.Message[dict[str, Any]]
    lease_renewal: vqs.LeaseRenewal
    queue_client: vqs_sync.QueueClient


def is_vercel_runtime() -> bool:
    try:
        value = os.environ["VERCEL"]
    except KeyError:
        return False
    return value.strip().casefold() in {"1", "yes", "on", "true"}


class _BaseChannel(virtual.Channel):
    """Kombu virtual channel backed by Vercel Queue leases."""

    do_restore = False
    supports_fanout = False
    from_transport_options: ClassVar[tuple[str, ...]] = (  # ty: ignore [invalid-attribute-override]
        *CLIENT_TRANSPORT_OPTIONS,
        *PUBLISH_TRANSPORT_OPTIONS,
        *LEASE_TRANSPORT_OPTIONS,
        *CONSUMER_TRANSPORT_OPTIONS,
        *QUEUE_TRANSPORT_OPTIONS,
    )

    token: str | None = None
    region: str | None = None
    base_url: str | None = None
    deployment: vqs.DeploymentOption = vqs.CURRENT_DEPLOYMENT
    timeout: vqs.Duration | None = 10.0
    requeue_delay_seconds: int = DEFAULT_REQUEUE_DELAY_SECONDS
    push_retry_delay_seconds: int = DEFAULT_PUSH_RETRY_DELAY_SECONDS
    push_handoff_wait_seconds: float = DEFAULT_PUSH_HANDOFF_WAIT_SECONDS
    lease_duration: vqs.Duration | None = None
    retention: vqs.Duration | None = None
    delay: vqs.Duration | None = None
    headers: Mapping[str, str] | None = None
    use_task_id_as_idempotency_key: bool = False
    consumer_group: str = DEFAULT_CONSUMER_GROUP
    queue_name_prefix: str = ""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.consumer_group = str(vqs.sanitize_name(self.consumer_group))
        if self.queue_name_prefix is None:
            self.queue_name_prefix = ""
        else:
            self.queue_name_prefix = str(self.queue_name_prefix)
        self._queue_client = vqs_sync.QueueClient(
            token=self.token,
            region=self.region,
            base_url=self.base_url,
            deployment=self.deployment,
            headers=self.headers,
            timeout=self.timeout,
        )
        self._message_transport = _KombuMessageTransport()
        self._messages_by_tag: dict[str, _TrackedDelivery] = {}
        self._consumed_queues_by_tag: dict[str, str] = {}
        self._poll_queue_offset = 0
        self._poll_topic_offsets: dict[str, int] = {}
        # Per-queue task to explicit-limit map for shard routing and polling,
        # frozen at first use to stay consistent with what build-time
        # introspection deployed.
        self._queue_shard_limits_by_queue: dict[str, dict[str, int]] = {}
        # VQS push callbacks can enter this channel concurrently on SDK
        # threads. Serialize handoff into Kombu so the prefetch gate, tracked
        # lease state, and private QoS bookkeeping move together.
        self._push_handoff_lock = threading.RLock()

    def _topic(self, queue: str) -> vqs.Topic[dict[str, Any]]:
        return vqs.Topic[dict[str, Any]](
            vqs.sanitize_name(f"{self.queue_name_prefix}{queue}"),
            transport=self._message_transport,
        )

    def _delivery_topic(
        self,
        queue: str,
        message: dict[str, Any],
    ) -> vqs.Topic[dict[str, Any]]:
        base = self._topic(queue)
        headers = message.get("headers")
        task_name = headers.get("task") if isinstance(headers, Mapping) else None
        if isinstance(task_name, str):
            limit = self._queue_shard_limits(queue).get(task_name)
            if limit is not None:
                return self._shard_topic(base, limit)
        return base

    def _shard_topic(
        self,
        base: vqs.Topic[dict[str, Any]],
        limit_seconds: int,
    ) -> vqs.Topic[dict[str, Any]]:
        return vqs.Topic[dict[str, Any]](
            vqs.SanitizedName(f"{base.name}_d{limit_seconds}"),
            transport=self._message_transport,
        )

    def _queue_shard_limits(self, queue: str) -> Mapping[str, int]:
        cached = self._queue_shard_limits_by_queue.get(queue)
        if cached is not None:
            return cached
        limits: dict[str, int] = {}
        for app in _active_apps_with_queue_name_prefix(self.queue_name_prefix):
            for task_name, limit in _queue_task_shard_limits(app, queue).items():
                previous = limits.get(task_name)
                # Apps sharing a topic namespace can disagree on a task's
                # limit, so route to the largest shard so the serving function
                # accommodates the longest requested limit.
                limits[task_name] = limit if previous is None else max(previous, limit)
        self._queue_shard_limits_by_queue[queue] = limits
        return limits

    def _put(self, queue: str, message: dict[str, Any], **kwargs: Any) -> None:
        # Kombu gives transports its already-normalized message envelope. Store
        # that envelope directly in VQS so receive paths can hand it back to
        # Kombu without reconstructing Celery protocol fields.
        self._queue_client.send(
            self._delivery_topic(queue, message),
            message,
            idempotency_key=self._idempotency_key(message),
            retention=self.retention,
            delay=self.delay,
            headers=self.headers,
        )

    @override
    def basic_ack(self, delivery_tag: str, multiple: bool = False) -> None:
        # Kombu ACKs by delivery tag; VQS ACKs by receipt handle. The receive
        # path records the full VQS message under Kombu's tag so this method can
        # translate the ACK back into a VQS lease deletion.
        # Celery/Kombu consumers do not use multiple ACKs with virtual
        # transports. If one does, Kombu clears the local QoS bookkeeping, but
        # VQS cannot expand multiple=True to earlier leases and can only
        # acknowledge the explicit tag's lease here.
        tracked = self._messages_by_tag.get(delivery_tag)
        try:
            self._ack_tracked_delivery(delivery_tag)
        finally:
            if tracked is not None and self._qos is not None:
                super().basic_ack(delivery_tag, multiple=multiple)

    @override
    def basic_reject(self, delivery_tag: str, requeue: bool = False) -> None:
        # Celery's reject(requeue=True) maps to making the VQS lease visible
        # again after requeue_delay_seconds. reject(requeue=False) is a terminal
        # disposition, so ACK the VQS lease instead of changing visibility.
        message = self._messages_by_tag.get(delivery_tag)
        if message is not None:
            if requeue:
                tracked_message = self._stop_tracking_delivery(delivery_tag)
                if tracked_message is not None:
                    message.queue_client.retry_after(tracked_message, self.requeue_delay_seconds)
            else:
                try:
                    message.queue_client.acknowledge(message.message)
                finally:
                    self._stop_tracking_delivery(delivery_tag)
        if message is not None and self._qos is not None:
            # The VQS follow-up above already handled requeue semantics. Tell
            # Kombu only to remove local QoS bookkeeping for this delivery.
            super().basic_reject(delivery_tag, requeue=False)

    @override
    def basic_get(self, queue: str, no_ack: bool = False, **kwargs: Any) -> Any:
        message = super().basic_get(queue, no_ack=no_ack, **kwargs)
        if message is not None and no_ack:
            # VQS has no server-side no_ack mode. Once Kombu has accepted the
            # delivery locally, delete the VQS lease immediately without
            # touching Kombu QoS bookkeeping; basic_get(no_ack=True) never added
            # this tag to QoS._delivered.
            self._ack_tracked_delivery(message.delivery_tag)
        return message

    @override
    def basic_consume(
        self,
        queue: str,
        no_ack: bool,
        callback: Any,
        consumer_tag: str,
        **kwargs: Any,
    ) -> None:
        def wrapped_callback(message: Any) -> Any:
            try:
                result = callback(message)
            except Exception:
                if no_ack:
                    self._release_failed_no_ack_delivery(message.delivery_tag)
                raise
            if no_ack:
                # Mirror basic_get(no_ack=True): successful local delivery is
                # the ACK point because Celery will not call basic_ack later.
                self._ack_tracked_delivery(message.delivery_tag)
            return result

        super().basic_consume(
            queue,
            no_ack=no_ack,
            callback=wrapped_callback,
            consumer_tag=consumer_tag,
            **kwargs,
        )
        self._consumed_queues_by_tag[consumer_tag] = queue

    @override
    def basic_cancel(self, consumer_tag: str) -> None:
        try:
            super().basic_cancel(consumer_tag)
        finally:
            self._consumed_queues_by_tag.pop(consumer_tag, None)

    def _consumes_queue(self, queue: str) -> bool:
        return queue in self._consumed_queues_by_tag.values()

    def _release_failed_no_ack_delivery(self, delivery_tag: str) -> None:
        tracked = self._messages_by_tag.get(delivery_tag)
        if tracked is None:
            return
        tracked_message = self._stop_tracking_delivery(delivery_tag)
        if tracked_message is None:
            return
        tracked.queue_client.retry_after(tracked_message, self.requeue_delay_seconds)

    def _restore(self, message: Any) -> None:
        return None

    def _restore_at_beginning(self, message: Any) -> None:
        return None

    def _purge(self, queue: str) -> int:
        raise ChannelError("Vercel Queue Service does not support queue purge")

    def _track_message(
        self,
        message: vqs.Message[dict[str, Any]],
        *,
        queue_client: vqs_sync.QueueClient | None = None,
    ) -> dict[str, Any]:
        queue_client = queue_client or self._queue_client
        payload = dict(message.payload)
        properties = payload.get("properties")
        properties = {} if not isinstance(properties, dict) else dict(properties)
        payload["properties"] = properties

        delivery_tag = self._next_delivery_tag()
        properties["delivery_tag"] = delivery_tag
        # The serialized envelope's original delivery tag was created at
        # publish time. VQS leases can redeliver the same envelope, so each
        # receive needs a fresh local tag for Kombu ACK/reject bookkeeping.
        tracked_message = vqs.Message(
            payload=payload,
            metadata=message.metadata,
        )
        lease_renewal = queue_client.run_lease_renewal(
            tracked_message,
            lease_duration=self.lease_duration,
        )
        lease_renewal.__enter__()
        self._messages_by_tag[delivery_tag] = _TrackedDelivery(
            message=tracked_message,
            lease_renewal=lease_renewal,
            queue_client=queue_client,
        )
        return payload

    def _stop_tracking_delivery(self, delivery_tag: str) -> vqs.Message[dict[str, Any]] | None:
        tracked = self._messages_by_tag.pop(delivery_tag, None)
        if tracked is None:
            return None
        tracked.lease_renewal.stop()
        return tracked.message

    def _ack_tracked_delivery(self, delivery_tag: str) -> None:
        tracked = self._messages_by_tag.get(delivery_tag)
        if tracked is None:
            return
        try:
            tracked.queue_client.acknowledge(tracked.message)
        finally:
            self._stop_tracking_delivery(delivery_tag)

    def _stop_all_tracked_deliveries(self) -> None:
        for delivery_tag in list(self._messages_by_tag):
            self._stop_tracking_delivery(delivery_tag)

    def _release_all_tracked_deliveries(self) -> None:
        for delivery_tag, tracked in list(self._messages_by_tag.items()):
            tracked_message = self._stop_tracking_delivery(delivery_tag)
            if tracked_message is None:
                continue
            try:
                tracked.queue_client.retry_after(tracked_message, 0)
            except Exception as exc:  # noqa: BLE001
                debug_log(
                    "celery.delivery_release_failed",
                    delivery_tag=delivery_tag,
                    exception_class=exc.__class__.__name__,
                    exception_message=str(exc),
                )

    def close(self) -> None:
        try:
            self._release_all_tracked_deliveries()
        finally:
            super().close()

    def _idempotency_key(self, message: dict[str, Any]) -> str | None:
        if not self.use_task_id_as_idempotency_key:
            return None
        headers = message.get("headers")
        if isinstance(headers, dict):
            task_id = headers.get("id")
            if task_id is not None:
                return str(task_id)
        properties = message.get("properties")
        if isinstance(properties, dict):
            correlation_id = properties.get("correlation_id")
            if correlation_id is not None:
                return str(correlation_id)
        return None

    @staticmethod
    def _delivery_tag(message: dict[str, Any]) -> str | None:
        properties = message.get("properties")
        if not isinstance(properties, dict):
            return None
        delivery_tag = properties.get("delivery_tag")
        if delivery_tag is None:
            return None
        return cast("str", delivery_tag)

    def _poll_get(self, queue: str, timeout: vqs.Duration | None = None) -> dict[str, Any]:
        del timeout
        for topic in self._poll_topic_order(queue):
            messages = self._queue_client.poll(
                topic,
                self.consumer_group,
                limit=1,
                lease_duration=self.lease_duration,
            )
            try:
                delivery = next(messages)
            except StopIteration:
                continue
            return self._track_message(delivery.accept())
        raise Empty

    def _topics_for_queue(self, queue: str) -> list[vqs.Topic[dict[str, Any]]]:
        base = self._topic(queue)
        shards = [
            self._shard_topic(base, limit)
            for limit in sorted(set(self._queue_shard_limits(queue).values()))
        ]
        return [base, *shards]

    def _poll_topic_order(self, queue: str) -> list[vqs.Topic[dict[str, Any]]]:
        # Rotate the starting topic so a busy base topic cannot starve shard
        # topics.
        topics = self._topics_for_queue(queue)
        if len(topics) <= 1:
            return topics
        offset = self._poll_topic_offsets.get(queue, 0) % len(topics)
        self._poll_topic_offsets[queue] = offset + 1
        return [*topics[offset:], *topics[:offset]]

    def _poll_queue_order(self, queues: list[str]) -> tuple[str, ...]:
        if not queues:
            return ()
        offset = self._poll_queue_offset % len(queues)
        self._poll_queue_offset = offset + 1
        return tuple(queues[offset:]) + tuple(queues[:offset])

    def _release_unhandled_poll_deliveries(self, deliveries: Iterator[Any]) -> None:
        for delivery in deliveries:
            message = delivery.accept()
            try:
                self._queue_client.retry_after(message, self.requeue_delay_seconds)
            except Exception as exc:  # noqa: BLE001
                debug_log(
                    "celery.poll_batch_release_failed",
                    topic=message.metadata.topic,
                    consumer_group=message.metadata.consumer_group,
                    message_id=message.metadata.message_id,
                    exception_class=exc.__class__.__name__,
                    exception_message=str(exc),
                )

    def _push_get(self, queue: str, timeout: vqs.Duration | None = None) -> dict[str, Any]:
        del queue, timeout
        raise Empty

    def _handle_push_queue_delivery(
        self,
        payload: dict[str, Any],
        metadata: vqs.MessageMetadata,
        *,
        queue: str,
    ) -> None:
        # Bouncing a push delivery with RetryAfter is a last resort: VQS paces
        # redelivery on its own schedule, which can be far slower than the
        # requested delay. While this callback runs the function instance is
        # guaranteed to be executing, so prefer waiting here for the handoff
        # lock, consumer readiness, and prefetch capacity over bouncing.
        deadline = time.monotonic() + max(self.push_handoff_wait_seconds, 0.0)
        if not self._acquire_push_handoff_lock(deadline):
            debug_log(
                "celery.push_handoff_busy",
                queue=queue,
                topic=metadata.topic,
                consumer_group=metadata.consumer_group,
                message_id=metadata.message_id,
                push_retry_delay_seconds=self.push_retry_delay_seconds,
            )
            raise vqs.RetryAfter(self.push_retry_delay_seconds)
        try:
            # Raising RetryAfter lets the queue SDK update VQS visibility when
            # Kombu has no active callback or its QoS prefetch window is full.
            if not self._wait_for_push_capacity(queue, deadline):
                debug_log(
                    "celery.push_handoff_unavailable",
                    queue=queue,
                    topic=metadata.topic,
                    consumer_group=metadata.consumer_group,
                    message_id=metadata.message_id,
                    callback_queues=sorted(self.connection._callbacks),
                    consumed_queues=sorted(set(self._consumed_queues_by_tag.values())),
                    can_consume=self.qos.can_consume(),
                    push_retry_delay_seconds=self.push_retry_delay_seconds,
                )
                raise vqs.RetryAfter(self.push_retry_delay_seconds)

            message = vqs.Message(payload=payload, metadata=metadata)
            payload = self._track_message(message)
            try:
                # _deliver enters Kombu's normal consumer path. That path is now
                # responsible for basic_ack/basic_reject, which in turn ACKs or
                # changes visibility for the tracked VQS lease by delivery tag.
                # This runs on the VQS subscriber callback thread, not Kombu's
                # consuming thread. The per-channel lock serializes this private
                # Kombu delivery/QoS path so concurrent push callbacks cannot
                # pass the prefetch gate and mutate QoS bookkeeping together.
                self.connection._deliver(payload, queue)
            except Exception as exc:
                delivery_tag = self._delivery_tag(payload)
                tracked_message: vqs.Message[dict[str, Any]] | None = message
                if delivery_tag is not None:
                    tracked_message = self._stop_tracking_delivery(delivery_tag)
                    if self._qos is not None:
                        super().basic_reject(delivery_tag, requeue=False)
                if tracked_message is not None:
                    self._queue_client.retry_after(tracked_message, self.push_retry_delay_seconds)
                debug_log(
                    "celery.push_handoff_failed",
                    queue=queue,
                    topic=metadata.topic,
                    consumer_group=metadata.consumer_group,
                    message_id=metadata.message_id,
                    callback_queues=sorted(self.connection._callbacks),
                    exception_class=exc.__class__.__name__,
                    exception_message=str(exc),
                    push_retry_delay_seconds=self.push_retry_delay_seconds,
                )
                raise
            # Celery queues the ACK/reject for this delivery as a pending
            # operation drained by the worker consumer loop, but on Vercel that
            # thread is suspended whenever no request is executing. Settle the
            # delivery now, while this push invocation still has compute, or
            # the lease silently expires and the task re-runs on redelivery.
            self._settle_push_delivery(payload, deadline)
        finally:
            self._push_handoff_lock.release()
        debug_log(
            "celery.push_handoff_succeeded",
            queue=queue,
            topic=metadata.topic,
            consumer_group=metadata.consumer_group,
            message_id=metadata.message_id,
            callback_queues=sorted(self.connection._callbacks),
        )
        # Delivery succeeded into Kombu. Do not let vercel.queue auto-ACK here;
        # Celery/Kombu owns manual ACK/reject semantics from this point onward.
        raise vqs.Handoff

    def _acquire_push_handoff_lock(self, deadline: float) -> bool:
        timeout = deadline - time.monotonic()
        if timeout <= 0:
            return self._push_handoff_lock.acquire(blocking=False)
        return self._push_handoff_lock.acquire(timeout=timeout)

    def _wait_for_push_capacity(self, queue: str, deadline: float) -> bool:
        while True:
            if self._consumes_queue(queue) and self.qos.can_consume():
                return True
            # Deferred ACKs are what free prefetch capacity, and consumer
            # registration happens on the worker startup thread; pump pending
            # operations because the worker loop may never get scheduled
            # between push requests.
            _flush_embedded_worker_operations()
            if self._consumes_queue(queue) and self.qos.can_consume():
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(_PUSH_WAIT_POLL_INTERVAL_SECONDS)

    def _settle_push_delivery(self, payload: dict[str, Any], deadline: float) -> None:
        delivery_tag = self._delivery_tag(payload)
        if delivery_tag is None:
            return
        while delivery_tag in self._messages_by_tag:
            if not _flush_embedded_worker_operations():
                # No embedded worker owns pending operations in this process
                # (e.g. a standalone `celery worker`); its own consumer loop
                # keeps running and will settle the delivery.
                return
            if delivery_tag not in self._messages_by_tag or time.monotonic() >= deadline:
                return
            time.sleep(_PUSH_SETTLE_POLL_INTERVAL_SECONDS)

    def _register_push_channel(self) -> None:
        with _push_channels_lock:
            _push_channels.append(cast("PushChannel | AutoChannel", self))
        debug_log(
            "celery.push_channel_registered",
            channel_class=self.__class__.__name__,
            consumer_group=self.consumer_group,
        )

    def _unregister_push_channel(self) -> None:
        with _push_channels_lock:
            if self in _push_channels:
                _push_channels.remove(cast("PushChannel | AutoChannel", self))
        debug_log(
            "celery.push_channel_unregistered",
            channel_class=self.__class__.__name__,
            consumer_group=self.consumer_group,
        )


class PollChannel(_BaseChannel):
    def _get(self, queue: str, timeout: vqs.Duration | None = None) -> dict[str, Any]:
        return self._poll_get(queue, timeout=timeout)

    def _get_many(
        self,
        queues: list[str],
        timeout: vqs.Duration | None = None,
    ) -> None:
        del timeout
        delivered = 0
        capacity_exhausted = False
        for queue in self._poll_queue_order(queues):
            if capacity_exhausted:
                break
            for topic in self._poll_topic_order(queue):
                # Recompute the budget per poll.
                remaining = self.qos.can_consume_max_estimate()
                limit = 10 if remaining is None else min(remaining, 10)
                if limit < 1:
                    capacity_exhausted = True
                    break
                deliveries = self._queue_client.poll(
                    topic,
                    self.consumer_group,
                    limit=limit,
                    lease_duration=self.lease_duration,
                )
                batch_delivered, capacity_exhausted = self._deliver_polled_batch(queue, deliveries)
                delivered += batch_delivered
                if capacity_exhausted:
                    break
        if delivered == 0:
            raise Empty

    def _deliver_polled_batch(
        self,
        queue: str,
        deliveries: Iterator[Any],
    ) -> tuple[int, bool]:
        delivered = 0
        try:
            for delivery in deliveries:
                payload = self._track_message(delivery.accept())
                self.connection._deliver(payload, queue)
                delivered += 1
                if not self.qos.can_consume():
                    self._release_unhandled_poll_deliveries(deliveries)
                    return delivered, True
        except Exception:
            self._release_unhandled_poll_deliveries(deliveries)
            raise
        return delivered, False


class PushChannel(_BaseChannel):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._register_push_channel()

    def _get(self, queue: str, timeout: vqs.Duration | None = None) -> dict[str, Any]:
        return self._push_get(queue, timeout=timeout)

    def _handle_queue_delivery(
        self,
        payload: dict[str, Any],
        metadata: vqs.MessageMetadata,
        *,
        queue: str,
    ) -> None:
        self._handle_push_queue_delivery(payload, metadata, queue=queue)

    def close(self) -> None:
        try:
            self._unregister_push_channel()
        finally:
            super().close()


class AutoChannel(_BaseChannel):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if is_vercel_runtime():
            self._register_push_channel()

    def _get(self, queue: str, timeout: vqs.Duration | None = None) -> dict[str, Any]:
        if is_vercel_runtime():
            return self._push_get(queue, timeout=timeout)
        return self._poll_get(queue, timeout=timeout)

    def _handle_queue_delivery(
        self,
        payload: dict[str, Any],
        metadata: vqs.MessageMetadata,
        *,
        queue: str,
    ) -> None:
        self._handle_push_queue_delivery(payload, metadata, queue=queue)

    def close(self) -> None:
        try:
            self._unregister_push_channel()
        finally:
            super().close()


def _find_push_channel(queue: str, consumer_group: str) -> PushChannel | AutoChannel | None:
    # Prefer a channel that can accept work immediately. If the only matching
    # channel is at capacity, return it anyway so _handle_queue_delivery can use
    # that channel's configured requeue delay instead of the package default.
    with _push_channels_lock:
        channels = tuple(_push_channels)
    debug_log(
        "celery.push_channel_lookup",
        queue=queue,
        consumer_group=consumer_group,
        channel_count=len(channels),
    )
    for channel in reversed(channels):
        if channel.closed:
            continue
        if channel.consumer_group != consumer_group:
            continue
        if channel._consumes_queue(queue) and channel.qos.can_consume():
            debug_log(
                "celery.push_channel_selected",
                queue=queue,
                consumer_group=consumer_group,
                channel_class=channel.__class__.__name__,
                ready=True,
            )
            return channel
    for channel in reversed(channels):
        if channel.closed:
            continue
        if channel.consumer_group != consumer_group:
            continue
        if channel._consumes_queue(queue):
            debug_log(
                "celery.push_channel_selected",
                queue=queue,
                consumer_group=consumer_group,
                channel_class=channel.__class__.__name__,
                ready=False,
            )
            return channel
    debug_log(
        "celery.push_channel_missing",
        queue=queue,
        consumer_group=consumer_group,
        channel_count=len(channels),
    )
    return None


def _make_queue_callback(queue: str) -> Any:
    def handle_queue_delivery(
        message: vqs.Message[Any],
    ) -> None:
        # This is the real VQS trigger callback. The queue SDK has already
        # accepted/deserialized the delivery and will perform follow-up lease
        # actions according to the directive raised here.
        consumer_group = str(message.metadata.consumer_group)
        channel = _find_push_channel(queue, consumer_group)
        if channel is None:
            # A delivery can race worker startup on a cold boot: the embedded
            # worker opens its push channel moments after the HTTP server
            # starts accepting push callbacks. Waiting briefly beats bouncing,
            # because VQS redelivery pacing is far coarser than this window.
            deadline = time.monotonic() + _PUSH_CHANNEL_WAIT_SECONDS
            while channel is None and time.monotonic() < deadline:
                time.sleep(_PUSH_WAIT_POLL_INTERVAL_SECONDS)
                channel = _find_push_channel(queue, consumer_group)
        if channel is None:
            raise vqs.RetryAfter(DEFAULT_PUSH_RETRY_DELAY_SECONDS)
        channel._handle_queue_delivery(
            message.payload,
            message.metadata,
            queue=queue,
        )

    handle_queue_delivery.__name__ = f"vercel_celery_{queue}_subscriber"
    return handle_queue_delivery


def _flush_embedded_worker_operations() -> bool:
    """Drain deferred consumer operations (ACKs/rejects) of embedded workers.

    Returns whether any embedded worker consumer was available to drain.
    Celery consumers defer message settlement to their consuming loop thread,
    which on Vercel is suspended outside of request handling; push delivery
    paths call this to run those operations on the delivering thread instead.
    ``perform_pending_operations`` pops from a plain list and handles each
    operation's errors, so concurrent draining by the worker loop is safe.
    """
    flushed = False
    with _embedded_workers_lock:
        embedded_workers = tuple(_embedded_workers.values())
    for embedded in embedded_workers:
        consumer = getattr(embedded.worker, "consumer", None)
        if consumer is None:
            continue
        try:
            consumer.perform_pending_operations()
        except Exception as exc:  # noqa: BLE001
            debug_log(
                "celery.pending_operations_flush_failed",
                exception_class=exc.__class__.__name__,
                exception_message=str(exc),
            )
            continue
        flushed = True
    return flushed


def _start_embedded_worker(app: Celery) -> None:
    _configure_celery_debug_logging()
    with _embedded_workers_lock:
        if app in _embedded_workers:
            debug_log("celery.embedded_worker_reused", app_main=getattr(app, "main", None))
            return
        loglevel = "DEBUG" if _queue_debug_enabled() else "INFO"
        debug_log("celery.embedded_worker_starting", app_main=getattr(app, "main", None))
        worker = app.WorkController(
            concurrency=1,
            hostname="vercel-celery-embedded-worker@localhost",
            pool="solo",
            loglevel=loglevel,
            without_gossip=True,
            without_heartbeat=True,
            without_mingle=True,
        )
        thread = threading.Thread(
            target=worker.start,
            name="vercel-celery-embedded-worker",
            daemon=True,
        )
        _embedded_workers[app] = _EmbeddedWorker(app=app, worker=worker, thread=thread)
        try:
            thread.start()
        except Exception:
            _embedded_workers.pop(app, None)
            debug_log(
                "celery.embedded_worker_thread_start_failed",
                app_main=getattr(app, "main", None),
            )
            raise
    _wait_for_embedded_worker_channel(worker)


def _wait_for_embedded_worker_channel(worker: Any) -> None:
    deadline = time.monotonic() + _EMBEDDED_WORKER_STARTUP_WAIT_SECONDS
    while time.monotonic() < deadline:
        if _embedded_worker_channel_ready(worker):
            debug_log("celery.embedded_worker_ready")
            return
        time.sleep(0.01)
    debug_log("celery.embedded_worker_ready_timeout")


def _embedded_worker_channel_ready(worker: Any) -> bool:
    consumer = getattr(worker, "consumer", None)
    if consumer is None:
        return False
    app = getattr(worker, "app", None)
    if app is None:
        return False
    return any(
        getattr(channel, "closed", True) is False and _channel_belongs_to_app(channel, app)
        for channel in tuple(_push_channels)
    )


def _channel_belongs_to_app(channel: PushChannel | AutoChannel, app: Celery) -> bool:
    connection = getattr(channel, "connection", None)
    client = getattr(connection, "client", None)
    return getattr(client, "app", None) is app


def _register_app_queue(
    app: Celery,
    queue: str,
    consumer_group: vqs.SanitizedName,
    queue_name_prefix: str,
) -> set[tuple[str, str]]:
    base_topic = vqs.sanitize_name(f"{queue_name_prefix}{queue}")
    base_key = (str(base_topic), str(consumer_group))

    def resolve_max_duration(
        *,
        key: tuple[str, str] = base_key,
    ) -> vqs.Duration | None:
        return _max_duration_for_subscription(key)

    # The base topic is always registered, because Celery routing is open-world
    # (callable routes and apply_async(queue=...) are invisible statically),
    # so the base topic must always have a consumer.
    _register_queue_subscription(
        app,
        queue,
        consumer_group,
        base_topic,
        resolve_max_duration,
    )
    desired_keys = {(str(base_topic), str(consumer_group))}

    # Tasks with an explicit hard time limit shard the queue into per-limit
    # topics served by their own functions.
    for limit in sorted(set(_queue_task_shard_limits(app, queue).values())):
        shard_topic = vqs.SanitizedName(f"{base_topic}_d{limit}")
        _register_queue_subscription(app, queue, consumer_group, shard_topic, limit)
        desired_keys.add((str(shard_topic), str(consumer_group)))
    return desired_keys


def _register_queue_subscription(
    app: Celery,
    queue: str,
    consumer_group: vqs.SanitizedName,
    topic: vqs.SanitizedName,
    max_duration: vqs.MaxDuration | None,
) -> None:
    app_queues = _registered_app_queues.setdefault(app, set())
    app_subscriptions = _registered_queue_subscriptions.setdefault(app, {})
    app_callbacks = _registered_callbacks.setdefault(app, {})
    key = (str(topic), str(consumer_group))
    if key in app_queues:
        return
    registered_queue = _registered_queue_subscription_queue(key)
    if registered_queue is not None:
        if registered_queue != queue:
            raise RuntimeError(
                "Celery app queue registration cannot map multiple Celery queue names "
                f"to Vercel Queue topic {topic!r} and consumer group {consumer_group!r}"
            )
        app_queues.add(key)
        app_subscriptions[key] = queue
        callback = _registered_queue_callback(key)
        if callback is not None:
            app_callbacks[key] = callback
        return
    callback = _make_queue_callback(queue)
    vqs.subscribe(
        topic=vqs.Topic(topic, transport=_KombuMessageTransport()),
        consumer_group=consumer_group,
        max_duration=max_duration,
    )(callback)
    app_callbacks[key] = callback
    app_subscriptions[key] = queue
    app_queues.add(key)


def _registered_queue_subscription_queue(key: tuple[str, str]) -> str | None:
    for app_subscriptions in tuple(_registered_queue_subscriptions.values()):
        queue = app_subscriptions.get(key)
        if queue is not None:
            return queue
    return None


def _registered_queue_callback(key: tuple[str, str]) -> Any | None:
    for app_callbacks in tuple(_registered_callbacks.values()):
        callback = app_callbacks.get(key)
        if callback is not None:
            return callback
    return None


def _active_apps_with_queue_name_prefix(queue_name_prefix: str) -> list[Celery]:
    return [
        app
        for app in list(celery_state._get_active_apps())
        if _app_queue_name_prefix(app) == queue_name_prefix
    ]


def _duration_seconds(value: vqs.Duration) -> float:
    if isinstance(value, timedelta):
        return value.total_seconds()
    return float(value)


def _max_duration_for_subscription(key: tuple[str, str]) -> vqs.Duration | None:
    values: list[vqs.Duration] = []
    with _registration_lock:
        subscribed_queues = [
            (app, app_subscriptions.get(key))
            for app, app_subscriptions in tuple(_registered_queue_subscriptions.items())
        ]
    for app, queue in subscribed_queues:
        if queue is None:
            continue
        value = _max_duration_for_app_queue(app, queue)
        if value is not None:
            values.append(value)
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    # Multiple live apps share this subscription; the function must
    # accommodate the longest requested limit.
    return max(values, key=_duration_seconds)


def _max_duration_for_app_queue(app: Celery, queue: str) -> vqs.Duration | None:
    option = _app_max_duration_option(app)
    if option is not None:
        if isinstance(option, (int, float, timedelta)):
            return option
        value = option.get(queue)
        if value is not None:
            return value
    global_limit = app.conf.task_time_limit
    if (
        global_limit is None
        or isinstance(global_limit, bool)
        or not isinstance(global_limit, (int, float))
        or not math.isfinite(global_limit)
        or global_limit <= 0
    ):
        return None
    limit_seconds = math.ceil(global_limit)
    return _capped_limit_seconds(
        limit_seconds,
        f"Celery task_time_limit of {limit_seconds}s",
    )


def _app_max_duration_option(
    app: Celery,
) -> vqs.Duration | Mapping[str, vqs.Duration] | None:
    options = getattr(app.conf, "broker_transport_options", None)
    if not isinstance(options, Mapping):
        return None
    value = options.get("max_duration")
    if value is None:
        return None
    if isinstance(value, Mapping):
        for queue_name, entry in value.items():
            # Keys are raw Celery queue names, before prefixing/sanitization.
            _validate_max_duration_value(
                entry,
                f"broker_transport_options['max_duration'][{queue_name!r}]",
            )
        return cast("Mapping[str, vqs.Duration]", value)
    _validate_max_duration_value(value, "broker_transport_options['max_duration']")
    return cast("vqs.Duration", value)


def _validate_max_duration_value(value: object, source: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float, timedelta)):
        raise TypeError(
            f"{source} must be a duration in seconds, a datetime.timedelta, "
            "or a mapping of Celery queue names to durations; "
            f"got {value!r}"
        )
    seconds = value.total_seconds() if isinstance(value, timedelta) else float(value)
    if (
        not math.isfinite(seconds)
        or seconds < _MAX_DURATION_MINIMUM_SECONDS
        or seconds > _MAX_DURATION_MAXIMUM_SECONDS
    ):
        raise ValueError(
            f"{source} must be between {_MAX_DURATION_MINIMUM_SECONDS} and "
            f"{_MAX_DURATION_MAXIMUM_SECONDS} seconds, got {value!r}"
        )


def _explicit_task_limit_seconds(task: Any) -> int | None:
    limit = getattr(task, "time_limit", None)
    if (
        limit is None
        or isinstance(limit, bool)
        or not isinstance(limit, (int, float))
        or not math.isfinite(limit)
        or limit <= 0
    ):
        # An explicitly unlimited (or nonsensical) task stays in the queue's
        # default group; there is no bound to shard on.
        return None
    return math.ceil(limit)


def _queue_task_shard_limits(app: Celery, queue: str) -> dict[str, int]:
    limits: dict[str, int] = {}
    # Read the raw task registry, because ``app.tasks`` would auto-finalize the app,
    # and this derivation must be side-effect free, so that callers that need the
    # complete task set run after finalize.
    tasks = getattr(app, "_tasks", None)
    if tasks is None:
        return limits
    routers = _static_task_routers(app)
    default_queue = _app_default_queue(app)
    for name, task in tuple(tasks.items()):
        if _static_task_queue(task, routers, default_queue) != queue:
            continue
        limit = _explicit_task_limit_seconds(task)
        if limit is not None:
            limits[str(name)] = _capped_limit_seconds(
                limit,
                f"Celery task {name!r} time_limit of {limit}s",
            )
    return limits


def _static_task_routers(app: Celery) -> tuple[MapRoute, ...]:
    try:
        routes = prepare_task_routes(getattr(app.conf, "task_routes", None))
    except Exception as exc:  # noqa: BLE001
        debug_log("celery.task_routes_prepare_failed", error=repr(exc))
        return ()
    return tuple(router for router in routes if isinstance(router, MapRoute))


def _app_default_queue(app: Celery) -> str | None:
    default_queue = getattr(app.conf, "task_default_queue", None)
    return str(default_queue) if default_queue else None


def _static_task_queue(
    task: Any,
    routers: tuple[MapRoute, ...],
    default_queue: str | None,
) -> str | None:
    task_queue = getattr(task, "queue", None)
    if task_queue is not None:
        return str(getattr(task_queue, "name", task_queue))
    task_name = getattr(task, "name", None)
    if task_name is None:
        return None
    for router in routers:
        route = router(str(task_name))
        if not route:
            continue
        route_queue = route.get("queue")
        if route_queue is None:
            # Routed by exchange/routing key only; the destination queue is
            # not statically known.
            return None
        return str(getattr(route_queue, "name", route_queue))
    return default_queue


def _app_queue_names(app: Celery) -> list[str]:
    # app.amqp.queues materializes Celery's default queue even when task_queues
    # is empty. Depending on Celery/Kombu version, iterating yields either queue
    # objects or mapping keys, so normalize both shapes to names.
    queues = app.amqp.queues
    queue_values = queues.values() if hasattr(queues, "values") else queues
    names: list[str] = []
    for queue in queue_values:
        name = getattr(queue, "name", queue)
        names.append(str(name))
    return names


def register_celery_app_queues(app: Celery, *, start_worker: bool = True) -> None:
    """Register a Celery app's queues as Vercel Queue topic subscribers.

    Use this when ``install_vercel_celery_integration`` was called with
    ``register_queues=False`` or when an app is created after automatic
    finalize-hook registration is no longer appropriate. The app must use the
    push transport, the auto transport while running on Vercel, or a compatible
    Vercel push transport subclass. When *start_worker* is ``True`` (default),
    an in-process solo Celery worker is started once for this app to consume
    push deliveries.
    """
    if not _app_uses_queue_registration_transport(app):
        raise RuntimeError(
            "Celery app queue registration requires a vercel-push broker transport, "
            "a vercel broker transport running on Vercel, or a Vercel push "
            "transport subclass"
        )
    # Registration reads the app's task registry, so tasks on an unfinalized app
    # are still pending decorators, so finalize first rather than silently
    # registering no shards.
    if not app.finalized:
        app.finalize()
    _configure_app_transport_defaults(app)
    _app_max_duration_option(app)
    consumer_group = _app_consumer_group(app)
    queue_name_prefix = _app_queue_name_prefix(app)
    with _registration_lock:
        desired_keys: set[tuple[str, str]] = set()
        for queue_name in _app_queue_names(app):
            desired_keys |= _register_app_queue(app, queue_name, consumer_group, queue_name_prefix)
        _prune_stale_app_registrations(app, desired_keys)
    if start_worker:
        _start_embedded_worker(app)


def _prune_stale_app_registrations(app: Celery, desired_keys: set[tuple[str, str]]) -> None:
    app_queues = _registered_app_queues.get(app)
    if app_queues is not None:
        app_queues &= desired_keys
    for registry in (_registered_queue_subscriptions, _registered_callbacks):
        entries = registry.get(app)
        if entries is None:
            continue
        for key in [key for key in entries if key not in desired_keys]:
            del entries[key]


def _register_finalized_app_queues(app: Celery) -> None:
    if not _app_uses_vercel_broker_transport(app):
        return
    _configure_app_transport_defaults(app)
    if _finalize_hook_state.register_queues and _app_uses_queue_registration_transport(app):
        # This hook fires from an unordered set that races the shared-task
        # constructors populating ``app._tasks``, so we need to defer registration
        # to ``on_after_finalize``, sent after every task is evaluated and bound.
        signal = getattr(app, "on_after_finalize", None)
        if signal is not None:
            signal.connect(_register_app_queues_on_after_finalize, weak=False)


def _register_app_queues_on_after_finalize(sender: Celery, **kwargs: Any) -> None:
    del kwargs
    _register_app_queues_if_eligible(sender)


def _register_app_queues_if_eligible(app: Celery) -> None:
    if not _app_uses_vercel_broker_transport(app):
        return
    _configure_app_transport_defaults(app)
    if _app_uses_queue_registration_transport(app):
        register_celery_app_queues(app)


def _is_vercel_transport_url(broker_url: str) -> bool:
    transport = urlparse(broker_url).scheme
    return bool(transport) and _is_vercel_transport_name(transport)


def _is_vercel_transport_name(transport: str) -> bool:
    resolved = _resolve_transport_class(transport)
    return resolved is not None and _is_vercel_transport_class(resolved)


def _register_existing_app_queues() -> None:
    for app in celery_state._get_active_apps():
        _register_app_queues_if_eligible(app)


def _configure_existing_app_defaults(
    *,
    broker_url: str | None = None,
    result_backend: str | None = None,
) -> None:
    for app in celery_state._get_active_apps():
        _configure_app_global_defaults(
            app,
            broker_url=broker_url,
            result_backend=result_backend,
        )


def _install_connection_transport_options_hook() -> None:
    if _finalize_hook_state.connection_transport_options_hook_installed:
        return
    original_connection = Celery._connection

    @wraps(original_connection)
    def _connection_with_prefix_default(
        self: Celery,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        connection = original_connection(self, *args, **kwargs)
        options = connection.transport_options
        if isinstance(options, dict) and "queue_name_prefix" not in options:
            transport = _resolve_transport_class(connection.transport_cls)
            if transport is not None and _is_vercel_transport_class(transport):
                options["queue_name_prefix"] = _app_queue_name_prefix(self)
        return connection

    cast("Any", Celery)._connection = _connection_with_prefix_default
    _finalize_hook_state.connection_transport_options_hook_installed = True


def _configure_app_global_defaults(
    app: Celery,
    *,
    broker_url: str | None,
    result_backend: str | None,
) -> None:
    defaults: dict[str, str] = {}
    if broker_url is not None and getattr(app.conf, "broker_url", None) is None:
        defaults["broker_url"] = broker_url
    if result_backend is not None and getattr(app.conf, "result_backend", None) is None:
        defaults["result_backend"] = result_backend
    if defaults:
        app.add_defaults(defaults)


def _app_uses_vercel_broker_transport(app: Celery) -> bool:
    for transport in _broker_transport_classes(app):
        if not _is_vercel_transport_class(transport):
            continue
        return True
    return False


def _app_uses_queue_registration_transport(app: Celery) -> bool:
    for transport in _broker_transport_classes(app):
        if not _is_vercel_transport_class(transport):
            continue
        channel = transport.Channel
        if issubclass(channel, PushChannel):
            return True
        if issubclass(channel, AutoChannel) and is_vercel_runtime():
            return True
    return False


def _is_vercel_transport_class(transport: TransportClass) -> bool:
    if not issubclass(transport, virtual.Transport):
        return False
    channel = getattr(transport, "Channel", None)
    return isinstance(channel, type) and issubclass(channel, _BaseChannel)


def _configure_app_transport_defaults(app: Celery) -> None:
    _set_app_consumer_group_default(app, _app_consumer_group(app))
    _set_app_queue_name_prefix_default(app, _app_queue_name_prefix(app))


def _app_consumer_group(app: Celery) -> vqs.SanitizedName:
    options = getattr(app.conf, "broker_transport_options", None)
    if isinstance(options, Mapping):
        value = options.get("consumer_group")
        if value is not None:
            return vqs.sanitize_name(str(value))
    return vqs.SanitizedName(DEFAULT_CONSUMER_GROUP)


def _app_queue_name_prefix(app: Celery) -> str:
    options = getattr(app.conf, "broker_transport_options", None)
    if isinstance(options, Mapping) and "queue_name_prefix" in options:
        value = options.get("queue_name_prefix")
        return "" if value is None else str(value)
    main = getattr(app, "main", None)
    if main:
        return f"celery-{main}-"
    return ""


def _set_app_consumer_group_default(
    app: Celery,
    consumer_group: vqs.SanitizedName,
) -> None:
    options = getattr(app.conf, "broker_transport_options", None)
    if isinstance(options, Mapping) and options.get("consumer_group") is not None:
        return
    updated_options = dict(options) if isinstance(options, Mapping) else {}
    updated_options["consumer_group"] = str(consumer_group)
    app.conf.broker_transport_options = updated_options


def _set_app_queue_name_prefix_default(
    app: Celery,
    queue_name_prefix: str,
) -> None:
    options = getattr(app.conf, "broker_transport_options", None)
    if isinstance(options, Mapping) and "queue_name_prefix" in options:
        return
    updated_options = dict(options) if isinstance(options, Mapping) else {}
    updated_options["queue_name_prefix"] = queue_name_prefix
    app.conf.broker_transport_options = updated_options


def _broker_transport_classes(app: Celery) -> Iterator[TransportClass]:
    conf = app.conf
    broker_transport = getattr(conf, "broker_transport", None)
    if isinstance(broker_transport, str):
        if resolved := _resolve_transport_class(broker_transport):
            yield resolved
    elif isinstance(broker_transport, type):
        yield broker_transport

    for name in ("broker_read_url", "broker_write_url", "broker_url"):
        for broker_url in _broker_urls(getattr(conf, name, None)):
            transport: str = urlparse(broker_url).scheme
            if transport and (resolved := _resolve_transport_class(transport)):
                yield resolved


def _broker_urls(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return tuple(part for part in value.split(";") if part)
    if isinstance(value, (list, tuple)):
        urls: list[str] = []
        for item in value:
            urls.extend(_broker_urls(item))
        return tuple(urls)
    return ()


def _resolve_transport_class(transport: str) -> TransportClass | None:
    try:
        resolved = resolve_transport(transport)
    except (AttributeError, ImportError, KeyError, TypeError):
        return None
    return resolved if isinstance(resolved, type) else None


def _install_app_finalize_hook(*, register_queues: bool) -> None:
    _finalize_hook_state.register_queues = _finalize_hook_state.register_queues or register_queues
    if _finalize_hook_state.installed:
        return
    # Celery exposes finalized apps through this internal hook; Celery itself
    # uses it for shared task binding. Installing here lets users define their
    # app normally and still get concrete VQS subscriptions once Celery has
    # synthesized the final queue set.
    celery_state.connect_on_app_finalize(_register_finalized_app_queues)
    _finalize_hook_state.installed = True


__all__ = [
    "__version__",
    "_configure_existing_app_defaults",
    "_register_existing_app_queues",
    "_set_default_broker_set_by_installer",
    "register_celery_app_queues",
]
