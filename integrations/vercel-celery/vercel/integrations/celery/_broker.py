from __future__ import annotations

from typing import Any, ClassVar, cast
from typing_extensions import override

import json
import logging
import os
import threading
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from urllib.parse import urlparse
from weakref import WeakKeyDictionary

from kombu.exceptions import ChannelError
from kombu.transport import resolve_transport, virtual
from kombu.transport.virtual.base import Empty
from kombu.utils import json as kombu_json

import vercel.queue as vqs
import vercel.queue.sync as vqs_sync
from celery import Celery, _state as celery_state
from vercel.headers import HeadersContext, get_headers_context

from .version import __version__

DEFAULT_CONSUMER_GROUP = "celery"
DEFAULT_REQUEUE_DELAY_SECONDS = 0
_EMBEDDED_WORKER_STARTUP_WAIT_SECONDS = 1.0
_QUEUE_LOGGER_NAME = "vercel.queue"
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
LEASE_TRANSPORT_OPTIONS = ("requeue_delay_seconds", "lease_duration")
CONSUMER_TRANSPORT_OPTIONS = ("consumer_group",)
# Celery apps can be short-lived in tests and app factories, so registration
# idempotency is keyed weakly by app object rather than by id(app). The values
# record only VQS-facing queue identity; when the Celery app is collected, its
# idempotency state disappears with it.
_registered_app_queues: WeakKeyDictionary[Celery, set[tuple[str, str]]] = WeakKeyDictionary()


@dataclass
class _EmbeddedWorker:
    worker: Any
    thread: threading.Thread


_embedded_workers: WeakKeyDictionary[Celery, _EmbeddedWorker] = WeakKeyDictionary()
_embedded_workers_lock = threading.RLock()

# vercel.queue stores subscribers weakly. Generated callbacks have no other
# natural owner, so keep strong references here for as long as this integration
# module is loaded.
_registered_callbacks: list[Any] = []

# Push deliveries enter through VQS subscriber callbacks, not Kombu polling. We
# keep live push channels here so a callback can hand the leased VQS message to
# whichever Kombu channel currently has a consumer and prefetch capacity.
_push_channels: list[PushChannel | AutoChannel] = []
_push_channels_lock = threading.RLock()


@dataclass
class _FinalizeHookState:
    installed: bool = False


_finalize_hook_state = _FinalizeHookState()


def debug_log(event: str, **fields: Any) -> None:
    if not _queue_debug_enabled():
        return
    payload = {"event": event, **fields}
    logging.getLogger(_QUEUE_LOGGER_NAME).info(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    )


def _queue_debug_enabled() -> bool:
    return os.environ.get("VERCEL_QUEUE_DEBUG") in {"1", "true"}


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
    headers_context: HeadersContext


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
    )

    token: str | None = None
    region: str | None = None
    base_url: str | None = None
    deployment: vqs.DeploymentOption = vqs.CURRENT_DEPLOYMENT
    timeout: vqs.Duration | None = 10.0
    requeue_delay_seconds: int = DEFAULT_REQUEUE_DELAY_SECONDS
    lease_duration: vqs.Duration | None = None
    retention: vqs.Duration | None = None
    delay: vqs.Duration | None = None
    headers: Mapping[str, str] | None = None
    use_task_id_as_idempotency_key: bool = False
    consumer_group: str = DEFAULT_CONSUMER_GROUP

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.consumer_group = str(vqs.sanitize_name(self.consumer_group))
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

    def _topic(self, queue: str) -> vqs.Topic[dict[str, Any]]:
        return vqs.Topic[dict[str, Any]](
            vqs.sanitize_name(queue),
            transport=self._message_transport,
        )

    def _put(self, queue: str, message: dict[str, Any], **kwargs: Any) -> None:
        # Kombu gives transports its already-normalized message envelope. Store
        # that envelope directly in VQS so receive paths can hand it back to
        # Kombu without reconstructing Celery protocol fields.
        self._queue_client.send(
            self._topic(queue),
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
        message = self._messages_by_tag.get(delivery_tag)
        if message is not None:
            message.headers_context.run(message.queue_client.acknowledge, message.message)
            self._stop_tracking_delivery(delivery_tag)
        if self._qos is not None:
            super().basic_ack(delivery_tag, multiple=multiple)

    @override
    def basic_reject(self, delivery_tag: str, requeue: bool = False) -> None:
        # Celery's reject(requeue=True) maps to making the VQS lease visible
        # again after requeue_delay_seconds. reject(requeue=False) is a terminal
        # disposition, so ACK the VQS lease instead of changing visibility.
        message = self._messages_by_tag.get(delivery_tag)
        if message is not None:
            if requeue:
                message.headers_context.run(
                    message.queue_client.extend_lease,
                    message.message,
                    self.requeue_delay_seconds,
                )
            else:
                message.headers_context.run(message.queue_client.acknowledge, message.message)
            self._stop_tracking_delivery(delivery_tag)
        if self._qos is not None:
            # The VQS follow-up above already handled requeue semantics. Tell
            # Kombu only to remove local QoS bookkeeping for this delivery.
            super().basic_reject(delivery_tag, requeue=False)

    @override
    def basic_get(self, queue: str, no_ack: bool = False, **kwargs: Any) -> Any:
        message = super().basic_get(queue, no_ack=no_ack, **kwargs)
        if message is not None and no_ack:
            # VQS has no server-side no_ack mode. Once Kombu has accepted the
            # delivery locally, delete the VQS lease immediately.
            self.basic_ack(message.delivery_tag)
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
                self.basic_ack(message.delivery_tag)
            return result

        super().basic_consume(
            queue,
            no_ack=no_ack,
            callback=wrapped_callback,
            consumer_tag=consumer_tag,
            **kwargs,
        )

    def _release_failed_no_ack_delivery(self, delivery_tag: str) -> None:
        tracked = self._messages_by_tag.get(delivery_tag)
        if tracked is None:
            return
        try:
            tracked.headers_context.run(
                tracked.queue_client.extend_lease,
                tracked.message,
                self.requeue_delay_seconds,
            )
        finally:
            self._stop_tracking_delivery(delivery_tag)

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
        headers_context: HeadersContext | None = None,
    ) -> dict[str, Any]:
        queue_client = queue_client or self._queue_client
        headers_context = headers_context or get_headers_context()
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
            headers_context=headers_context,
        )
        lease_renewal.__enter__()
        self._messages_by_tag[delivery_tag] = _TrackedDelivery(
            message=tracked_message,
            lease_renewal=lease_renewal,
            queue_client=queue_client,
            headers_context=headers_context,
        )
        return payload

    def _stop_tracking_delivery(self, delivery_tag: str) -> vqs.Message[dict[str, Any]] | None:
        tracked = self._messages_by_tag.pop(delivery_tag, None)
        if tracked is None:
            return None
        tracked.lease_renewal.stop()
        return tracked.message

    def _stop_all_tracked_deliveries(self) -> None:
        for delivery_tag in list(self._messages_by_tag):
            self._stop_tracking_delivery(delivery_tag)

    def close(self) -> None:
        try:
            self._stop_all_tracked_deliveries()
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
        messages = self._queue_client.poll(
            self._topic(queue),
            self.consumer_group,
            limit=1,
            lease_duration=self.lease_duration,
        )
        try:
            delivery = next(messages)
        except StopIteration as exc:
            raise Empty from exc
        return self._track_message(delivery.accept())

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
        # Raising RetryAfter lets the queue SDK update VQS visibility when
        # Kombu has no active callback or its QoS prefetch window is full.
        if queue not in self.connection._callbacks or not self.qos.can_consume():
            debug_log(
                "celery.push_handoff_unavailable",
                queue=queue,
                topic=metadata.topic,
                consumer_group=metadata.consumer_group,
                message_id=metadata.message_id,
                callback_queues=sorted(self.connection._callbacks),
                can_consume=self.qos.can_consume(),
                requeue_delay_seconds=self.requeue_delay_seconds,
            )
            raise vqs.RetryAfter(self.requeue_delay_seconds)

        message = vqs.Message(payload=payload, metadata=metadata)
        headers_context = get_headers_context()
        payload = self._track_message(message, headers_context=headers_context)
        try:
            # _deliver enters Kombu's normal consumer path. That path is now
            # responsible for basic_ack/basic_reject, which in turn ACKs or
            # changes visibility for the tracked VQS lease by delivery tag.
            self.connection._deliver(payload, queue)
        except Exception as exc:
            delivery_tag = self._delivery_tag(payload)
            tracked_message: vqs.Message[dict[str, Any]] | None = message
            if delivery_tag is not None:
                tracked_message = self._stop_tracking_delivery(delivery_tag)
                if self._qos is not None:
                    super().basic_reject(delivery_tag, requeue=False)
            if tracked_message is not None:
                headers_context.run(
                    self._queue_client.extend_lease,
                    tracked_message,
                    self.requeue_delay_seconds,
                )
            debug_log(
                "celery.push_handoff_failed",
                queue=queue,
                topic=metadata.topic,
                consumer_group=metadata.consumer_group,
                message_id=metadata.message_id,
                callback_queues=sorted(self.connection._callbacks),
                exception_class=exc.__class__.__name__,
                exception_message=str(exc),
                requeue_delay_seconds=self.requeue_delay_seconds,
            )
            raise
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
        if queue in channel.connection._callbacks and channel.qos.can_consume():
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
        if queue in channel.connection._callbacks:
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
        channel = _find_push_channel(queue, str(message.metadata.consumer_group))
        if channel is None:
            raise vqs.RetryAfter(DEFAULT_REQUEUE_DELAY_SECONDS)
        channel._handle_queue_delivery(
            message.payload,
            message.metadata,
            queue=queue,
        )

    handle_queue_delivery.__name__ = f"vercel_celery_{queue}_subscriber"
    return handle_queue_delivery


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
        _embedded_workers[app] = _EmbeddedWorker(worker=worker, thread=thread)
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
    return any(getattr(channel, "closed", True) is False for channel in tuple(_push_channels))


def _register_app_queue(app: Celery, queue: str, consumer_group: vqs.SanitizedName) -> None:
    app_queues = _registered_app_queues.setdefault(app, set())
    topic = vqs.sanitize_name(queue)
    key = (str(topic), str(consumer_group))
    if key in app_queues:
        return
    callback = _make_queue_callback(queue)
    vqs.subscribe(
        topic=vqs.Topic(topic, transport=_KombuMessageTransport()),
        consumer_group=consumer_group,
    )(callback)
    _registered_callbacks.append(callback)
    app_queues.add(key)


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
    consumer_group = _app_consumer_group(app)
    _set_app_consumer_group_default(app, consumer_group)
    for queue_name in _app_queue_names(app):
        _register_app_queue(app, queue_name, consumer_group)
    if start_worker:
        _start_embedded_worker(app)


def _register_finalized_app_queues(app: Celery) -> None:
    if _app_uses_queue_registration_transport(app):
        register_celery_app_queues(app)


def _register_app_queues_if_eligible(app: Celery) -> None:
    if _app_uses_queue_registration_transport(app):
        register_celery_app_queues(app)


def _register_existing_app_queues() -> None:
    for app in celery_state._get_active_apps():
        _register_app_queues_if_eligible(app)


def _app_uses_queue_registration_transport(app: Celery) -> bool:
    for transport in _broker_transport_classes(app):
        if not issubclass(transport, virtual.Transport):
            continue
        if transport.Channel is PushChannel:
            return True
        if transport.Channel is AutoChannel and is_vercel_runtime():
            return True
    return False


def _app_consumer_group(app: Celery) -> vqs.SanitizedName:
    options = getattr(app.conf, "broker_transport_options", None)
    if isinstance(options, Mapping):
        value = options.get("consumer_group")
        if value is not None:
            return vqs.sanitize_name(str(value))
    main = getattr(app, "main", None)
    if main:
        return vqs.sanitize_name(f"celery-{main}")
    return vqs.SanitizedName(DEFAULT_CONSUMER_GROUP)


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


def _install_app_finalize_hook() -> None:
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
    "_register_existing_app_queues",
    "register_celery_app_queues",
]
