from __future__ import annotations

from typing import Any
from typing_extensions import TypedDict, Unpack

import logging
import math
import os
import queue
import threading
import time
import warnings
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from weakref import WeakKeyDictionary, ref

import dramatiq
import dramatiq.broker as dramatiq_broker
import vercel.cache as vcache
import vercel.queue as vqs
import vercel.queue.sync as vqs_sync
from dramatiq.broker import Broker, Consumer, MessageProxy
from dramatiq.common import current_millis, dq_name, q_name
from dramatiq.errors import ActorNotFound, QueueNotFound
from dramatiq.message import Message as DramatiqMessage
from dramatiq.middleware import TimeLimit
from dramatiq.results import Results
from dramatiq.results.backend import ResultBackend
from dramatiq.worker import Worker

from ._result_backend import VercelRuntimeCacheBackend
from .version import __version__

DEFAULT_CONSUMER_GROUP = "dramatiq"
DEFAULT_REQUEUE_DELAY_SECONDS = 0

# Bounds enforced by vercel.queue's subscribe(max_duration=...). Duplicated
# here so misconfigured limits fail at declaration with actionable context.
_MAX_DURATION_MINIMUM_SECONDS = 1
_MAX_DURATION_MAXIMUM_SECONDS = 1800
DEFAULT_PUSH_RETRY_DELAY_SECONDS = 1
DEFAULT_PUSH_HANDOFF_WAIT_SECONDS = 30.0
_PUSH_WAIT_POLL_INTERVAL_SECONDS = 0.05
_PUSH_SETTLE_POLL_INTERVAL_SECONDS = 0.01
_DEBUG_ENV = "VERCEL_DRAMATIQ_DEBUG"
_DEBUG_LOGGER_NAMES = (
    "vercel.integrations.dramatiq",
    "dramatiq",
    "dramatiq.broker",
    "dramatiq.worker",
)


def _capped_limit_seconds(limit_seconds: int, source: str) -> int:
    if limit_seconds <= _MAX_DURATION_MAXIMUM_SECONDS:
        return limit_seconds
    warnings.warn(
        f"{source} exceeds the {_MAX_DURATION_MAXIMUM_SECONDS}s maximum "
        f"supported by deployed functions; capping max_duration at "
        f"{_MAX_DURATION_MAXIMUM_SECONDS}s",
        stacklevel=2,
    )
    return _MAX_DURATION_MAXIMUM_SECONDS


def _max_duration_option_seconds(value: object) -> float | None:
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    return None


def _validate_max_duration_option(option: object) -> None:
    if option is None:
        return
    entries: Iterable[tuple[object, object]] = (
        option.items() if isinstance(option, Mapping) else ((None, option),)
    )
    for queue_name, value in entries:
        seconds = _max_duration_option_seconds(value)
        if (
            seconds is None
            or seconds < _MAX_DURATION_MINIMUM_SECONDS
            or seconds > _MAX_DURATION_MAXIMUM_SECONDS
        ):
            where = f" for queue {queue_name!r}" if queue_name is not None else ""
            raise ValueError(
                f"max_duration{where} must be a number of seconds or timedelta "
                f"between {_MAX_DURATION_MINIMUM_SECONDS} and "
                f"{_MAX_DURATION_MAXIMUM_SECONDS} seconds, got {value!r}"
            )


def _is_vercel_runtime() -> bool:
    try:
        value = os.environ["VERCEL"]
    except KeyError:
        return False
    return value.strip().casefold() in {"1", "yes", "on", "true"}


def _dramatiq_debug_enabled() -> bool:
    return os.environ.get(_DEBUG_ENV, "").strip().casefold() in {"1", "yes", "on", "true"}


def _configure_dramatiq_debug_logging() -> None:
    if not _dramatiq_debug_enabled():
        return
    for logger_name in _DEBUG_LOGGER_NAMES:
        logging.getLogger(logger_name).setLevel(logging.DEBUG)


class VercelQueueBrokerOptions(TypedDict, total=False):
    token: str | None
    """Vercel API token used by the underlying Vercel Queue client."""

    region: str | None
    """Vercel region used by the underlying Vercel Queue client."""

    base_url: str | None
    """Override base URL for the underlying Vercel Queue client."""

    deployment: vqs.DeploymentOption
    """Deployment partition used when sending and receiving Vercel Queue messages."""

    timeout: vqs.Duration | None
    """Request timeout used by the underlying Vercel Queue client."""

    headers: Mapping[str, str] | None
    """Additional headers sent by the underlying Vercel Queue client."""

    consumer_group: str
    """Vercel Queue consumer group used for subscriptions and polling."""

    retention: vqs.Duration | None
    """Optional retention duration applied to enqueued Vercel Queue messages."""

    lease_duration: vqs.Duration | None
    """Optional lease duration for polled or pushed Vercel Queue messages."""

    requeue_delay_seconds: int
    """Visibility delay used when Dramatiq requeues or rejects a message."""

    push_retry_delay_seconds: int
    """Visibility delay used when push delivery cannot be handed to a worker."""

    push_handoff_wait_seconds: float
    """Maximum request-time wait for push worker readiness and settlement."""

    queue_name_prefix: str | None
    """Prefix applied to Dramatiq queue names before VQS topic sanitization."""

    use_message_id_as_idempotency_key: bool
    """Use Dramatiq message IDs as VQS idempotency keys when publishing."""

    poll: bool
    """Force polling mode when true or push mode when false."""

    max_duration: vqs.Duration | Mapping[str, vqs.Duration] | None
    """Execution time limit for the deployed functions serving Dramatiq queues."""

    middleware: list[Any] | None
    """Dramatiq middleware list passed to the base broker."""


@dataclass
class _TrackedMessage:
    message: vqs.Message[bytes]
    lease_renewal: vqs.LeaseRenewal
    settlement: threading.Event


class _VercelQueueMessageProxy(MessageProxy):
    def __init__(
        self,
        message: DramatiqMessage[Any],
        *,
        queue_name: str,
        delivery: vqs.Message[bytes],
        lease_renewal: vqs.LeaseRenewal,
    ) -> None:
        super().__init__(message)
        self.settlement = threading.Event()
        self._queue_name = queue_name
        self._tracked = _TrackedMessage(
            message=delivery,
            lease_renewal=lease_renewal,
            settlement=self.settlement,
        )

    def stop_lease_renewal(self) -> None:
        self._tracked.lease_renewal.stop()

    @property
    def vqs_message(self) -> vqs.Message[bytes]:
        return self._tracked.message


class _VercelQueueConsumer(Consumer):
    def __init__(
        self,
        broker: VercelQueueBroker,
        queue_name: str,
        prefetch: int,
        timeout: int,
    ) -> None:
        self.broker = broker
        self.queue_name = queue_name
        self.prefetch = prefetch
        self.timeout = timeout
        self._slots = threading.BoundedSemaphore(prefetch)
        self._push_queue: queue.Queue[_VercelQueueMessageProxy] = queue.Queue()
        self._closed = False
        self._lock = threading.Lock()

    def ack(self, message: MessageProxy) -> None:
        proxy = self._as_proxy(message)
        try:
            self.broker.acknowledge_message(proxy.vqs_message)
        finally:
            self._finish_message(proxy)

    def nack(self, message: MessageProxy) -> None:
        proxy = self._as_proxy(message)
        try:
            self.broker.acknowledge_message(proxy.vqs_message)
            self.broker.dead_letters_by_queue[self.queue_name].append(proxy)
        finally:
            self._finish_message(proxy)

    def requeue(self, messages: Iterable[MessageProxy]) -> None:
        for message in messages:
            proxy = self._as_proxy(message)
            try:
                self.broker.extend_message_lease(proxy.vqs_message)
            finally:
                self._finish_message(proxy)

    def __next__(self) -> MessageProxy | None:
        if self._closed:
            return None
        if not self._slots.acquire(timeout=self.timeout / 1000):
            return None

        try:
            delivery = self._next_delivery()
            if delivery is None:
                self._slots.release()
                return None
            if isinstance(delivery, _VercelQueueMessageProxy):
                return delivery
            return self._wrap_delivery(delivery)
        except Exception:
            self._slots.release()
            raise

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self.broker.remove_consumer(self)

    @property
    def closed(self) -> bool:
        return self._closed

    def can_accept_push(self) -> bool:
        return not self._closed and self._push_queue.qsize() < self.prefetch

    def put_push_delivery(self, delivery: vqs.Message[bytes]) -> _VercelQueueMessageProxy | None:
        if not self.can_accept_push():
            return None
        proxy = self._wrap_delivery(delivery)
        self._push_queue.put(proxy)
        return proxy

    def _next_delivery(self) -> _VercelQueueMessageProxy | vqs.Message[bytes] | None:
        try:
            return self._push_queue.get_nowait()
        except queue.Empty:
            pass

        if not self.broker.poll:
            try:
                return self._push_queue.get(timeout=self.timeout / 1000)
            except queue.Empty:
                return None

        for delivery in self.broker.poll_messages(self.queue_name, self.prefetch):
            return delivery
        return None

    def _wrap_delivery(self, delivery: vqs.Message[bytes]) -> _VercelQueueMessageProxy:
        message = DramatiqMessage.decode(delivery.payload)
        lease_renewal = self.broker.start_lease_renewal(delivery)
        return _VercelQueueMessageProxy(
            message,
            queue_name=self.queue_name,
            delivery=delivery,
            lease_renewal=lease_renewal,
        )

    def _finish_message(self, message: _VercelQueueMessageProxy) -> None:
        try:
            message.stop_lease_renewal()
        finally:
            message.settlement.set()
            self._slots.release()

    @staticmethod
    def _as_proxy(message: MessageProxy) -> _VercelQueueMessageProxy:
        if not isinstance(message, _VercelQueueMessageProxy):
            raise TypeError("message was not produced by this consumer")
        return message


class VercelQueueBroker(Broker):
    def __init__(
        self,
        **options: Unpack[VercelQueueBrokerOptions],
    ) -> None:
        """Create a Dramatiq broker backed by Vercel Queue Service.

        Queue client options such as ``token``, ``region``, ``base_url``,
        ``deployment``, ``timeout``, and ``headers`` are forwarded to the
        underlying Vercel Queue client. Broker-specific options control the
        consumer group, message retention, lease duration, requeue delay, and
        Dramatiq middleware.

        By default, the broker uses push delivery when ``VERCEL`` is truthy in
        the environment and poll delivery otherwise. Pass ``poll=False`` or
        ``poll=True`` to force a delivery mode.
        """
        token = options.get("token")
        region = options.get("region")
        base_url = options.get("base_url")
        deployment = options.get("deployment", vqs.CURRENT_DEPLOYMENT)
        timeout = options.get("timeout", 10.0)
        headers = options.get("headers")
        middleware = options.get("middleware")
        super().__init__(middleware=middleware)
        _configure_dramatiq_debug_logging()
        self.max_duration = options.get("max_duration")
        _validate_max_duration_option(self.max_duration)
        self._framework_time_limit_middleware: TimeLimit | None = None
        self._framework_time_limit_ms: float | None = None
        if middleware is None:
            # Remember the ``TimeLimit`` instance (and value) that dramatiq's
            # default middleware set installed, so its built-in limit is not
            # mistaken for user intent when deriving ``max_duration``.
            for entry in self.middleware:
                if isinstance(entry, TimeLimit):
                    self._framework_time_limit_middleware = entry
                    self._framework_time_limit_ms = entry.time_limit
                    break
        self.queues: dict[str, object] = {}
        self.consumer_group = options.get("consumer_group", DEFAULT_CONSUMER_GROUP)
        self.retention = options.get("retention")
        self.lease_duration = options.get("lease_duration")
        self.requeue_delay_seconds = options.get(
            "requeue_delay_seconds",
            DEFAULT_REQUEUE_DELAY_SECONDS,
        )
        self.push_retry_delay_seconds = options.get(
            "push_retry_delay_seconds",
            DEFAULT_PUSH_RETRY_DELAY_SECONDS,
        )
        self.push_handoff_wait_seconds = options.get(
            "push_handoff_wait_seconds",
            DEFAULT_PUSH_HANDOFF_WAIT_SECONDS,
        )
        self.queue_name_prefix = options.get("queue_name_prefix") or ""
        self.use_message_id_as_idempotency_key = options.get(
            "use_message_id_as_idempotency_key",
            False,
        )
        self.poll = options.get("poll", not _is_vercel_runtime())
        self.dead_letters_by_queue: dict[str, list[MessageProxy]] = {}
        self._queue_client = vqs_sync.QueueClient(
            token=token,
            region=region,
            base_url=base_url,
            deployment=deployment,
            headers=headers,
            timeout=timeout,
        )
        self._consumers: list[_VercelQueueConsumer] = []
        self._consumers_lock = threading.RLock()
        self._registered_callbacks: dict[str, Any] = {}
        self._registered_topics: dict[str, tuple[vqs.Topic[bytes], str]] = {}
        self._subscriptions_lock = threading.RLock()
        self._poll_rotation: dict[str, int] = {}
        self._push_handoff_lock = threading.RLock()

    @property
    def dead_letters(self) -> list[MessageProxy]:
        return [message for messages in self.dead_letters_by_queue.values() for message in messages]

    def consume(self, queue_name: str, prefetch: int = 1, timeout: int = 30000) -> Consumer:
        if queue_name not in self.queues:
            raise QueueNotFound(queue_name)
        consumer = _VercelQueueConsumer(self, queue_name, prefetch, timeout)
        with self._consumers_lock:
            self._consumers.append(consumer)
        return consumer

    def declare_queue(self, queue_name: str) -> None:
        if queue_name in self.queues:
            return

        self.emit_before("declare_queue", queue_name)
        self.queues[queue_name] = object()
        self.dead_letters_by_queue.setdefault(queue_name, [])
        self.emit_after("declare_queue", queue_name)

        delayed_name = dq_name(queue_name)
        self.queues[delayed_name] = object()
        self.dead_letters_by_queue.setdefault(delayed_name, [])
        self.delay_queues.add(delayed_name)
        self.emit_after("declare_delay_queue", delayed_name)
        self._sync_queue_subscriptions()

    def declare_actor(self, actor: Any) -> None:
        # Warn about capped time limits once per actor, at declaration.
        self._explicit_actor_limit_seconds(actor, warn=True)
        super().declare_actor(actor)
        self._sync_queue_subscriptions()

    def enqueue(
        self,
        message: DramatiqMessage[Any],
        *,
        delay: int | None = None,
    ) -> DramatiqMessage[Any]:
        queue_name = message.queue_name
        vqs_delay: vqs.Duration | None = None
        if delay is not None:
            queue_name = dq_name(queue_name)
            message = message.copy(
                queue_name=queue_name,
                options={"eta": current_millis() + delay},
            )
            vqs_delay = delay / 1000

        if queue_name not in self.queues:
            raise QueueNotFound(queue_name)

        self.emit_before("enqueue", message, delay)
        self._queue_client.send(
            self._delivery_topic(message, queue_name),
            message.encode(),
            idempotency_key=(
                message.message_id if self.use_message_id_as_idempotency_key else None
            ),
            retention=self.retention,
            delay=vqs_delay,
        )
        self.emit_after("enqueue", message, delay)
        return message

    def close(self) -> None:
        for consumer in list(self._consumers):
            consumer.close()

    def flush(self, queue_name: str) -> None:
        del queue_name
        raise NotImplementedError("Vercel Queue Service does not support queue purge")

    def flush_all(self) -> None:
        raise NotImplementedError("Vercel Queue Service does not support queue purge")

    def join(self, queue_name: str, *, timeout: int | None = None) -> None:
        del queue_name, timeout
        raise NotImplementedError("Vercel Queue Service does not support queue join")

    def acknowledge_message(self, message: vqs.Message[bytes]) -> None:
        self._queue_client.acknowledge(message)

    def extend_message_lease(self, message: vqs.Message[bytes]) -> None:
        self._queue_client.retry_after(message, self.requeue_delay_seconds)

    def poll_messages(self, queue_name: str, prefetch: int) -> Iterable[vqs.Message[bytes]]:
        del prefetch
        topics = self._topics_for_queue(queue_name)
        # Rotation queue topic for each config so that the same busy topic can't be
        # continually selected when other topics may have messages.
        start = self._poll_rotation.get(queue_name, 0) % len(topics)
        self._poll_rotation[queue_name] = start + 1
        for topic in topics[start:] + topics[:start]:
            deliveries = self._queue_client.poll(
                topic,
                self.consumer_group,
                limit=1,
                lease_duration=self.lease_duration,
            )
            for delivery in deliveries:
                yield delivery.accept()
                return

    def start_lease_renewal(self, message: vqs.Message[bytes]) -> vqs.LeaseRenewal:
        lease_renewal = self._queue_client.run_lease_renewal(
            message,
            lease_duration=self.lease_duration,
        )
        lease_renewal.start()
        return lease_renewal

    def topic_for_queue(
        self,
        queue_name: str,
        limit_seconds: int | None = None,
    ) -> vqs.Topic[bytes]:
        base = vqs.sanitize_name(f"{self.queue_name_prefix}{queue_name}")
        if limit_seconds is None:
            return vqs.Topic[bytes](base)
        return vqs.Topic[bytes](vqs.SanitizedName(f"{base}_d{limit_seconds}"))

    def max_duration_for_queue(self, queue_name: str) -> vqs.Duration | None:
        parent_queue = q_name(queue_name)
        option = self.max_duration
        if option is not None:
            if isinstance(option, (int, float, timedelta)):
                return option
            value = option.get(parent_queue)
            if value is not None:
                return value
        limit_ms = self._user_time_limit_ms()
        if limit_ms is None or not math.isfinite(limit_ms) or limit_ms <= 0:
            return None
        return _capped_limit_seconds(
            math.ceil(limit_ms / 1000),
            f"TimeLimit middleware time_limit={limit_ms!r} ms",
        )

    def _user_time_limit_ms(self) -> float | None:
        limits: list[float] = []
        for entry in self.middleware:
            if not isinstance(entry, TimeLimit):
                continue
            if (
                entry is self._framework_time_limit_middleware
                and entry.time_limit == self._framework_time_limit_ms
            ):
                continue
            limits.append(entry.time_limit)
        if not limits:
            return None
        # Every TimeLimit middleware enforces its own limit, so the smallest
        # one is the effective default.
        return min(limits)

    def _explicit_actor_limit_seconds(self, actor: Any, *, warn: bool = False) -> int | None:
        options = getattr(actor, "options", None)
        if not isinstance(options, Mapping):
            return None
        limit_ms = options.get("time_limit")
        if (
            limit_ms is None
            or isinstance(limit_ms, bool)
            or not isinstance(limit_ms, (int, float))
            or not math.isfinite(limit_ms)
            or limit_ms <= 0
        ):
            # An explicitly unlimited (or nonsensical) actor stays in the
            # queue's default group; there is no bound to shard on.
            return None
        limit_seconds = math.ceil(limit_ms / 1000)
        if warn:
            return _capped_limit_seconds(
                limit_seconds,
                f"actor {getattr(actor, 'actor_name', actor)!r} time_limit={limit_ms!r} ms",
            )
        return min(limit_seconds, _MAX_DURATION_MAXIMUM_SECONDS)

    def _queue_subscription_groups(self, queue_name: str) -> list[int | None]:
        shard_limits: set[int] = set()
        has_default_group = False
        for actor in list(self.actors.values()):
            if actor.queue_name != queue_name:
                continue
            limit = self._explicit_actor_limit_seconds(actor)
            if limit is None:
                has_default_group = True
            else:
                shard_limits.add(limit)
        groups: list[int | None] = [None] if has_default_group else []
        groups.extend(sorted(shard_limits))
        return groups

    def _delivery_topic(
        self,
        message: DramatiqMessage[Any],
        queue_name: str,
    ) -> vqs.Topic[bytes]:
        actor = self.actors.get(message.actor_name)
        if actor is not None:
            limit = self._explicit_actor_limit_seconds(actor)
            return self.topic_for_queue(queue_name, limit)
        base = self.topic_for_queue(queue_name)
        with self._subscriptions_lock:
            queue_is_subscribed = any(
                mapped_queue == queue_name for _, mapped_queue in self._registered_topics.values()
            )
            base_registered = str(base.name) in self._registered_callbacks
        if queue_is_subscribed and not base_registered:
            raise ActorNotFound(message.actor_name)
        return base

    def _topics_for_queue(self, queue_name: str) -> list[vqs.Topic[bytes]]:
        with self._subscriptions_lock:
            topics = [
                topic
                for topic, mapped_queue in self._registered_topics.values()
                if mapped_queue == queue_name
            ]
        # A queue can be consumed before any subscription is registered
        # (e.g. declared without actors); poll its base topic.
        return topics or [self.topic_for_queue(queue_name)]

    def remove_consumer(self, consumer: _VercelQueueConsumer) -> None:
        with self._consumers_lock:
            if consumer in self._consumers:
                self._consumers.remove(consumer)

    def handle_push_message(self, delivery: vqs.Message[bytes]) -> None:
        queue_name = self._queue_for_topic(delivery.metadata.topic)
        if queue_name is None:
            raise vqs.RetryAfter(self.push_retry_delay_seconds)

        deadline = time.monotonic() + max(self.push_handoff_wait_seconds, 0.0)
        if not self._acquire_push_handoff_lock(deadline):
            raise vqs.RetryAfter(self.push_retry_delay_seconds)
        try:
            proxy = self._handoff_push_delivery(queue_name, delivery, deadline)
            if proxy is None:
                raise vqs.RetryAfter(self.push_retry_delay_seconds)
        finally:
            self._push_handoff_lock.release()
        self._wait_for_push_settlement(proxy)
        raise vqs.Handoff

    def _acquire_push_handoff_lock(self, deadline: float) -> bool:
        timeout = deadline - time.monotonic()
        if timeout <= 0:
            return self._push_handoff_lock.acquire(blocking=False)
        return self._push_handoff_lock.acquire(timeout=timeout)

    def _handoff_push_delivery(
        self,
        queue_name: str,
        delivery: vqs.Message[bytes],
        deadline: float,
    ) -> _VercelQueueMessageProxy | None:
        while True:
            with self._consumers_lock:
                consumers = tuple(self._consumers)
            for consumer in reversed(consumers):
                if consumer.queue_name != queue_name:
                    continue
                proxy = consumer.put_push_delivery(delivery)
                if proxy is not None:
                    return proxy
            if time.monotonic() >= deadline:
                return None
            time.sleep(_PUSH_WAIT_POLL_INTERVAL_SECONDS)

    def _wait_for_push_settlement(
        self,
        proxy: _VercelQueueMessageProxy,
    ) -> None:
        proxy.settlement.wait()

    def _queue_for_topic(self, topic: str) -> str | None:
        registered = self._registered_topics.get(topic)
        if registered is not None:
            return registered[1]
        # Base topics stay resolvable even before their subscription exists.
        for queue_name in self.queues:
            if self.topic_for_queue(queue_name).name == topic:
                return queue_name
        return None

    def register_queue_callbacks(self) -> None:
        self._sync_queue_subscriptions()

    def _sync_queue_subscriptions(self) -> None:
        with self._subscriptions_lock:
            parent_queues = sorted(
                queue_name
                for queue_name in self.get_declared_queues()
                if q_name(queue_name) == queue_name
            )
            desired: set[str] = set()
            for queue_name in parent_queues:
                for limit in self._queue_subscription_groups(queue_name):
                    desired.add(self._register_queue_callback(queue_name, limit))
                    desired.add(self._register_queue_callback(dq_name(queue_name), limit))
            for topic_key in [key for key in self._registered_topics if key not in desired]:
                del self._registered_callbacks[topic_key]
                del self._registered_topics[topic_key]

    def _register_queue_callback(
        self,
        queue_name: str,
        limit_seconds: int | None = None,
    ) -> str:
        topic = self.topic_for_queue(queue_name, limit_seconds)
        topic_key = str(topic.name)
        if topic_key in self._registered_callbacks:
            return topic_key

        def handle_queue_delivery(
            message: vqs.Message[bytes],
            *,
            broker: VercelQueueBroker = self,
        ) -> None:
            vcache.prime_runtime_cache()
            broker.handle_push_message(message)

        handle_queue_delivery.__name__ = f"vercel_dramatiq_{topic.name}_subscriber"

        max_duration: vqs.MaxDuration | None
        if limit_seconds is None:
            # TimeLimit middleware may be configured after this registration
            # runs, so resolve the default group's limit at introspection
            # time, referencing the broker weakly to not keep it alive.
            broker_ref = ref(self)

            def resolve_max_duration() -> vqs.Duration | None:
                broker = broker_ref()
                if broker is None:
                    return None
                return broker.max_duration_for_queue(queue_name)

            max_duration = resolve_max_duration
        else:
            max_duration = limit_seconds

        vqs.subscribe(
            topic=topic,
            consumer_group=self.consumer_group,
            retry_after=self.push_retry_delay_seconds,
            max_duration=max_duration,
        )(handle_queue_delivery)
        self._registered_callbacks[topic_key] = handle_queue_delivery
        self._registered_topics[topic_key] = (topic, queue_name)
        return topic_key


@dataclass
class _EmbeddedWorker:
    broker: VercelQueueBroker
    worker: Worker


_embedded_workers: WeakKeyDictionary[VercelQueueBroker, _EmbeddedWorker] = WeakKeyDictionary()
_embedded_workers_lock = threading.RLock()


def register_dramatiq_queues(
    *,
    broker: VercelQueueBroker | None = None,
    start_worker: bool = True,
) -> None:
    """Register declared Dramatiq queues as VQS subscribers for push delivery."""
    resolved_broker = broker or dramatiq.get_broker()
    if not isinstance(resolved_broker, VercelQueueBroker):
        raise TypeError("Dramatiq queue registration requires VercelQueueBroker")
    broker = resolved_broker
    broker.register_queue_callbacks()
    if start_worker:
        _start_embedded_worker(broker)


def _start_embedded_worker(broker: VercelQueueBroker) -> None:
    _configure_dramatiq_debug_logging()
    with _embedded_workers_lock:
        if broker in _embedded_workers:
            return
        queues = broker.get_declared_queues() - broker.get_declared_delay_queues()
        worker = Worker(broker, queues=queues, worker_threads=1, worker_timeout=100)
        worker.start()
        _embedded_workers[broker] = _EmbeddedWorker(broker=broker, worker=worker)


def install_vercel_dramatiq_integration(
    *,
    set_default_broker: bool = True,
    install_results_backend: bool = True,
    results_backend: ResultBackend | None = None,
    **broker_options: Unpack[VercelQueueBrokerOptions],
) -> None:
    """Install Vercel Queue Service as Dramatiq's default broker.

    When ``set_default_broker`` is true, this sets Dramatiq's global broker to
    a new ``VercelQueueBroker`` only if no broker has been configured yet. Any
    additional keyword arguments are passed to ``VercelQueueBroker``; use the
    ``poll`` option there to override automatic push/poll selection.
    """
    if set_default_broker and dramatiq_broker.global_broker is None:
        broker = VercelQueueBroker(**broker_options)
        if install_results_backend:
            broker.add_middleware(Results(backend=results_backend or VercelRuntimeCacheBackend()))
        dramatiq.set_broker(broker)


__all__ = [
    "VercelQueueBroker",
    "VercelRuntimeCacheBackend",
    "__version__",
    "install_vercel_dramatiq_integration",
    "register_dramatiq_queues",
]
