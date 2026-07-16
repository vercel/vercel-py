from __future__ import annotations

from typing import Any
from typing_extensions import TypedDict, Unpack

import logging
import os
import queue
import threading
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from weakref import WeakKeyDictionary

import dramatiq
import dramatiq.broker as dramatiq_broker
import vercel.cache as vcache
import vercel.queue as vqs
import vercel.queue.sync as vqs_sync
from dramatiq.broker import Broker, Consumer, MessageProxy
from dramatiq.common import current_millis, dq_name
from dramatiq.errors import QueueNotFound
from dramatiq.message import Message as DramatiqMessage
from dramatiq.results import Results
from dramatiq.results.backend import ResultBackend
from dramatiq.worker import Worker

from ._result_backend import VercelRuntimeCacheBackend
from .version import __version__

DEFAULT_CONSUMER_GROUP = "dramatiq"
DEFAULT_REQUEUE_DELAY_SECONDS = 0
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
        self._register_queue_callback(queue_name)

        delayed_name = dq_name(queue_name)
        self.queues[delayed_name] = object()
        self.dead_letters_by_queue.setdefault(delayed_name, [])
        self.delay_queues.add(delayed_name)
        self.emit_after("declare_delay_queue", delayed_name)
        self._register_queue_callback(delayed_name)

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
            self.topic_for_queue(queue_name),
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
        deliveries = self._queue_client.poll(
            self.topic_for_queue(queue_name),
            self.consumer_group,
            limit=1,
            lease_duration=self.lease_duration,
        )
        for delivery in deliveries:
            yield delivery.accept()

    def start_lease_renewal(self, message: vqs.Message[bytes]) -> vqs.LeaseRenewal:
        lease_renewal = self._queue_client.run_lease_renewal(
            message,
            lease_duration=self.lease_duration,
        )
        lease_renewal.start()
        return lease_renewal

    def topic_for_queue(self, queue_name: str) -> vqs.Topic[bytes]:
        return vqs.Topic[bytes](
            vqs.sanitize_name(f"{self.queue_name_prefix}{queue_name}"),
        )

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
        for queue_name in self.queues:
            if self.topic_for_queue(queue_name).name == topic:
                return queue_name
        return None

    def register_queue_callbacks(self) -> None:
        queue_names = sorted(
            self.get_declared_queues(),
            key=lambda queue_name: (queue_name.endswith(".DQ"), queue_name),
        )
        for queue_name in queue_names:
            self._register_queue_callback(queue_name)

    def _register_queue_callback(self, queue_name: str) -> None:
        if queue_name in self._registered_callbacks:
            return
        topic = self.topic_for_queue(queue_name)

        def handle_queue_delivery(
            message: vqs.Message[bytes],
            *,
            broker: VercelQueueBroker = self,
        ) -> None:
            vcache.prime_runtime_cache()
            broker.handle_push_message(message)

        handle_queue_delivery.__name__ = f"vercel_dramatiq_{topic.name}_subscriber"
        vqs.subscribe(
            topic=topic,
            consumer_group=self.consumer_group,
            retry_after=self.push_retry_delay_seconds,
        )(handle_queue_delivery)
        self._registered_callbacks[queue_name] = handle_queue_delivery


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
