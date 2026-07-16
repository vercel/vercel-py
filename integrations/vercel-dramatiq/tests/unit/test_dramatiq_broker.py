from __future__ import annotations

from typing import Any, ClassVar, cast
from typing_extensions import Self

import logging
import threading
import time
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import dramatiq
import dramatiq.broker as dramatiq_broker
import pytest
from dramatiq.broker import MessageProxy, Middleware
from dramatiq.errors import QueueNotFound
from dramatiq.message import Message as DramatiqMessage
from dramatiq.results import Results

import vercel.integrations.dramatiq._broker as vqs_dramatiq
from vercel.queue import Duration, Handoff, Message, MessageMetadata, RetryAfter, Topic


@dataclass
class FakeSubscription:
    topic: object
    consumer_group: str
    callback: Any


@dataclass
class FakeLeaseRenewal:
    message: Message[bytes]
    lease_duration: Duration | None
    entered: int = 0
    stopped: int = 0

    def __enter__(self) -> Self:
        self.start()
        return self

    def start(self) -> None:
        self.entered += 1

    def stop(self) -> None:
        self.stopped += 1


@dataclass(frozen=True)
class FakeDelivery:
    message: Message[bytes]

    def accept(self) -> Message[bytes]:
        return self.message


class FakeSyncQueueClient:
    instances: ClassVar[list[FakeSyncQueueClient]] = []
    subscriptions: ClassVar[list[FakeSubscription]] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.sent: list[dict[str, Any]] = []
        self.message_batches: list[list[Message[bytes]]] = []
        self.acknowledged: list[MessageMetadata] = []
        self.visibility_changes: list[tuple[MessageMetadata, Duration]] = []
        self.lease_renewals: list[FakeLeaseRenewal] = []
        self.closed = False
        FakeSyncQueueClient.instances.append(self)

    def send(self, topic: Topic[bytes], payload: bytes, **kwargs: Any) -> None:
        self.sent.append({"topic": topic, "payload": payload, "kwargs": kwargs})

    def poll(
        self,
        topic: Topic[bytes],
        consumer_group: str,
        **kwargs: Any,
    ) -> Iterator[FakeDelivery]:
        self.last_poll = {"topic": topic, "consumer_group": consumer_group, "kwargs": kwargs}
        for message in self.message_batches.pop(0) if self.message_batches else []:
            yield FakeDelivery(message)

    def accept_and_handle(
        self,
        raw_body: bytes | Iterable[bytes],
        headers: Mapping[str, str],
        **kwargs: Any,
    ) -> None:
        del kwargs
        payload = raw_body if isinstance(raw_body, bytes) else b"".join(raw_body)
        message = fake_vqs_message(payload, topic=headers.get("topic", "emails"))
        for subscription in self.subscriptions:
            topic = topic_name(subscription.topic)
            if topic == message.metadata.topic:
                try:
                    subscription.callback(message)
                except Handoff:
                    pass
                return

    def acknowledge(self, message: Message[bytes] | MessageMetadata) -> None:
        self.acknowledged.append(message.metadata if isinstance(message, Message) else message)

    def extend_lease(self, message: Message[bytes] | MessageMetadata, duration: Duration) -> None:
        metadata = message.metadata if isinstance(message, Message) else message
        self.visibility_changes.append((metadata, duration))

    def retry_after(self, message: Message[bytes] | MessageMetadata, delay: Duration) -> None:
        metadata = message.metadata if isinstance(message, Message) else message
        self.visibility_changes.append((metadata, delay))

    def run_lease_renewal(
        self,
        message: Message[bytes],
        lease_duration: Duration | None = None,
    ) -> FakeLeaseRenewal:
        renewal = FakeLeaseRenewal(message=message, lease_duration=lease_duration)
        self.lease_renewals.append(renewal)
        return renewal

    def close(self) -> None:
        self.closed = True


class RecordingMiddleware(Middleware):
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    def before_declare_queue(self, broker: Any, queue_name: str) -> None:
        self.events.append(("before_declare_queue", queue_name))

    def after_declare_queue(self, broker: Any, queue_name: str) -> None:
        self.events.append(("after_declare_queue", queue_name))

    def after_declare_delay_queue(self, broker: Any, queue_name: str) -> None:
        self.events.append(("after_declare_delay_queue", queue_name))

    def before_enqueue(
        self,
        broker: Any,
        message: DramatiqMessage[Any],
        delay: int | None,
    ) -> None:
        self.events.append(("before_enqueue", f"{message.queue_name}:{delay}"))

    def after_enqueue(
        self,
        broker: Any,
        message: DramatiqMessage[Any],
        delay: int | None,
    ) -> None:
        self.events.append(("after_enqueue", f"{message.queue_name}:{delay}"))


@pytest.fixture(autouse=True)
def fake_queue_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VERCEL", raising=False)
    FakeSyncQueueClient.instances.clear()
    FakeSyncQueueClient.subscriptions.clear()
    monkeypatch.setattr(vqs_dramatiq.vqs_sync, "QueueClient", FakeSyncQueueClient)


@pytest.fixture(autouse=True)
def isolated_dramatiq_broker() -> Iterator[None]:
    old_broker = dramatiq_broker.global_broker
    dramatiq_broker.global_broker = None
    try:
        yield
    finally:
        dramatiq_broker.global_broker = old_broker


@pytest.fixture(autouse=True)
def fake_queue_subscribe(monkeypatch: pytest.MonkeyPatch) -> list[FakeSubscription]:
    subscriptions: list[FakeSubscription] = []

    def subscribe(
        *,
        topic: object = None,
        consumer_group: str = "dramatiq",
        **kwargs: Any,
    ) -> Any:
        def decorator(callback: Any) -> Any:
            subscriptions.append(FakeSubscription(topic, consumer_group, callback))
            FakeSyncQueueClient.subscriptions.append(
                FakeSubscription(topic, consumer_group, callback)
            )
            return callback

        return decorator

    monkeypatch.setattr(vqs_dramatiq.vqs, "subscribe", subscribe)
    return subscriptions


def broker(**kwargs: Any) -> vqs_dramatiq.VercelQueueBroker:
    return vqs_dramatiq.VercelQueueBroker(**kwargs)


def dramatiq_message(queue_name: str = "emails") -> DramatiqMessage[Any]:
    return DramatiqMessage(
        queue_name=queue_name,
        actor_name="send_email",
        args=("user_1",),
        kwargs={},
        options={},
        message_id="msg-1",
    )


def fake_vqs_message(
    payload: bytes,
    *,
    topic: str = "emails",
    consumer_group: str = "dramatiq",
) -> Message[bytes]:
    return Message(
        payload=payload,
        metadata=MessageMetadata(
            message_id="vqs_1",
            delivery_count=1,
            created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            topic=topic,
            consumer_group=vqs_dramatiq.vqs.sanitize_name(consumer_group),
            receipt_handle="rh_1",
            content_type="application/octet-stream",
        ),
    )


def queue_client() -> FakeSyncQueueClient:
    return FakeSyncQueueClient.instances[0]


def topic_name(topic: object) -> str:
    return str(topic.name) if isinstance(topic, Topic) else cast("str", topic)


def test_declare_queue_creates_normal_and_delay_queue_once() -> None:
    middleware = RecordingMiddleware()
    subject = broker(middleware=[middleware])

    subject.declare_queue("emails")
    subject.declare_queue("emails")

    assert subject.get_declared_queues() == {"emails", "emails.DQ"}
    assert subject.get_declared_delay_queues() == {"emails.DQ"}
    assert middleware.events == [
        ("before_declare_queue", "emails"),
        ("after_declare_queue", "emails"),
        ("after_declare_delay_queue", "emails.DQ"),
    ]


def test_installer_sets_vercel_as_default_broker_for_actor_declaration(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    vqs_dramatiq.install_vercel_dramatiq_integration(consumer_group="workers")

    @dramatiq.actor(queue_name="emails")
    def send_email(user_id: str) -> None:
        del user_id

    subject = dramatiq.get_broker()

    assert isinstance(subject, vqs_dramatiq.VercelQueueBroker)
    assert isinstance(subject.get_results_backend(), vqs_dramatiq.VercelRuntimeCacheBackend)
    assert send_email.queue_name == "emails"
    assert subject.get_declared_queues() == {"emails", "emails.DQ"}
    assert [(topic_name(sub.topic), sub.consumer_group) for sub in fake_queue_subscribe] == [
        ("emails", "workers"),
        ("emails_DDQ", "workers"),
    ]


def test_installer_can_skip_default_broker() -> None:
    vqs_dramatiq.install_vercel_dramatiq_integration(set_default_broker=False)

    assert dramatiq_broker.global_broker is None


def test_installer_can_skip_results_backend() -> None:
    vqs_dramatiq.install_vercel_dramatiq_integration(install_results_backend=False)

    subject = dramatiq.get_broker()

    assert isinstance(subject, vqs_dramatiq.VercelQueueBroker)
    assert not any(isinstance(middleware, Results) for middleware in subject.middleware)


def test_installer_keeps_existing_default_broker() -> None:
    existing = broker()
    dramatiq.set_broker(existing)

    vqs_dramatiq.install_vercel_dramatiq_integration(consumer_group="workers")

    assert dramatiq.get_broker() is existing


@pytest.mark.parametrize("value", ["1", "yes", "on", "true", "YeS", " TRUE "])
def test_broker_defaults_to_push_for_truthy_vercel_env(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    monkeypatch.setenv("VERCEL", value)

    assert broker().poll is False


@pytest.mark.parametrize("value", [None, "", "0", "no", "off", "false", "anything"])
def test_broker_defaults_to_poll_for_falsey_vercel_env(
    monkeypatch: pytest.MonkeyPatch,
    value: str | None,
) -> None:
    if value is None:
        monkeypatch.delenv("VERCEL", raising=False)
    else:
        monkeypatch.setenv("VERCEL", value)

    assert broker().poll is True


def test_broker_defaults_to_polling_off_vercel() -> None:
    assert broker().poll is True


def test_broker_defaults_to_push_on_vercel(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL", "1")

    assert broker().poll is False


def test_broker_poll_option_overrides_runtime_detection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert broker(poll=False).poll is False

    monkeypatch.setenv("VERCEL", "1")

    assert broker(poll=True).poll is True


def test_enqueue_sends_encoded_message_to_mapped_topic() -> None:
    middleware = RecordingMiddleware()
    subject = broker(middleware=[middleware], retention=timedelta(minutes=5))
    subject.declare_queue("emails.high")
    message = dramatiq_message("emails.high")

    returned = subject.enqueue(message)

    sent = queue_client().sent[0]
    assert returned is message
    assert topic_name(sent["topic"]) == "emails_Dhigh"
    assert isinstance(sent["topic"], Topic)
    assert sent["topic"].transport is None
    assert sent["payload"] == message.encode()
    assert sent["kwargs"]["idempotency_key"] is None
    assert sent["kwargs"]["retention"] == timedelta(minutes=5)
    assert sent["kwargs"]["delay"] is None
    assert middleware.events[-2:] == [
        ("before_enqueue", "emails.high:None"),
        ("after_enqueue", "emails.high:None"),
    ]


def test_enqueue_can_opt_into_message_id_idempotency_key() -> None:
    subject = broker(use_message_id_as_idempotency_key=True)
    subject.declare_queue("emails")

    subject.enqueue(dramatiq_message("emails"))

    assert queue_client().sent[0]["kwargs"]["idempotency_key"] == "msg-1"


def test_queue_name_prefix_applies_before_topic_sanitization() -> None:
    subject = broker(queue_name_prefix="app.example-")

    assert topic_name(subject.topic_for_queue("emails.default")) == "app_Dexample-emails_Ddefault"


def test_delayed_enqueue_targets_delay_queue_and_sets_eta() -> None:
    subject = broker(push_handoff_wait_seconds=0)
    subject.declare_queue("emails")
    message = dramatiq_message("emails")

    before = int(time.time() * 1000)
    returned = subject.enqueue(message, delay=2500)
    after = int(time.time() * 1000)

    sent = queue_client().sent[0]
    assert returned.queue_name == "emails.DQ"
    assert before + 2500 <= returned.options["eta"] <= after + 2500
    assert topic_name(sent["topic"]) == "emails_DDQ"
    assert isinstance(sent["topic"], Topic)
    assert sent["topic"].transport is None
    assert sent["payload"] == returned.encode()
    assert sent["kwargs"]["delay"] == pytest.approx(2.5)


def test_enqueue_unknown_queue_raises_queue_not_found() -> None:
    subject = broker(push_handoff_wait_seconds=0)

    with pytest.raises(QueueNotFound):
        subject.enqueue(dramatiq_message("missing"))


def test_push_consumer_waits_for_push_and_does_not_poll() -> None:
    subject = broker(poll=False)
    subject.declare_queue("emails")
    consumer = subject.consume("emails", prefetch=1, timeout=1000)

    assert next(consumer) is None
    assert not hasattr(queue_client(), "last_poll")


def test_poll_consumer_polls_vqs_when_push_buffer_is_empty() -> None:
    subject = broker(poll=True, lease_duration=30)
    subject.declare_queue("emails.high")
    queue_client().message_batches.append([
        fake_vqs_message(dramatiq_message("emails.high").encode())
    ])

    message = subject.consume("emails.high", prefetch=3, timeout=1).__next__()

    assert message is not None
    assert message.queue_name == "emails.high"
    assert queue_client().last_poll == {
        "topic": subject.topic_for_queue("emails.high"),
        "consumer_group": "dramatiq",
        "kwargs": {
            "limit": 1,
            "lease_duration": 30,
        },
    }
    assert queue_client().lease_renewals[0].entered == 1
    assert queue_client().lease_renewals[0].lease_duration == 30


def test_ack_nack_and_requeue_apply_vqs_dispositions() -> None:
    subject = broker(poll=True, requeue_delay_seconds=8)
    subject.declare_queue("emails")
    first = fake_vqs_message(dramatiq_message("emails").encode())
    second = fake_vqs_message(dramatiq_message("emails").copy(message_id="msg-2").encode())
    third = fake_vqs_message(dramatiq_message("emails").copy(message_id="msg-3").encode())
    queue_client().message_batches.extend([[first], [second], [third]])
    consumer = subject.consume("emails", prefetch=3, timeout=1)

    first_proxy = consumer.__next__()
    second_proxy = consumer.__next__()
    third_proxy = consumer.__next__()
    assert first_proxy is not None
    assert second_proxy is not None
    assert third_proxy is not None

    consumer.ack(first_proxy)
    consumer.nack(second_proxy)
    consumer.requeue([third_proxy])

    assert queue_client().acknowledged == [first.metadata, second.metadata]
    assert subject.dead_letters == [second_proxy]
    assert queue_client().visibility_changes == [(third.metadata, 8)]
    assert [renewal.stopped for renewal in queue_client().lease_renewals] == [1, 1, 1]


def test_consume_unknown_queue_raises_queue_not_found() -> None:
    with pytest.raises(QueueNotFound):
        broker().consume("missing")


def test_declare_queue_registers_mapped_topic_callbacks_once(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    subject = broker(consumer_group="workers")
    subject.declare_queue("emails.high")
    subject.declare_queue("emails.high")

    assert [(topic_name(sub.topic), sub.consumer_group) for sub in fake_queue_subscribe] == [
        ("emails_Dhigh", "workers"),
        ("emails_Dhigh_DDQ", "workers"),
    ]
    assert len(subject._registered_callbacks) == 2


def test_registered_callback_hands_push_to_consumer_and_waits_for_ack(
    fake_queue_subscribe: list[FakeSubscription],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    subject = broker(poll=False, push_handoff_wait_seconds=1)
    subject.declare_queue("emails")
    consumer = subject.consume("emails", prefetch=1, timeout=1000)
    received: list[MessageProxy] = []
    primed = 0

    def prime_runtime_cache() -> None:
        nonlocal primed
        primed += 1

    monkeypatch.setattr(vqs_dramatiq.vcache, "prime_runtime_cache", prime_runtime_cache)

    def worker() -> None:
        message = consumer.__next__()
        assert message is not None
        received.append(message)
        consumer.ack(message)

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        with pytest.raises(Handoff):
            fake_queue_subscribe[0].callback(
                fake_vqs_message(dramatiq_message("emails").encode(), topic="emails")
            )
    finally:
        thread.join(timeout=1)

    assert received
    assert received[0].queue_name == "emails"
    assert len(queue_client().acknowledged) == 1
    assert primed == 1


def test_push_callback_waits_for_settlement_before_handoff(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    subject = broker(poll=False, push_handoff_wait_seconds=1)
    subject.declare_queue("emails")
    consumer = subject.consume("emails", prefetch=1, timeout=1000)
    received: list[MessageProxy] = []
    ready = threading.Event()

    def worker() -> None:
        ready.set()
        message = consumer.__next__()
        assert message is not None
        received.append(message)
        consumer.ack(message)

    thread = threading.Thread(target=worker)
    thread.start()
    assert ready.wait(timeout=1)
    try:
        with pytest.raises(Handoff):
            fake_queue_subscribe[0].callback(
                fake_vqs_message(dramatiq_message("emails").encode(), topic="emails")
            )
    finally:
        thread.join(timeout=1)

    assert received
    assert len(queue_client().acknowledged) == 1


def test_push_settlement_wait_is_not_handoff_deadline_bounded(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    subject = broker(poll=False, push_handoff_wait_seconds=0.01)
    subject.declare_queue("emails")
    consumer = subject.consume("emails", prefetch=1, timeout=1000)
    release = threading.Event()
    received: list[MessageProxy] = []

    def worker() -> None:
        message = consumer.__next__()
        assert message is not None
        received.append(message)
        assert release.wait(timeout=1)
        consumer.ack(message)

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        started = threading.Event()
        result: list[type[BaseException]] = []

        def callback() -> None:
            started.set()
            try:
                fake_queue_subscribe[0].callback(
                    fake_vqs_message(dramatiq_message("emails").encode(), topic="emails")
                )
            except BaseException as exc:  # noqa: BLE001
                result.append(type(exc))

        callback_thread = threading.Thread(target=callback)
        callback_thread.start()
        assert started.wait(timeout=1)
        time.sleep(0.05)
        assert result == []
        release.set()
        callback_thread.join(timeout=1)
    finally:
        release.set()
        thread.join(timeout=1)

    assert received
    assert result == [Handoff]
    assert len(queue_client().acknowledged) == 1


def test_push_callback_retries_when_no_consumer_or_prefetch_full(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    subject = broker(requeue_delay_seconds=4, push_handoff_wait_seconds=0)
    subject.declare_queue("emails")

    with pytest.raises(RetryAfter) as no_consumer:
        fake_queue_subscribe[0].callback(
            fake_vqs_message(dramatiq_message("emails").encode(), topic="emails")
        )
    consumer = subject.consume("emails", prefetch=1, timeout=1)
    first = subject._handoff_push_delivery(
        "emails",
        fake_vqs_message(dramatiq_message("emails").encode(), topic="emails"),
        time.monotonic(),
    )
    assert first is not None
    with pytest.raises(RetryAfter) as full:
        fake_queue_subscribe[0].callback(
            fake_vqs_message(dramatiq_message("emails").encode(), topic="emails")
        )

    assert no_consumer.value.timeout_seconds == 1
    assert full.value.timeout_seconds == 1
    message = consumer.__next__()
    assert message is not None
    consumer.ack(message)


def test_push_callback_uses_push_retry_delay() -> None:
    subject = broker(push_retry_delay_seconds=9)

    with pytest.raises(RetryAfter) as exc_info:
        subject.handle_push_message(fake_vqs_message(b"{}", topic="missing"))

    assert exc_info.value.timeout_seconds == 9


def test_queue_name_mapping_handles_invalid_topic_characters() -> None:
    assert vqs_dramatiq.vqs.sanitize_name("emails.DQ") == "emails_DDQ"
    assert vqs_dramatiq.vqs.sanitize_name("team/email_high") == "team_Semail__high"
    assert vqs_dramatiq.vqs.sanitize_name("plain-queue_1") == "plain-queue__1"


def test_debug_env_enables_dramatiq_loggers(monkeypatch: pytest.MonkeyPatch) -> None:
    logger = logging.getLogger("dramatiq")
    original_level = logger.level
    monkeypatch.setenv("VERCEL_DRAMATIQ_DEBUG", "1")
    try:
        broker()
        assert logger.level == logging.DEBUG
    finally:
        logger.setLevel(original_level)


def test_close_closes_consumers_and_reuses_single_client() -> None:
    subject = broker()
    subject.declare_queue("emails")
    subject.consume("emails", prefetch=1, timeout=1)
    subject.enqueue(dramatiq_message("emails"))

    subject.close()

    assert len(FakeSyncQueueClient.instances) == 1
    assert queue_client().closed is False


def test_unsupported_operations_raise_clear_errors() -> None:
    subject = broker()

    with pytest.raises(NotImplementedError, match="purge"):
        subject.flush("emails")
    with pytest.raises(NotImplementedError, match="purge"):
        subject.flush_all()
    with pytest.raises(NotImplementedError, match="join"):
        subject.join("emails")


def test_proxy_type_check_rejects_foreign_message() -> None:
    subject = broker(poll=True)
    subject.declare_queue("emails")
    consumer = subject.consume("emails", prefetch=1, timeout=1)
    foreign = cast("Any", object())

    with pytest.raises(TypeError, match="not produced"):
        consumer.ack(foreign)
