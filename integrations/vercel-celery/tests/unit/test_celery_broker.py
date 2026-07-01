from __future__ import annotations

from typing import Any, ClassVar, cast
from typing_extensions import Self

import inspect
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from celery import Celery as CeleryApp
from celery.app import backends as celery_backends
from celery.app.defaults import DEFAULTS as CELERY_DEFAULTS
from kombu import Queue
from kombu.exceptions import ChannelError
from kombu.transport import TRANSPORT_ALIASES
from kombu.transport.virtual.base import Empty

import vercel.integrations.celery as public_vqs_celery
import vercel.integrations.celery._broker as vqs_celery
from vercel.headers import get_headers, set_headers
from vercel.queue import (
    Duration,
    Handoff,
    Message,
    MessageMetadata,
    RetryAfter,
    SanitizedName,
    Topic,
)
from vercel.queue._internal import subscribers as queue_subscribers

_REAL_START_EMBEDDED_WORKER = vqs_celery._start_embedded_worker


@dataclass
class FakeSubscription:
    topic: str | None
    consumer_group: str
    callback: Any
    raw_topic: object | None = None


@dataclass
class FakeClientOptions:
    transport_options: dict[str, Any] = field(default_factory=dict)


@dataclass
class FakeConnection:
    client: FakeClientOptions
    channel_max: int = 10
    channels: list[Any] = field(default_factory=list)
    _used_channel_ids: list[int] = field(default_factory=list)
    _callbacks: dict[str, Any] = field(default_factory=dict)
    delivered: list[tuple[dict[str, Any], str]] = field(default_factory=list)

    def close_channel(self, channel: object) -> None:
        pass

    def _deliver(self, message: dict[str, Any], queue: str) -> None:
        self.delivered.append((message, queue))
        self._callbacks[queue](message)


class FakeMessage(Message[dict[str, Any]]):
    def __init__(
        self,
        payload: dict[str, Any],
        *,
        topic: str = "emails",
        consumer_group: str = "celery",
    ) -> None:
        super().__init__(
            payload=payload,
            metadata=MessageMetadata(
                message_id="msg_1",
                delivery_count=1,
                created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                topic=topic,
                consumer_group=SanitizedName(consumer_group),
                receipt_handle="rh_1",
                content_type="application/json",
            ),
        )


@dataclass(frozen=True)
class FakeDelivery:
    message: FakeMessage

    def accept(self) -> FakeMessage:
        return self.message


@dataclass
class FakeLeaseRenewal:
    message: Message[dict[str, Any]]
    lease_duration: Duration | None
    entered: int = 0
    closed: int = 0

    def __enter__(self) -> Self:
        self.entered += 1
        return self

    def close(self) -> None:
        self.closed += 1

    def stop(self) -> None:
        self.close()


class FakeSyncQueueClient:
    instances: ClassVar[list[FakeSyncQueueClient]] = []
    token_value: ClassVar[str] = "materialized-token"

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.sent: list[dict[str, Any]] = []
        self.message_batches: list[list[FakeMessage]] = []
        self.acknowledged: list[MessageMetadata] = []
        self.ack_headers: list[dict[str, str] | None] = []
        self.visibility_changes: list[tuple[MessageMetadata, Duration]] = []
        self.visibility_headers: list[dict[str, str] | None] = []
        self.lease_renewals: list[FakeLeaseRenewal] = []
        self.accepted: list[dict[str, Any]] = []
        self.closed = False
        self.ack_error: Exception | None = None
        self.extend_error: Exception | None = None
        FakeSyncQueueClient.instances.append(self)

    def send(self, topic: str, payload: dict[str, Any], **kwargs: Any) -> None:
        self.sent.append({"topic": topic, "payload": payload, "kwargs": kwargs})

    def poll(
        self,
        topic: str,
        consumer_group: str,
        **kwargs: Any,
    ) -> Iterator[FakeDelivery]:
        batch = self.message_batches.pop(0) if self.message_batches else []
        self.last_topic = topic
        self.last_consumer_group = consumer_group
        self.last_poll_kwargs = kwargs
        for message in batch:
            yield FakeDelivery(message)

    def acknowledge(self, message: Message[dict[str, Any]] | MessageMetadata) -> None:
        if self.ack_error is not None:
            raise self.ack_error
        self.ack_headers.append(dict(get_headers()) if get_headers() is not None else None)
        self.acknowledged.append(message.metadata if isinstance(message, Message) else message)

    def extend_lease(
        self,
        message: Message[dict[str, Any]] | MessageMetadata,
        duration: Duration,
    ) -> None:
        if self.extend_error is not None:
            raise self.extend_error
        metadata = message.metadata if isinstance(message, Message) else message
        self.visibility_headers.append(dict(get_headers()) if get_headers() is not None else None)
        self.visibility_changes.append((metadata, duration))

    def run_lease_renewal(
        self,
        message: Message[dict[str, Any]],
        lease_duration: Duration | None = None,
        headers_context: Any | None = None,
    ) -> FakeLeaseRenewal:
        del headers_context
        renewal = FakeLeaseRenewal(message=message, lease_duration=lease_duration)
        self.lease_renewals.append(renewal)
        return renewal

    def close(self) -> None:
        self.closed = True

    @property
    def http(self) -> FakeSyncQueueClient:
        return self

    async def token(self, token: str | None) -> str:
        return token or self.token_value


@pytest.fixture(autouse=True)
def fake_queue_clients(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeSyncQueueClient.instances.clear()
    FakeSyncQueueClient.token_value = "materialized-token"
    monkeypatch.setattr(vqs_celery.vqs_sync, "QueueClient", FakeSyncQueueClient)


@pytest.fixture(autouse=True)
def fake_queue_subscribe(monkeypatch: pytest.MonkeyPatch) -> list[FakeSubscription]:
    subscriptions: list[FakeSubscription] = []

    def subscribe(
        *,
        topic: object | None = None,
        consumer_group: str = "celery",
        **kwargs: object,
    ) -> Any:
        del kwargs

        def decorator(callback: Any) -> Any:
            subscriptions.append(
                FakeSubscription(
                    topic=topic_name(topic),
                    consumer_group=consumer_group,
                    callback=callback,
                    raw_topic=topic,
                )
            )
            return callback

        return decorator

    monkeypatch.setattr(vqs_celery.vqs, "subscribe", subscribe)
    return subscriptions


@pytest.fixture(autouse=True)
def clean_celery_integration_state() -> Iterator[None]:
    original_broker_url = CELERY_DEFAULTS.get("broker_url")
    original_result_backend = CELERY_DEFAULTS.get("result_backend")
    CELERY_DEFAULTS["broker_url"] = None
    CELERY_DEFAULTS["result_backend"] = None
    vqs_celery._registered_app_queues.clear()
    vqs_celery._embedded_workers.clear()
    vqs_celery._registered_callbacks.clear()
    vqs_celery._push_channels.clear()
    vqs_celery._finalize_hook_state.installed = False
    try:
        yield
    finally:
        CELERY_DEFAULTS["broker_url"] = original_broker_url
        CELERY_DEFAULTS["result_backend"] = original_result_backend
        vqs_celery._registered_app_queues.clear()
        vqs_celery._embedded_workers.clear()
        vqs_celery._registered_callbacks.clear()
        vqs_celery._push_channels.clear()
        vqs_celery._finalize_hook_state.installed = False


@pytest.fixture(autouse=True)
def fake_embedded_worker_start(monkeypatch: pytest.MonkeyPatch) -> list[CeleryApp]:
    started: list[CeleryApp] = []

    def start(app: CeleryApp) -> None:
        started.append(app)

    monkeypatch.setattr(vqs_celery, "_start_embedded_worker", start)
    return started


@pytest.fixture(autouse=True)
def clean_vercel_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VERCEL", raising=False)


def make_poll_channel(**transport_options: Any) -> vqs_celery.PollChannel:
    return vqs_celery.PollChannel(FakeConnection(FakeClientOptions(transport_options)))


def make_push_channel(**transport_options: Any) -> vqs_celery.PushChannel:
    return vqs_celery.PushChannel(FakeConnection(FakeClientOptions(transport_options)))


def make_auto_channel(**transport_options: Any) -> vqs_celery.AutoChannel:
    return vqs_celery.AutoChannel(FakeConnection(FakeClientOptions(transport_options)))


def topic_name(topic: object) -> str:
    return str(topic.name) if isinstance(topic, Topic) else cast("str", topic)


def topic_transport(topic: object) -> object | None:
    return topic.transport if isinstance(topic, Topic) else None


def configure_push_broker(app: CeleryApp, broker_url: str = "vercel-push://") -> None:
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    app.conf.broker_url = broker_url


def message(delivery_tag: str = "tag_1") -> dict[str, Any]:
    return {"body": "payload", "properties": {"delivery_tag": delivery_tag}}


def track(channel: vqs_celery._BaseChannel, tag: str, queued_message: FakeMessage) -> None:
    renewal = channel._queue_client.run_lease_renewal(queued_message)
    renewal.__enter__()
    cast("Any", channel._messages_by_tag)[tag] = vqs_celery._TrackedDelivery(
        message=queued_message,
        lease_renewal=renewal,
        queue_client=channel._queue_client,
        headers_context=vqs_celery.get_headers_context(),
    )


def fake_renewal(tracked: vqs_celery._TrackedDelivery) -> FakeLeaseRenewal:
    return cast("FakeLeaseRenewal", tracked.lease_renewal)


def test_install_vercel_celery_integration_clean_break() -> None:
    TRANSPORT_ALIASES.pop("vercel", None)
    celery_backends.BACKEND_ALIASES.pop("vercel-runtime-cache", None)

    public_vqs_celery.install_vercel_celery_integration()

    assert TRANSPORT_ALIASES["vercel"] == "vercel.integrations.celery:VercelQueueTransport"
    assert TRANSPORT_ALIASES["vercel-poll"] == (
        "vercel.integrations.celery:VercelQueuePollTransport"
    )
    assert TRANSPORT_ALIASES["vercel-push"] == (
        "vercel.integrations.celery:VercelQueuePushTransport"
    )
    assert celery_backends.BACKEND_ALIASES["vercel-runtime-cache"] == (
        "vercel.integrations.celery._result_backend:VercelRuntimeCacheBackend"
    )
    assert CeleryApp("default-broker").conf.broker_url == "vercel://"
    assert CeleryApp("default-result-backend").conf.result_backend == ("vercel-runtime-cache://")
    assert vqs_celery._finalize_hook_state.installed is True


def test_install_vercel_celery_integration_can_skip_queue_registration() -> None:
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)

    assert TRANSPORT_ALIASES["vercel"] == "vercel.integrations.celery:VercelQueueTransport"
    assert TRANSPORT_ALIASES["vercel-poll"] == (
        "vercel.integrations.celery:VercelQueuePollTransport"
    )
    assert TRANSPORT_ALIASES["vercel-push"] == (
        "vercel.integrations.celery:VercelQueuePushTransport"
    )
    assert vqs_celery._finalize_hook_state.installed is False


def test_install_vercel_celery_integration_can_skip_default_broker() -> None:
    public_vqs_celery.install_vercel_celery_integration(set_default_broker=False)

    assert CeleryApp("no-default-broker").conf.broker_url is None


def test_install_vercel_celery_integration_keeps_existing_default_broker() -> None:
    CELERY_DEFAULTS["broker_url"] = "memory://"

    public_vqs_celery.install_vercel_celery_integration()

    assert CeleryApp("existing-default-broker").conf.broker_url == "memory://"


def test_install_vercel_celery_integration_can_skip_default_result_backend() -> None:
    public_vqs_celery.install_vercel_celery_integration(set_default_result_backend=False)

    assert CeleryApp("no-default-result-backend").conf.result_backend is None


def test_install_vercel_celery_integration_keeps_existing_default_result_backend() -> None:
    CELERY_DEFAULTS["result_backend"] = "cache+memory://"

    public_vqs_celery.install_vercel_celery_integration()

    assert CeleryApp("existing-default-result-backend").conf.result_backend == ("cache+memory://")


@pytest.mark.parametrize("value", ["1", "yes", "on", "true", "YeS", " TRUE "])
def test_vercel_runtime_detection_accepts_truthy_values(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    monkeypatch.setenv("VERCEL", value)

    assert vqs_celery.is_vercel_runtime() is True


@pytest.mark.parametrize("value", [None, "", "0", "no", "off", "false", "anything"])
def test_vercel_runtime_detection_rejects_falsey_values(
    monkeypatch: pytest.MonkeyPatch,
    value: str | None,
) -> None:
    if value is None:
        monkeypatch.delenv("VERCEL", raising=False)
    else:
        monkeypatch.setenv("VERCEL", value)

    assert vqs_celery.is_vercel_runtime() is False


def test_poll_channel_configures_queue_client_from_transport_options() -> None:
    channel = make_poll_channel(
        token="token",
        region="sfo1",
        base_url="https://queue.test",
        deployment="dpl_1",
        headers={"x-test": "ok"},
        timeout=timedelta(seconds=3.5),
    )

    assert channel.consumer_group == "celery"
    assert FakeSyncQueueClient.instances[0].kwargs == {
        "token": "token",
        "region": "sfo1",
        "base_url": "https://queue.test",
        "deployment": "dpl_1",
        "headers": {"x-test": "ok"},
        "timeout": timedelta(seconds=3.5),
    }


def test_consumer_group_and_lease_duration_are_shared() -> None:
    poll_channel = make_poll_channel(consumer_group="workers", lease_duration=30)
    push_channel = make_push_channel(consumer_group="workers", lease_duration=30)

    assert poll_channel.consumer_group == "workers"
    assert poll_channel.lease_duration == 30
    assert push_channel.consumer_group == "workers"
    assert push_channel.lease_duration == 30


def test_poll_put_publishes_native_kombu_message() -> None:
    channel = make_poll_channel(retention=60, delay=2, headers={"x-test": "ok"})
    payload = message()

    channel._put("emails", payload)

    assert FakeSyncQueueClient.instances[0].sent == [
        {
            "topic": channel._topic("emails"),
            "payload": payload,
            "kwargs": {
                "idempotency_key": None,
                "retention": 60,
                "delay": 2,
                "headers": {"x-test": "ok"},
            },
        }
    ]


def test_poll_put_normalizes_queue_name_to_topic() -> None:
    channel = make_poll_channel()
    payload = message()

    channel._put("emails.high", payload)

    sent_topic = FakeSyncQueueClient.instances[0].sent[0]["topic"]
    assert topic_name(sent_topic) == "emails_Dhigh"
    assert topic_transport(sent_topic) is channel._message_transport


@pytest.mark.asyncio
async def test_message_transport_round_trips_kombu_bytes_and_body_encoding() -> None:
    channel = make_poll_channel()
    payload = {
        "body": b"\xff\x00payload",
        "content-encoding": "binary",
        "content-type": "application/octet-stream",
        "headers": {"nested": b"bytes"},
        "properties": {"delivery_tag": "tag_1", "body_encoding": "base64"},
    }

    encoded = channel._message_transport.serialize(payload)
    decoded = await channel._message_transport.deserialize(
        _one_chunk_async(encoded),
        content_type=channel._message_transport.content_type,
    )

    assert channel._message_transport.content_type == "application/json"
    assert decoded == payload


async def _one_chunk_async(payload: bytes) -> AsyncIterator[bytes]:
    yield payload


def test_put_can_use_task_id_as_idempotency_key() -> None:
    channel = make_poll_channel(use_task_id_as_idempotency_key=True)

    channel._put("emails", {"headers": {"id": "task_1"}, "properties": {}})
    channel._put("emails", {"headers": {}, "properties": {"correlation_id": "corr_1"}})

    sent = FakeSyncQueueClient.instances[0].sent
    assert sent[0]["kwargs"]["idempotency_key"] == "task_1"
    assert sent[1]["kwargs"]["idempotency_key"] == "corr_1"


def test_poll_get_polls_one_delivery_and_tracks_delivery_tag() -> None:
    channel = make_poll_channel(consumer_group="workers", lease_duration=30)
    queued_message = FakeMessage(message())
    FakeSyncQueueClient.instances[0].message_batches.append([queued_message])

    payload = channel._get("emails")
    delivery_tag = payload["properties"]["delivery_tag"]

    assert delivery_tag != "tag_1"
    assert payload == {"body": "payload", "properties": {"delivery_tag": delivery_tag}}
    tracked = channel._messages_by_tag[delivery_tag]
    renewal = fake_renewal(tracked)
    assert tracked.message.payload == payload
    assert tracked.message.metadata == queued_message.metadata
    assert renewal.message == tracked.message
    assert renewal.lease_duration == 30
    assert renewal.entered == 1
    poll_topic = FakeSyncQueueClient.instances[0].last_topic
    assert topic_name(poll_topic) == "emails"
    assert topic_transport(poll_topic) is channel._message_transport
    assert FakeSyncQueueClient.instances[0].last_consumer_group == "workers"
    assert FakeSyncQueueClient.instances[0].last_poll_kwargs == {
        "limit": 1,
        "lease_duration": 30,
    }


def test_poll_get_normalizes_queue_name_to_topic() -> None:
    channel = make_poll_channel()
    queued_message = FakeMessage(message(), topic="emails_Dhigh")
    FakeSyncQueueClient.instances[0].message_batches.append([queued_message])

    channel._get("emails.high")

    assert topic_name(FakeSyncQueueClient.instances[0].last_topic) == "emails_Dhigh"


def test_poll_get_raises_empty_when_no_delivery_available() -> None:
    channel = make_poll_channel()
    FakeSyncQueueClient.instances[0].message_batches.append([])

    with pytest.raises(Empty):
        channel._get("emails")


def test_auto_channel_polls_when_not_running_on_vercel() -> None:
    channel = make_auto_channel(consumer_group="workers", lease_duration=30)
    queued_message = FakeMessage(message())
    FakeSyncQueueClient.instances[0].message_batches.append([queued_message])

    payload = channel._get("emails")

    assert payload["body"] == "payload"
    assert FakeSyncQueueClient.instances[0].last_consumer_group == "workers"
    assert vqs_celery._push_channels == []


def test_auto_channel_uses_push_when_running_on_vercel(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL", "1")
    channel = make_auto_channel()

    with pytest.raises(Empty):
        channel._get("emails")

    assert vqs_celery._push_channels == [channel]


def test_install_vercel_celery_integration_is_idempotent() -> None:
    public_vqs_celery.install_vercel_celery_integration()
    public_vqs_celery.install_vercel_celery_integration()

    assert vqs_celery._finalize_hook_state.installed is True


def test_install_vercel_celery_integration_registers_existing_apps(
    monkeypatch: pytest.MonkeyPatch,
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    monkeypatch.setenv("VERCEL", "1")
    app = CeleryApp("existing-app")

    public_vqs_celery.install_vercel_celery_integration()

    assert app.finalized is False
    assert ("celery", "celery-existing-app") in [
        (sub.topic, sub.consumer_group) for sub in fake_queue_subscribe
    ]


def test_register_celery_app_queues_accepts_push_broker_transport(
    fake_queue_subscribe: list[FakeSubscription],
    fake_embedded_worker_start: list[CeleryApp],
) -> None:
    app = CeleryApp("push-transport")
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    app.conf.broker_transport = "vercel-push"
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["emails"]
    assert [sub.consumer_group for sub in fake_queue_subscribe] == ["celery-push-transport"]
    topic = fake_queue_subscribe[0].raw_topic
    assert isinstance(topic, Topic)
    assert getattr(type(topic), "__topic_origin__", None) is None
    assert isinstance(topic.transport, vqs_celery._KombuMessageTransport)
    assert fake_queue_subscribe[0].callback.__annotations__["message"] == "vqs.Message[Any]"
    assert fake_embedded_worker_start == [app]


def test_register_celery_app_queues_can_skip_embedded_worker(
    fake_queue_subscribe: list[FakeSubscription],
    fake_embedded_worker_start: list[CeleryApp],
) -> None:
    app = CeleryApp("push-no-worker")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app, start_worker=False)

    assert [sub.topic for sub in fake_queue_subscribe] == ["emails"]
    assert fake_embedded_worker_start == []


def test_start_embedded_worker_is_idempotent_per_app(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workers: list[Any] = []

    class FakeWorkController:
        consumer = object()

        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            workers.append(self)

        def start(self) -> None:
            pass

    class FakeThread:
        def __init__(self, target: Any, name: str, daemon: Any) -> None:
            self.target = target
            self.name = name
            self.daemon = daemon
            self.started = False

        def start(self) -> None:
            self.started = True

    app = CeleryApp("worker-idempotent")
    monkeypatch.setattr(app, "WorkController", FakeWorkController)
    monkeypatch.setattr(vqs_celery.threading, "Thread", FakeThread)
    monkeypatch.setattr(vqs_celery, "_wait_for_embedded_worker_channel", lambda worker: None)

    _REAL_START_EMBEDDED_WORKER(app)
    _REAL_START_EMBEDDED_WORKER(app)

    assert len(workers) == 1
    embedded = vqs_celery._embedded_workers[app]
    assert isinstance(embedded.thread, FakeThread)
    assert embedded.thread.started is True


def test_start_embedded_worker_uses_solo_worker_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_options: list[dict[str, Any]] = []

    class FakeWorkController:
        consumer = object()

        def __init__(self, **kwargs: Any) -> None:
            worker_options.append(kwargs)

        def start(self) -> None:
            pass

    class FakeThread:
        def __init__(self, target: Any, name: str, daemon: Any) -> None:
            self.target = target
            self.name = name
            self.daemon = daemon

        def start(self) -> None:
            pass

    app = CeleryApp("worker-options")
    monkeypatch.setattr(app, "WorkController", FakeWorkController)
    monkeypatch.setattr(vqs_celery.threading, "Thread", FakeThread)
    monkeypatch.setattr(vqs_celery, "_wait_for_embedded_worker_channel", lambda worker: None)

    _REAL_START_EMBEDDED_WORKER(app)

    assert worker_options == [
        {
            "concurrency": 1,
            "pool": "solo",
            "loglevel": "INFO",
            "without_gossip": True,
            "without_heartbeat": True,
            "without_mingle": True,
        }
    ]
    embedded = vqs_celery._embedded_workers[app]
    assert isinstance(embedded.thread, FakeThread)
    assert embedded.thread.name == "vercel-celery-embedded-worker"
    assert embedded.thread.daemon is True


def test_register_celery_app_queues_registers_untyped_message_callback(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("push-untyped-callback")
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    app.conf.broker_transport = "vercel-push"
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app)

    topic = fake_queue_subscribe[0].raw_topic
    assert isinstance(topic, Topic)
    plan = queue_subscribers._build_invocation_plan(
        fake_queue_subscribe[0].callback,
        topic_payload_annotation=inspect.Signature.empty,
    )
    assert plan.mode == "message"
    assert plan.payload_adapter is None


def test_register_celery_app_queues_accepts_auto_broker_on_vercel(
    monkeypatch: pytest.MonkeyPatch,
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    monkeypatch.setenv("VERCEL", "true")
    app = CeleryApp("auto-transport")
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    app.conf.broker_url = "vercel://"
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["emails"]


def test_register_celery_app_queues_rejects_auto_broker_off_vercel(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("auto-transport-local")
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    app.conf.broker_url = "vercel://"
    app.conf.task_queues = (Queue("emails"),)

    with pytest.raises(RuntimeError, match="vercel broker transport running on Vercel"):
        vqs_celery.register_celery_app_queues(app)

    assert fake_queue_subscribe == []


def test_register_celery_app_queues_normalizes_subscription_topic(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("push-normalized-topic")
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    app.conf.broker_transport = "vercel-push"
    app.conf.task_queues = (Queue("emails.high"),)

    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["emails_Dhigh"]


def test_register_celery_app_queues_honors_transport_consumer_group(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("push-consumer-group")
    configure_push_broker(app)
    app.conf.broker_transport_options = {"consumer_group": "api/celery_worker.py"}
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["emails"]
    assert [sub.consumer_group for sub in fake_queue_subscribe] == ["api_Scelery__worker_Dpy"]
    assert app.conf.broker_transport_options["consumer_group"] == "api/celery_worker.py"


def test_register_celery_app_queues_derives_consumer_group_from_main(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("push.consumer_group")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["emails"]
    assert [sub.consumer_group for sub in fake_queue_subscribe] == ["celery-push_Dconsumer__group"]
    assert app.conf.broker_transport_options["consumer_group"] == "celery-push_Dconsumer__group"


def test_register_celery_app_queues_falls_back_without_main(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp()
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["emails"]
    assert [sub.consumer_group for sub in fake_queue_subscribe] == ["celery"]
    assert app.conf.broker_transport_options["consumer_group"] == "celery"


def test_register_celery_app_queues_accepts_push_transport_subclass(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    class CustomPushTransport(public_vqs_celery.VercelQueuePushTransport):
        pass

    app = CeleryApp("subclass-push")
    app.conf.broker_transport = CustomPushTransport
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["emails"]


@pytest.mark.parametrize(
    ("broker_url", "broker_transport"),
    [
        ("vercel-poll://", None),
        ("memory://", None),
        ("redis://", None),
        (None, None),
        (None, "vercel-poll"),
        (None, "vercel"),
    ],
)
def test_register_celery_app_queues_rejects_non_push_brokers(
    broker_url: str | None,
    broker_transport: str | None,
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("not-push")
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    if broker_url is not None:
        app.conf.broker_url = broker_url
    if broker_transport is not None:
        app.conf.broker_transport = broker_transport

    with pytest.raises(RuntimeError, match="vercel-push broker transport"):
        vqs_celery.register_celery_app_queues(app)

    assert fake_queue_subscribe == []


def test_app_finalize_skips_non_push_brokers(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("finalize-skip")
    app.conf.broker_url = "memory://"
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery._register_finalized_app_queues(app)

    assert fake_queue_subscribe == []


def test_app_finalize_skips_auto_broker_off_vercel(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("finalize-auto-local")
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    app.conf.broker_url = "vercel://"
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery._register_finalized_app_queues(app)

    assert fake_queue_subscribe == []


def test_app_finalize_registers_auto_broker_on_vercel(
    monkeypatch: pytest.MonkeyPatch,
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    monkeypatch.setenv("VERCEL", "yes")
    app = CeleryApp("finalize-auto-vercel")
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    app.conf.broker_url = "vercel://"
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery._register_finalized_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["emails"]


def test_app_finalize_registers_push_brokers(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("finalize-push")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery._register_finalized_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["emails"]


def test_app_finalize_registers_subscriptions_for_configured_queues(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("configured")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"), Queue("reports"))

    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["emails", "reports"]
    assert [sub.consumer_group for sub in fake_queue_subscribe] == [
        "celery-configured",
        "celery-configured",
    ]
    assert len(vqs_celery._registered_callbacks) == 2


def test_app_finalize_registers_synthesized_default_queue(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("default")
    configure_push_broker(app)

    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["celery"]
    assert len(vqs_celery._registered_callbacks) == 1


def test_app_finalize_registration_is_idempotent(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("idempotent")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app)
    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["emails"]
    assert len(vqs_celery._registered_callbacks) == 1


def test_registered_callback_delivers_to_active_push_channel() -> None:
    app = CeleryApp("callback")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)
    channel = make_push_channel()
    queued_message = FakeMessage(message())
    received: list[Any] = []
    channel.basic_consume("emails", no_ack=False, callback=received.append, consumer_tag="ctag")

    vqs_celery.register_celery_app_queues(app)
    with pytest.raises(Handoff):
        vqs_celery._registered_callbacks[0](queued_message)
    delivery_tag = received[0].delivery_tag

    assert len(received) == 1
    assert delivery_tag != "tag_1"
    assert channel.connection.delivered[0][0]["properties"]["delivery_tag"] == delivery_tag
    assert channel.connection.delivered[0][1] == "emails"
    tracked = channel._messages_by_tag[delivery_tag]
    renewal = fake_renewal(tracked)
    assert tracked.message.payload == channel.connection.delivered[0][0]
    assert tracked.message.metadata == queued_message.metadata
    assert renewal.message == tracked.message
    assert renewal.lease_duration is None
    assert renewal.entered == 1


def test_push_delivery_follow_ups_use_delivery_header_contexts() -> None:
    app = CeleryApp("callback-context")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)
    channel = make_push_channel(requeue_delay_seconds=7)
    received: list[Any] = []
    channel.basic_consume("emails", no_ack=False, callback=received.append, consumer_tag="ctag")
    vqs_celery.register_celery_app_queues(app)

    set_headers({"x-vercel-oidc-token": "token-1"})
    with pytest.raises(Handoff):
        vqs_celery._registered_callbacks[0](FakeMessage(message(), topic="emails"))
    first_tag = received[-1].delivery_tag

    set_headers({"x-vercel-oidc-token": "token-2"})
    with pytest.raises(Handoff):
        vqs_celery._registered_callbacks[0](FakeMessage(message(), topic="emails"))
    second_tag = received[-1].delivery_tag

    set_headers({"x-vercel-oidc-token": "current"})
    channel.basic_ack(first_tag)
    channel.basic_reject(second_tag, requeue=True)

    assert channel._queue_client.ack_headers == [{"x-vercel-oidc-token": "token-1"}]
    assert channel._queue_client.visibility_headers == [{"x-vercel-oidc-token": "token-2"}]
    assert get_headers() == {"x-vercel-oidc-token": "current"}
    assert len(FakeSyncQueueClient.instances) == 1


def test_registered_callback_matches_push_channel_consumer_group() -> None:
    app = CeleryApp("callback-groups")
    configure_push_broker(app)
    app.conf.broker_transport_options = {"consumer_group": "api/celery_worker.py"}
    app.conf.task_queues = (Queue("emails"),)
    celery_channel = make_push_channel(consumer_group="celery")
    workers_channel = make_push_channel(consumer_group="api/celery_worker.py")
    queued_message = FakeMessage(message(), consumer_group="api_Scelery__worker_Dpy")
    celery_received: list[Any] = []
    workers_received: list[Any] = []
    celery_channel.basic_consume(
        "emails",
        no_ack=False,
        callback=celery_received.append,
        consumer_tag="celery-ctag",
    )
    workers_channel.basic_consume(
        "emails",
        no_ack=False,
        callback=workers_received.append,
        consumer_tag="workers-ctag",
    )

    vqs_celery.register_celery_app_queues(app)
    assert workers_channel.consumer_group == "api_Scelery__worker_Dpy"
    with pytest.raises(Handoff):
        vqs_celery._registered_callbacks[0](queued_message)

    assert celery_received == []
    assert len(workers_received) == 1
    assert celery_channel.connection.delivered == []
    assert workers_channel.connection.delivered[0][1] == "emails"


def test_registered_callback_retries_when_consumer_group_has_no_channel() -> None:
    app = CeleryApp("callback-group-retry")
    configure_push_broker(app)
    app.conf.broker_transport_options = {"consumer_group": "workers"}
    app.conf.task_queues = (Queue("emails"),)
    channel = make_push_channel(consumer_group="celery")
    queued_message = FakeMessage(message(), consumer_group="workers")
    received: list[Any] = []
    channel.basic_consume("emails", no_ack=False, callback=received.append, consumer_tag="ctag")

    vqs_celery.register_celery_app_queues(app)
    with pytest.raises(RetryAfter) as exc_info:
        vqs_celery._registered_callbacks[0](queued_message)

    assert exc_info.value.timeout_seconds == 0
    assert received == []
    assert channel.connection.delivered == []


def test_registered_callback_retries_without_active_channel() -> None:
    app = CeleryApp("callback-retry")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)
    queued_message = FakeMessage(message())

    vqs_celery.register_celery_app_queues(app)
    with pytest.raises(RetryAfter) as exc_info:
        vqs_celery._registered_callbacks[0](queued_message)

    assert exc_info.value.timeout_seconds == 0


def test_find_push_channel_uses_stable_snapshot_when_registry_changes() -> None:
    class FakeQoS:
        def can_consume(self) -> bool:
            return True

    class FakePushChannel:
        consumer_group = "celery"
        qos = FakeQoS()

        def __init__(self, callbacks: dict[str, Any], *, mutate_on_closed: bool = False) -> None:
            self.connection = FakeConnection(FakeClientOptions())
            self.connection._callbacks = callbacks
            self.mutate_on_closed = mutate_on_closed

        @property
        def closed(self) -> bool:
            if self.mutate_on_closed and ready_channel in vqs_celery._push_channels:
                vqs_celery._push_channels.remove(cast("Any", ready_channel))
            return False

    ready_channel = FakePushChannel({"emails": lambda value: None})
    mutating_channel = FakePushChannel({"other": lambda value: None}, mutate_on_closed=True)
    vqs_celery._push_channels[:] = cast(
        "Any",
        [ready_channel, mutating_channel],
    )

    assert vqs_celery._find_push_channel("emails", "celery") is ready_channel
    assert ready_channel not in vqs_celery._push_channels


def test_push_queue_callback_dispatches_to_consumer_and_hands_off_lifecycle() -> None:
    channel = make_push_channel(lease_duration=45)
    queued_message = FakeMessage(message())
    received: list[Any] = []
    channel.basic_consume("emails", no_ack=False, callback=received.append, consumer_tag="ctag")

    with pytest.raises(Handoff):
        channel._handle_queue_delivery(
            queued_message.payload,
            queued_message.metadata,
            queue="emails",
        )
    delivery_tag = received[0].delivery_tag

    assert len(received) == 1
    assert delivery_tag != "tag_1"
    assert channel.connection.delivered[0][0]["properties"]["delivery_tag"] == delivery_tag
    assert channel.connection.delivered[0][1] == "emails"
    tracked = channel._messages_by_tag[delivery_tag]
    renewal = fake_renewal(tracked)
    assert tracked.message.payload == channel.connection.delivered[0][0]
    assert tracked.message.metadata == queued_message.metadata
    assert tracked.queue_client is channel._queue_client
    assert len(FakeSyncQueueClient.instances) == 1
    assert renewal.message == tracked.message
    assert renewal.lease_duration == 45
    assert renewal.entered == 1
    assert delivery_tag in channel.qos._delivered

    channel.basic_ack(delivery_tag)

    assert channel._queue_client.acknowledged == [queued_message.metadata]
    assert renewal.closed == 1
    assert channel._messages_by_tag == {}


def test_poll_get_starts_lease_renewal_with_default_duration() -> None:
    channel = make_poll_channel()
    queued_message = FakeMessage(message())
    FakeSyncQueueClient.instances[0].message_batches.append([queued_message])

    payload = channel._get("emails")
    delivery_tag = payload["properties"]["delivery_tag"]

    renewal = fake_renewal(channel._messages_by_tag[delivery_tag])
    assert renewal.lease_duration is None
    assert renewal.entered == 1


def test_push_queue_callback_uses_registered_queue_name() -> None:
    channel = make_push_channel()
    queued_message = FakeMessage(message(), topic="metadata-topic")
    received: list[Any] = []
    channel.basic_consume("override", no_ack=False, callback=received.append, consumer_tag="ctag")

    with pytest.raises(Handoff):
        channel._handle_queue_delivery(
            queued_message.payload,
            queued_message.metadata,
            queue="override",
        )

    assert len(received) == 1
    assert received[0].delivery_tag != "tag_1"
    assert (
        channel.connection.delivered[0][0]["properties"]["delivery_tag"] == received[0].delivery_tag
    )
    assert channel.connection.delivered[0][1] == "override"


def test_push_queue_callback_retries_when_no_consumer_is_registered() -> None:
    channel = make_push_channel()
    queued_message = FakeMessage(message())

    with pytest.raises(RetryAfter) as exc_info:
        channel._handle_queue_delivery(
            queued_message.payload,
            queued_message.metadata,
            queue="unknown",
        )

    assert exc_info.value.timeout_seconds == 0
    assert FakeSyncQueueClient.instances[0].acknowledged == []
    assert FakeSyncQueueClient.instances[0].visibility_changes == []
    assert channel._messages_by_tag == {}


def test_push_queue_callback_retries_when_prefetch_is_exhausted() -> None:
    channel = make_push_channel(requeue_delay_seconds=8)
    channel.qos.prefetch_count = 1
    cast("Any", channel.qos._delivered)["existing"] = object()
    queued_message = FakeMessage(message())
    channel.connection._callbacks["emails"] = lambda value: None

    with pytest.raises(RetryAfter) as exc_info:
        channel._handle_queue_delivery(
            queued_message.payload,
            queued_message.metadata,
            queue="emails",
        )

    assert exc_info.value.timeout_seconds == 8
    assert FakeSyncQueueClient.instances[0].acknowledged == []
    assert FakeSyncQueueClient.instances[0].visibility_changes == []
    assert channel._messages_by_tag == {}


def test_push_accept_releases_lease_when_callback_fails() -> None:
    channel = make_push_channel(requeue_delay_seconds=9)
    queued_message = FakeMessage(message())

    def fail(message: Any) -> None:
        del message
        raise RuntimeError("consumer failed")

    channel.basic_consume("emails", no_ack=False, callback=fail, consumer_tag="ctag")

    with pytest.raises(RuntimeError, match="consumer failed"):
        channel._handle_queue_delivery(
            queued_message.payload,
            queued_message.metadata,
            queue="emails",
        )

    assert FakeSyncQueueClient.instances[0].acknowledged == []
    assert FakeSyncQueueClient.instances[0].visibility_changes == [(queued_message.metadata, 9)]
    assert FakeSyncQueueClient.instances[0].lease_renewals[0].closed == 1
    assert channel._messages_by_tag == {}
    assert len(channel.qos._dirty) == 1


def test_push_get_never_polls() -> None:
    channel = make_push_channel()

    with pytest.raises(Empty):
        channel._get("emails")


def test_basic_ack_acknowledges_leased_delivery() -> None:
    channel = make_poll_channel()
    queued_message = FakeMessage({})
    track(channel, "tag_1", queued_message)

    channel.basic_ack("tag_1")

    assert FakeSyncQueueClient.instances[0].acknowledged == [queued_message.metadata]
    assert FakeSyncQueueClient.instances[0].lease_renewals[0].closed == 1
    assert channel._messages_by_tag == {}


def test_basic_ack_keeps_delivery_tracked_when_ack_fails() -> None:
    channel = make_poll_channel()
    queued_message = FakeMessage({})
    track(channel, "tag_1", queued_message)
    FakeSyncQueueClient.instances[0].ack_error = RuntimeError("ack failed")

    with pytest.raises(RuntimeError, match="ack failed"):
        channel.basic_ack("tag_1")

    assert channel._messages_by_tag["tag_1"].message is queued_message


def test_basic_reject_requeues_by_changing_visibility() -> None:
    channel = make_poll_channel(requeue_delay_seconds=7)
    queued_message = FakeMessage({})
    track(channel, "tag_1", queued_message)

    channel.basic_reject("tag_1", requeue=True)

    assert FakeSyncQueueClient.instances[0].visibility_changes == [(queued_message.metadata, 7)]
    assert FakeSyncQueueClient.instances[0].acknowledged == []
    assert FakeSyncQueueClient.instances[0].lease_renewals[0].closed == 1
    assert channel._messages_by_tag == {}


def test_basic_reject_keeps_delivery_tracked_when_visibility_change_fails() -> None:
    channel = make_poll_channel(requeue_delay_seconds=7)
    queued_message = FakeMessage({})
    track(channel, "tag_1", queued_message)
    FakeSyncQueueClient.instances[0].extend_error = RuntimeError("extend failed")

    with pytest.raises(RuntimeError, match="extend failed"):
        channel.basic_reject("tag_1", requeue=True)

    assert channel._messages_by_tag["tag_1"].message is queued_message


def test_basic_reject_without_requeue_acknowledges_leased_delivery() -> None:
    channel = make_poll_channel()
    queued_message = FakeMessage({})
    track(channel, "tag_1", queued_message)

    channel.basic_reject("tag_1", requeue=False)

    assert FakeSyncQueueClient.instances[0].acknowledged == [queued_message.metadata]
    assert FakeSyncQueueClient.instances[0].visibility_changes == []
    assert FakeSyncQueueClient.instances[0].lease_renewals[0].closed == 1
    assert channel._messages_by_tag == {}


def test_basic_get_no_ack_acknowledges_immediately_and_does_not_track() -> None:
    channel = make_poll_channel()
    queued_message = FakeMessage(message())
    FakeSyncQueueClient.instances[0].message_batches.append([queued_message])

    received = channel.basic_get("emails", no_ack=True)

    assert received is not None
    assert FakeSyncQueueClient.instances[0].acknowledged == [queued_message.metadata]
    assert channel._messages_by_tag == {}


def test_basic_consume_no_ack_acknowledges_after_delivery() -> None:
    channel = make_poll_channel()
    queued_message = FakeMessage(message())
    FakeSyncQueueClient.instances[0].message_batches.append([queued_message])
    received: list[Any] = []

    channel.basic_consume("emails", no_ack=True, callback=received.append, consumer_tag="ctag")
    channel.drain_events()

    assert len(received) == 1
    assert FakeSyncQueueClient.instances[0].acknowledged == [queued_message.metadata]
    assert channel._messages_by_tag == {}


def test_basic_consume_no_ack_does_not_ack_when_callback_fails() -> None:
    channel = make_poll_channel(requeue_delay_seconds=6)
    queued_message = FakeMessage(message())
    FakeSyncQueueClient.instances[0].message_batches.append([queued_message])

    def fail(message: Any) -> None:
        del message
        raise RuntimeError("delivery failed")

    channel.basic_consume("emails", no_ack=True, callback=fail, consumer_tag="ctag")

    with pytest.raises(RuntimeError, match="delivery failed"):
        channel.drain_events()

    assert FakeSyncQueueClient.instances[0].acknowledged == []
    assert FakeSyncQueueClient.instances[0].visibility_changes == [(queued_message.metadata, 6)]
    assert FakeSyncQueueClient.instances[0].lease_renewals[0].closed == 1
    assert channel._messages_by_tag == {}


def test_push_accept_no_ack_acknowledges_after_delivery() -> None:
    channel = make_push_channel()
    queued_message = FakeMessage(message())
    received: list[Any] = []

    channel.basic_consume("emails", no_ack=True, callback=received.append, consumer_tag="ctag")
    with pytest.raises(Handoff):
        channel._handle_queue_delivery(
            queued_message.payload,
            queued_message.metadata,
            queue="emails",
        )

    assert len(received) == 1
    assert FakeSyncQueueClient.instances[0].acknowledged == [queued_message.metadata]
    assert channel._messages_by_tag == {}


def test_queue_purge_raises_channel_error() -> None:
    channel = make_poll_channel()

    with pytest.raises(ChannelError, match="does not support queue purge"):
        channel.queue_purge("emails")


def test_close_does_not_republish_unacked_messages() -> None:
    channel = make_push_channel()
    track(channel, "tag_1", FakeMessage({}))
    renewal = FakeSyncQueueClient.instances[0].lease_renewals[0]

    channel.close()

    assert channel.closed
    assert renewal.closed == 1


def test_celery_integration_does_not_import_internal_lease_helpers() -> None:
    source = Path(vqs_celery.__file__).read_text(encoding="utf-8")

    assert "vercel.queue._internal.lease" not in source
