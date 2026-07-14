from __future__ import annotations

from typing import Any, ClassVar, cast
from typing_extensions import Self

import gc
import inspect
import json
import logging
import sys
import threading
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from weakref import ref

import pytest
from celery import Celery as CeleryApp
from celery.app import backends as celery_backends
from celery.app.defaults import DEFAULTS as CELERY_DEFAULTS
from kombu import Connection, Queue
from kombu.exceptions import ChannelError
from kombu.transport import TRANSPORT_ALIASES
from kombu.transport.virtual.base import Empty

import vercel.integrations.celery as public_vqs_celery
import vercel.integrations.celery._broker as vqs_celery
from vercel.headers import get_headers, set_headers
from vercel.queue import (
    CommunicationError as QueueCommunicationError,
    Duration,
    Handoff,
    Message,
    MessageMetadata,
    RetryAfter,
    SanitizedName,
    TokenResolutionError,
    Topic,
    UnauthorizedError,
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
        self.send_headers: list[dict[str, str] | None] = []
        self.message_batches: list[list[FakeMessage]] = []
        self.acknowledged: list[MessageMetadata] = []
        self.ack_headers: list[dict[str, str] | None] = []
        self.poll_headers: list[dict[str, str] | None] = []
        self.visibility_changes: list[tuple[MessageMetadata, Duration]] = []
        self.visibility_headers: list[dict[str, str] | None] = []
        self.lease_renewals: list[FakeLeaseRenewal] = []
        self.accepted: list[dict[str, Any]] = []
        self.closed = False
        self.ack_error: Exception | None = None
        self.extend_error: Exception | None = None
        FakeSyncQueueClient.instances.append(self)

    def send(self, topic: str, payload: dict[str, Any], **kwargs: Any) -> None:
        headers = get_headers()
        self.send_headers.append(dict(headers) if headers is not None else None)
        self.sent.append({"topic": topic, "payload": payload, "kwargs": kwargs})

    def poll(
        self,
        topic: str,
        consumer_group: str,
        **kwargs: Any,
    ) -> Iterator[FakeDelivery]:
        headers = get_headers()
        self.poll_headers.append(dict(headers) if headers is not None else None)
        batch = self.message_batches.pop(0) if self.message_batches else []
        self.last_topic = topic
        self.last_consumer_group = consumer_group
        self.last_poll_kwargs = kwargs
        for message in batch:
            yield FakeDelivery(message)

    def acknowledge(self, message: Message[dict[str, Any]] | MessageMetadata) -> None:
        if self.ack_error is not None:
            raise self.ack_error
        headers = get_headers()
        self.ack_headers.append(dict(headers) if headers is not None else None)
        self.acknowledged.append(message.metadata if isinstance(message, Message) else message)

    def extend_lease(
        self,
        message: Message[dict[str, Any]] | MessageMetadata,
        duration: Duration,
    ) -> None:
        if self.extend_error is not None:
            raise self.extend_error
        metadata = message.metadata if isinstance(message, Message) else message
        headers = get_headers()
        self.visibility_headers.append(dict(headers) if headers is not None else None)
        self.visibility_changes.append((metadata, duration))

    def retry_after(
        self,
        message: Message[dict[str, Any]] | MessageMetadata,
        delay: Duration,
    ) -> None:
        self.extend_lease(message, delay)

    def run_lease_renewal(
        self,
        message: Message[dict[str, Any]],
        lease_duration: Duration | None = None,
    ) -> FakeLeaseRenewal:
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
    vqs_celery._set_default_broker_set_by_installer(value=False)
    vqs_celery._registered_app_queues.clear()
    vqs_celery._registered_queue_subscriptions.clear()
    vqs_celery._embedded_workers.clear()
    vqs_celery._registered_callbacks.clear()
    vqs_celery._push_channels.clear()
    vqs_celery._finalize_hook_state.installed = False
    vqs_celery._finalize_hook_state.register_queues = False
    try:
        yield
    finally:
        CELERY_DEFAULTS["broker_url"] = original_broker_url
        CELERY_DEFAULTS["result_backend"] = original_result_backend
        vqs_celery._set_default_broker_set_by_installer(value=False)
        vqs_celery._registered_app_queues.clear()
        vqs_celery._registered_queue_subscriptions.clear()
        vqs_celery._embedded_workers.clear()
        vqs_celery._registered_callbacks.clear()
        vqs_celery._push_channels.clear()
        vqs_celery._finalize_hook_state.installed = False
        vqs_celery._finalize_hook_state.register_queues = False
        set_headers(None)


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
    )


def fake_renewal(tracked: vqs_celery._TrackedDelivery) -> FakeLeaseRenewal:
    return cast("FakeLeaseRenewal", tracked.lease_renewal)


def registered_callbacks() -> list[Any]:
    return [
        callback
        for app_callbacks in vqs_celery._registered_callbacks.values()
        for callback in app_callbacks.values()
    ]


def registered_callback(index: int = 0) -> Any:
    return registered_callbacks()[index]


def registered_callback_count() -> int:
    return len({id(callback) for callback in registered_callbacks()})


def celery_debug_events(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    return [
        json.loads(record.message)
        for record in caplog.records
        if record.name == "vercel.integrations.celery"
    ]


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
    assert vqs_celery._finalize_hook_state.installed is True
    assert vqs_celery._finalize_hook_state.register_queues is False


def test_install_vercel_celery_integration_seeds_prefix_without_queue_registration() -> None:
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    app = CeleryApp("producer-only")
    app.conf.broker_url = "vercel-poll://"

    with app.connection_for_write() as connection:
        channel = connection.channel()

    assert channel.queue_name_prefix == "celery-producer-only-"


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


def test_queue_name_prefix_maps_celery_queue_to_vqs_topic() -> None:
    channel = make_poll_channel(queue_name_prefix="celery-billing-")

    topic = channel._topic("emails")

    assert topic_name(topic) == "celery-billing-emails"
    assert topic_transport(topic) is channel._message_transport


def test_queue_name_prefix_can_be_disabled() -> None:
    channel = make_poll_channel(queue_name_prefix="")

    assert topic_name(channel._topic("emails")) == "emails"


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


def test_poll_put_normalizes_prefixed_topic_after_joining() -> None:
    channel = make_poll_channel(queue_name_prefix="celery-billing.api-")
    payload = message()

    channel._put("emails.high", payload)

    sent_topic = FakeSyncQueueClient.instances[0].sent[0]["topic"]
    assert topic_name(sent_topic) == "celery-billing_Dapi-emails_Dhigh"


def test_task_publish_seeds_prefix_before_channel_creation() -> None:
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    app = CeleryApp("publisher-app")

    @app.task(name="publisher.add")
    def add(left: int, right: int) -> int:
        return left + right

    add.chunks(zip(range(2), range(2), strict=False), 1).apply_async()

    sent_topics = [topic_name(sent["topic"]) for sent in FakeSyncQueueClient.instances[0].sent]
    assert sent_topics == ["celery-publisher-app-celery", "celery-publisher-app-celery"]


def test_channel_seeds_prefix_for_default_vercel_broker() -> None:
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)

    app = CeleryApp("default-prefix")
    with app.connection_for_write() as connection:
        channel = connection.channel()

    assert app.conf.broker_transport_options == {}
    assert channel.queue_name_prefix == "celery-default-prefix-"


def test_channel_skips_prefix_for_explicit_non_vercel_broker() -> None:
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)

    app = CeleryApp("memory-prefix", broker="memory://")
    with app.connection_for_write() as connection:
        channel = connection.channel()

    assert "queue_name_prefix" not in app.conf.broker_transport_options
    assert "queue_name_prefix" not in connection.transport_options
    assert channel.__class__.__module__.startswith("kombu.")


def test_channel_seeds_prefix_for_explicit_vercel_broker() -> None:
    public_vqs_celery.install_vercel_celery_integration(
        register_queues=False,
        set_default_broker=False,
    )

    app = CeleryApp("poll-prefix", broker="vercel-poll://")
    with app.connection_for_write() as connection:
        channel = connection.channel()

    assert app.conf.broker_transport_options == {}
    assert channel.queue_name_prefix == "celery-poll-prefix-"


def test_channel_preserves_explicit_empty_prefix() -> None:
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)

    app = CeleryApp(
        "empty-prefix",
        broker_transport_options={"queue_name_prefix": ""},
    )
    with app.connection_for_write() as connection:
        channel = connection.channel()

    assert not app.conf.broker_transport_options["queue_name_prefix"]
    assert not connection.transport_options["queue_name_prefix"]
    assert not channel.queue_name_prefix


def test_plain_kombu_connection_keeps_empty_prefix_without_celery_app() -> None:
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)

    with Connection("vercel-poll://") as connection:
        channel = connection.channel()

    assert not channel.queue_name_prefix


def test_celery_connection_prefix_uses_owner_not_current_app() -> None:
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    owner = CeleryApp("owner-prefix")
    current = CeleryApp("current-prefix")
    current.set_current()

    with owner.connection_for_write() as connection:
        channel = connection.channel()

    assert channel.queue_name_prefix == "celery-owner-prefix-"
    assert connection.transport_options["queue_name_prefix"] == "celery-owner-prefix-"
    assert owner.conf.broker_transport_options == {}


def test_install_applies_defaults_to_existing_loaded_app() -> None:
    app = CeleryApp("existing-loaded")
    assert app.conf.broker_url is None
    assert app.conf.result_backend is None

    public_vqs_celery.install_vercel_celery_integration(register_queues=False)

    assert app.conf.broker_url == "vercel://"
    assert app.conf.result_backend == "vercel-runtime-cache://"


def test_config_from_object_keeps_transport_options_without_shadowing() -> None:
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    app = CeleryApp("object-config")

    app.config_from_object(
        {
            "broker_url": "vercel-poll://",
            "broker_transport_options": {
                "consumer_group": "workers",
                "lease_duration": 30,
            },
        },
        force=True,
    )
    app.finalize()
    channel = make_poll_channel(**app.conf.broker_transport_options)

    assert app.conf.broker_transport_options == {
        "consumer_group": "workers",
        "lease_duration": 30,
        "queue_name_prefix": "celery-object-config-",
    }
    assert channel.queue_name_prefix == "celery-object-config-"


def test_config_from_object_module_keeps_transport_options_without_shadowing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_module = tmp_path / "celery_module_config.py"
    config_module.write_text(
        "broker_url = 'vercel-poll://'\n"
        "broker_transport_options = {\n"
        "    'consumer_group': 'module-workers',\n"
        "    'lease_duration': 45,\n"
        "}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    app = CeleryApp("module-config")

    app.config_from_object("celery_module_config", force=True)
    app.finalize()
    channel = make_poll_channel(**app.conf.broker_transport_options)

    assert app.conf.broker_transport_options == {
        "consumer_group": "module-workers",
        "lease_duration": 45,
        "queue_name_prefix": "celery-module-config-",
    }
    assert channel.queue_name_prefix == "celery-module-config-"
    sys.modules.pop("celery_module_config", None)


def test_config_from_envvar_keeps_transport_options_without_shadowing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_module = tmp_path / "celery_config.py"
    config_module.write_text(
        "broker_url = 'vercel-poll://'\n"
        "broker_transport_options = {\n"
        "    'consumer_group': 'workers',\n"
        "    'lease_duration': 30,\n"
        "}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("CELERY_CONFIG_MODULE", "celery_config")

    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    app = CeleryApp("env-config")

    app.config_from_envvar("CELERY_CONFIG_MODULE", force=True)
    app.finalize()
    channel = make_poll_channel(**app.conf.broker_transport_options)

    assert app.conf.broker_transport_options == {
        "consumer_group": "workers",
        "lease_duration": 30,
        "queue_name_prefix": "celery-env-config-",
    }
    assert channel.queue_name_prefix == "celery-env-config-"
    sys.modules.pop("celery_config", None)


def test_post_finalize_transport_reconfiguration_keeps_channel_and_registration_prefix(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("post-finalize-config")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)
    app.finalize()

    app.conf.update(broker_transport_options={"consumer_group": "workers"})
    with app.connection_for_write() as connection:
        channel = connection.channel()
        vqs_celery.register_celery_app_queues(app, start_worker=False)

    assert channel.consumer_group == "workers"
    assert channel.queue_name_prefix == "celery-post-finalize-config-"
    assert [sub.topic for sub in fake_queue_subscribe] == ["celery-post-finalize-config-emails"]
    assert [sub.consumer_group for sub in fake_queue_subscribe] == ["workers"]


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


def test_poll_drain_events_batches_to_available_prefetch() -> None:
    channel = make_poll_channel(lease_duration=30)
    channel.qos.prefetch_count = 3
    queued_messages = [FakeMessage(message(f"tag_{index}"), topic="emails") for index in range(3)]
    FakeSyncQueueClient.instances[0].message_batches.append(queued_messages)
    received: list[Any] = []

    channel.basic_consume("emails", no_ack=False, callback=received.append, consumer_tag="ctag")
    channel.drain_events()

    assert len(received) == 3
    assert FakeSyncQueueClient.instances[0].last_poll_kwargs == {
        "limit": 3,
        "lease_duration": 30,
    }
    assert len(channel._messages_by_tag) == 3
    assert len(channel.qos._delivered) == 3


def test_poll_drain_events_caps_batch_size_at_vqs_limit() -> None:
    channel = make_poll_channel()
    channel.qos.prefetch_count = 20
    FakeSyncQueueClient.instances[0].message_batches.append([])
    channel.basic_consume("emails", no_ack=False, callback=lambda value: None, consumer_tag="ctag")

    with pytest.raises(Empty):
        channel.drain_events()

    assert FakeSyncQueueClient.instances[0].last_poll_kwargs["limit"] == 10


def test_poll_drain_events_rotates_starting_queue() -> None:
    channel = make_poll_channel()
    channel.qos.prefetch_count = 1
    channel.basic_consume("critical", no_ack=False, callback=lambda value: None, consumer_tag="c1")
    channel.basic_consume("bulk", no_ack=False, callback=lambda value: None, consumer_tag="c2")

    assert channel._poll_queue_order(["critical", "bulk"]) == ("critical", "bulk")
    assert channel._poll_queue_order(["critical", "bulk"]) == ("bulk", "critical")
    assert channel._poll_queue_order(["critical", "bulk"]) == ("critical", "bulk")


def test_poll_drain_events_releases_unhandled_batch_remainder_on_delivery_failure() -> None:
    channel = make_poll_channel(requeue_delay_seconds=6)
    queued_messages = [
        FakeMessage(message(f"tag_{index}"), topic="emails", consumer_group="celery")
        for index in range(4)
    ]
    FakeSyncQueueClient.instances[0].message_batches.append(queued_messages)
    seen: list[Any] = []

    def callback(value: Any) -> None:
        seen.append(value)
        if len(seen) == 2:
            raise RuntimeError("decode failed")

    channel.qos.prefetch_count = 10
    channel.basic_consume("emails", no_ack=False, callback=callback, consumer_tag="ctag")

    with pytest.raises(RuntimeError, match="decode failed"):
        channel.drain_events()

    assert [metadata for metadata, _ in FakeSyncQueueClient.instances[0].visibility_changes] == [
        queued_messages[2].metadata,
        queued_messages[3].metadata,
    ]
    assert [duration for _, duration in FakeSyncQueueClient.instances[0].visibility_changes] == [
        6,
        6,
    ]
    assert len(channel._messages_by_tag) == 2


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


def test_auto_channel_drain_events_does_not_poll_on_vercel(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL", "1")
    channel = make_auto_channel()
    channel.basic_consume("emails", no_ack=False, callback=lambda value: None, consumer_tag="ctag")

    with pytest.raises(Empty):
        channel.drain_events()

    assert not hasattr(FakeSyncQueueClient.instances[0], "last_poll_kwargs")


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
    assert ("celery-existing-app-celery", "celery") in [
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

    assert [sub.topic for sub in fake_queue_subscribe] == ["celery-push-transport-emails"]
    assert [sub.consumer_group for sub in fake_queue_subscribe] == ["celery"]
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

    assert [sub.topic for sub in fake_queue_subscribe] == ["celery-push-no-worker-emails"]
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
            "hostname": "vercel-celery-embedded-worker@localhost",
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


def test_start_embedded_worker_thread_does_not_inherit_header_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_headers: list[dict[str, str] | None] = []
    worker_started = threading.Event()

    class FakeWorkController:
        consumer = object()

        def __init__(self, **kwargs: Any) -> None:
            pass

        def start(self) -> None:
            headers = get_headers()
            seen_headers.append(dict(headers) if headers is not None else None)
            worker_started.set()

    app = CeleryApp("worker-context")
    monkeypatch.setattr(app, "WorkController", FakeWorkController)
    monkeypatch.setattr(vqs_celery, "_wait_for_embedded_worker_channel", lambda worker: None)

    set_headers({"x-vercel-oidc-token": "boot"})
    try:
        _REAL_START_EMBEDDED_WORKER(app)
    finally:
        set_headers(None)

    assert worker_started.wait(5)
    assert seen_headers == [None]


def test_transports_treat_auth_and_network_errors_as_recoverable() -> None:
    for transport in (
        public_vqs_celery.VercelQueueTransport,
        public_vqs_celery.VercelQueuePollTransport,
        public_vqs_celery.VercelQueuePushTransport,
    ):
        assert TokenResolutionError in transport.connection_errors
        assert UnauthorizedError in transport.connection_errors
        assert QueueCommunicationError in transport.connection_errors


def test_embedded_worker_ready_requires_channel_for_same_app() -> None:
    @dataclass
    class FakeWorker:
        consumer: ClassVar[object] = object()
        app: CeleryApp

    @dataclass
    class FakeClient:
        app: CeleryApp

    @dataclass
    class FakeChannelConnection:
        client: FakeClient

    class FakeChannel:
        closed = False

        def __init__(self, app: CeleryApp) -> None:
            self.connection = FakeChannelConnection(FakeClient(app))

    first_app = CeleryApp("ready-first")
    second_app = CeleryApp("ready-second")
    vqs_celery._push_channels[:] = cast("Any", [FakeChannel(first_app)])

    assert vqs_celery._embedded_worker_channel_ready(FakeWorker(second_app)) is False
    assert vqs_celery._embedded_worker_channel_ready(FakeWorker(first_app)) is True


def test_register_celery_app_queues_registers_untyped_message_callback(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("push-untyped-callback")
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    app.conf.broker_transport = "vercel-push"
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app, start_worker=False)

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

    assert [sub.topic for sub in fake_queue_subscribe] == ["celery-auto-transport-emails"]


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

    assert [sub.topic for sub in fake_queue_subscribe] == [
        "celery-push-normalized-topic-emails_Dhigh"
    ]


def test_register_celery_app_queues_honors_transport_queue_name_prefix(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("push-prefix")
    configure_push_broker(app)
    app.conf.broker_transport_options = {"queue_name_prefix": "jobs-"}
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["jobs-emails"]
    assert app.conf.broker_transport_options["queue_name_prefix"] == "jobs-"


def test_register_celery_app_queues_can_disable_queue_name_prefix(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("push-prefix-disabled")
    configure_push_broker(app)
    app.conf.broker_transport_options = {"queue_name_prefix": ""}
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["emails"]


def test_register_celery_app_queues_reuses_shared_topic_subscription(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    first = CeleryApp("first-shared")
    second = CeleryApp("second-shared")
    configure_push_broker(first)
    configure_push_broker(second)
    first.conf.broker_transport_options = {"queue_name_prefix": "shared-"}
    second.conf.broker_transport_options = {"queue_name_prefix": "shared-"}
    first.conf.task_queues = (Queue("emails"),)
    second.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(first)
    vqs_celery.register_celery_app_queues(second)

    assert [sub.topic for sub in fake_queue_subscribe] == ["shared-emails"]
    assert registered_callback_count() == 1
    assert ("shared-emails", "celery") in vqs_celery._registered_app_queues[first]
    assert ("shared-emails", "celery") in vqs_celery._registered_app_queues[second]
    assert vqs_celery._registered_queue_subscriptions[first]["shared-emails", "celery"] == (
        "emails"
    )
    assert vqs_celery._registered_queue_subscriptions[second]["shared-emails", "celery"] == (
        "emails"
    )


def test_registered_queue_subscriptions_are_scoped_to_live_apps(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("weak-subscription", set_as_current=False)
    app_ref = ref(app)
    configure_push_broker(app)
    app.conf.broker_transport_options = {"queue_name_prefix": "weak-"}
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app, start_worker=False)

    assert app in vqs_celery._registered_queue_subscriptions
    app.close()
    del app

    for _ in range(3):
        gc.collect()
        if app_ref() is None:
            break

    assert app_ref() is None
    assert list(vqs_celery._registered_queue_subscriptions.items()) == []


def test_registered_callbacks_are_scoped_to_live_apps(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("weak-callback", set_as_current=False)
    app_ref = ref(app)
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app, start_worker=False)

    assert registered_callback_count() == 1
    assert app in vqs_celery._registered_callbacks
    assert len(fake_queue_subscribe) == 1
    app.close()
    del app

    for _ in range(3):
        gc.collect()
        if app_ref() is None:
            break

    assert app_ref() is None
    assert registered_callback_count() == 0


def test_register_celery_app_queues_honors_transport_consumer_group(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("push-consumer-group")
    configure_push_broker(app)
    app.conf.broker_transport_options = {"consumer_group": "api/celery_worker.py"}
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["celery-push-consumer-group-emails"]
    assert [sub.consumer_group for sub in fake_queue_subscribe] == ["api_Scelery__worker_Dpy"]
    assert app.conf.broker_transport_options["consumer_group"] == "api/celery_worker.py"
    assert app.conf.broker_transport_options["queue_name_prefix"] == "celery-push-consumer-group-"


def test_register_celery_app_queues_derives_queue_name_prefix_from_main(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("push.consumer_group")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["celery-push_Dconsumer__group-emails"]
    assert [sub.consumer_group for sub in fake_queue_subscribe] == ["celery"]
    assert app.conf.broker_transport_options["consumer_group"] == "celery"
    assert app.conf.broker_transport_options["queue_name_prefix"] == "celery-push.consumer_group-"


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
    assert not app.conf.broker_transport_options["queue_name_prefix"]


def test_register_celery_app_queues_accepts_push_transport_subclass(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    class CustomPushTransport(public_vqs_celery.VercelQueuePushTransport):
        pass

    app = CeleryApp("subclass-push")
    app.conf.broker_transport = CustomPushTransport
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["celery-subclass-push-emails"]


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


def test_app_finalize_does_not_seed_defaults_for_non_vercel_brokers() -> None:
    app = CeleryApp("finalize-memory")
    app.conf.broker_url = "memory://"

    vqs_celery._register_finalized_app_queues(app)

    assert not app.conf.broker_transport_options


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

    assert fake_queue_subscribe == []
    assert app.conf.broker_transport_options["queue_name_prefix"] == "celery-finalize-auto-vercel-"


def test_app_finalize_registers_push_brokers(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("finalize-push")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery._register_finalized_app_queues(app)

    assert fake_queue_subscribe == []
    assert app.conf.broker_transport_options["queue_name_prefix"] == "celery-finalize-push-"


def test_app_finalize_registers_subscriptions_for_configured_queues(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("configured")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"), Queue("reports"))

    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == [
        "celery-configured-emails",
        "celery-configured-reports",
    ]
    assert [sub.consumer_group for sub in fake_queue_subscribe] == [
        "celery",
        "celery",
    ]
    assert registered_callback_count() == 2


def test_app_finalize_registers_synthesized_default_queue(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("default")
    configure_push_broker(app)

    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["celery-default-celery"]
    assert registered_callback_count() == 1


def test_app_finalize_registration_is_idempotent(
    fake_queue_subscribe: list[FakeSubscription],
) -> None:
    app = CeleryApp("idempotent")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)

    vqs_celery.register_celery_app_queues(app)
    vqs_celery.register_celery_app_queues(app)

    assert [sub.topic for sub in fake_queue_subscribe] == ["celery-idempotent-emails"]
    assert registered_callback_count() == 1


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
        registered_callback()(queued_message)
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


def test_push_delivery_follow_ups_do_not_replay_delivery_headers() -> None:
    app = CeleryApp("callback-no-context-replay")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)
    channel = make_push_channel(requeue_delay_seconds=7)
    received: list[Any] = []
    channel.basic_consume("emails", no_ack=False, callback=received.append, consumer_tag="ctag")
    vqs_celery.register_celery_app_queues(app)

    set_headers({"x-vercel-oidc-token": "token-1"})
    with pytest.raises(Handoff):
        registered_callback()(FakeMessage(message(), topic="emails"))
    first_tag = received[-1].delivery_tag

    set_headers({"x-vercel-oidc-token": "token-2"})
    with pytest.raises(Handoff):
        registered_callback()(FakeMessage(message(), topic="emails"))
    second_tag = received[-1].delivery_tag

    set_headers({"x-vercel-oidc-token": "current"})
    channel.basic_ack(first_tag)
    channel.basic_reject(second_tag, requeue=True)

    fake_client = cast("FakeSyncQueueClient", channel._queue_client)
    assert fake_client.ack_headers == [{"x-vercel-oidc-token": "current"}]
    assert fake_client.visibility_headers == [{"x-vercel-oidc-token": "current"}]
    assert get_headers() == {"x-vercel-oidc-token": "current"}
    assert len(FakeSyncQueueClient.instances) == 1


def deliver_push_message(headers: dict[str, str]) -> None:
    set_headers(headers)
    try:
        with pytest.raises(Handoff):
            registered_callback()(FakeMessage(message(), topic="emails"))
    finally:
        set_headers(None)


def test_publish_without_request_context_does_not_install_delivery_headers() -> None:
    app = CeleryApp("no-context-fallback-put")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)
    channel = make_push_channel()
    received: list[Any] = []
    channel.basic_consume("emails", no_ack=False, callback=received.append, consumer_tag="ctag")
    vqs_celery.register_celery_app_queues(app)

    deliver_push_message({"x-vercel-oidc-token": "token-1"})
    channel._put("emails", message())

    deliver_push_message({"x-vercel-oidc-token": "token-2"})
    channel._put("emails", message())

    fake_client = cast("FakeSyncQueueClient", channel._queue_client)
    assert fake_client.send_headers == [None, None]


def test_publish_preserves_ambient_request_context() -> None:
    app = CeleryApp("context-ambient")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)
    channel = make_push_channel()
    received: list[Any] = []
    channel.basic_consume("emails", no_ack=False, callback=received.append, consumer_tag="ctag")
    vqs_celery.register_celery_app_queues(app)

    deliver_push_message({"x-vercel-oidc-token": "stale"})
    set_headers({"x-vercel-oidc-token": "current"})
    try:
        channel._put("emails", message())
    finally:
        set_headers(None)

    fake_client = cast("FakeSyncQueueClient", channel._queue_client)
    assert fake_client.send_headers == [{"x-vercel-oidc-token": "current"}]


def test_poll_without_request_context_does_not_install_delivery_headers() -> None:
    app = CeleryApp("no-context-fallback-poll")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)
    push_channel = make_push_channel()
    received: list[Any] = []
    push_channel.basic_consume(
        "emails",
        no_ack=False,
        callback=received.append,
        consumer_tag="ctag",
    )
    vqs_celery.register_celery_app_queues(app)
    deliver_push_message({"x-vercel-oidc-token": "token-1"})

    channel = make_poll_channel()
    fake_client = cast("FakeSyncQueueClient", channel._queue_client)
    fake_client.message_batches.append([FakeMessage(message())])
    payload = channel._get("emails")
    channel.basic_ack(payload["properties"]["delivery_tag"])

    assert fake_client.poll_headers == [None]
    assert fake_client.ack_headers == [None]


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
        registered_callback()(queued_message)

    assert celery_received == []
    assert len(workers_received) == 1
    assert celery_channel.connection.delivered == []
    assert workers_channel.connection.delivered[0][1] == "emails"


def test_registered_callback_uses_channel_that_consumes_queue() -> None:
    connection = FakeConnection(FakeClientOptions())
    consuming_channel = vqs_celery.PushChannel(connection)
    idle_channel = vqs_celery.PushChannel(connection)
    received: list[Any] = []
    consuming_channel.basic_consume(
        "emails",
        no_ack=False,
        callback=received.append,
        consumer_tag="ctag",
    )

    assert "emails" in idle_channel.connection._callbacks
    assert vqs_celery._find_push_channel("emails", "celery") is consuming_channel

    with pytest.raises(Handoff):
        vqs_celery._make_queue_callback("emails")(FakeMessage(message()))

    delivery_tag = received[0].delivery_tag
    assert delivery_tag in consuming_channel._messages_by_tag
    assert idle_channel._messages_by_tag == {}


def test_registered_callback_retries_when_consumer_group_has_no_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(vqs_celery, "_PUSH_CHANNEL_WAIT_SECONDS", 0.0)
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
        registered_callback()(queued_message)

    assert exc_info.value.timeout_seconds == 1
    assert received == []
    assert channel.connection.delivered == []


def test_registered_callback_retries_without_active_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(vqs_celery, "_PUSH_CHANNEL_WAIT_SECONDS", 0.0)
    app = CeleryApp("callback-retry")
    configure_push_broker(app)
    app.conf.task_queues = (Queue("emails"),)
    queued_message = FakeMessage(message())

    vqs_celery.register_celery_app_queues(app)
    with pytest.raises(RetryAfter) as exc_info:
        registered_callback()(queued_message)

    assert exc_info.value.timeout_seconds == 1


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

        def _consumes_queue(self, queue: str) -> bool:
            return queue in self.connection._callbacks

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

    assert cast("FakeSyncQueueClient", channel._queue_client).acknowledged == [
        queued_message.metadata
    ]
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
    channel = make_push_channel(push_handoff_wait_seconds=0)
    queued_message = FakeMessage(message())

    with pytest.raises(RetryAfter) as exc_info:
        channel._handle_queue_delivery(
            queued_message.payload,
            queued_message.metadata,
            queue="unknown",
        )

    assert exc_info.value.timeout_seconds == 1
    assert FakeSyncQueueClient.instances[0].acknowledged == []
    assert FakeSyncQueueClient.instances[0].visibility_changes == []
    assert channel._messages_by_tag == {}


def test_push_queue_callback_retries_when_prefetch_is_exhausted() -> None:
    channel = make_push_channel(
        requeue_delay_seconds=8,
        push_retry_delay_seconds=3,
        push_handoff_wait_seconds=0,
    )
    channel.qos.prefetch_count = 1
    cast("Any", channel.qos._delivered)["existing"] = object()
    queued_message = FakeMessage(message())
    channel.basic_consume("emails", no_ack=False, callback=lambda value: None, consumer_tag="ctag")

    with pytest.raises(RetryAfter) as exc_info:
        channel._handle_queue_delivery(
            queued_message.payload,
            queued_message.metadata,
            queue="emails",
        )

    assert exc_info.value.timeout_seconds == 3
    assert FakeSyncQueueClient.instances[0].acknowledged == []
    assert FakeSyncQueueClient.instances[0].visibility_changes == []
    assert channel._messages_by_tag == {}


@dataclass
class _FakeWorkController:
    consumer: Any


def _register_fake_embedded_worker(app: CeleryApp, consumer: Any) -> None:
    vqs_celery._embedded_workers[app] = vqs_celery._EmbeddedWorker(
        app=app,
        worker=_FakeWorkController(consumer),
        thread=cast("Any", None),
    )


def test_push_queue_delivery_settles_deferred_ack_inline() -> None:
    app = CeleryApp("inline-settle")
    channel = make_push_channel()
    queued_message = FakeMessage(message())
    pending_operations: list[Any] = []
    received: list[Any] = []

    def consume(delivered: Any) -> None:
        received.append(delivered)
        # Celery consumers defer settlement into the worker's pending
        # operations; emulate that instead of acking inline.
        pending_operations.append(lambda: channel.basic_ack(delivered.delivery_tag))

    class FakeConsumer:
        def perform_pending_operations(self) -> None:
            while pending_operations:
                pending_operations.pop()()

    _register_fake_embedded_worker(app, FakeConsumer())
    channel.basic_consume("emails", no_ack=False, callback=consume, consumer_tag="ctag")

    with pytest.raises(Handoff):
        channel._handle_queue_delivery(
            queued_message.payload,
            queued_message.metadata,
            queue="emails",
        )

    assert len(received) == 1
    assert cast("FakeSyncQueueClient", channel._queue_client).acknowledged == [
        queued_message.metadata
    ]
    assert channel._messages_by_tag == {}
    assert pending_operations == []


def test_push_queue_delivery_waits_for_prefetch_capacity() -> None:
    app = CeleryApp("capacity-wait")
    channel = make_push_channel(push_handoff_wait_seconds=5)
    channel.qos.prefetch_count = 1
    cast("Any", channel.qos._delivered)["existing"] = object()
    queued_message = FakeMessage(message())
    received: list[Any] = []

    def consume(delivered: Any) -> None:
        received.append(delivered)
        channel.basic_ack(delivered.delivery_tag)

    class FakeConsumer:
        def perform_pending_operations(self) -> None:
            # Emulate the deferred ACK of a previously delivered message
            # freeing prefetch capacity.
            cast("Any", channel.qos._delivered).pop("existing", None)

    _register_fake_embedded_worker(app, FakeConsumer())
    channel.basic_consume("emails", no_ack=False, callback=consume, consumer_tag="ctag")

    with pytest.raises(Handoff):
        channel._handle_queue_delivery(
            queued_message.payload,
            queued_message.metadata,
            queue="emails",
        )

    assert len(received) == 1


def test_push_queue_delivery_waits_for_handoff_lock() -> None:
    channel = make_push_channel(push_handoff_wait_seconds=5)
    queued_message = FakeMessage(message())
    received: list[Any] = []

    def consume(delivered: Any) -> None:
        received.append(delivered)
        channel.basic_ack(delivered.delivery_tag)

    channel.basic_consume("emails", no_ack=False, callback=consume, consumer_tag="ctag")

    held = threading.Event()
    release = threading.Event()

    def hold_lock() -> None:
        with channel._push_handoff_lock:
            held.set()
            release.wait(5)

    holder = threading.Thread(target=hold_lock)
    holder.start()
    assert held.wait(5)
    releaser = threading.Timer(0.1, release.set)
    releaser.start()
    try:
        with pytest.raises(Handoff):
            channel._handle_queue_delivery(
                queued_message.payload,
                queued_message.metadata,
                queue="emails",
            )
    finally:
        release.set()
        holder.join(5)
        releaser.cancel()

    assert len(received) == 1


def test_push_accept_releases_lease_when_callback_fails() -> None:
    channel = make_push_channel(requeue_delay_seconds=9, push_retry_delay_seconds=4)
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
    assert FakeSyncQueueClient.instances[0].visibility_changes == [(queued_message.metadata, 4)]
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


def test_basic_ack_stops_tracking_when_ack_fails() -> None:
    channel = make_poll_channel()
    queued_message = FakeMessage({})
    track(channel, "tag_1", queued_message)
    cast("Any", channel.qos._delivered)["tag_1"] = object()
    FakeSyncQueueClient.instances[0].ack_error = RuntimeError("ack failed")

    with pytest.raises(RuntimeError, match="ack failed"):
        channel.basic_ack("tag_1")

    assert FakeSyncQueueClient.instances[0].lease_renewals[0].closed == 1
    assert channel._messages_by_tag == {}
    assert channel.qos._dirty == {"tag_1"}


def test_basic_reject_requeues_by_changing_visibility() -> None:
    channel = make_poll_channel(requeue_delay_seconds=7)
    queued_message = FakeMessage({})
    track(channel, "tag_1", queued_message)

    channel.basic_reject("tag_1", requeue=True)

    assert FakeSyncQueueClient.instances[0].visibility_changes == [(queued_message.metadata, 7)]
    assert FakeSyncQueueClient.instances[0].acknowledged == []
    assert FakeSyncQueueClient.instances[0].lease_renewals[0].closed == 1
    assert channel._messages_by_tag == {}


def test_basic_reject_stops_tracking_when_visibility_change_fails() -> None:
    channel = make_poll_channel(requeue_delay_seconds=7)
    queued_message = FakeMessage({})
    track(channel, "tag_1", queued_message)
    FakeSyncQueueClient.instances[0].extend_error = RuntimeError("extend failed")

    with pytest.raises(RuntimeError, match="extend failed"):
        channel.basic_reject("tag_1", requeue=True)

    assert FakeSyncQueueClient.instances[0].lease_renewals[0].closed == 1
    assert channel._messages_by_tag == {}


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


def test_basic_ack_for_unknown_tag_does_not_dirty_qos() -> None:
    channel = make_poll_channel()

    channel.basic_ack("unknown-tag")

    assert channel.qos._dirty == set()


def test_basic_reject_for_unknown_tag_does_not_dirty_qos() -> None:
    channel = make_poll_channel()

    channel.basic_reject("unknown-tag", requeue=True)

    assert channel.qos._dirty == set()


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
    assert channel.qos._dirty == set()


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
    assert channel.qos._dirty == set()


def test_debug_log_uses_celery_debug_env(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_CELERY_DEBUG", "1")
    caplog.set_level(logging.INFO, logger="vercel.integrations.celery")

    vqs_celery.debug_log(
        "celery.test",
        queue="emails",
        message_id="msg_1",
        ignored=None,
    )

    [event] = celery_debug_events(caplog)
    assert event == {
        "event": "celery.test",
        "message_id": "msg_1",
        "queue": "emails",
    }


def test_debug_log_ignores_queue_debug_env(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.delenv("VERCEL_CELERY_DEBUG", raising=False)
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level(logging.INFO, logger="vercel.integrations.celery")

    vqs_celery.debug_log("celery.test", queue="emails")

    assert celery_debug_events(caplog) == []


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
    assert FakeSyncQueueClient.instances[0].visibility_changes == [
        (FakeSyncQueueClient.instances[0].lease_renewals[0].message.metadata, 0)
    ]


def test_celery_integration_does_not_import_internal_lease_helpers() -> None:
    source = Path(vqs_celery.__file__).read_text(encoding="utf-8")

    assert "vercel.queue._internal.lease" not in source
