from __future__ import annotations

# Settings must be configured before importing django.tasks.
# ruff: noqa: I001

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import pytest

from django.conf import settings

if not settings.configured:
    settings.configure(
        SECRET_KEY="tests",
        USE_TZ=True,
        INSTALLED_APPS=[],
        TASKS={
            "default": {
                "BACKEND": "vercel.integrations.django.VercelQueuesBackend",
                "QUEUES": ["default"],
                "OPTIONS": {
                    "result_namespace": "django.results",
                    "result_ttl_seconds": 120,
                },
            }
        },
    )

import django
from django.tasks import TaskResultStatus, task, task_backends
from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_enqueued, task_finished, task_started

import vercel.integrations.django as public_api
import vercel.integrations.django._backend as vqs_django
from vercel.queue import Message, MessageMetadata, RetryAfter

django.setup()


@task
def add_one(value: int) -> int:
    return value + 1


@task(takes_context=True)
def attempt_number(context: Any) -> int:
    return context.attempt


@task
def fail_forever() -> None:
    raise RuntimeError("nope")


@task
async def async_add_one(value: int) -> int:
    return value + 1


@task(takes_context=True)
async def async_attempt_number(context: Any) -> int:
    return context.attempt


@task
async def async_fail_forever() -> None:
    raise RuntimeError("async nope")


@dataclass
class FakeSubscription:
    topic: Any
    consumer_group: str
    max_attempts: int | None
    callback: Any


def topic_name(topic: Any) -> str:
    return str(getattr(topic, "name", topic))


class FakeRuntimeCache:
    instances: ClassVar[list[FakeRuntimeCache]] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.values: dict[str, object] = {}
        self.set_options: dict[str, dict[str, object]] = {}
        FakeRuntimeCache.instances.append(self)

    def get(self, key: str) -> object | None:
        return self.values.get(key)

    def set(self, key: str, value: object, options: dict[str, object]) -> None:
        self.values[key] = value
        self.set_options[key] = options


class FakeSyncQueueClient:
    instances: ClassVar[list[FakeSyncQueueClient]] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.sent: list[dict[str, Any]] = []
        FakeSyncQueueClient.instances.append(self)

    def send(self, topic: Any, payload: dict[str, Any], **kwargs: Any) -> str:
        self.sent.append({"topic": topic, "payload": payload, "kwargs": kwargs})
        return "msg_1"


class FakeAsyncQueueClient:
    instances: ClassVar[list[FakeAsyncQueueClient]] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.sent: list[dict[str, Any]] = []
        FakeAsyncQueueClient.instances.append(self)

    async def send(self, topic: Any, payload: dict[str, Any], **kwargs: Any) -> str:
        self.sent.append({"topic": topic, "payload": payload, "kwargs": kwargs})
        return "msg_async"


@pytest.fixture(autouse=True)
def clean_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[FakeSubscription]]:
    FakeSyncQueueClient.instances.clear()
    FakeAsyncQueueClient.instances.clear()
    FakeRuntimeCache.instances.clear()
    vqs_django._registered_subscribers.clear()
    task_backends.close_all()
    monkeypatch.setattr(vqs_django, "RuntimeCache", FakeRuntimeCache)
    monkeypatch.setattr(vqs_django.vqs_sync, "QueueClient", FakeSyncQueueClient)
    monkeypatch.setattr(vqs_django.vqs, "QueueClient", FakeAsyncQueueClient)
    existing_backend = task_backends["default"]
    if isinstance(existing_backend, vqs_django.VercelQueuesBackend):
        existing_backend._sync_queue_client = None
        existing_backend._async_queue_client = None
        existing_backend._results = vqs_django._RuntimeCacheResults(
            namespace=existing_backend._cfg.result_namespace,
            ttl=existing_backend._cfg.result_ttl_seconds,
        )
    subscriptions: list[FakeSubscription] = []

    def subscribe(
        *,
        topic: Any = None,
        consumer_group: str = "default",
        max_attempts: int | None = None,
        **kwargs: Any,
    ) -> Any:
        del kwargs

        def decorator(callback: Any) -> Any:
            subscriptions.append(
                FakeSubscription(
                    topic=topic,
                    consumer_group=consumer_group,
                    max_attempts=max_attempts,
                    callback=callback,
                )
            )
            return callback

        return decorator

    monkeypatch.setattr(vqs_django.vqs, "subscribe", subscribe)
    try:
        yield subscriptions
    finally:
        vqs_django._registered_subscribers.clear()
        task_backends.close_all()


def backend() -> vqs_django.VercelQueuesBackend:
    return cast("vqs_django.VercelQueuesBackend", task_backends["default"])


def configure_backend(
    alias: str,
    queues: list[str],
    *,
    options: object | None = None,
) -> vqs_django.VercelQueuesBackend:
    params: dict[str, Any] = {
        "BACKEND": "vercel.integrations.django.VercelQueuesBackend",
        "QUEUES": queues,
    }
    if options is not None:
        params["OPTIONS"] = options
    task_backends.settings[alias] = params
    return cast("vqs_django.VercelQueuesBackend", task_backends[alias])


def sent_envelope(*, asynchronous: bool = False) -> dict[str, Any]:
    if asynchronous:
        return FakeAsyncQueueClient.instances[0].sent[-1]["payload"]
    return FakeSyncQueueClient.instances[0].sent[-1]["payload"]


def message(payload: dict[str, Any], *, message_id: str = "msg_1") -> Message[Any]:
    return Message(
        payload=payload,
        metadata=MessageMetadata(
            message_id=message_id,
            delivery_count=1,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            topic=payload["queue"],
            consumer_group=vqs_django.vqs.sanitize_name("django-tasks"),
            receipt_handle="rh_1",
            content_type="application/json",
        ),
    )


def test_public_api_is_minimal() -> None:
    assert public_api.__all__ == [
        "VercelQueuesBackend",
        "__version__",
        "install_vercel_django_task_integration",
    ]


@pytest.mark.parametrize(
    ("options", "error", "match"),
    [
        ({"token": "secret"}, ValueError, "Unknown.*token"),
        ({"result_namespace": ""}, ValueError, "non-empty string"),
        ({"result_namespace": 1}, ValueError, "non-empty string"),
        ({"result_ttl_seconds": 0}, ValueError, "positive integer"),
        ({"result_ttl_seconds": True}, ValueError, "positive integer"),
        ([], TypeError, "dictionary"),
    ],
)
def test_backend_options_are_strict(
    options: object,
    error: type[Exception],
    match: str,
) -> None:
    with pytest.raises(error, match=match):
        configure_backend("invalid", ["default"], options=options)


def test_default_backend_is_installed_only_by_installer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    django_default = {"default": {"BACKEND": "django.tasks.backends.immediate.ImmediateBackend"}}

    class FakeSettings:
        configured = True
        TASKS: ClassVar[dict[str, dict[str, str]]] = dict(django_default)

        @staticmethod
        def is_overridden(setting: str) -> bool:
            assert setting == "TASKS"
            return False

    class FakeImmediateBackend:
        closed = False

        def close(self) -> None:
            self.closed = True

    existing_backend = FakeImmediateBackend()
    fake_task_backends = SimpleNamespace(
        settings=dict(django_default),
        _connections=SimpleNamespace(default=existing_backend),
    )
    monkeypatch.setattr(vqs_django, "settings", FakeSettings())
    monkeypatch.setattr(vqs_django, "global_settings", SimpleNamespace(TASKS={}))
    monkeypatch.setattr(vqs_django, "task_backends", fake_task_backends)

    vqs_django._configure_default_task_backend()

    expected = {"BACKEND": "vercel.integrations.django.VercelQueuesBackend"}
    assert {"default": expected} == FakeSettings.TASKS
    assert fake_task_backends.settings == {"default": expected}
    assert existing_backend.closed is True
    assert not hasattr(fake_task_backends._connections, "default")


def test_installer_can_configure_publish_side_without_registering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registered = False

    def register(backend_alias: str) -> None:
        del backend_alias
        nonlocal registered
        registered = True

    monkeypatch.setattr(vqs_django, "_register_task_queues", register)

    vqs_django.install_vercel_django_task_integration(register_queues=False)

    assert registered is False


def test_installer_preserves_explicit_task_backends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured_backends = {"custom": {"BACKEND": "example.CustomBackend"}}

    class FakeSettings:
        configured = True
        TASKS = configured_backends

        @staticmethod
        def is_overridden(setting: str) -> bool:
            assert setting == "TASKS"
            return True

    monkeypatch.setattr(vqs_django, "settings", FakeSettings())
    monkeypatch.setattr(vqs_django, "global_settings", SimpleNamespace(TASKS={}))
    monkeypatch.setattr(
        vqs_django,
        "task_backends",
        SimpleNamespace(settings=dict(configured_backends)),
    )

    vqs_django._configure_default_task_backend()

    assert configured_backends == FakeSettings.TASKS


def test_enqueue_sends_private_envelope_and_stores_result() -> None:
    result = add_one.enqueue(41)

    client = FakeSyncQueueClient.instances[0]
    assert client.kwargs == {}
    assert topic_name(client.sent[0]["topic"]) == "default"
    assert isinstance(client.sent[0]["topic"].transport, vqs_django._TaskEnvelopeTransport)
    assert client.sent[0]["payload"] == {
        "version": 1,
        "task": add_one.module_path,
        "queue": "default",
        "args": [41],
        "kwargs": {},
    }
    assert client.sent[0]["kwargs"] == {"delay": None}
    assert result.id == "msg_1"
    assert result.status == TaskResultStatus.READY
    cache = FakeRuntimeCache.instances[0]
    assert cache.kwargs == {"namespace": "django_Dresults", "strict": True}
    assert cache.set_options[result.id] == {"name": result.id, "ttl": 120}
    assert add_one.get_result(result.id).args == [41]


def test_enqueue_normalizes_transport_topic_but_preserves_logical_queue() -> None:
    subject = configure_backend("normalized", ["emails.high"])
    task_obj = subject._task_from_module_path(
        module_path=add_one.module_path,
        queue_name="emails.high",
    )

    subject.enqueue(task_obj, [41], {})

    sent = FakeSyncQueueClient.instances[0].sent[0]
    assert topic_name(sent["topic"]) == "emails_Dhigh"
    assert sent["payload"]["queue"] == "emails.high"


def test_enqueue_maps_run_after_to_delay() -> None:
    add_one.using(run_after=datetime.now(UTC) + timedelta(seconds=90)).enqueue(1)

    assert FakeSyncQueueClient.instances[0].sent[0]["kwargs"]["delay"] in {89, 90}


def test_enqueue_reuses_sync_client_and_close_clears_both_clients() -> None:
    add_one.enqueue(1)
    add_one.enqueue(2)
    subject = backend()
    subject._async_client()

    assert len(FakeSyncQueueClient.instances) == 1
    assert [item["payload"]["args"] for item in FakeSyncQueueClient.instances[0].sent] == [
        [1],
        [2],
    ]

    subject.close()

    assert subject._sync_queue_client is None
    assert subject._async_queue_client is None


@pytest.mark.asyncio
async def test_native_async_enqueue_has_sync_parity_and_reuses_client() -> None:
    first = await add_one.aenqueue(1)
    second = await add_one.aenqueue(2)

    assert first.id == second.id == "msg_async"
    assert len(FakeAsyncQueueClient.instances) == 1
    assert [item["payload"]["args"] for item in FakeAsyncQueueClient.instances[0].sent] == [
        [1],
        [2],
    ]
    assert set(sent_envelope(asynchronous=True)) == {"version", "task", "queue", "args", "kwargs"}


def test_private_result_record_round_trip() -> None:
    subject = backend()
    result = add_one.enqueue(1)
    record = subject._serialize_result(result)

    restored = subject._deserialize_result(record)

    assert restored.id == result.id
    assert restored.task.module_path == add_one.module_path
    assert restored.status == TaskResultStatus.READY
    assert restored.args == [1]


def test_get_result_raises_for_missing_or_malformed_results() -> None:
    cache = FakeRuntimeCache.instances[0]
    cache.values["bad-wrapper"] = {"record": {}}
    cache.values["bad-record"] = vqs_django._wrap_result_record({"version": 1})

    for result_id in ("missing", "bad-wrapper", "bad-record"):
        with pytest.raises(TaskResultDoesNotExist):
            backend().get_result(result_id)


def test_runtime_cache_namespace_and_ttl_are_fixed_set_options() -> None:
    subject = configure_backend(
        "custom-results",
        ["default"],
        options={"result_namespace": "my.results", "result_ttl_seconds": 45},
    )
    task_obj = subject._task_from_module_path(module_path=add_one.module_path, queue_name="default")

    result = subject.enqueue(task_obj, [1], {})

    cache = FakeRuntimeCache.instances[-1]
    assert cache.kwargs == {"namespace": "my_Dresults", "strict": True}
    assert cache.set_options[result.id] == {"name": result.id, "ttl": 45}


def test_installer_registration_is_idempotent_and_normalizes_topics(
    clean_state: list[FakeSubscription],
) -> None:
    subject = configure_backend("normalized", ["emails.high"])
    del subject

    vqs_django.install_vercel_django_task_integration("normalized")
    vqs_django.install_vercel_django_task_integration("normalized")

    assert [
        (topic_name(item.topic), item.consumer_group, item.max_attempts) for item in clean_state
    ] == [("emails_Dhigh", "django-tasks", 3)]
    assert set(vqs_django._registered_subscribers) == {
        ("normalized", "emails_Dhigh", "django-tasks")
    }


@pytest.mark.asyncio
async def test_deferred_execution_success_and_context(
    clean_state: list[FakeSubscription],
) -> None:
    result = add_one.enqueue(2)
    payload = sent_envelope()
    vqs_django.install_vercel_django_task_integration()

    await clean_state[0].callback(message(payload, message_id=result.id))

    refreshed = add_one.get_result(result.id)
    assert refreshed.status == TaskResultStatus.SUCCESSFUL
    assert refreshed.return_value == 3
    assert refreshed.attempts == 1

    context_result = attempt_number.enqueue()
    await clean_state[0].callback(message(sent_envelope(), message_id=context_result.id))
    assert attempt_number.get_result(context_result.id).return_value == 1


@pytest.mark.asyncio
async def test_execution_emits_lifecycle_signals(
    clean_state: list[FakeSubscription],
) -> None:
    seen: list[tuple[str, TaskResultStatus]] = []

    def receiver(sender: object, task_result: Any, signal: Any, **kwargs: Any) -> None:
        del sender, kwargs
        if signal is task_enqueued:
            name = "enqueued"
        elif signal is task_started:
            name = "started"
        else:
            name = "finished"
        seen.append((name, task_result.status))

    for signal in (task_enqueued, task_started, task_finished):
        signal.connect(receiver, weak=False)
    try:
        result = add_one.enqueue(1)
        vqs_django.install_vercel_django_task_integration()
        await clean_state[0].callback(message(sent_envelope(), message_id=result.id))
    finally:
        for signal in (task_enqueued, task_started, task_finished):
            signal.disconnect(receiver)

    assert seen == [
        ("enqueued", TaskResultStatus.READY),
        ("started", TaskResultStatus.RUNNING),
        ("finished", TaskResultStatus.SUCCESSFUL),
    ]


@pytest.mark.asyncio
async def test_retryable_and_terminal_failures(
    clean_state: list[FakeSubscription],
) -> None:
    result = fail_forever.enqueue()
    payload = sent_envelope()
    vqs_django.install_vercel_django_task_integration()

    with pytest.raises(RetryAfter) as first:
        await clean_state[0].callback(message(payload, message_id=result.id))
    with pytest.raises(RetryAfter) as second:
        await clean_state[0].callback(message(payload, message_id=result.id))
    await clean_state[0].callback(message(payload, message_id=result.id))

    assert first.value.timeout_seconds == 5
    assert second.value.timeout_seconds == 10
    refreshed = fail_forever.get_result(result.id)
    assert refreshed.status == TaskResultStatus.FAILED
    assert refreshed.attempts == 3
    assert len(refreshed.errors) == 3


@pytest.mark.asyncio
async def test_async_task_success_and_context(
    clean_state: list[FakeSubscription],
) -> None:
    result = async_add_one.enqueue(2)
    vqs_django.install_vercel_django_task_integration()
    await clean_state[0].callback(message(sent_envelope(), message_id=result.id))
    assert async_add_one.get_result(result.id).return_value == 3

    context_result = async_attempt_number.enqueue()
    await clean_state[0].callback(message(sent_envelope(), message_id=context_result.id))
    assert async_attempt_number.get_result(context_result.id).return_value == 1


@pytest.mark.asyncio
async def test_async_task_retryable_and_terminal_failure(
    clean_state: list[FakeSubscription],
) -> None:
    result = async_fail_forever.enqueue()
    payload = sent_envelope()
    vqs_django.install_vercel_django_task_integration()

    with pytest.raises(RetryAfter):
        await clean_state[0].callback(message(payload, message_id=result.id))
    with pytest.raises(RetryAfter):
        await clean_state[0].callback(message(payload, message_id=result.id))
    await clean_state[0].callback(message(payload, message_id=result.id))

    refreshed = async_fail_forever.get_result(result.id)
    assert refreshed.status == TaskResultStatus.FAILED
    assert len(refreshed.errors) == 3


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        (None, TypeError),
        ({"version": 2, "task": "x", "queue": "q", "args": [], "kwargs": {}}, ValueError),
        ({"version": 1, "task": "", "queue": "q", "args": [], "kwargs": {}}, TypeError),
        ({"version": 1, "task": "x", "queue": "q", "args": {}, "kwargs": {}}, TypeError),
    ],
)
def test_private_envelope_rejects_malformed_payloads(
    payload: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        vqs_django._parse_envelope(payload)
