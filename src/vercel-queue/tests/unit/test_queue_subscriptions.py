from __future__ import annotations

from typing import Annotated, Any, ClassVar, Final, Literal, Optional, TypeVar, Union, cast

import gc
import json
import logging
import re
from collections.abc import AsyncIterable, Callable, Iterable
from dataclasses import replace
from datetime import timedelta
from operator import getitem

import anyio
import anyio.lowlevel
import pytest
from pydantic import BaseModel

from vercel.queue import (
    ByteBufferTransport,
    ByteStreamTransport,
    DuplicateSubscriptionError,
    Message,
    MessageMetadata,
    PayloadValidationError,
    QueueError,
    QueueSubscriber,
    RetryAfter,
    SanitizedName,
    StrContainer,
    Subscription,
    SubscriptionError,
    TextBufferTransport,
    TextStreamTransport,
    Topic,
    UnhandledMessageError,
    get_subscriptions,
    sanitize_name,
    subscribe,
)
from vercel.queue._internal import subscribers as queue_subscribers
from vercel.queue._internal.constants import DEFAULT_RETRY_AFTER_SECONDS
from vercel.queue._internal.names import normalize_name
from vercel.queue._internal.streams import (
    AsyncStreamPayload,
    SyncStreamPayload,
    SyncTextStreamPayload,
)
from vercel.queue._internal.subscribers import (
    call_subscribers,
    call_subscribers_sync,
    infer_subscriber_transport,
    poll_targets_for_subscriber,
    register_embedded_dispatcher,
)
from vercel.queue.devserver import EmbeddedQueueDevServer
from vercel.queue.embedded import embedded_queue_service
from vercel.queue.testing import clear_subscriptions

from .helpers import (
    CREATED_AT_DT,
    make_metadata,
    wait_until,
)


def _typecheck_subscribe_typed_topic_examples() -> None:
    class Payload(BaseModel):
        count: int

    topic = Topic[Payload]("typecheck")

    @subscribe(topic=topic)
    def direct_payload(payload: Payload) -> None:
        del payload

    @subscribe(topic=topic)
    def direct_message(message: Message[Payload]) -> None:
        del message

    def factory_payload(payload: Payload) -> None:
        del payload

    def factory_message(message: Message[Payload]) -> None:
        del message

    subscribe(topic=topic)(factory_payload)
    subscribe(topic=topic)(factory_message)
    subscribe(None, topic=topic)(factory_payload)

    subscriber: QueueSubscriber[[Payload], None] = direct_payload
    del subscriber

    raw_subscriber: QueueSubscriber[[Payload], None] = factory_payload  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
    del raw_subscriber

    del direct_payload, direct_message


def _typecheck_str_container_examples() -> None:
    list_topics: StrContainer = ["events-one"]
    tuple_topics: StrContainer = ("events-one", "events-two")
    set_topics: StrContainer = {"events-one"}
    frozenset_topics: StrContainer = frozenset({"events-one"})
    del list_topics, tuple_topics, set_topics, frozenset_topics

    string_topics: StrContainer = "events-one"  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
    del string_topics


def _queue_debug_events(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    return [
        json.loads(record.message) for record in caplog.records if record.name == "vercel.queue"
    ]


class _OneShotSubscriberRef:
    def __init__(self, func: Callable[..., object], *, live_once: bool = True) -> None:
        self._func = func
        self._live_once = live_once
        self.calls = 0

    def __call__(self) -> Callable[..., object] | None:
        self.calls += 1
        if self._live_once and self.calls == 1:
            return self._func
        return None


def _replace_first_subscription_ref(subscriber_ref: object) -> None:
    snapshot = queue_subscribers._registry_snapshot
    subscription = snapshot.subscriptions[0]
    replacement = replace(subscription, func_ref=cast("Any", subscriber_ref))

    def replace_refs(
        subscriptions: tuple[Any, ...],
    ) -> tuple[Any, ...]:
        return tuple(replacement if sub is subscription else sub for sub in subscriptions)

    queue_subscribers._registry_snapshot = queue_subscribers._RegistrySnapshot(
        subscriptions=replace_refs(snapshot.subscriptions),
        wildcard_by_consumer={
            key: replace_refs(value) for key, value in snapshot.wildcard_by_consumer.items()
        },
        exact_by_consumer_topic={
            key: replace_refs(value) for key, value in snapshot.exact_by_consumer_topic.items()
        },
        prefix_by_consumer={
            key: replace_refs(value) for key, value in snapshot.prefix_by_consumer.items()
        },
        dispatchers=snapshot.dispatchers,
    )


def test_get_subscriptions_returns_subscription_snapshot(
    isolated_subscriptions: None,
) -> None:
    calls: list[str] = []

    @subscribe(topic="*")
    def catch_all(payload: object) -> None:
        calls.append("all")

    @subscribe(topic="orders")
    def orders(payload: object) -> None:
        calls.append("orders")

    @subscribe(topic="events-*")
    def events(payload: object) -> None:
        calls.append("events")

    snapshot = get_subscriptions()

    assert snapshot == (
        Subscription(func=catch_all, topic="*", consumer_group=snapshot[0].consumer_group),
        Subscription(func=orders, topic="orders", consumer_group=snapshot[1].consumer_group),
        Subscription(func=events, topic="events-*", consumer_group=snapshot[2].consumer_group),
    )


def test_subscribe_typed_topic_registers_exact_topic_and_payload_handler(
    isolated_subscriptions: None,
) -> None:
    class Payload(BaseModel):
        count: int

    calls: list[Payload] = []

    @subscribe(topic=Topic[Payload]("typed-orders"), consumer_group="test-group")
    def handle(payload: Payload) -> None:
        calls.append(payload)

    assert get_subscriptions() == (
        Subscription(func=handle, topic="typed-orders", consumer_group="test-group"),
    )

    call_subscribers_sync(
        Message(
            payload={"count": "3"},
            metadata=make_metadata("typed-orders", consumer_group="test-group"),
        )
    )

    assert calls == [Payload(count=3)]


def test_subscribe_typed_topic_accepts_message_handler(
    isolated_subscriptions: None,
) -> None:
    class Payload(BaseModel):
        count: int

    calls: list[Message[Payload]] = []

    @subscribe(topic=Topic[Payload]("typed-messages"), consumer_group="test-group")
    def handle(message: Message[Payload]) -> None:
        calls.append(message)

    metadata = make_metadata("typed-messages", consumer_group="test-group")
    call_subscribers_sync(Message(payload={"count": "4"}, metadata=metadata))

    assert len(calls) == 1
    assert calls[0].payload == Payload(count=4)
    assert calls[0].metadata is metadata


def test_subscribe_typed_topic_drives_untyped_handler_validation(
    isolated_subscriptions: None,
) -> None:
    class Payload(BaseModel):
        count: int

    calls: list[Payload] = []

    @subscribe(topic=Topic[Payload]("typed-untyped"), consumer_group="test-group")
    def handle(payload: object) -> None:
        calls.append(cast("Payload", payload))

    call_subscribers_sync(
        Message(
            payload={"count": "5"},
            metadata=make_metadata("typed-untyped", consumer_group="test-group"),
        )
    )

    assert calls == [Payload(count=5)]


def test_subscribe_typed_topic_drives_unparameterized_message_validation(
    isolated_subscriptions: None,
) -> None:
    class Payload(BaseModel):
        count: int

    calls: list[Message[object]] = []

    def handle(message: Message[object]) -> None:
        calls.append(message)

    handle.__annotations__["message"] = Message
    cast("Any", subscribe)(topic=Topic[Payload]("typed-message"), consumer_group="test-group")(
        handle
    )

    call_subscribers_sync(
        Message(
            payload={"count": "6"},
            metadata=make_metadata("typed-message", consumer_group="test-group"),
        )
    )

    assert calls[0].payload == Payload(count=6)


@pytest.mark.parametrize(
    ("topic", "transport_type"),
    [
        (Topic[bytes]("typed-bytes"), ByteBufferTransport),
        (Topic[str]("typed-text"), TextBufferTransport),
        (Topic[SyncStreamPayload]("typed-stream"), ByteStreamTransport),
    ],
)
def test_subscribe_typed_topic_drives_untyped_handler_transport_inference(
    isolated_subscriptions: None,
    topic: Topic[object],
    transport_type: type[object],
) -> None:
    def handle(payload: object) -> None:
        del payload

    subscribe(topic=topic, consumer_group="test-group")(handle)

    assert isinstance(
        infer_subscriber_transport(make_metadata(str(topic.name), consumer_group="test-group")),
        transport_type,
    )


def test_subscribe_typed_topic_rejects_handler_topic_type_mismatch(
    isolated_subscriptions: None,
) -> None:
    class Payload(BaseModel):
        count: int

    with pytest.raises(SubscriptionError, match="incompatible with topic payload annotation"):

        @cast("Any", subscribe)(topic=Topic[Payload]("typed-mismatch"), consumer_group="test-group")
        def handle(payload: dict[str, str]) -> None:
            del payload


def test_subscribe_unspecialized_topic_uses_handler_annotation(
    isolated_subscriptions: None,
) -> None:
    class Payload(BaseModel):
        count: int

    calls: list[Payload] = []

    @subscribe(topic=Topic("untyped-topic"), consumer_group="test-group")
    def handle(payload: Payload) -> None:
        calls.append(payload)

    assert get_subscriptions() == (
        Subscription(func=handle, topic="untyped-topic", consumer_group="test-group"),
    )

    call_subscribers_sync(
        Message(
            payload={"count": "7"},
            metadata=make_metadata("untyped-topic", consumer_group="test-group"),
        )
    )

    assert calls == [Payload(count=7)]


def test_get_subscriptions_prunes_dead_subscribers(
    isolated_subscriptions: None,
) -> None:
    def register() -> None:
        @subscribe(topic="dead")
        def handle(payload: object) -> None:
            del payload

    register()
    gc.collect()

    assert get_subscriptions() == ()


def test_call_subscribers_sync_invokes_callable_captured_during_matching(
    isolated_subscriptions: None,
) -> None:
    calls: list[object] = []
    metadata = make_metadata("emails", consumer_group="test-group")

    @subscribe(topic="emails", consumer_group="test-group")
    def handle(payload: object) -> None:
        calls.append(payload)

    subscriber_ref = _OneShotSubscriberRef(handle)
    _replace_first_subscription_ref(subscriber_ref)

    call_subscribers_sync(Message(payload={"ok": True}, metadata=metadata))

    assert calls == [{"ok": True}]
    assert subscriber_ref.calls == 1


@pytest.mark.anyio
async def test_call_subscribers_invokes_callable_captured_during_matching(
    isolated_subscriptions: None,
) -> None:
    calls: list[object] = []
    metadata = make_metadata("emails", consumer_group="test-group")

    @subscribe(topic="emails", consumer_group="test-group")
    async def handle(payload: object) -> None:
        calls.append(payload)

    subscriber_ref = _OneShotSubscriberRef(handle)
    _replace_first_subscription_ref(subscriber_ref)

    await call_subscribers(Message(payload={"ok": True}, metadata=metadata))

    assert calls == [{"ok": True}]
    assert subscriber_ref.calls == 1


def test_call_subscribers_sync_debug_logs_handler_start(
    isolated_subscriptions: None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level(logging.INFO, logger="vercel.queue")
    calls: list[object] = []
    metadata = MessageMetadata(
        message_id="msg-debug-sync",
        delivery_count=3,
        created_at=CREATED_AT_DT,
        topic="emails",
        consumer_group=SanitizedName("test-group"),
        region="iad1",
    )

    @subscribe(topic="emails", consumer_group="test-group")
    def handle(payload: object) -> None:
        calls.append(payload)

    call_subscribers_sync(Message(payload={"ok": True}, metadata=metadata))

    assert calls == [{"ok": True}]
    assert _queue_debug_events(caplog)[-1] == {
        "event": "message.handler_start",
        "message_id": "msg-debug-sync",
        "topic": "emails",
        "consumer_group": "test-group",
        "delivery_count": 3,
        "region": "iad1",
        "handler": f"{handle.__module__}.{handle.__qualname__}",
    }


@pytest.mark.anyio
async def test_call_subscribers_debug_logs_handler_start(
    isolated_subscriptions: None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level(logging.INFO, logger="vercel.queue")
    calls: list[object] = []
    metadata = MessageMetadata(
        message_id="msg-debug-async",
        delivery_count=2,
        created_at=CREATED_AT_DT,
        topic="emails",
        consumer_group=SanitizedName("test-group"),
    )

    @subscribe(topic="emails", consumer_group="test-group")
    async def handle(payload: object) -> None:
        calls.append(payload)

    await call_subscribers(Message(payload={"ok": True}, metadata=metadata))

    assert calls == [{"ok": True}]
    assert _queue_debug_events(caplog)[-1] == {
        "event": "message.handler_start",
        "message_id": "msg-debug-async",
        "topic": "emails",
        "consumer_group": "test-group",
        "delivery_count": 2,
        "handler": f"{handle.__module__}.{handle.__qualname__}",
    }


def test_poll_targets_preserve_sanitized_consumer_group(
    isolated_subscriptions: None,
) -> None:
    @subscribe(topic="emails", consumer_group="team/email_high")
    def handle(payload: object) -> None:
        del payload

    ((topic, consumer_group),) = poll_targets_for_subscriber(handle, None)
    assert topic == "emails"
    assert isinstance(consumer_group, SanitizedName)
    assert consumer_group == "team_Semail__high"
    # Polling feeds targets back through normalize_name; a SanitizedName
    # must pass through unchanged instead of being escaped a second time.
    assert normalize_name(consumer_group) is consumer_group

    ((_, explicit_topic_group),) = poll_targets_for_subscriber(handle, ["emails"])
    assert isinstance(explicit_topic_group, SanitizedName)
    assert explicit_topic_group == "team_Semail__high"


def test_call_subscribers_sync_raises_when_matching_refs_are_dead(
    isolated_subscriptions: None,
) -> None:
    calls: list[object] = []
    metadata = make_metadata("emails", consumer_group="test-group")

    @subscribe(topic="emails", consumer_group="test-group")
    def handle(payload: object) -> None:
        calls.append(payload)

    subscriber_ref = _OneShotSubscriberRef(handle, live_once=False)
    _replace_first_subscription_ref(subscriber_ref)

    with pytest.raises(UnhandledMessageError):
        call_subscribers_sync(Message(payload={"ok": True}, metadata=metadata))

    assert calls == []
    assert subscriber_ref.calls == 1


def test_subscribe_rejects_duplicate_exact_topic_in_consumer_group(
    isolated_subscriptions: None,
) -> None:
    @subscribe(topic="events-created", consumer_group="test-group")
    def first(payload: object) -> None:
        del payload

    with pytest.raises(
        DuplicateSubscriptionError,
        match=re.escape(
            "'events-created' overlaps existing topic pattern 'events-created' "
            f"for consumer group 'test-group'; conflicting handler: "
            f"{first.__module__}.{first.__qualname__}"
        ),
    ):

        @subscribe(topic="events-created", consumer_group="test-group")
        def second(payload: object) -> None:
            del payload


@pytest.mark.parametrize("topic", ["events-created", "events-*"])
def test_subscribe_rejects_wildcard_overlap_in_consumer_group(
    isolated_subscriptions: None,
    topic: str,
) -> None:
    @subscribe(topic="*", consumer_group="test-group")
    def catch_all(payload: object) -> None:
        del payload

    with pytest.raises(
        DuplicateSubscriptionError,
        match=re.escape(
            f"{topic!r} overlaps existing topic pattern '*' for consumer group 'test-group'; "
            f"conflicting handler: {catch_all.__module__}.{catch_all.__qualname__}"
        ),
    ):

        @subscribe(topic=topic, consumer_group="test-group")
        def specific(payload: object) -> None:
            del payload


def test_subscribe_rejects_prefix_exact_overlap_in_consumer_group(
    isolated_subscriptions: None,
) -> None:
    @subscribe(topic="events-*", consumer_group="test-group")
    def prefix(payload: object) -> None:
        del payload

    with pytest.raises(
        DuplicateSubscriptionError,
        match=re.escape(
            "'events-created' overlaps existing topic pattern 'events-*' "
            f"for consumer group 'test-group'; conflicting handler: "
            f"{prefix.__module__}.{prefix.__qualname__}"
        ),
    ):

        @subscribe(topic="events-created", consumer_group="test-group")
        def exact(payload: object) -> None:
            del payload


def test_subscribe_rejects_prefix_prefix_overlap_in_consumer_group(
    isolated_subscriptions: None,
) -> None:
    @subscribe(topic="events-*", consumer_group="test-group")
    def prefix(payload: object) -> None:
        del payload

    with pytest.raises(
        DuplicateSubscriptionError,
        match=re.escape(
            "'events-created-*' overlaps existing topic pattern 'events-*' "
            f"for consumer group 'test-group'; conflicting handler: "
            f"{prefix.__module__}.{prefix.__qualname__}"
        ),
    ):

        @subscribe(topic="events-created-*", consumer_group="test-group")
        def narrower_prefix(payload: object) -> None:
            del payload


def test_subscribe_allows_overlapping_patterns_across_consumer_groups(
    isolated_subscriptions: None,
) -> None:
    calls: list[str] = []

    @subscribe(topic="events-*", consumer_group="analytics")
    def analytics(payload: object) -> None:
        del payload
        calls.append("analytics")

    @subscribe(topic="events-created", consumer_group="test-group")
    def test_group(payload: object) -> None:
        del payload
        calls.append("test-group")

    call_subscribers_sync(
        Message(
            payload={},
            metadata=make_metadata("events-created", consumer_group="test-group"),
        )
    )
    call_subscribers_sync(
        Message(
            payload={},
            metadata=make_metadata("events-created", consumer_group="analytics"),
        )
    )

    assert calls == ["test-group", "analytics"]


def test_matching_subscriptions_uses_only_delivery_consumer_group(
    isolated_subscriptions: None,
) -> None:
    calls: list[str] = []
    metadata = make_metadata("events-created", consumer_group="test-group")

    @subscribe(topic="events-created", consumer_group="analytics")
    def analytics_exact(payload: object) -> None:
        del payload
        calls.append("analytics-exact")

    @subscribe(topic="events-*", consumer_group="prefix-analytics")
    def analytics_prefix(payload: object) -> None:
        del payload
        calls.append("analytics-prefix")

    @subscribe(topic="events-created", consumer_group="test-group")
    def test_group_exact(payload: object) -> None:
        del payload
        calls.append("test-group-exact")

    call_subscribers_sync(Message(payload={}, metadata=metadata))

    assert calls == ["test-group-exact"]


def test_clear_subscriptions_publishes_empty_snapshot(isolated_subscriptions: None) -> None:
    @subscribe(topic="emails", consumer_group="test-group")
    def handle(payload: object) -> None:
        del payload

    assert len(get_subscriptions()) == 1

    clear_subscriptions()

    assert get_subscriptions() == ()


def test_clear_subscriptions_clears_embedded_dispatchers(
    isolated_subscriptions: None,
) -> None:
    records: list[tuple[str, str]] = []

    class Dispatcher:
        def register_subscription(
            self,
            *,
            topic: str,
            consumer_group: str,
            retry_after_seconds: int | None,
            initial_delay_seconds: int | None,
            max_concurrency: int | None,
            max_attempts: int | None,
        ) -> None:
            del retry_after_seconds
            del initial_delay_seconds
            del max_concurrency
            del max_attempts
            records.append((topic, consumer_group))

    dispatcher = Dispatcher()
    register_embedded_dispatcher(dispatcher)

    @subscribe(topic="emails", consumer_group="test-group")
    def first(payload: object) -> None:
        del payload

    clear_subscriptions()

    @subscribe(topic="orders", consumer_group="test-group")
    def second(payload: object) -> None:
        del payload

    assert records == [("emails", "test-group")]


def test_payload_validation_error_is_queue_error() -> None:
    assert issubclass(PayloadValidationError, QueueError)


def test_duplicate_subscription_error_is_subscription_error() -> None:
    assert issubclass(DuplicateSubscriptionError, SubscriptionError)


def test_get_subscriptions_returns_trigger_metadata(
    isolated_subscriptions: None,
) -> None:
    @subscribe(
        topic="orders-*",
        consumer_group="test-group",
        retry_after=timedelta(minutes=2),
        initial_delay=5.9,
        max_concurrency=4,
        max_attempts=12,
    )
    def handle(payload: object) -> None:
        del payload

    subscriptions = get_subscriptions()

    assert len(subscriptions) == 1
    assert subscriptions[0].func is handle
    assert subscriptions[0].topic == "orders-*"
    assert subscriptions[0].consumer_group == "test-group"
    assert subscriptions[0].retry_after_seconds == 120
    assert subscriptions[0].initial_delay_seconds == 5
    assert subscriptions[0].max_concurrency == 4
    assert subscriptions[0].max_attempts == 12


def test_default_consumer_group_uses_shared_sanitizer(
    isolated_subscriptions: None,
) -> None:
    def handle(payload: object) -> None:
        del payload

    handle.__module__ = "tests.queue/module"
    handle.__qualname__ = "worker_group.handle.v1"

    subscribe(topic="emails")(handle)

    assert get_subscriptions()[0].consumer_group == sanitize_name(
        "tests.queue/module.worker_group.handle.v1"
    )


def test_default_consumer_group_fallback_uses_shared_sanitizer(
    isolated_subscriptions: None,
) -> None:
    def handle(payload: object) -> None:
        del payload

    handle.__module__ = cast("Any", None)
    handle.__qualname__ = "worker_group.handle.v1"

    subscribe(topic="emails")(handle)

    assert get_subscriptions()[0].consumer_group == sanitize_name("worker_group.handle.v1")


def test_subscribe_rejects_invalid_trigger_metadata(
    isolated_subscriptions: None,
) -> None:
    with pytest.raises(ValueError, match="max_concurrency must be non-negative"):

        @subscribe(topic="emails", max_concurrency=-1)
        def handle(payload: object) -> None:
            del payload

    with pytest.raises(TypeError, match="max_attempts must be an integer"):

        @subscribe(topic="emails", max_attempts=cast("Any", 1.5))
        def handle_float(payload: object) -> None:
            del payload

    with pytest.raises(ValueError, match="retry_after must be between 1 and 86400 seconds"):

        @subscribe(topic="emails", retry_after=0)
        def handle_retry_zero(payload: object) -> None:
            del payload

    with pytest.raises(ValueError, match="retry_after must be between 1 and 86400 seconds"):

        @subscribe(topic="emails", retry_after=86401)
        def handle_retry_too_large(payload: object) -> None:
            del payload

    with pytest.raises(ValueError, match="initial_delay must be between 0 and 86400 seconds"):

        @subscribe(topic="emails", initial_delay=86401)
        def handle_initial_delay_too_large(payload: object) -> None:
            del payload


def test_subscribe_rejects_invalid_signatures(isolated_subscriptions: None) -> None:
    with pytest.raises(TypeError, match="exactly one required payload"):

        @subscribe(topic="emails")
        def no_parameters() -> None:
            pass

    with pytest.raises(TypeError, match="exactly one required payload"):

        @subscribe(topic="emails")
        def two_required(payload: object, metadata: MessageMetadata) -> None:
            pass

    with pytest.raises(TypeError, match="keyword-only parameters must have defaults"):

        @subscribe(topic="emails")
        def required_keyword(payload: object, *, required: bool) -> None:
            pass

    with pytest.raises(TypeError, match=r"\*args or \*\*kwargs"):

        @subscribe(topic="emails")
        def varargs(payload: object, *args: object) -> None:
            pass

    with pytest.raises(TypeError, match=r"\*args or \*\*kwargs"):

        @subscribe(topic="emails")
        def kwargs(payload: object, **extra: object) -> None:
            pass


def test_subscribe_rejects_invalid_annotations(isolated_subscriptions: None) -> None:
    TPayload = TypeVar("TPayload")

    def class_var(payload: int) -> None:
        pass

    class_var.__annotations__["payload"] = ClassVar[int]

    def final(payload: int) -> None:
        pass

    final.__annotations__["payload"] = Final[int]

    with pytest.raises(TypeError, match="unsupported queue subscriber payload annotation"):

        @subscribe(topic="emails")
        def type_var(payload: TPayload) -> None:
            pass

    with pytest.raises(TypeError, match="unsupported queue subscriber payload annotation"):
        subscribe(topic="emails")(class_var)

    with pytest.raises(TypeError, match="unsupported queue subscriber payload annotation"):
        subscribe(topic="emails")(final)

    with pytest.raises(TypeError, match="unsupported bare queue subscriber payload annotation"):

        @subscribe(topic="emails")
        def bare_list(payload: list) -> None:
            pass

    with pytest.raises(TypeError, match="unsupported queue subscriber payload annotation"):

        @subscribe(topic="emails")
        def union_type_var(payload: int | TPayload) -> None:
            pass

    with pytest.raises(TypeError, match="unsupported bare queue subscriber payload annotation"):

        @subscribe(topic="emails")
        def union_bare_list(payload: list | int) -> None:
            pass

    def union_class_var(payload: int) -> None:
        pass

    annotated_class_var = getitem(Annotated, (getitem(ClassVar, int), "metadata"))
    union_class_var.__annotations__["payload"] = annotated_class_var | str

    with pytest.raises(TypeError, match="unsupported queue subscriber payload annotation"):
        subscribe(topic="emails")(union_class_var)


def test_subscribe_ignores_unrelated_unresolved_annotations(
    isolated_subscriptions: None,
) -> None:
    def unresolved_return(payload: object) -> None:
        del payload

    unresolved_return.__annotations__["return"] = "UndefinedReturn"
    subscribe(topic="emails")(unresolved_return)

    def unresolved_keyword_only(payload: object, *, extra: object = None) -> None:
        del payload, extra

    unresolved_keyword_only.__annotations__["extra"] = "MissingExtra"
    subscribe(topic="emails")(unresolved_keyword_only)

    def unresolved_defaulted_positional(
        payload: object,
        extra: object = None,
    ) -> None:
        del payload, extra

    unresolved_defaulted_positional.__annotations__["extra"] = "MissingExtra"
    subscribe(topic="emails")(unresolved_defaulted_positional)

    assert get_subscriptions()[0].func is unresolved_return
    assert get_subscriptions()[1].func is unresolved_keyword_only
    assert get_subscriptions()[2].func is unresolved_defaulted_positional


def test_subscribe_invalid_signature_precedes_annotation_resolution(
    isolated_subscriptions: None,
) -> None:
    def two_required(payload: object, metadata: object) -> None:
        del payload, metadata

    two_required.__annotations__["metadata"] = "MissingMetadata"
    with pytest.raises(TypeError, match="exactly one required payload"):
        subscribe(topic="emails")(two_required)


def test_subscribe_resolves_future_annotation_local_aliases(
    isolated_subscriptions: None,
) -> None:
    byte_alias = bytes
    text_alias = str
    bytes_iterable_alias = Iterable[bytes]
    text_async_iterable_alias = AsyncIterable[str]

    class Payload(BaseModel):
        count: int

    payload_alias = Payload
    assert byte_alias is bytes
    assert text_alias is str
    assert bytes_iterable_alias == Iterable[bytes]
    assert text_async_iterable_alias == AsyncIterable[str]
    assert payload_alias is Payload

    def bytes_payload(payload: object) -> None:
        del payload

    bytes_payload.__annotations__["payload"] = "byte_alias"
    subscribe(topic="bytes", consumer_group="test-group")(bytes_payload)

    assert isinstance(
        infer_subscriber_transport(make_metadata("bytes", consumer_group="test-group")),
        ByteBufferTransport,
    )

    def text_payload(payload: object) -> None:
        del payload

    text_payload.__annotations__["payload"] = "text_alias"
    subscribe(topic="text", consumer_group="test-group")(text_payload)

    assert isinstance(
        infer_subscriber_transport(make_metadata("text", consumer_group="test-group")),
        TextBufferTransport,
    )

    def bytes_stream(payload: object) -> None:
        del payload

    bytes_stream.__annotations__["payload"] = "bytes_iterable_alias"
    subscribe(topic="bytes-stream", consumer_group="test-group")(bytes_stream)

    assert isinstance(
        infer_subscriber_transport(make_metadata("bytes-stream", consumer_group="test-group")),
        ByteStreamTransport,
    )

    def text_stream(payload: object) -> None:
        del payload

    text_stream.__annotations__["payload"] = "text_async_iterable_alias"
    subscribe(topic="text-stream", consumer_group="test-group")(text_stream)

    assert isinstance(
        infer_subscriber_transport(make_metadata("text-stream", consumer_group="test-group")),
        TextStreamTransport,
    )

    def stream_payload(payload: AsyncStreamPayload) -> None:
        del payload

    subscribe(topic="stream-payload", consumer_group="test-group")(stream_payload)

    assert isinstance(
        infer_subscriber_transport(make_metadata("stream-payload", consumer_group="test-group")),
        ByteStreamTransport,
    )

    def text_stream_payload(payload: SyncTextStreamPayload) -> None:
        del payload

    subscribe(topic="text-stream-payload", consumer_group="test-group")(text_stream_payload)

    assert isinstance(
        infer_subscriber_transport(
            make_metadata("text-stream-payload", consumer_group="test-group")
        ),
        TextStreamTransport,
    )

    message_calls: list[Payload] = []

    def message_payload(message: Message[Payload]) -> None:
        message_calls.append(message.payload)

    message_payload.__annotations__["message"] = "Message[payload_alias]"
    subscribe(topic="message", consumer_group="test-group")(message_payload)

    call_subscribers_sync(
        Message(
            payload={"count": "3"},
            metadata=make_metadata("message", consumer_group="test-group"),
        )
    )

    assert message_calls == [Payload(count=3)]

    annotated_calls: list[Payload] = []

    def annotated_payload(payload: object) -> None:
        annotated_calls.append(cast("Payload", payload))

    annotated_payload.__annotations__["payload"] = "Annotated[payload_alias, 'metadata']"
    subscribe(topic="annotated", consumer_group="test-group")(annotated_payload)

    call_subscribers_sync(
        Message(
            payload={"count": "4"},
            metadata=make_metadata("annotated", consumer_group="test-group"),
        )
    )

    assert annotated_calls == [Payload(count=4)]

    dict_calls: list[dict[str, Payload]] = []

    def dict_payload(payload: dict[str, Payload]) -> None:
        dict_calls.append(payload)

    dict_payload.__annotations__["payload"] = "dict[str, payload_alias]"
    subscribe(topic="dict", consumer_group="test-group")(dict_payload)

    call_subscribers_sync(
        Message(
            payload={"item": {"count": "5"}},
            metadata=make_metadata("dict", consumer_group="test-group"),
        )
    )

    assert dict_calls == [{"item": Payload(count=5)}]


def test_subscribe_resolves_nested_forward_refs_in_payload_annotation(
    isolated_subscriptions: None,
) -> None:
    class Payload(BaseModel):
        count: int

    calls: list[dict[str, Payload]] = []
    nested_alias = dict[str, "Payload"]
    assert nested_alias == dict[str, "Payload"]

    def handle(payload: dict[str, Payload]) -> None:
        calls.append(payload)

    handle.__annotations__["payload"] = "nested_alias"
    subscribe(topic="nested", consumer_group="test-group")(handle)

    call_subscribers_sync(
        Message(
            payload={"item": {"count": "6"}},
            metadata=make_metadata("nested", consumer_group="test-group"),
        )
    )

    assert calls == [{"item": Payload(count=6)}]


def test_subscribe_resolves_complex_container_forward_refs(
    isolated_subscriptions: None,
) -> None:
    class Payload(BaseModel):
        count: int

    list_alias = list["Payload"]
    tuple_alias = tuple["Payload", ...]
    dict_list_alias = dict[str, list["Payload"]]
    assert list_alias == list["Payload"]
    assert tuple_alias == tuple["Payload", ...]
    assert dict_list_alias == dict[str, list["Payload"]]

    list_calls: list[list[Payload]] = []

    def list_payload(payload: list[Payload]) -> None:
        list_calls.append(payload)

    list_payload.__annotations__["payload"] = "list_alias"
    subscribe(topic="complex-list", consumer_group="test-group")(list_payload)

    tuple_calls: list[tuple[Payload, ...]] = []

    def tuple_payload(payload: tuple[Payload, ...]) -> None:
        tuple_calls.append(payload)

    tuple_payload.__annotations__["payload"] = "tuple_alias"
    subscribe(topic="complex-tuple", consumer_group="test-group")(tuple_payload)

    dict_list_calls: list[dict[str, list[Payload]]] = []

    def dict_list_payload(payload: dict[str, list[Payload]]) -> None:
        dict_list_calls.append(payload)

    dict_list_payload.__annotations__["payload"] = "dict_list_alias"
    subscribe(topic="complex-dict-list", consumer_group="test-group")(dict_list_payload)

    call_subscribers_sync(
        Message(
            payload=[{"count": "11"}],
            metadata=make_metadata("complex-list", consumer_group="test-group"),
        )
    )
    call_subscribers_sync(
        Message(
            payload=[{"count": "12"}, {"count": "13"}],
            metadata=make_metadata("complex-tuple", consumer_group="test-group"),
        )
    )
    call_subscribers_sync(
        Message(
            payload={"items": [{"count": "14"}]},
            metadata=make_metadata("complex-dict-list", consumer_group="test-group"),
        )
    )

    assert list_calls == [[Payload(count=11)]]
    assert tuple_calls == [(Payload(count=12), Payload(count=13))]
    assert dict_list_calls == [{"items": [Payload(count=14)]}]


def test_subscribe_resolves_union_forward_refs(
    isolated_subscriptions: None,
) -> None:
    class Payload(BaseModel):
        count: int

    pep604_alias = dict[str, "Payload"] | None
    optional_alias = getitem(Optional, dict[str, "Payload"])
    union_alias = getitem(Union, (dict[str, "Payload"], None))
    assert pep604_alias == dict[str, "Payload"] | None
    assert optional_alias == dict[str, "Payload"] | None
    assert union_alias == dict[str, "Payload"] | None

    pep604_calls: list[dict[str, Payload] | None] = []

    def pep604_payload(payload: dict[str, Payload] | None) -> None:
        pep604_calls.append(payload)

    pep604_payload.__annotations__["payload"] = "pep604_alias"
    subscribe(topic="union-pep604", consumer_group="test-group")(pep604_payload)

    optional_calls: list[dict[str, Payload] | None] = []

    def optional_payload(payload: dict[str, Payload] | None) -> None:
        optional_calls.append(payload)

    optional_payload.__annotations__["payload"] = "optional_alias"
    subscribe(topic="union-optional", consumer_group="test-group")(optional_payload)

    union_calls: list[dict[str, Payload] | None] = []

    def union_payload(payload: dict[str, Payload] | None) -> None:
        union_calls.append(payload)

    union_payload.__annotations__["payload"] = "union_alias"
    subscribe(topic="union-typing", consumer_group="test-group")(union_payload)

    call_subscribers_sync(
        Message(
            payload={"item": {"count": "15"}},
            metadata=make_metadata("union-pep604", consumer_group="test-group"),
        )
    )
    call_subscribers_sync(
        Message(
            payload=None,
            metadata=make_metadata("union-optional", consumer_group="test-group"),
        )
    )
    call_subscribers_sync(
        Message(
            payload={"item": {"count": "16"}},
            metadata=make_metadata("union-typing", consumer_group="test-group"),
        )
    )

    assert pep604_calls == [{"item": Payload(count=15)}]
    assert optional_calls == [None]
    assert union_calls == [{"item": Payload(count=16)}]


def test_subscribe_resolves_message_complex_forward_refs(
    isolated_subscriptions: None,
) -> None:
    class Payload(BaseModel):
        count: int

    message_alias = Message[dict[str, "Payload"]]
    assert message_alias == Message[dict[str, "Payload"]]

    calls: list[Message[dict[str, Payload]]] = []

    def handle(message: Message[dict[str, Payload]]) -> None:
        calls.append(message)

    handle.__annotations__["message"] = "message_alias"
    subscribe(topic="message-complex", consumer_group="test-group")(handle)

    metadata = make_metadata("message-complex", consumer_group="test-group")
    call_subscribers_sync(Message(payload={"item": {"count": "17"}}, metadata=metadata))

    assert len(calls) == 1
    assert calls[0].payload == {"item": Payload(count=17)}
    assert calls[0].metadata is metadata


def test_subscribe_resolves_pydantic_model_field_forward_refs(
    isolated_subscriptions: None,
) -> None:
    class Child(BaseModel):
        count: int

    class Payload(BaseModel):
        child: Child

    calls: list[Payload] = []

    def handle(payload: Payload) -> None:
        calls.append(payload)

    handle.__annotations__["payload"] = "Payload"
    subscribe(topic="model-field-ref", consumer_group="test-group")(handle)

    call_subscribers_sync(
        Message(
            payload={"child": {"count": "7"}},
            metadata=make_metadata("model-field-ref", consumer_group="test-group"),
        )
    )

    assert calls == [Payload(child=Child(count=7))]


def test_subscribe_resolves_pydantic_model_complex_field_forward_refs(
    isolated_subscriptions: None,
) -> None:
    class Child(BaseModel):
        count: int

    class Payload(BaseModel):
        children: list[Child]
        child_by_name: dict[str, Child]
        maybe_child: Child | None

    calls: list[Payload] = []

    def handle(payload: Payload) -> None:
        calls.append(payload)

    handle.__annotations__["payload"] = "Payload"
    subscribe(topic="model-complex-forward-refs", consumer_group="test-group")(handle)

    call_subscribers_sync(
        Message(
            payload={
                "children": [{"count": "18"}],
                "child_by_name": {"primary": {"count": "19"}},
                "maybe_child": {"count": "20"},
            },
            metadata=make_metadata("model-complex-forward-refs", consumer_group="test-group"),
        )
    )

    assert calls == [
        Payload(
            children=[Child(count=18)],
            child_by_name={"primary": Child(count=19)},
            maybe_child=Child(count=20),
        )
    ]


def test_subscribe_resolves_pydantic_model_field_refs_to_outer_scope(
    isolated_subscriptions: None,
) -> None:
    class Child(BaseModel):
        count: int

    def register() -> tuple[type[BaseModel], list[BaseModel]]:
        class Payload(BaseModel):
            child: Child

        calls: list[BaseModel] = []

        def handle(payload: Payload) -> None:
            calls.append(payload)

        handle.__annotations__["payload"] = "Payload"
        subscribe(topic="outer-model-field-ref", consumer_group="test-group")(handle)
        return Payload, calls

    payload_model, calls = register()

    call_subscribers_sync(
        Message(
            payload={"child": {"count": "8"}},
            metadata=make_metadata("outer-model-field-ref", consumer_group="test-group"),
        )
    )

    assert calls == [payload_model(child=Child(count=8))]


def test_subscribe_resolves_nested_alias_and_model_refs_to_outer_scope(
    isolated_subscriptions: None,
) -> None:
    class Child(BaseModel):
        count: int

    def register() -> tuple[type[BaseModel], list[object]]:
        class Payload(BaseModel):
            child: Child

        calls: list[object] = []
        payload_alias = dict[str, "Payload"]
        assert payload_alias == dict[str, "Payload"]

        def handle(payload: dict[str, Payload]) -> None:
            calls.append(payload)

        handle.__annotations__["payload"] = "payload_alias"
        subscribe(topic="outer-alias-model-ref", consumer_group="test-group")(handle)
        return Payload, calls

    payload_model, calls = register()

    call_subscribers_sync(
        Message(
            payload={"item": {"child": {"count": "9"}}},
            metadata=make_metadata("outer-alias-model-ref", consumer_group="test-group"),
        )
    )

    assert calls == [{"item": payload_model(child=Child(count=9))}]


def test_subscribe_preserves_annotated_metadata_and_literal_string_values(
    isolated_subscriptions: None,
) -> None:
    class Payload(BaseModel):
        count: int

    calls: list[dict[Literal["primary"], Payload]] = []
    payload_alias = Annotated[dict[Literal["primary"], "Payload"], "metadata"]
    assert payload_alias == Annotated[dict[Literal["primary"], "Payload"], "metadata"]

    def handle(payload: dict[Literal["primary"], Payload]) -> None:
        calls.append(payload)

    handle.__annotations__["payload"] = "payload_alias"
    subscribe(topic="literal-forward-ref", consumer_group="test-group")(handle)

    call_subscribers_sync(
        Message(
            payload={"primary": {"count": "10"}},
            metadata=make_metadata("literal-forward-ref", consumer_group="test-group"),
        )
    )

    assert calls == [{"primary": Payload(count=10)}]


def test_subscribe_raises_for_unresolved_payload_annotation(
    isolated_subscriptions: None,
) -> None:
    def handle(payload: object) -> None:
        del payload

    handle.__annotations__["payload"] = "MissingPayload"

    with pytest.raises(
        TypeError,
        match="could not resolve queue subscriber type annotations",
    ):
        subscribe(topic="emails")(handle)


def test_subscribe_raises_subscription_error_for_unresolved_payload_annotation(
    isolated_subscriptions: None,
) -> None:
    def handle(payload: object) -> None:
        del payload

    handle.__annotations__["payload"] = "MissingPayload"

    with pytest.raises(
        SubscriptionError,
        match="could not resolve queue subscriber type annotations",
    ) as exc_info:
        subscribe(topic="unresolved-payload")(handle)

    assert isinstance(exc_info.value, QueueError)
    assert "handle" in str(exc_info.value)


def test_subscribe_raises_subscription_error_for_unresolved_pydantic_model_refs(
    isolated_subscriptions: None,
) -> None:
    payload_model = type(
        "Payload",
        (BaseModel,),
        {"__module__": __name__, "__annotations__": {"missing": "MissingPayload"}},
    )

    def handle(payload: object) -> None:
        del payload

    handle.__annotations__["payload"] = payload_model

    with pytest.raises(
        SubscriptionError,
        match="could not resolve queue subscriber payload model annotations",
    ) as exc_info:
        subscribe(topic="unresolved-model-ref")(handle)

    assert isinstance(exc_info.value, QueueError)
    assert "Payload" in str(exc_info.value)


def test_retry_after_validates_duration() -> None:
    assert RetryAfter().timeout_seconds == DEFAULT_RETRY_AFTER_SECONDS
    assert RetryAfter(timedelta(seconds=2.5)).timeout_seconds == 2
    assert repr(RetryAfter(3)) == "RetryAfter(timeout_seconds=3)"

    with pytest.raises(ValueError, match="delay must be non-negative"):
        RetryAfter(timedelta(seconds=-1))

    with pytest.raises(TypeError, match="duration must be an int or float"):
        RetryAfter(cast("Any", bool(1)))


def test_sync_client_send_does_not_invoke_in_process_subscribers(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    calls: list[dict[str, bool]] = []

    @subscribe(topic="in-process")
    def handle(payload: dict[str, bool]) -> None:
        calls.append(payload)

    result = eqs.get_sync_client(token="token", deployment="dpl_1").send(
        "in-process",
        {"ok": True},
        idempotency_key="idem_1",
        delay=2,
        retention=60,
        headers={"x-user": "ok"},
    )

    assert result == "msg_1"
    assert calls == []
    assert eqs.state.by_id["msg_1"].topic == "in-process"


@pytest.mark.anyio
async def test_async_client_send_does_not_invoke_in_process_subscribers(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    calls: list[dict[str, bool]] = []

    @subscribe(topic="in-process")
    async def handle(payload: dict[str, bool]) -> None:
        calls.append(payload)

    result = await eqs.get_async_client(token="token", deployment="dpl_1").send(
        "in-process",
        {"ok": True},
        idempotency_key="idem_1",
        delay=2,
        retention=60,
        headers={"x-user": "ok"},
    )

    assert result == "msg_1"
    assert calls == []
    assert eqs.state.by_id["msg_1"].topic == "in-process"


def test_subscribe_rejects_empty_consumer_group(isolated_subscriptions: None) -> None:
    with pytest.raises(ValueError, match="consumer_group must be a non-empty string"):

        @subscribe(topic="emails", consumer_group="")
        def handle(payload: object) -> None:
            del payload


def test_subscribe_sanitizes_consumer_group(isolated_subscriptions: None) -> None:
    @subscribe(topic="emails", consumer_group="api/handle_orders.py")
    def handle(payload: object) -> None:
        del payload

    assert get_subscriptions() == (
        Subscription(
            func=handle,
            topic="emails",
            consumer_group="api_Shandle__orders_Dpy",
        ),
    )


def test_subscribe_trusts_sanitized_consumer_group(isolated_subscriptions: None) -> None:
    @subscribe(topic="emails", consumer_group=SanitizedName("api_Shandle_orders_Dpy"))
    def handle(payload: object) -> None:
        del payload

    assert get_subscriptions() == (
        Subscription(
            func=handle,
            topic="emails",
            consumer_group="api_Shandle_orders_Dpy",
        ),
    )


def test_subscribe_rejects_invalid_sanitized_consumer_group(
    isolated_subscriptions: None,
) -> None:
    with pytest.raises(ValueError, match="Invalid queue name"):
        SanitizedName("test-group.v1")


def test_subscribe_rejects_invalid_topic(isolated_subscriptions: None) -> None:
    with pytest.raises(TypeError, match="topic must be a string"):

        @subscribe(topic=cast("Any", ("tuple-desc", lambda topic: topic == "tuple")))
        def handle_tuple_filter(payload: object) -> None:
            del payload

    with pytest.raises(ValueError, match="Invalid queue topic"):

        @subscribe(topic="emails.v1")
        def handle(payload: object) -> None:
            del payload

    with pytest.raises(ValueError, match="Invalid queue topic"):

        @subscribe(topic="user-*-data")
        def handle_middle_wildcard(payload: object) -> None:
            del payload


@pytest.mark.anyio
async def test_embedded_queue_service_dispatches_existing_subscription(
    isolated_subscriptions: None,
) -> None:
    calls: list[dict[str, bool]] = []

    @subscribe(topic="jobs", consumer_group="test-group")
    async def handle(payload: dict[str, bool]) -> None:
        calls.append(payload)

    async with embedded_queue_service() as service:
        client = service.get_async_client()
        message_id = await client.send("jobs", {"ok": True})
        assert message_id is not None
        await wait_until(lambda: service.server.state.by_id[message_id].acknowledged)

    assert calls == [{"ok": True}]


@pytest.mark.anyio
async def test_embedded_queue_service_exact_subscription_does_not_match_prefix(
    isolated_subscriptions: None,
) -> None:
    calls: list[dict[str, str]] = []

    @subscribe(topic="jobs", consumer_group="test-group")
    async def handle(payload: dict[str, str]) -> None:
        calls.append(payload)

    async with embedded_queue_service() as service:
        client = service.get_async_client()
        unmatched_id = await client.send("jobs-extra", {"topic": "jobs-extra"})
        matched_id = await client.send("jobs", {"topic": "jobs"})
        assert unmatched_id is not None
        assert matched_id is not None

        await wait_until(lambda: service.server.state.by_id[matched_id].acknowledged)
        await anyio.lowlevel.checkpoint()
        assert not service.server.state.by_id[unmatched_id].acknowledged_for("test-group")

    assert calls == [{"topic": "jobs"}]


@pytest.mark.anyio
async def test_embedded_queue_service_wildcard_subscription_matches_prefix(
    isolated_subscriptions: None,
) -> None:
    calls: list[dict[str, str]] = []

    @subscribe(topic="jobs-*", consumer_group="test-group")
    async def handle(payload: dict[str, str]) -> None:
        calls.append(payload)

    async with embedded_queue_service() as service:
        client = service.get_async_client()
        message_id = await client.send("jobs-extra", {"topic": "jobs-extra"})
        assert message_id is not None
        await wait_until(lambda: service.server.state.by_id[message_id].acknowledged)

    assert calls == [{"topic": "jobs-extra"}]


@pytest.mark.anyio
async def test_embedded_queue_service_dispatches_new_subscription(
    isolated_subscriptions: None,
) -> None:
    calls: list[dict[str, bool]] = []

    async with embedded_queue_service() as service:
        client = service.get_async_client()
        message_id = await client.send("jobs", {"ok": True})

        @subscribe(topic="jobs", consumer_group="test-group")
        async def handle(payload: dict[str, bool]) -> None:
            calls.append(payload)

        assert message_id is not None
        await wait_until(lambda: service.server.state.by_id[message_id].acknowledged)

    assert calls == [{"ok": True}]


@pytest.mark.anyio
async def test_embedded_queue_service_dispatches_delayed_message(
    isolated_subscriptions: None,
) -> None:
    calls: list[dict[str, bool]] = []

    @subscribe(topic="jobs", consumer_group="test-group")
    async def handle(payload: dict[str, bool]) -> None:
        calls.append(payload)

    async with embedded_queue_service(manual_clock=True) as service:
        client = service.get_async_client()
        message_id = await client.send("jobs", {"ok": True}, delay=10)
        await anyio.lowlevel.checkpoint()
        assert message_id is not None
        assert not service.server.state.by_id[message_id].acknowledged

        service.server.shift(10)
        service.dispatcher.wake()
        await wait_until(lambda: service.server.state.by_id[message_id].acknowledged)

    assert calls == [{"ok": True}]


@pytest.mark.anyio
async def test_embedded_queue_service_honors_retry_after(
    isolated_subscriptions: None,
) -> None:
    calls = 0

    @subscribe(topic="jobs", consumer_group="test-group", retry_after=5)
    async def handle(payload: dict[str, bool]) -> None:
        nonlocal calls
        del payload
        calls += 1
        if calls == 1:
            raise RuntimeError("try later")

    async with embedded_queue_service(manual_clock=True) as service:
        client = service.get_async_client()
        message_id = await client.send("jobs", {"ok": True})
        assert message_id is not None
        await wait_until(lambda: calls == 1)
        assert not service.server.state.by_id[message_id].acknowledged
        await _wait_for_retry_lease(service, message_id, "test-group", 5)

        service.server.shift(4)
        service.dispatcher.wake()
        await anyio.lowlevel.checkpoint()
        assert calls == 1

        service.server.shift(1)
        service.dispatcher.wake()
        await wait_until(lambda: service.server.state.by_id[message_id].acknowledged)

    assert calls == 2


@pytest.mark.anyio
async def test_embedded_queue_service_sleeps_until_retry_lease_deadline(
    isolated_subscriptions: None,
) -> None:
    @subscribe(topic="jobs", consumer_group="test-group", retry_after=5)
    async def handle(payload: dict[str, bool]) -> None:
        del payload
        raise RuntimeError("try later")

    async with embedded_queue_service(manual_clock=True) as service:
        client = service.get_async_client()
        message_id = await client.send("jobs", {"ok": True})
        assert message_id is not None
        await _wait_for_retry_lease(service, message_id, "test-group", 5)

        delay = service.dispatcher._next_registration_visible_delay()

    assert delay == pytest.approx(5.0)


@pytest.mark.anyio
async def test_embedded_queue_service_uses_default_retry_after(
    isolated_subscriptions: None,
) -> None:
    calls = 0

    @subscribe(topic="jobs", consumer_group="test-group")
    async def handle(payload: dict[str, bool]) -> None:
        nonlocal calls
        del payload
        calls += 1
        if calls == 1:
            raise RuntimeError("try later")

    async with embedded_queue_service(manual_clock=True) as service:
        client = service.get_async_client()
        message_id = await client.send("jobs", {"ok": True})
        assert message_id is not None
        await wait_until(lambda: calls == 1)
        assert not service.server.state.by_id[message_id].acknowledged
        await _wait_for_retry_lease(service, message_id, "test-group", 60)

        service.server.shift(59)
        service.dispatcher.wake()
        await anyio.lowlevel.checkpoint()
        assert calls == 1

        service.server.shift(1)
        service.dispatcher.wake()
        await wait_until(lambda: service.server.state.by_id[message_id].acknowledged)

    assert calls == 2


@pytest.mark.anyio
async def test_embedded_queue_service_clamps_zero_max_concurrency(
    isolated_subscriptions: None,
) -> None:
    started: list[int] = []
    max_parallel = 0
    active = 0
    release = anyio.Event()

    @subscribe(topic="jobs", consumer_group="test-group", max_concurrency=0)
    async def handle(payload: dict[str, int]) -> None:
        nonlocal active, max_parallel
        started.append(payload["index"])
        active += 1
        max_parallel = max(max_parallel, active)
        try:
            await release.wait()
        finally:
            active -= 1

    async with embedded_queue_service() as service:
        client = service.get_async_client()
        first_id = await client.send("jobs", {"index": 1})
        second_id = await client.send("jobs", {"index": 2})
        assert first_id is not None
        assert second_id is not None

        await wait_until(lambda: started == [1])
        await anyio.sleep(0.05)
        assert started == [1]

        release.set()
        await wait_until(
            lambda: (
                service.server.state.by_id[first_id].acknowledged
                and service.server.state.by_id[second_id].acknowledged
            )
        )

    assert started == [1, 2]
    assert max_parallel == 1


@pytest.mark.anyio
async def test_embedded_queue_service_honors_max_concurrency_limit(
    isolated_subscriptions: None,
) -> None:
    started: list[int] = []
    max_parallel = 0
    active = 0
    release = anyio.Event()

    @subscribe(topic="jobs", consumer_group="test-group", max_concurrency=2)
    async def handle(payload: dict[str, int]) -> None:
        nonlocal active, max_parallel
        started.append(payload["index"])
        active += 1
        max_parallel = max(max_parallel, active)
        try:
            await release.wait()
        finally:
            active -= 1

    async with embedded_queue_service() as service:
        client = service.get_async_client()
        message_ids = [await client.send("jobs", {"index": index}) for index in range(1, 4)]
        assert all(message_id is not None for message_id in message_ids)

        await wait_until(lambda: sorted(started) == [1, 2])
        await anyio.sleep(0.05)
        assert sorted(started) == [1, 2]

        release.set()
        await wait_until(lambda: len(started) == 3)
        await wait_until(
            lambda: all(
                service.server.state.by_id[str(message_id)].acknowledged
                for message_id in message_ids
            )
        )

    assert sorted(started) == [1, 2, 3]
    assert max_parallel == 2


@pytest.mark.anyio
async def test_embedded_queue_service_does_not_spin_when_concurrency_is_saturated(
    isolated_subscriptions: None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level(logging.INFO, logger="vercel.queue")
    started: list[int] = []
    release = anyio.Event()

    @subscribe(topic="jobs", consumer_group="test-group", max_concurrency=2)
    async def handle(payload: dict[str, int]) -> None:
        started.append(payload["index"])
        await release.wait()

    async with embedded_queue_service() as service:
        client = service.get_async_client()
        message_ids = [await client.send("jobs", {"index": index}) for index in range(1, 4)]
        assert all(message_id is not None for message_id in message_ids)

        await wait_until(lambda: sorted(started) == [1, 2])
        for _ in range(10):
            await anyio.lowlevel.checkpoint()

        zero_delay_wakes = []
        for event in _queue_debug_events(caplog):
            if event["event"] != "embedded.no_message_after_wake":
                continue
            sleep_delay = event["sleep_delay_seconds"]
            assert isinstance(sleep_delay, int | float)
            if sleep_delay <= 0.0:
                zero_delay_wakes.append(event)
        assert zero_delay_wakes == []

        release.set()
        await wait_until(
            lambda: all(
                service.server.state.by_id[str(message_id)].acknowledged
                for message_id in message_ids
            )
        )


@pytest.mark.anyio
async def test_embedded_queue_service_default_concurrency_is_unbounded(
    isolated_subscriptions: None,
) -> None:
    started: list[int] = []
    release = anyio.Event()

    @subscribe(topic="jobs", consumer_group="test-group")
    async def handle(payload: dict[str, int]) -> None:
        started.append(payload["index"])
        await release.wait()

    async with embedded_queue_service() as service:
        client = service.get_async_client()
        message_ids = [await client.send("jobs", {"index": index}) for index in range(1, 4)]
        assert all(message_id is not None for message_id in message_ids)

        await wait_until(lambda: sorted(started) == [1, 2, 3])

        release.set()
        await wait_until(
            lambda: all(
                service.server.state.by_id[str(message_id)].acknowledged
                for message_id in message_ids
            )
        )

    assert sorted(started) == [1, 2, 3]


@pytest.mark.anyio
async def test_embedded_dispatcher_debug_logs_success_and_retry_after(
    isolated_subscriptions: None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level(logging.INFO, logger="vercel.queue")
    calls = 0

    @subscribe(topic="jobs", consumer_group="test-group", retry_after=5)
    async def handle(payload: dict[str, bool]) -> None:
        nonlocal calls
        del payload
        calls += 1
        if calls == 1:
            raise RuntimeError("try later")

    async with embedded_queue_service(manual_clock=True) as service:
        client = service.get_async_client()
        message_id = await client.send("jobs", {"ok": True})
        assert message_id is not None
        await wait_until(lambda: calls == 1)
        await _wait_for_retry_lease(service, message_id, "test-group", 5)
        service.server.shift(5)
        service.dispatcher.wake()
        await wait_until(lambda: service.server.state.by_id[message_id].acknowledged)

    event_names = [event["event"] for event in _queue_debug_events(caplog)]
    assert "embedded.subscription_registered" in event_names
    assert "embedded.delivery_scheduled" in event_names
    assert "embedded.delivery_failure" in event_names
    assert "embedded.retry_after_applied" in event_names
    assert "embedded.delivery_success" in event_names


@pytest.mark.anyio
async def test_embedded_queue_service_honors_max_attempts(
    isolated_subscriptions: None,
) -> None:
    calls = 0

    @subscribe(topic="jobs", consumer_group="test-group", retry_after=1, max_attempts=2)
    async def handle(payload: dict[str, bool]) -> None:
        nonlocal calls
        del payload
        calls += 1
        raise RuntimeError("always fails")

    async with embedded_queue_service(manual_clock=True) as service:
        client = service.get_async_client()
        message_id = await client.send("jobs", {"ok": True})
        assert message_id is not None
        await wait_until(lambda: calls == 1)
        await _wait_for_retry_lease(service, message_id, "test-group", 1)

        service.server.shift(1)
        service.dispatcher.wake()
        await wait_until(lambda: service.server.state.by_id[message_id].acknowledged)

    assert calls == 2


@pytest.mark.anyio
async def test_embedded_dispatcher_debug_logs_max_attempts_acknowledgement(
    isolated_subscriptions: None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "true")
    caplog.set_level(logging.INFO, logger="vercel.queue")
    calls = 0

    @subscribe(topic="jobs", consumer_group="test-group", retry_after=1, max_attempts=2)
    async def handle(payload: dict[str, bool]) -> None:
        nonlocal calls
        del payload
        calls += 1
        raise RuntimeError("always fails")

    async with embedded_queue_service(manual_clock=True) as service:
        client = service.get_async_client()
        message_id = await client.send("jobs", {"ok": True})
        assert message_id is not None
        await wait_until(lambda: calls == 1)
        await _wait_for_retry_lease(service, message_id, "test-group", 1)
        service.server.shift(1)
        service.dispatcher.wake()
        await wait_until(lambda: service.server.state.by_id[message_id].acknowledged)

    assert any(
        event["event"] == "embedded.max_attempts_acknowledgement" and event["attempts"] == 2
        for event in _queue_debug_events(caplog)
    )


async def _wait_for_retry_lease(
    service: Any,
    message_id: str,
    consumer_group: str,
    seconds: int,
) -> None:
    expected = service.server.now + timedelta(seconds=seconds)
    await wait_until(
        lambda: (
            service.server.state.by_id[message_id].lease_deadline_by_consumer.get(consumer_group)
            == expected
        )
    )


@pytest.mark.anyio
async def test_embedded_queue_service_honors_initial_delay(
    isolated_subscriptions: None,
) -> None:
    calls = 0

    @subscribe(topic="jobs", consumer_group="test-group", initial_delay=5)
    async def handle(payload: dict[str, bool]) -> None:
        nonlocal calls
        del payload
        calls += 1

    async with embedded_queue_service(manual_clock=True) as service:
        client = service.get_async_client()
        message_id = await client.send("jobs", {"ok": True})
        assert message_id is not None
        await anyio.lowlevel.checkpoint()
        assert calls == 0

        service.server.shift(5)
        service.dispatcher.wake()
        await wait_until(lambda: service.server.state.by_id[message_id].acknowledged)

    assert calls == 1


@pytest.mark.anyio
async def test_embedded_queue_service_clients_have_no_close_state() -> None:
    async with embedded_queue_service() as service:
        client = service.get_async_client()
        assert not hasattr(client, "closed")
        assert not hasattr(client, "close")
