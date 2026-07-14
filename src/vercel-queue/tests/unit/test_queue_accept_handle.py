from __future__ import annotations

from typing import Annotated, Any, cast

import inspect
import json
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from dataclasses import dataclass
from datetime import timedelta

import pytest
from pydantic import BaseModel

from vercel.headers import get_headers, set_headers
from vercel.queue import (
    ALL_DEPLOYMENTS,
    ByteBufferTransport,
    Handoff,
    LeaseRenewal,
    Message,
    PayloadValidationError,
    QueueClient,
    RawJsonTransport,
    RetryAfter,
    TextBufferTransport,
    TextStreamTransport,
    UnhandledMessageError,
    subscribe,
)
from vercel.queue._internal import lease as queue_lease
from vercel.queue._internal.client import _AsyncMessageLifecycle
from vercel.queue._internal.client_sync import _MessageLifecycle
from vercel.queue._internal.constants import (
    CLOUD_EVENT_HEADER_VQS_CREATED_AT,
    CLOUD_EVENT_HEADER_VQS_MESSAGE_ID,
    CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE,
    CONTENT_TYPE_OCTET_STREAM,
    HEADER_CONTENT_TYPE,
)
from vercel.queue._internal.streams import (
    AsyncStreamPayload,
    AsyncTextStreamPayload,
    SyncStreamPayload,
    SyncTextStreamPayload,
)
from vercel.queue.devserver import EmbeddedQueueDevServer
from vercel.queue.sync import QueueClient as SyncQueueClient, accept_and_handle

from .helpers import (
    async_delivery,
    callback_headers,
    callback_subscribe,
    make_leased_metadata,
    sync_delivery,
    sync_push_message,
)


class _AsyncRequestLike:
    def __init__(self, body: bytes, headers: dict[str, str]) -> None:
        self._body = body
        self._headers = headers

    @property
    def headers(self) -> dict[str, str]:
        return self._headers

    async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        del chunk_size
        yield self._body

    async def get_body(self) -> bytes:
        raise AssertionError("get_body should not be called")


def _queue_debug_events(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    return [
        json.loads(record.message) for record in caplog.records if record.name == "vercel.queue"
    ]


def test_accept_message_can_extend_lease(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    message = sync_push_message(eqs, client, {"ok": True})

    client.extend_lease(message, timedelta(seconds=5))

    stored = eqs.state.by_id[message.metadata.message_id]
    assert stored.lease_deadline_by_consumer["test-group"] == (eqs.state.now + timedelta(seconds=5))
    assert not stored.acknowledged


def test_sync_message_lifecycle_ack_stops_renewal_without_wait(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level("INFO", logger="vercel.queue")
    waits: list[bool] = []
    acknowledged: list[str] = []

    def stop(self: LeaseRenewal, *, wait: bool = True) -> None:
        del self
        waits.append(wait)

    class Client:
        def acknowledge(self, message: Message[Any]) -> None:
            acknowledged.append(message.metadata.message_id)

        def _extend_lease(self, message: Message[Any], duration: int) -> None:
            del message, duration
            raise AssertionError("ACK must not extend visibility")

    monkeypatch.setattr(LeaseRenewal, "stop", stop)
    message = Message(
        payload={"ok": True},
        metadata=make_leased_metadata("emails", message_id="m-ack"),
    )
    lifecycle = _MessageLifecycle(
        message,
        client=cast("SyncQueueClient", Client()),
        lease_duration=30,
    )

    assert lifecycle.__exit__(None, None, None) is None

    assert waits == [False]
    assert acknowledged == ["m-ack"]
    assert _queue_debug_events(caplog)[-1] == {
        "event": "message.ack",
        "message_id": "m-ack",
        "topic": "emails",
        "consumer_group": "c",
        "delivery_count": 1,
    }


def test_sync_message_lifecycle_retry_after_waits_for_renewal_stop(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level("INFO", logger="vercel.queue")
    waits: list[bool] = []
    extensions: list[int] = []

    def stop(self: LeaseRenewal, *, wait: bool = True) -> None:
        del self
        waits.append(wait)

    class Client:
        def acknowledge(self, message: Message[Any]) -> None:
            del message
            raise AssertionError("RetryAfter must not acknowledge")

        async def _extend_lease(self, message: Message[Any], duration: int) -> None:
            del message
            extensions.append(duration)

    monkeypatch.setattr(LeaseRenewal, "stop", stop)
    message = Message(
        payload={"ok": True},
        metadata=make_leased_metadata("emails", message_id="m-retry"),
    )
    lifecycle = _MessageLifecycle(
        message,
        client=cast("SyncQueueClient", Client()),
        lease_duration=30,
    )

    retry_after = RetryAfter(12)

    assert lifecycle.__exit__(RetryAfter, retry_after, None) is True

    assert waits == [True]
    assert extensions == [12]
    assert _queue_debug_events(caplog)[-1] == {
        "event": "message.retry_after",
        "message_id": "m-retry",
        "topic": "emails",
        "consumer_group": "c",
        "delivery_count": 1,
        "retry_after_seconds": 12,
    }


def test_sync_message_lifecycle_handoff_waits_for_renewal_stop(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level("INFO", logger="vercel.queue")
    waits: list[bool] = []

    def stop(self: LeaseRenewal, *, wait: bool = True) -> None:
        del self
        waits.append(wait)

    class Client:
        def acknowledge(self, message: Message[Any]) -> None:
            del message
            raise AssertionError("Handoff must not acknowledge")

        def _extend_lease(self, message: Message[Any], duration: int) -> None:
            del message, duration
            raise AssertionError("Handoff must not extend visibility")

    monkeypatch.setattr(LeaseRenewal, "stop", stop)
    message = Message(
        payload={"ok": True},
        metadata=make_leased_metadata("emails", message_id="m-handoff"),
    )
    lifecycle = _MessageLifecycle(
        message,
        client=cast("SyncQueueClient", Client()),
        lease_duration=30,
    )

    handoff = Handoff()

    assert lifecycle.__exit__(Handoff, handoff, None) is True
    assert waits == [True]
    assert _queue_debug_events(caplog)[-1] == {
        "event": "message.handoff",
        "message_id": "m-handoff",
        "topic": "emails",
        "consumer_group": "c",
        "delivery_count": 1,
    }


@pytest.mark.anyio
async def test_async_message_lifecycle_ack_stops_renewal_without_wait(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level("INFO", logger="vercel.queue")
    waits: list[bool] = []
    acknowledged: list[str] = []

    async def stop_async(self: LeaseRenewal, *, wait: bool = True) -> None:
        del self
        waits.append(wait)

    class Client:
        async def acknowledge(self, message: Message[Any]) -> None:
            acknowledged.append(message.metadata.message_id)

        async def _extend_lease(self, message: Message[Any], duration: int) -> None:
            del message, duration
            raise AssertionError("ACK must not extend visibility")

    monkeypatch.setattr(LeaseRenewal, "stop_async", stop_async)
    message = Message(
        payload={"ok": True},
        metadata=make_leased_metadata("emails", message_id="m-async-ack"),
    )
    lifecycle = _AsyncMessageLifecycle(
        message,
        client=cast("QueueClient", Client()),
        lease_duration=30,
    )

    assert await lifecycle.__aexit__(None, None, None) is None

    assert waits == [False]
    assert acknowledged == ["m-async-ack"]
    assert _queue_debug_events(caplog)[-1] == {
        "event": "message.ack",
        "message_id": "m-async-ack",
        "topic": "emails",
        "consumer_group": "c",
        "delivery_count": 1,
    }


@pytest.mark.anyio
async def test_async_message_lifecycle_retry_after_waits_for_renewal_stop(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level("INFO", logger="vercel.queue")
    waits: list[bool] = []
    extensions: list[int] = []

    async def stop_async(self: LeaseRenewal, *, wait: bool = True) -> None:
        del self
        waits.append(wait)

    class Client:
        async def acknowledge(self, message: Message[Any]) -> None:
            del message
            raise AssertionError("RetryAfter must not acknowledge")

        async def _extend_lease(self, message: Message[Any], duration: int) -> None:
            del message
            extensions.append(duration)

    monkeypatch.setattr(LeaseRenewal, "stop_async", stop_async)
    message = Message(
        payload={"ok": True},
        metadata=make_leased_metadata("emails", message_id="m-async-retry"),
    )
    lifecycle = _AsyncMessageLifecycle(
        message,
        client=cast("QueueClient", Client()),
        lease_duration=30,
    )
    retry_after = RetryAfter(12)

    assert await lifecycle.__aexit__(RetryAfter, retry_after, None) is True

    assert waits == [True]
    assert extensions == [12]
    assert _queue_debug_events(caplog)[-1] == {
        "event": "message.retry_after",
        "message_id": "m-async-retry",
        "topic": "emails",
        "consumer_group": "c",
        "delivery_count": 1,
        "retry_after_seconds": 12,
    }


@pytest.mark.anyio
async def test_async_message_lifecycle_handoff_waits_for_renewal_stop(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level("INFO", logger="vercel.queue")
    waits: list[bool] = []

    async def stop_async(self: LeaseRenewal, *, wait: bool = True) -> None:
        del self
        waits.append(wait)

    class Client:
        async def acknowledge(self, message: Message[Any]) -> None:
            del message
            raise AssertionError("Handoff must not acknowledge")

        async def _extend_lease(self, message: Message[Any], duration: int) -> None:
            del message, duration
            raise AssertionError("Handoff must not extend visibility")

    monkeypatch.setattr(LeaseRenewal, "stop_async", stop_async)
    message = Message(
        payload={"ok": True},
        metadata=make_leased_metadata("emails", message_id="m-async-handoff"),
    )
    lifecycle = _AsyncMessageLifecycle(
        message,
        client=cast("QueueClient", Client()),
        lease_duration=30,
    )
    handoff = Handoff()

    assert await lifecycle.__aexit__(Handoff, handoff, None) is True
    assert waits == [True]
    assert _queue_debug_events(caplog)[-1] == {
        "event": "message.handoff",
        "message_id": "m-async-handoff",
        "topic": "emails",
        "consumer_group": "c",
        "delivery_count": 1,
    }


def test_accept_message_can_acknowledge(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    message = sync_push_message(eqs, client, {"ok": True})

    client.acknowledge(message)

    assert eqs.state.by_id[message.metadata.message_id].acknowledged


def test_accept_and_handle_invokes_matching_subscriber_and_acknowledges(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    calls: list[dict[str, bool]] = []
    delivery = sync_delivery(eqs, {"ok": True})

    @callback_subscribe(topic="emails")
    def handle(payload: dict[str, bool]) -> None:
        calls.append(payload)

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        delivery.body,
        delivery.headers,
        lease_duration=30,
    )

    assert calls == [{"ok": True}]
    assert eqs.state.by_id[delivery.message_id].acknowledged


def test_accept_and_handle_filters_by_consumer_group(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    calls: list[str] = []
    delivery = sync_delivery(eqs, {"ok": True})

    @subscribe(topic="emails", consumer_group="tests")
    def handle_tests(payload: object) -> None:
        del payload
        calls.append("tests")

    @subscribe(topic="emails", consumer_group="analytics")
    def handle_analytics(payload: object) -> None:
        del payload
        calls.append("analytics")

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        delivery.body,
        delivery.headers,
        lease_duration=30,
    )

    assert calls == ["tests"]
    assert eqs.state.by_id[delivery.message_id].acknowledged


def test_accept_and_handle_wildcard_topic_matches_prefix(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    matched = sync_delivery(eqs, {"kind": "created"}, topic="events-created")
    unmatched = sync_delivery(eqs, {"kind": "ignored"}, topic="orders-created")
    calls: list[str] = []

    @callback_subscribe(topic="events-*")
    def handle(message: Message[dict[str, str]]) -> None:
        calls.append(message.metadata.topic)

    assert isinstance(matched.client, SyncQueueClient)
    matched.client.accept_and_handle(
        matched.body,
        matched.headers,
        lease_duration=30,
    )
    assert isinstance(unmatched.client, SyncQueueClient)
    with pytest.raises(UnhandledMessageError):
        unmatched.client.accept_and_handle(
            unmatched.body,
            unmatched.headers,
            lease_duration=30,
        )

    assert calls == ["events-created"]
    assert eqs.state.by_id[matched.message_id].acknowledged
    assert not eqs.state.by_id[unmatched.message_id].acknowledged


def test_accept_and_handle_validates_pydantic_payload(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    class Payload(BaseModel):
        count: int

    valid = sync_delivery(eqs, {"count": "3"}, topic="typed")
    invalid = sync_delivery(eqs, {"count": "bad"}, topic="typed")
    calls: list[tuple[Payload, str]] = []

    @callback_subscribe(topic="typed")
    def handle(message: Message[Payload]) -> None:
        calls.append((message.payload, message.metadata.message_id))

    assert isinstance(valid.client, SyncQueueClient)
    valid.client.accept_and_handle(
        valid.body,
        valid.headers,
        lease_duration=30,
    )
    assert isinstance(invalid.client, SyncQueueClient)
    with pytest.raises(PayloadValidationError):
        invalid.client.accept_and_handle(
            invalid.body,
            invalid.headers,
            lease_duration=30,
        )

    assert calls == [(Payload(count=3), valid.message_id)]
    assert eqs.state.by_id[valid.message_id].acknowledged
    assert not eqs.state.by_id[invalid.message_id].acknowledged


def test_accept_and_handle_validates_dataclass_payload(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    @dataclass
    class Payload:
        to: str

    delivery = sync_delivery(eqs, {"to": "a@b.com"})
    calls: list[Payload] = []

    @callback_subscribe(topic="emails")
    def handle(payload: Payload) -> None:
        calls.append(payload)

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        delivery.body,
        delivery.headers,
        lease_duration=30,
    )

    assert calls == [Payload(to="a@b.com")]
    assert eqs.state.by_id[delivery.message_id].acknowledged


def test_accept_and_handle_allows_defaulted_extra_parameters(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = sync_delivery(eqs, {"ok": True})
    calls: list[tuple[dict[str, bool], str, bool]] = []

    @callback_subscribe(topic="emails")
    def handle(
        payload: dict[str, bool],
        label: str = "default",
        *,
        enabled: bool = True,
    ) -> None:
        calls.append((payload, label, enabled))

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        delivery.body,
        delivery.headers,
        lease_duration=30,
    )

    assert calls == [({"ok": True}, "default", True)]
    assert eqs.state.by_id[delivery.message_id].acknowledged


@pytest.mark.parametrize("annotation", [inspect.Signature.empty, Any, object])
def test_accept_and_handle_accepts_raw_payload_annotations(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
    annotation: object,
) -> None:
    delivery = sync_delivery(eqs, {"ok": True})
    calls: list[object] = []

    def handle(payload: object) -> None:
        calls.append(payload)

    if annotation is inspect.Signature.empty:
        handle.__annotations__.pop("payload")
    else:
        handle.__annotations__["payload"] = annotation
    callback_subscribe(topic="emails")(handle)

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        delivery.body,
        delivery.headers,
        lease_duration=30,
    )

    assert calls == [{"ok": True}]
    assert eqs.state.by_id[delivery.message_id].acknowledged


@pytest.mark.parametrize("annotation", [Message, Message[Any], Message[object]])
def test_accept_and_handle_accepts_raw_message_annotations(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
    annotation: object,
) -> None:
    delivery = sync_delivery(eqs, {"ok": True})
    calls: list[Message[object]] = []

    def handle(message: Message[object]) -> None:
        calls.append(message)

    handle.__annotations__["message"] = annotation
    callback_subscribe(topic="emails")(handle)

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        delivery.body,
        delivery.headers,
        lease_duration=30,
    )

    assert [(call.payload, call.metadata.message_id) for call in calls] == [
        ({"ok": True}, delivery.message_id)
    ]
    assert eqs.state.by_id[delivery.message_id].acknowledged


@pytest.mark.anyio
async def test_async_accept_and_handle_finalizes_unconsumed_stream_payload(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    consumed = False
    delivery = await async_delivery(
        eqs,
        b"raw",
        transport=ByteBufferTransport(),
    )

    async def body() -> AsyncIterator[bytes]:
        nonlocal consumed
        consumed = True
        yield delivery.body

    @callback_subscribe(topic="emails")
    async def handle(payload: AsyncIterable[bytes]) -> None:
        assert isinstance(payload, AsyncStreamPayload)

    assert isinstance(delivery.client, QueueClient)
    await delivery.client.accept_and_handle(
        body(),
        {
            **delivery.headers,
            HEADER_CONTENT_TYPE: CONTENT_TYPE_OCTET_STREAM,
        },
        lease_duration=30,
    )

    assert consumed
    assert eqs.state.by_id[delivery.message_id].acknowledged


@pytest.mark.anyio
async def test_async_accept_and_handle_finalizes_unconsumed_text_stream_payload(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    consumed = False
    delivery = await async_delivery(
        eqs,
        b"raw",
        transport=ByteBufferTransport(),
    )

    async def body() -> AsyncIterator[bytes]:
        nonlocal consumed
        consumed = True
        yield delivery.body

    @callback_subscribe(topic="emails")
    async def handle(payload: AsyncIterable[str]) -> None:
        assert isinstance(payload, AsyncTextStreamPayload)

    assert isinstance(delivery.client, QueueClient)
    await delivery.client.accept_and_handle(
        body(),
        {
            **delivery.headers,
            HEADER_CONTENT_TYPE: TextStreamTransport.content_type,
        },
        lease_duration=30,
    )

    assert consumed
    assert eqs.state.by_id[delivery.message_id].acknowledged


@pytest.mark.anyio
async def test_async_accept_and_handle_infers_byte_stream_transport(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = await async_delivery(
        eqs,
        b"raw",
        transport=ByteBufferTransport(),
    )
    calls: list[bytes] = []

    async def body() -> AsyncIterator[bytes]:
        yield delivery.body

    @callback_subscribe(topic="emails")
    async def handle(payload: AsyncIterable[bytes]) -> None:
        chunks = bytearray()
        async for chunk in payload:
            chunks.extend(chunk)
        calls.append(bytes(chunks))

    assert isinstance(delivery.client, QueueClient)
    await delivery.client.accept_and_handle(
        body(),
        {
            **delivery.headers,
            HEADER_CONTENT_TYPE: CONTENT_TYPE_OCTET_STREAM,
        },
        lease_duration=30,
    )

    assert calls == [b"raw"]
    assert eqs.state.by_id[delivery.message_id].acknowledged


@pytest.mark.anyio
async def test_async_accept_and_handle_accepts_request_like_message(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = await async_delivery(eqs, {"ok": True})
    calls: list[dict[str, bool]] = []

    @callback_subscribe(topic="emails")
    async def handle(payload: dict[str, bool]) -> None:
        calls.append(payload)

    request = _AsyncRequestLike(delivery.body, delivery.headers)

    assert isinstance(delivery.client, QueueClient)
    await delivery.client.accept_and_handle(
        request,
        lease_duration=30,
    )

    assert calls == [{"ok": True}]
    assert eqs.state.by_id[delivery.message_id].acknowledged


@pytest.mark.anyio
async def test_async_accept_and_handle_awaits_async_subscriber(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = await async_delivery(eqs, {"ok": True})
    calls: list[dict[str, bool]] = []

    @callback_subscribe(topic="emails")
    async def handle(payload: dict[str, bool]) -> None:
        calls.append(payload)

    assert isinstance(delivery.client, QueueClient)
    await delivery.client.accept_and_handle(
        delivery.body,
        delivery.headers,
        lease_duration=30,
    )

    assert calls == [{"ok": True}]
    assert eqs.state.by_id[delivery.message_id].acknowledged


@pytest.mark.anyio
async def test_async_accept_and_handle_infers_message_byte_stream_transport(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = await async_delivery(
        eqs,
        b"raw",
        transport=ByteBufferTransport(),
    )
    calls: list[tuple[bytes, str]] = []

    async def body() -> AsyncIterator[bytes]:
        yield delivery.body

    @callback_subscribe(topic="emails")
    async def handle(message: Message[AsyncIterable[bytes]]) -> None:
        chunks = bytearray()
        async for chunk in message.payload:
            chunks.extend(chunk)
        calls.append((bytes(chunks), message.metadata.topic))

    assert isinstance(delivery.client, QueueClient)
    await delivery.client.accept_and_handle(
        body(),
        {
            **delivery.headers,
            HEADER_CONTENT_TYPE: CONTENT_TYPE_OCTET_STREAM,
        },
        lease_duration=30,
    )

    assert calls == [(b"raw", "emails")]
    assert eqs.state.by_id[delivery.message_id].acknowledged


@pytest.mark.anyio
async def test_async_accept_and_handle_infers_text_stream_transport(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = await async_delivery(
        eqs,
        b"raw",
        transport=ByteBufferTransport(),
    )
    calls: list[str] = []

    async def body() -> AsyncIterator[bytes]:
        yield delivery.body

    @callback_subscribe(topic="emails")
    async def handle(payload: AsyncIterable[str]) -> None:
        text = ""
        async for chunk in payload:
            text += chunk
        calls.append(text)

    assert isinstance(delivery.client, QueueClient)
    await delivery.client.accept_and_handle(
        body(),
        {
            **delivery.headers,
            HEADER_CONTENT_TYPE: TextStreamTransport.content_type,
        },
        lease_duration=30,
    )

    assert calls == ["raw"]
    assert eqs.state.by_id[delivery.message_id].acknowledged


@pytest.mark.anyio
async def test_async_accept_and_handle_infers_byte_buffer_transport(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = await async_delivery(
        eqs,
        b"raw",
        transport=ByteBufferTransport(),
    )
    calls: list[tuple[bytes, str]] = []

    async def body() -> AsyncIterator[bytes]:
        yield delivery.body[:2]
        yield delivery.body[2:]

    @callback_subscribe(topic="emails")
    async def handle(message: Message[bytes]) -> None:
        calls.append((message.payload, message.metadata.topic))

    assert isinstance(delivery.client, QueueClient)
    await delivery.client.accept_and_handle(
        body(),
        {
            **delivery.headers,
            HEADER_CONTENT_TYPE: CONTENT_TYPE_OCTET_STREAM,
        },
        lease_duration=30,
    )

    assert calls == [(b"raw", "emails")]
    assert eqs.state.by_id[delivery.message_id].acknowledged


@pytest.mark.anyio
async def test_async_accept_and_handle_infers_text_buffer_transport(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = await async_delivery(
        eqs,
        b"raw",
        transport=ByteBufferTransport(),
    )
    calls: list[tuple[str, str]] = []

    async def body() -> AsyncIterator[bytes]:
        yield delivery.body[:2]
        yield delivery.body[2:]

    @callback_subscribe(topic="emails")
    async def handle(message: Message[str]) -> None:
        calls.append((message.payload, message.metadata.topic))

    assert isinstance(delivery.client, QueueClient)
    await delivery.client.accept_and_handle(
        body(),
        {
            **delivery.headers,
            HEADER_CONTENT_TYPE: TextBufferTransport.content_type,
        },
        lease_duration=30,
    )

    assert calls == [("raw", "emails")]
    assert eqs.state.by_id[delivery.message_id].acknowledged


def test_sync_accept_and_handle_finalizes_unconsumed_stream_payload(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    consumed = False
    delivery = sync_delivery(
        eqs,
        b"raw",
        transport=ByteBufferTransport(),
    )

    def body() -> Iterator[bytes]:
        nonlocal consumed
        consumed = True
        yield delivery.body

    @callback_subscribe(topic="emails")
    def handle(payload: Iterable[bytes]) -> None:
        assert isinstance(payload, SyncStreamPayload)

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        body(),
        {
            **delivery.headers,
            HEADER_CONTENT_TYPE: CONTENT_TYPE_OCTET_STREAM,
        },
        lease_duration=30,
    )

    assert consumed
    assert eqs.state.by_id[delivery.message_id].acknowledged


def test_sync_accept_and_handle_finalizes_unconsumed_text_stream_payload(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    consumed = False
    delivery = sync_delivery(
        eqs,
        b"raw",
        transport=ByteBufferTransport(),
    )

    def body() -> Iterator[bytes]:
        nonlocal consumed
        consumed = True
        yield delivery.body

    @callback_subscribe(topic="emails")
    def handle(payload: Iterable[str]) -> None:
        assert isinstance(payload, SyncTextStreamPayload)

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        body(),
        {
            **delivery.headers,
            HEADER_CONTENT_TYPE: TextStreamTransport.content_type,
        },
        lease_duration=30,
    )

    assert consumed
    assert eqs.state.by_id[delivery.message_id].acknowledged


def test_sync_accept_and_handle_infers_byte_stream_transport(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = sync_delivery(
        eqs,
        b"raw",
        transport=ByteBufferTransport(),
    )
    calls: list[bytes] = []

    def body() -> Iterator[bytes]:
        yield delivery.body

    @callback_subscribe(topic="emails")
    def handle(payload: Iterable[bytes]) -> None:
        calls.append(b"".join(payload))

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        body(),
        {
            **delivery.headers,
            HEADER_CONTENT_TYPE: CONTENT_TYPE_OCTET_STREAM,
        },
        lease_duration=30,
    )

    assert calls == [b"raw"]
    assert eqs.state.by_id[delivery.message_id].acknowledged


def test_sync_accept_and_handle_infers_message_byte_stream_transport(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = sync_delivery(
        eqs,
        b"raw",
        transport=ByteBufferTransport(),
    )
    calls: list[tuple[bytes, str]] = []

    def body() -> Iterator[bytes]:
        yield delivery.body

    @callback_subscribe(topic="emails")
    def handle(message: Message[Iterable[bytes]]) -> None:
        calls.append((b"".join(message.payload), message.metadata.topic))

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        body(),
        {
            **delivery.headers,
            HEADER_CONTENT_TYPE: CONTENT_TYPE_OCTET_STREAM,
        },
        lease_duration=30,
    )

    assert calls == [(b"raw", "emails")]
    assert eqs.state.by_id[delivery.message_id].acknowledged


def test_sync_accept_and_handle_infers_text_stream_transport(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = sync_delivery(
        eqs,
        b"raw",
        transport=ByteBufferTransport(),
    )
    calls: list[str] = []

    def body() -> Iterator[bytes]:
        yield delivery.body

    @callback_subscribe(topic="emails")
    def handle(payload: Iterable[str]) -> None:
        calls.append("".join(payload))

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        body(),
        {
            **delivery.headers,
            HEADER_CONTENT_TYPE: TextStreamTransport.content_type,
        },
        lease_duration=30,
    )

    assert calls == ["raw"]
    assert eqs.state.by_id[delivery.message_id].acknowledged


def test_sync_accept_and_handle_infers_byte_buffer_transport(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = sync_delivery(
        eqs,
        b"raw",
        transport=ByteBufferTransport(),
    )
    calls: list[bytes] = []

    def body() -> Iterator[bytes]:
        yield delivery.body[:2]
        yield delivery.body[2:]

    @callback_subscribe(topic="emails")
    def handle(payload: bytes) -> None:
        calls.append(payload)

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        body(),
        {
            **delivery.headers,
            HEADER_CONTENT_TYPE: CONTENT_TYPE_OCTET_STREAM,
        },
        lease_duration=30,
    )

    assert calls == [b"raw"]
    assert eqs.state.by_id[delivery.message_id].acknowledged


def test_sync_accept_and_handle_infers_text_buffer_transport(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = sync_delivery(
        eqs,
        b"raw",
        transport=ByteBufferTransport(),
    )
    calls: list[str] = []

    def body() -> Iterator[bytes]:
        yield delivery.body[:2]
        yield delivery.body[2:]

    @callback_subscribe(topic="emails")
    def handle(payload: str) -> None:
        calls.append(payload)

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        body(),
        {
            **delivery.headers,
            HEADER_CONTENT_TYPE: TextBufferTransport.content_type,
        },
        lease_duration=30,
    )

    assert calls == ["raw"]
    assert eqs.state.by_id[delivery.message_id].acknowledged


def test_accept_and_handle_infers_json_transport_for_union_with_buffer_types(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = sync_delivery(eqs, "raw", transport=RawJsonTransport[str]())
    calls: list[bytes | str] = []

    @callback_subscribe(topic="emails")
    def handle(payload: bytes | str) -> None:
        calls.append(payload)

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        delivery.body,
        delivery.headers,
        lease_duration=30,
    )

    assert calls == ["raw"]
    assert eqs.state.by_id[delivery.message_id].acknowledged


def test_accept_and_handle_accepts_concrete_payload_annotations(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    class Payload(BaseModel):
        count: int

    calls: list[object] = []

    @callback_subscribe(topic="model")
    def model(payload: Message[Payload]) -> None:
        calls.append(payload.payload)

    @callback_subscribe(topic="annotated")
    def annotated(payload: Annotated[Payload, "metadata"]) -> None:
        calls.append(payload)

    @callback_subscribe(topic="dict")
    def typed_dict(payload: dict[str, bool]) -> None:
        calls.append(payload)

    @callback_subscribe(topic="list")
    def typed_list(payload: list[int]) -> None:
        calls.append(payload)

    @callback_subscribe(topic="union")
    def union(payload: int | str) -> None:
        calls.append(payload)

    deliveries = [
        sync_delivery(eqs, {"count": "3"}, topic="model"),
        sync_delivery(eqs, {"count": "4"}, topic="annotated"),
        sync_delivery(eqs, {"ok": True}, topic="dict"),
        sync_delivery(eqs, ["1", 2], topic="list"),
        sync_delivery(eqs, 5, topic="union"),
    ]
    for delivery in deliveries:
        assert isinstance(delivery.client, SyncQueueClient)
        delivery.client.accept_and_handle(
            delivery.body,
            delivery.headers,
            lease_duration=30,
        )

    assert calls == [
        Payload(count=3),
        Payload(count=4),
        {"ok": True},
        [1, 2],
        5,
    ]
    for delivery in deliveries:
        assert eqs.state.by_id[delivery.message_id].acknowledged


def test_accept_and_handle_validates_union_payload_annotation(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    valid_str = sync_delivery(
        eqs,
        "value",
        topic="union",
        transport=RawJsonTransport[str](),
    )
    valid_int = sync_delivery(eqs, 42, topic="union")
    invalid = sync_delivery(eqs, {"invalid": True}, topic="union")
    calls: list[int | str] = []

    @callback_subscribe(topic="union")
    def handle(payload: int | str) -> None:
        calls.append(payload)

    for delivery in [valid_str, valid_int]:
        assert isinstance(delivery.client, SyncQueueClient)
        delivery.client.accept_and_handle(
            delivery.body,
            delivery.headers,
            lease_duration=30,
        )
    assert isinstance(invalid.client, SyncQueueClient)
    with pytest.raises(PayloadValidationError):
        invalid.client.accept_and_handle(
            invalid.body,
            invalid.headers,
            lease_duration=30,
        )

    assert calls == ["value", 42]
    assert eqs.state.by_id[valid_str.message_id].acknowledged
    assert eqs.state.by_id[valid_int.message_id].acknowledged
    assert not eqs.state.by_id[invalid.message_id].acknowledged


def test_accept_and_handle_validates_message_union_payload_annotation(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    valid_str = sync_delivery(
        eqs,
        "value",
        topic="union",
        transport=RawJsonTransport[str](),
    )
    valid_int = sync_delivery(eqs, 42, topic="union")
    invalid = sync_delivery(eqs, {"invalid": True}, topic="union")
    calls: list[tuple[int | str, str]] = []

    @callback_subscribe(topic="union")
    def handle(message: Message[int | str]) -> None:
        calls.append((message.payload, message.metadata.message_id))

    for delivery in [valid_str, valid_int]:
        assert isinstance(delivery.client, SyncQueueClient)
        delivery.client.accept_and_handle(
            delivery.body,
            delivery.headers,
            lease_duration=30,
        )
    assert isinstance(invalid.client, SyncQueueClient)
    with pytest.raises(PayloadValidationError):
        invalid.client.accept_and_handle(
            invalid.body,
            invalid.headers,
            lease_duration=30,
        )

    assert calls == [("value", valid_str.message_id), (42, valid_int.message_id)]
    assert eqs.state.by_id[valid_str.message_id].acknowledged
    assert eqs.state.by_id[valid_int.message_id].acknowledged
    assert not eqs.state.by_id[invalid.message_id].acknowledged


def test_sync_accept_and_handle_top_level_wrapper(
    eqs: EmbeddedQueueDevServer,
    monkeypatch: pytest.MonkeyPatch,
    isolated_subscriptions: None,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_TOKEN", "token")
    monkeypatch.setenv("VERCEL_DEPLOYMENT_ID", "dpl_1")
    monkeypatch.setenv("VERCEL_QUEUE_BASE_URL", eqs.base_url)
    delivery = sync_delivery(
        eqs,
        {"ok": True},
        client=eqs.get_sync_client(token="token", deployment="dpl_1"),
    )
    calls: list[dict[str, bool]] = []

    @callback_subscribe(topic="emails")
    def handle(payload: dict[str, bool]) -> None:
        calls.append(payload)

    accept_and_handle(delivery.body, delivery.headers, lease_duration=30)

    assert calls == [{"ok": True}]
    assert eqs.state.by_id[delivery.message_id].acknowledged


def test_accept_and_handle_metadata_only_fetches_and_acknowledges(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    client = eqs.get_sync_client(token="token", deployment=ALL_DEPLOYMENTS)
    message_id = client.send("emails", {"ok": True})
    assert message_id is not None
    calls: list[dict[str, bool]] = []

    @callback_subscribe(topic="emails")
    def handle(payload: dict[str, bool]) -> None:
        calls.append(payload)

    headers = {
        key: value
        for key, value in callback_headers().items()
        if key
        not in {
            CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE,
            CLOUD_EVENT_HEADER_VQS_CREATED_AT,
        }
    }
    headers[CLOUD_EVENT_HEADER_VQS_MESSAGE_ID] = message_id
    client.accept_and_handle(
        b"",
        headers,
        lease_duration=20,
    )

    assert calls == [{"ok": True}]
    stored = eqs.state.by_id[message_id]
    assert stored.lease_deadline_by_consumer["tests"] == (eqs.state.now + timedelta(seconds=30))
    assert stored.acknowledged


def test_accept_and_handle_raised_retry_after_extends_without_ack(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = sync_delivery(eqs, {"ok": True})

    @callback_subscribe(topic="emails")
    def handle(payload: object) -> None:
        raise RetryAfter(45)

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        delivery.body,
        delivery.headers,
        lease_duration=30,
    )

    stored = eqs.state.by_id[delivery.message_id]
    assert stored.lease_deadline_by_consumer["tests"] == (eqs.state.now + timedelta(seconds=45))
    assert not stored.acknowledged


def test_accept_and_handle_raised_retry_after_zero_releases_without_ack(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = sync_delivery(eqs, {"ok": True})

    @callback_subscribe(topic="emails")
    def handle(payload: object) -> None:
        del payload
        raise RetryAfter(0)

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        delivery.body,
        delivery.headers,
        lease_duration=30,
    )

    stored = eqs.state.by_id[delivery.message_id]
    assert stored.lease_deadline_by_consumer["tests"] == eqs.state.now
    assert not stored.acknowledged


def test_accept_and_handle_raised_retry_after_rejects_above_server_max(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = sync_delivery(eqs, {"ok": True})

    @callback_subscribe(topic="emails")
    def handle(payload: object) -> None:
        del payload
        raise RetryAfter(3601)

    assert isinstance(delivery.client, SyncQueueClient)
    with pytest.raises(ValueError, match="duration cannot exceed 3600 seconds"):
        delivery.client.accept_and_handle(
            delivery.body,
            delivery.headers,
            lease_duration=30,
        )


@pytest.mark.anyio
async def test_async_accept_and_handle_retry_after_extends_without_ack(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(queue_lease, "_LEASE_STOP_WAIT_TIMEOUT_SECONDS", 0.01)
    delivery = await async_delivery(eqs, {"ok": True})

    @callback_subscribe(topic="emails")
    async def handle(payload: object) -> None:
        raise RetryAfter(12)

    assert isinstance(delivery.client, QueueClient)
    await delivery.client.accept_and_handle(
        delivery.body,
        delivery.headers,
        lease_duration=30,
    )

    stored = eqs.state.by_id[delivery.message_id]
    assert stored.lease_deadline_by_consumer["tests"] == (eqs.state.now + timedelta(seconds=12))
    assert not stored.acknowledged


def test_accept_and_handle_normal_return_acknowledges(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = sync_delivery(eqs, {"ok": True})

    @callback_subscribe(topic="emails")
    def handle(payload: object) -> None:
        del payload

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        delivery.body,
        delivery.headers,
        lease_duration=30,
    )

    assert eqs.state.by_id[delivery.message_id].acknowledged


def test_accept_and_handle_raised_handoff_leaves_delivery_open(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = sync_delivery(eqs, {"ok": True})

    @callback_subscribe(topic="emails")
    def handle(payload: object) -> None:
        raise Handoff

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        delivery.body,
        delivery.headers,
        lease_duration=30,
    )

    stored = eqs.state.by_id[delivery.message_id]
    assert stored.lease_deadline_by_consumer["tests"] == (eqs.state.now + timedelta(seconds=300))
    assert not stored.acknowledged


@pytest.mark.parametrize("directive", [Handoff(), RetryAfter(45)])
def test_accept_and_handle_handoff_or_retry_raise_leaves_unacked(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
    directive: Handoff | RetryAfter,
) -> None:
    delivery = sync_delivery(eqs, {"ok": True})
    calls: list[str] = []

    @callback_subscribe(topic="emails")
    def first(payload: object) -> None:
        del payload
        calls.append("first")
        raise directive

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        delivery.body,
        delivery.headers,
        lease_duration=30,
    )

    stored = eqs.state.by_id[delivery.message_id]
    assert calls == ["first"]
    assert not stored.acknowledged
    if isinstance(directive, RetryAfter):
        assert stored.lease_deadline_by_consumer["tests"] == (eqs.state.now + timedelta(seconds=45))


def test_accept_and_handle_subscriber_exception_leaves_unacked(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = sync_delivery(eqs, {"ok": True})

    @callback_subscribe(topic="emails")
    def handle(payload: object) -> None:
        raise ValueError("boom")

    assert isinstance(delivery.client, SyncQueueClient)
    with pytest.raises(ValueError, match="boom"):
        delivery.client.accept_and_handle(
            delivery.body,
            delivery.headers,
            lease_duration=30,
        )

    assert not eqs.state.by_id[delivery.message_id].acknowledged


def test_sync_accept_and_handle_installs_delivery_headers_context(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    seen_headers: list[dict[str, str]] = []
    set_headers({"x-existing": "outer"})
    delivery = sync_delivery(eqs, {"ok": True})

    @callback_subscribe(topic="emails")
    def handle(payload: object) -> None:
        seen_headers.append(dict(get_headers() or {}))

    assert isinstance(delivery.client, SyncQueueClient)
    delivery.client.accept_and_handle(
        delivery.body,
        {
            **delivery.headers,
            "x-vercel-oidc-token": "push-token",
        },
    )

    assert seen_headers[0]["x-vercel-oidc-token"] == "push-token"
    assert get_headers() == {"x-existing": "outer"}


@pytest.mark.anyio
async def test_async_accept_and_handle_installs_delivery_headers_context(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    seen_headers: list[dict[str, str]] = []
    set_headers({"x-existing": "outer"})
    delivery = await async_delivery(eqs, {"ok": True})

    @callback_subscribe(topic="emails")
    async def handle(payload: object) -> None:
        seen_headers.append(dict(get_headers() or {}))

    assert isinstance(delivery.client, QueueClient)
    await delivery.client.accept_and_handle(
        delivery.body,
        {
            **delivery.headers,
            "x-vercel-oidc-token": "push-token",
        },
    )

    assert seen_headers[0]["x-vercel-oidc-token"] == "push-token"
    assert get_headers() == {"x-existing": "outer"}


def test_accept_and_handle_no_matching_subscriber_raises_clearly(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    delivery = sync_delivery(eqs, {"ok": True})

    @subscribe(topic="orders")
    def handle(payload: object) -> None:
        pass

    assert isinstance(delivery.client, SyncQueueClient)
    with pytest.raises(
        UnhandledMessageError,
        match="No queue subscribers found for topic 'emails' and consumer group 'tests'",
    ):
        delivery.client.accept_and_handle(
            delivery.body,
            delivery.headers,
            lease_duration=30,
        )

    assert not eqs.state.by_id[delivery.message_id].acknowledged
