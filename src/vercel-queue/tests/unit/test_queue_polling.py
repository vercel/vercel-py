from __future__ import annotations

from typing import Any, ClassVar, ForwardRef, cast

import json
import logging
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timedelta, timezone

import anyio
import anyio.lowlevel
import pytest
import time_machine
from pydantic import BaseModel

from vercel.queue import (
    ALL_DEPLOYMENTS,
    ByteBufferTransport,
    ByteStreamTransport,
    Delivery,
    Handoff,
    InvalidLimitError,
    LeaseRenewal,
    Message,
    MessageAlreadyProcessedError,
    MessageCorruptedError,
    MessageLeaseExpiredError,
    MessageLockedError,
    MessageMetadata,
    MessageNotFoundError,
    MessageNotInFlightError,
    QueueClient,
    ReceiptHandleMismatchError,
    RetryAfter,
    SanitizedName,
    ServiceError,
    SubscriptionError,
    TextBufferTransport,
    TextStreamTransport,
    Topic,
)
from vercel.queue._internal.asynctools import iter_coroutine
from vercel.queue._internal.streams import (
    AsyncStreamPayload,
    AsyncTextStreamPayload,
    SyncStreamPayload,
    SyncTextStreamPayload,
)
from vercel.queue.devserver import EmbeddedQueueDevServer
from vercel.queue.sync import QueueClient as SyncQueueClient

from .helpers import (
    CREATED_AT,
    CREATED_AT_DT,
    collect_async_stream,
    make_leased_metadata,
    malformed_multipart_body,
    mock_response,
    multipart_body,
    queue_httpx_module,
)


def _queue_debug_events(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    return [
        json.loads(record.message) for record in caplog.records if record.name == "vercel.queue"
    ]


def test_receive_ack_and_visibility(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    client.send("emails", {"ok": True})

    deliveries: list[Delivery[Any]] = list(client.poll("emails", "test-group", limit=1))
    assert len(deliveries) == 1
    delivery = deliveries[0]
    assert isinstance(delivery, Delivery)
    message = delivery.message

    assert message.payload == {"ok": True}
    assert message.metadata.delivery_count == 1
    assert message.metadata.created_at == eqs.state.now
    assert message.metadata.expires_at == eqs.state.now + timedelta(days=1)
    assert message.metadata.region == "iad1"
    assert not hasattr(message.metadata, "messageId")
    assert not hasattr(message.metadata, "deliveryCount")
    assert not hasattr(message.metadata, "createdAt")
    stored = eqs.state.by_id["msg_1"]
    assert stored.lease_deadline_by_consumer["test-group"] == (
        eqs.state.now + timedelta(seconds=300)
    )
    client.extend_lease(message, 30)
    assert stored.lease_deadline_by_consumer["test-group"] == (
        eqs.state.now + timedelta(seconds=30)
    )
    client.acknowledge(message)
    assert stored.acknowledged


def test_poll_iterates_one_receive_request(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    client.send("emails", {"ok": True})
    deliveries: list[Delivery[Any]] = list(
        client.poll(
            "emails",
            "test-group",
            limit=1,
        )
    )

    assert len(deliveries) == 1
    message = deliveries[0].message
    assert message.payload == {"ok": True}
    stored = eqs.state.by_id[message.metadata.message_id]
    assert stored.delivery_count_by_consumer["test-group"] == 1


def test_sync_poll_sanitizes_consumer_group(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    client.send("emails", {"ok": True})

    delivery: Delivery[Any] = next(client.poll("emails", "api/worker.py"))

    assert delivery.message.metadata.consumer_group == "api_Sworker_Dpy"
    stored = eqs.state.by_id[delivery.message.metadata.message_id]
    assert stored.delivery_count_by_consumer["api_Sworker_Dpy"] == 1


def test_sync_poll_trusts_sanitized_consumer_group(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    client.send("emails", {"ok": True})

    delivery: Delivery[Any] = next(client.poll("emails", SanitizedName("worker_group")))

    assert delivery.message.metadata.consumer_group == "worker_group"
    stored = eqs.state.by_id[delivery.message.metadata.message_id]
    assert stored.delivery_count_by_consumer["worker_group"] == 1


@pytest.mark.anyio
async def test_async_poll_sanitizes_consumer_group(
    eqs: EmbeddedQueueDevServer,
) -> None:
    await eqs.client.send("emails", {"ok": True})

    delivery: Delivery[Any] = await anext(eqs.client.poll("emails", "api/worker.py"))

    assert delivery.message.metadata.consumer_group == "api_Sworker_Dpy"
    stored = eqs.state.by_id[delivery.message.metadata.message_id]
    assert stored.delivery_count_by_consumer["api_Sworker_Dpy"] == 1


@pytest.mark.anyio
async def test_async_poll_trusts_sanitized_consumer_group(
    eqs: EmbeddedQueueDevServer,
) -> None:
    await eqs.client.send("emails", {"ok": True})

    delivery: Delivery[Any] = await anext(eqs.client.poll("emails", SanitizedName("worker_group")))

    assert delivery.message.metadata.consumer_group == "worker_group"
    stored = eqs.state.by_id[delivery.message.metadata.message_id]
    assert stored.delivery_count_by_consumer["worker_group"] == 1


def test_poll_yields_delivery_and_clean_exit_acknowledges(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    client.send("emails", {"ok": True})

    delivery: Delivery[Any] = next(client.poll("emails", "test-group", lease_duration=30))

    assert isinstance(delivery, Delivery)
    with delivery as message:
        assert isinstance(message, Message)
        assert message.payload == {"ok": True}

    assert eqs.state.by_id[message.metadata.message_id].acknowledged


def test_poll_delivery_accept_hands_off_lifecycle(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    client.send("emails", {"ok": True})

    delivery: Delivery[Any] = next(client.poll("emails", "test-group", lease_duration=30))
    message = delivery.accept()

    assert message.payload == {"ok": True}
    stored = eqs.state.by_id[message.metadata.message_id]
    assert not stored.acknowledged

    client.extend_lease(message, 60)
    assert stored.lease_deadline_by_consumer["test-group"] == (
        eqs.state.now + timedelta(seconds=60)
    )

    client.acknowledge(message)
    assert stored.acknowledged


def test_poll_delivery_accept_rejects_context_manager_use(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    client.send("emails", {"ok": True})

    delivery: Delivery[Any] = next(client.poll("emails", "test-group", lease_duration=30))
    delivery.accept()

    with pytest.raises(RuntimeError, match="accepted delivery cannot be used"), delivery:
        pass


def test_poll_delivery_enter_rejects_accept(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    client.send("emails", {"ok": True})

    delivery: Delivery[Any] = next(client.poll("emails", "test-group", lease_duration=30))

    with delivery, pytest.raises(RuntimeError, match="entered delivery cannot be accepted"):
        delivery.accept()


def test_poll_delivery_exception_does_not_acknowledge(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    client.send("emails", {"ok": True})
    delivery: Delivery[Any] = next(client.poll("emails", "test-group", lease_duration=30))

    with pytest.raises(RuntimeError, match="boom"), delivery as message:
        raise RuntimeError("boom")

    assert not eqs.state.by_id[message.metadata.message_id].acknowledged


def test_poll_delivery_retry_after_changes_visibility_without_ack(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    client.send("emails", {"ok": True})
    delivery: Delivery[Any] = next(client.poll("emails", "test-group", lease_duration=30))

    with delivery as message:
        raise RetryAfter(12)

    stored = eqs.state.by_id[message.metadata.message_id]
    assert not stored.acknowledged
    assert stored.lease_deadline_by_consumer["test-group"] == (
        eqs.state.now + timedelta(seconds=12)
    )


def test_poll_delivery_handoff_suppresses_follow_up(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    client.send("emails", {"ok": True})
    delivery: Delivery[Any] = next(client.poll("emails", "test-group", lease_duration=30))

    with delivery as message:
        raise Handoff

    stored = eqs.state.by_id[message.metadata.message_id]
    assert not stored.acknowledged
    assert stored.lease_deadline_by_consumer["test-group"] == (
        eqs.state.now + timedelta(seconds=30)
    )


def test_poll_delivery_sync_stream_payload_finalizes_on_exit() -> None:
    drained: list[bytes] = []

    def chunks() -> Iterator[bytes]:
        for chunk in (b"a", b"b"):
            drained.append(chunk)
            yield chunk

    message = Message(
        payload=SyncStreamPayload(chunks()),
        metadata=make_leased_metadata("emails"),
    )

    class Client:
        def acknowledge(self, message: Message[Any]) -> None:
            del message

        def extend_lease(self, message: Message[Any], duration: int) -> None:
            del message, duration

    delivery: Delivery[SyncStreamPayload] = Delivery(
        message,
        client=cast("SyncQueueClient", Client()),
        lease_duration=30,
    )

    with delivery:
        pass

    assert drained == [b"a", b"b"]


def test_poll_delivery_starts_and_stops_sync_renewal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = 0
    waits: list[bool] = []

    def start(self: LeaseRenewal) -> LeaseRenewal:
        nonlocal started
        started += 1
        return self

    def stop(self: LeaseRenewal, *, wait: bool = True) -> None:
        del self
        waits.append(wait)

    class Client:
        def acknowledge(self, message: Message[Any]) -> None:
            del message

        def extend_lease(self, message: Message[Any], duration: int) -> None:
            del message, duration

    monkeypatch.setattr(LeaseRenewal, "__enter__", start)
    monkeypatch.setattr(LeaseRenewal, "stop", stop)
    message = Message(payload={"ok": True}, metadata=make_leased_metadata("emails"))
    delivery: Delivery[dict[str, bool]] = Delivery(
        message,
        client=cast("SyncQueueClient", Client()),
        lease_duration=30,
    )

    with delivery as entered:
        assert entered is message

    assert started == 1
    assert waits == [False]


@pytest.mark.anyio
async def test_async_poll_yields_delivery_and_clean_exit_acknowledges(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_async_client(base_url=eqs.base_url)
    await client.send("emails", {"ok": True})

    delivery: Delivery[Any] = await anext(client.poll("emails", "test-group", lease_duration=30))

    assert isinstance(delivery, Delivery)
    async with delivery as message:
        assert isinstance(message, Message)
        assert message.payload == {"ok": True}

    assert eqs.state.by_id[message.metadata.message_id].acknowledged


@pytest.mark.anyio
async def test_async_poll_delivery_accept_hands_off_lifecycle(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_async_client(base_url=eqs.base_url)
    await client.send("emails", {"ok": True})

    delivery: Delivery[Any] = await anext(client.poll("emails", "test-group", lease_duration=30))
    message = delivery.accept()

    assert message.payload == {"ok": True}
    stored = eqs.state.by_id[message.metadata.message_id]
    assert not stored.acknowledged

    await client.extend_lease(message, 60)
    assert stored.lease_deadline_by_consumer["test-group"] == (
        eqs.state.now + timedelta(seconds=60)
    )

    await client.acknowledge(message)
    assert stored.acknowledged


@pytest.mark.anyio
async def test_async_poll_delivery_accept_rejects_context_manager_use(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_async_client(base_url=eqs.base_url)
    await client.send("emails", {"ok": True})

    delivery: Delivery[Any] = await anext(client.poll("emails", "test-group", lease_duration=30))
    delivery.accept()

    with pytest.raises(RuntimeError, match="accepted delivery cannot be used"):
        async with delivery:
            pass


@pytest.mark.anyio
async def test_async_poll_delivery_exception_does_not_acknowledge(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_async_client(base_url=eqs.base_url)
    await client.send("emails", {"ok": True})
    delivery: Delivery[Any] = await anext(client.poll("emails", "test-group", lease_duration=30))

    with pytest.raises(RuntimeError, match="boom"):
        async with delivery as message:
            raise RuntimeError("boom")

    assert not eqs.state.by_id[message.metadata.message_id].acknowledged


@pytest.mark.anyio
async def test_async_poll_delivery_retry_after_changes_visibility_without_ack(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_async_client(base_url=eqs.base_url)
    await client.send("emails", {"ok": True})
    delivery: Delivery[Any] = await anext(client.poll("emails", "test-group", lease_duration=30))

    async with delivery as message:
        raise RetryAfter(12)

    stored = eqs.state.by_id[message.metadata.message_id]
    assert not stored.acknowledged
    assert stored.lease_deadline_by_consumer["test-group"] == (
        eqs.state.now + timedelta(seconds=12)
    )


@pytest.mark.anyio
async def test_async_poll_delivery_handoff_suppresses_follow_up(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_async_client(base_url=eqs.base_url)
    await client.send("emails", {"ok": True})
    delivery: Delivery[Any] = await anext(client.poll("emails", "test-group", lease_duration=30))

    async with delivery as message:
        raise Handoff

    stored = eqs.state.by_id[message.metadata.message_id]
    assert not stored.acknowledged
    assert stored.lease_deadline_by_consumer["test-group"] == (
        eqs.state.now + timedelta(seconds=30)
    )


@pytest.mark.anyio
async def test_async_poll_delivery_stream_payload_finalizes_on_exit() -> None:
    closed = 0

    async def chunks() -> AsyncIterator[bytes]:
        yield b"a"
        yield b"b"

    async def close() -> None:
        nonlocal closed
        closed += 1

    message = Message(
        payload=AsyncStreamPayload(chunks(), on_close=close),
        metadata=make_leased_metadata("emails"),
    )

    class Client:
        async def acknowledge(self, message: Message[Any]) -> None:
            del message

        async def extend_lease(self, message: Message[Any], duration: int) -> None:
            del message, duration

    delivery: Delivery[AsyncStreamPayload] = Delivery(
        message,
        client=cast("QueueClient", Client()),
        lease_duration=30,
    )

    async with delivery:
        pass

    assert closed == 1


@pytest.mark.anyio
async def test_async_poll_delivery_starts_and_stops_renewal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = 0
    waits: list[bool] = []

    async def start_async(self: LeaseRenewal) -> LeaseRenewal:
        nonlocal started
        started += 1
        return self

    async def stop_async(self: LeaseRenewal, *, wait: bool = True) -> None:
        del self
        waits.append(wait)

    class Client:
        async def acknowledge(self, message: Message[Any]) -> None:
            del message

        async def extend_lease(self, message: Message[Any], duration: int) -> None:
            del message, duration

    monkeypatch.setattr(LeaseRenewal, "start_async", start_async)
    monkeypatch.setattr(LeaseRenewal, "stop_async", stop_async)
    message = Message(payload={"ok": True}, metadata=make_leased_metadata("emails"))
    delivery: Delivery[dict[str, bool]] = Delivery(
        message,
        client=cast("QueueClient", Client()),
        lease_duration=30,
    )

    async with delivery as entered:
        assert entered is message

    assert started == 1
    assert waits == [False]


def test_extend_lease_zero_releases_message(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    client.send("emails", {"ok": True})
    delivery: Delivery[Any] = next(client.poll("emails", "test-group", lease_duration=30))
    message = delivery.message

    client.extend_lease(message, 0)

    stored = eqs.state.by_id[message.metadata.message_id]
    assert stored.lease_deadline_by_consumer["test-group"] == eqs.state.now


def test_visibility_timeout_allows_server_max(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    client.send("emails", {"ok": True})

    delivery: Delivery[Any] = next(client.poll("emails", "test-group", lease_duration=3600))
    message = delivery.message
    stored = eqs.state.by_id[message.metadata.message_id]
    assert stored.lease_deadline_by_consumer["test-group"] == (
        eqs.state.now + timedelta(seconds=3600)
    )
    client.extend_lease(message, 3600)
    assert stored.lease_deadline_by_consumer["test-group"] == (
        eqs.state.now + timedelta(seconds=3600)
    )


def test_visibility_timeout_rejects_above_server_max(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    client.send("emails", {"ok": True})

    with pytest.raises(ValueError, match="lease_duration cannot exceed 3600 seconds"):
        list(client.poll("emails", "test-group", lease_duration=3601))

    delivery: Delivery[Any] = next(client.poll("emails", "test-group", lease_duration=30))
    message = delivery.message
    with pytest.raises(ValueError, match="duration cannot exceed 3600 seconds"):
        client.extend_lease(message, 3601)


def test_poll_by_id_decodes_buffered_message(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    message_id = client.send("emails", {"ok": True})
    assert message_id is not None

    message: Message[Any] = iter_coroutine(
        client._poll_by_id(
            "emails",
            "test-group",
            message_id,
            lease_duration=15,
        )
    )

    assert message.payload == {"ok": True}
    assert message.metadata.created_at == eqs.state.now
    assert message.metadata.expires_at == eqs.state.now + timedelta(days=1)
    assert message.metadata.region == "iad1"
    assert eqs.state.by_id[message_id].lease_deadline_by_consumer["test-group"] == (
        eqs.state.now + timedelta(seconds=15)
    )


def test_poll_by_id_uses_default_visibility_timeout(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    message_id = client.send("emails", {"ok": True})
    assert message_id is not None

    iter_coroutine(
        client._poll_by_id(
            "emails",
            "test-group",
            message_id,
        )
    )

    assert eqs.state.by_id[message_id].lease_deadline_by_consumer["test-group"] == (
        eqs.state.now + timedelta(seconds=300)
    )


def _single_response_client_factory(response: Any) -> Any:
    queue_httpx = queue_httpx_module()

    def handler(request: Any) -> Any:
        del request
        return response

    return lambda **kwargs: queue_httpx.Client(
        transport=queue_httpx.MockTransport(handler),
        **kwargs,
    )


def test_poll_raises_on_missing_multipart_receipt_handle() -> None:
    boundary = "queue-boundary"
    response = mock_response(
        200,
        headers={"Content-Type": f"multipart/mixed; boundary={boundary}"},
        content=malformed_multipart_body(
            boundary,
            [
                b"Content-Type: application/json",
                b"Vqs-Message-Id: msg_1",
                f"Vqs-Timestamp: {CREATED_AT}".encode(),
            ],
        ),
    )

    with pytest.raises(MessageCorruptedError, match="Vqs-Receipt-Handle"):
        list(
            SyncQueueClient(
                token="token",
                deployment=ALL_DEPLOYMENTS,
                http_client_factory=_single_response_client_factory(response),
            ).poll(
                "emails",
                "test-group",
            )
        )


def test_poll_by_id_raises_on_missing_multipart_message_id() -> None:
    boundary = "queue-boundary"
    response = mock_response(
        200,
        headers={"Content-Type": f"multipart/mixed; boundary={boundary}"},
        content=malformed_multipart_body(
            boundary,
            [
                b"Content-Type: application/json",
                b"Vqs-Receipt-Handle: rh_1",
                f"Vqs-Timestamp: {CREATED_AT}".encode(),
            ],
        ),
    )

    with pytest.raises(MessageCorruptedError, match="Vqs-Message-Id"):
        iter_coroutine(
            SyncQueueClient(
                token="token",
                deployment=ALL_DEPLOYMENTS,
                http_client_factory=_single_response_client_factory(response),
            )._poll_by_id(
                "emails",
                "test-group",
                "msg_1",
            )
        )


def test_poll_metadata_allows_missing_expires_at() -> None:
    boundary = "queue-boundary"
    response = mock_response(
        200,
        headers={"Content-Type": f"multipart/mixed; boundary={boundary}"},
        content=multipart_body(boundary, expires_at=None),
    )
    delivery: Delivery[Any] = next(
        SyncQueueClient(
            token="token",
            deployment=ALL_DEPLOYMENTS,
            http_client_factory=_single_response_client_factory(response),
        ).poll("emails", "test-group")
    )
    message = delivery.message

    assert message.metadata.created_at == CREATED_AT_DT
    assert message.metadata.expires_at is None


def test_receive_can_return_sync_stream_payload(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    send_topic = Topic[bytes]("emails", transport=ByteBufferTransport())
    poll_topic = Topic[SyncStreamPayload]("emails", transport=ByteStreamTransport())
    client.send(send_topic, b"raw")

    messages = client.poll(poll_topic, "test-group")
    delivery = next(messages)
    with delivery as message:
        assert isinstance(message.payload, SyncStreamPayload)
        assert b"".join(message.payload) == b"raw"


def test_receive_can_return_sync_text_stream_payload(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    send_topic = Topic[str]("emails", transport=TextBufferTransport())
    poll_topic = Topic[SyncTextStreamPayload]("emails", transport=TextStreamTransport())
    client.send(send_topic, "caf\u00e9")

    messages = client.poll(poll_topic, "test-group")
    delivery = next(messages)
    with delivery as message:
        assert isinstance(message.payload, SyncTextStreamPayload)
        assert "".join(message.payload) == "caf\u00e9"


def test_sync_poll_infers_byte_transport_from_typed_topic(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    topic = Topic[bytes]("bytes")
    client.send(topic, b"raw")

    delivery = next(client.poll(topic, "test-group"))

    with delivery as message:
        assert message.payload == b"raw"


def test_sync_poll_infers_text_transport_from_typed_topic(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    topic = Topic[str]("text")
    client.send(topic, "caf\u00e9")

    delivery = next(client.poll(topic, "test-group"))

    with delivery as message:
        assert message.payload == "caf\u00e9"


def test_sync_poll_infers_stream_transport_from_typed_topic(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    topic = Topic[SyncStreamPayload]("stream")
    client.send(Topic[bytes](topic.name, transport=ByteBufferTransport()), b"raw")

    delivery = next(client.poll(topic, "test-group"))

    with delivery as message:
        assert isinstance(message.payload, SyncStreamPayload)
        assert b"".join(message.payload) == b"raw"


def test_sync_poll_infers_typed_json_model_from_typed_topic(
    eqs: EmbeddedQueueDevServer,
) -> None:
    class Payload(BaseModel):
        count: int

    client = eqs.get_sync_client()
    topic = Topic[Payload]("models")
    client.send(topic.name, {"count": "3"})

    delivery = next(client.poll(topic, "test-group"))

    with delivery as message:
        assert message.payload == Payload(count=3)


def test_sync_poll_infers_typed_json_container_from_typed_topic(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    topic = Topic[dict[str, str]]("containers")
    client.send(topic, {"ok": "yes"})

    delivery = next(client.poll(topic, "test-group"))

    with delivery as message:
        assert message.payload == {"ok": "yes"}


def test_poll_topic_transport_overrides_typed_topic(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    topic = Topic[str]("override", transport=TextBufferTransport())
    client.send(topic, "text")

    delivery = next(client.poll(topic, "test-group"))

    with delivery as message:
        assert message.payload == "text"


def test_poll_unspecialized_topics_still_use_raw_json(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    client.send("raw-string", {"count": "3"})
    client.send(Topic("raw-topic"), {"count": "4"})

    string_delivery: Delivery[Any] = next(client.poll("raw-string", "test-group"))
    topic_delivery: Delivery[Any] = next(client.poll(Topic("raw-topic"), "test-group"))

    with string_delivery as message:
        assert message.payload == {"count": "3"}
    with topic_delivery as message:
        assert message.payload == {"count": "4"}


def test_topic_sanitized_name_is_not_double_encoded(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    topic: Topic[Any] = Topic(SanitizedName("team_Semail__high"))
    client.send(topic, {"count": "5"})

    delivery: Delivery[Any] = next(client.poll(topic, "test-group"))

    with delivery as message:
        assert message.metadata.topic == "team_Semail__high"
        assert message.payload == {"count": "5"}


def test_poll_rejects_specialized_topic_forward_ref(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    topic = Topic.__class_getitem__(ForwardRef("Payload"))("forward-ref")
    client.send(topic.name, {"count": 3})

    with pytest.raises(SubscriptionError, match="unsupported queue subscriber payload annotation"):
        next(client.poll(topic, "test-group"))


@pytest.mark.anyio
async def test_poll_by_id_error_mapping_is_shared(
    eqs: EmbeddedQueueDevServer,
) -> None:
    sync_client = eqs.get_sync_client()
    async_client = eqs.get_async_client(base_url=eqs.base_url)
    group = "test-group"

    with pytest.raises(MessageNotFoundError):
        iter_coroutine(sync_client._poll_by_id("emails", group, "msg_missing"))

    with pytest.raises(MessageNotFoundError):
        await async_client._poll_by_id("emails", group, "msg_missing")

    processed_id = sync_client.send("emails", {"processed": True})
    assert processed_id is not None
    processed: Message[Any] = iter_coroutine(sync_client._poll_by_id("emails", group, processed_id))
    sync_client.acknowledge(processed)
    with pytest.raises(MessageAlreadyProcessedError):
        iter_coroutine(sync_client._poll_by_id("emails", group, processed_id))

    original_id = await async_client.send("emails", {"ok": True}, idempotency_key="same")
    duplicate_id = await async_client.send("emails", {"ok": True}, idempotency_key="same")
    assert original_id is not None
    assert duplicate_id is not None

    redirected_message: Message[Any] = await async_client._poll_by_id(
        "emails",
        group,
        duplicate_id,
    )

    assert redirected_message.metadata.message_id == original_id
    assert redirected_message.payload == {"ok": True}

    locked_id = await async_client.send("emails", {"locked": True})
    assert locked_id is not None
    await async_client._poll_by_id("emails", group, locked_id, lease_duration=7)
    with pytest.raises(MessageLockedError) as locked_info:
        await async_client._poll_by_id("emails", group, locked_id)
    assert locked_info.value.status_code == 409
    assert locked_info.value.retry_after == 7


@pytest.mark.anyio
async def test_debug_logs_duplicate_redirect(
    eqs: EmbeddedQueueDevServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level(logging.INFO, logger="vercel.queue")
    client = eqs.get_async_client(base_url=eqs.base_url)
    original_id = await client.send("emails", {"ok": True}, idempotency_key="same")
    duplicate_id = await client.send("emails", {"ok": True}, idempotency_key="same")
    assert original_id is not None
    assert duplicate_id is not None

    message: Message[Any] = await client._poll_by_id("emails", "test-group", duplicate_id)

    assert message.metadata.message_id == original_id
    assert any(
        event["event"] == "receive.redirect_duplicate"
        and event["requested_message_id"] == duplicate_id
        and event["original_message_id"] == original_id
        for event in _queue_debug_events(caplog)
    )


@pytest.mark.anyio
async def test_poll_by_id_handles_unsupported_error_shapes(
    eqs: EmbeddedQueueDevServer,
) -> None:
    group = "test-group"

    with time_machine.travel(datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc), tick=False):
        eqs.app._server.respond_once(
            method="POST",
            action="message_id",
            status_code=409,
            headers={"Retry-After": "Thu, 01 Jan 2026 00:00:09 GMT"},
        )
        with pytest.raises(MessageLockedError) as date_locked_info:
            await eqs.client._poll_by_id(
                "emails",
                group,
                "msg_missing",
            )
        assert date_locked_info.value.retry_after == 9

    eqs.app._server.respond_once(
        method="POST",
        action="message_id",
        status_code=409,
        headers={"Retry-After": "0"},
    )
    with pytest.raises(MessageLockedError) as zero_locked_info:
        await eqs.client._poll_by_id(
            "emails",
            group,
            "msg_missing",
        )
    assert zero_locked_info.value.retry_after == 1

    eqs.app._server.respond_once(
        method="POST",
        action="message_id",
        status_code=409,
        body=json.dumps({"error": "unknown lock reason"}).encode(),
    )
    with pytest.raises(MessageLockedError) as unknown_locked_info:
        await eqs.client._poll_by_id(
            "emails",
            group,
            "msg_missing",
        )
    assert unknown_locked_info.value.reason == "unknown lock reason"

    eqs.app._server.respond_once(
        method="POST",
        action="message_id",
        status_code=418,
        body=b"teapot",
    )
    with pytest.raises(ServiceError, match="teapot") as service_info:
        await eqs.client._poll_by_id(
            "emails",
            group,
            "msg_missing",
        )
    assert service_info.value.status_code == 418


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("server_error", "error_type"),
    [
        ("Message is not currently in-flight", MessageNotInFlightError),
        ("Message lease has expired", MessageLeaseExpiredError),
        ("Receipt handle does not match current lease holder", ReceiptHandleMismatchError),
    ],
)
async def test_poll_by_id_maps_specific_409_errors(
    eqs: EmbeddedQueueDevServer,
    server_error: str,
    error_type: type[MessageLockedError],
) -> None:
    eqs.app._server.respond_once(
        method="POST",
        action="message_id",
        status_code=409,
        body=json.dumps({"error": server_error}).encode(),
    )

    with pytest.raises(error_type) as exc_info:
        await eqs.client._poll_by_id("emails", "test-group", "msg_missing")

    assert isinstance(exc_info.value, MessageLockedError)
    assert exc_info.value.reason == server_error


@pytest.mark.anyio
async def test_poll_by_id_409_duplicate_redirect_takes_precedence(
    eqs: EmbeddedQueueDevServer,
) -> None:
    original_id = await eqs.client.send("emails", {"ok": True})
    assert original_id is not None
    eqs.app._server.respond_once(
        method="POST",
        action="message_id",
        status_code=409,
        body=json.dumps({
            "error": "This messageId was a duplicate - use originalMessageId instead",
            "originalMessageId": original_id,
        }).encode(),
    )

    message: Message[Any] = await eqs.client._poll_by_id(
        "emails",
        "test-group",
        "msg_duplicate",
    )

    assert message.metadata.message_id == original_id


def test_acknowledge_normalizes_unexpected_transport_error(
    eqs: EmbeddedQueueDevServer,
) -> None:
    eqs.app._server.respond_once(
        method="DELETE",
        action="lease",
        status_code=302,
        body=b"redirect",
    )
    metadata = MessageMetadata(
        message_id="msg_1",
        delivery_count=1,
        created_at=CREATED_AT_DT,
        topic="emails",
        consumer_group=SanitizedName("test-group"),
        receipt_handle="rh_1",
    )

    with pytest.raises(ServiceError, match="redirect") as service_info:
        eqs.get_sync_client(token="token", deployment=ALL_DEPLOYMENTS).acknowledge(metadata)
    assert service_info.value.status_code == 302


def test_invalid_receive_limit() -> None:
    group = "test-group"
    with pytest.raises(InvalidLimitError):
        list(
            SyncQueueClient(token="token", deployment=ALL_DEPLOYMENTS).poll(
                "emails",
                group,
                limit=11,
            )
        )


@pytest.mark.anyio
async def test_async_receive_can_return_text_stream_payload(
    eqs: EmbeddedQueueDevServer,
) -> None:
    send_topic = Topic[str]("emails", transport=TextBufferTransport())
    poll_topic = Topic[AsyncTextStreamPayload]("emails", transport=TextStreamTransport())
    await eqs.client.send(send_topic, "caf\u00e9")
    group = "test-group"
    async for delivery in eqs.client.poll(poll_topic, group):
        async with delivery as message:
            assert isinstance(message.payload, AsyncTextStreamPayload)
            assert [chunk async for chunk in message.payload] == ["caf\u00e9"]
        break


@pytest.mark.anyio
async def test_async_poll_infers_byte_transport_from_typed_topic(
    eqs: EmbeddedQueueDevServer,
) -> None:
    topic = Topic[bytes]("async-bytes")
    await eqs.client.send(topic, b"raw")

    async for delivery in eqs.client.poll(topic, "test-group"):
        async with delivery as message:
            assert message.payload == b"raw"
        break


@pytest.mark.anyio
async def test_async_poll_infers_text_stream_transport_from_typed_topic(
    eqs: EmbeddedQueueDevServer,
) -> None:
    topic = Topic[AsyncTextStreamPayload]("async-text-stream")
    await eqs.client.send(Topic[str](topic.name, transport=TextBufferTransport()), "caf\u00e9")

    async for delivery in eqs.client.poll(topic, "test-group"):
        async with delivery as message:
            assert isinstance(message.payload, AsyncTextStreamPayload)
            assert [chunk async for chunk in message.payload] == ["caf\u00e9"]
        break


@pytest.mark.anyio
async def test_async_poll_by_id_stream_payload_supports_reader_methods(
    eqs: EmbeddedQueueDevServer,
) -> None:
    message_id = await eqs.client.send(
        Topic[bytes]("emails", transport=ByteBufferTransport()),
        b"raw",
    )
    assert message_id is not None

    group = "test-group"
    message = await eqs.client._poll_by_id(
        "emails",
        group,
        message_id,
        transport=ByteStreamTransport(),
    )

    assert isinstance(message.payload, AsyncStreamPayload)
    assert await message.payload.read() == b"raw"


@pytest.mark.anyio
async def test_async_poll_by_id_stream_payload_keeps_response_open() -> None:
    closed = False
    release = anyio.Event()

    class StreamingResponse:
        status_code = 200
        headers: ClassVar[dict[str, str]] = {"Content-Type": "multipart/mixed; boundary=boundary"}

        @property
        def text(self) -> str:
            return ""

        def json(self) -> object:
            return {}

        async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
            del chunk_size
            yield b"\r\n".join([
                b"--boundary",
                b"Content-Type: application/octet-stream",
                b"Vqs-Message-Id: msg_1",
                b"Vqs-Receipt-Handle: rh_1",
                b"Vqs-Delivery-Count: 1",
                f"Vqs-Timestamp: {CREATED_AT}".encode(),
                b"",
                b"ra",
            ])
            await release.wait()
            yield b"w\r\n--boundary--\r\n"

    class StreamContext:
        async def __aenter__(self) -> StreamingResponse:
            return StreamingResponse()

        async def __aexit__(self, *args: object) -> None:
            nonlocal closed
            closed = True

    class Runtime:
        async def token(self, token: str | None) -> str:
            return token or "token"

        def stream_post(self, url: str, *, headers: dict[str, str]) -> StreamContext:
            del url, headers
            return StreamContext()

        async def post(self, *args: object, **kwargs: object) -> object:
            raise AssertionError

        async def delete(self, *args: object, **kwargs: object) -> object:
            raise AssertionError

        async def patch(self, *args: object, **kwargs: object) -> object:
            raise AssertionError

    client = QueueClient(token="token", deployment=ALL_DEPLOYMENTS)
    client._runtime = cast("Any", Runtime())

    message = await client._poll_by_id(
        "emails",
        "test-group",
        "msg_1",
        transport=ByteStreamTransport(),
    )

    assert isinstance(message.payload, AsyncStreamPayload)
    assert not closed
    result: bytes | None = None
    async with anyio.create_task_group() as task_group:

        async def collect_payload() -> None:
            nonlocal result
            result = await collect_async_stream(message.payload)

        task_group.start_soon(collect_payload)
        await anyio.lowlevel.checkpoint()
        assert result is None
        release.set()
    assert result == b"raw"
    assert closed
