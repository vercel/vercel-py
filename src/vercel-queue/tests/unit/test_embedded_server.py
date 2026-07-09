from __future__ import annotations

from typing import Any

import base64
import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from uuid import uuid4

import pytest

from vercel.queue import (
    BadRequestError,
    ByteBufferTransport,
    Delivery,
    Message,
    MessageLockedError,
    ProtocolError,
    QueueClient,
    SanitizedName,
    TextBufferTransport,
    Topic,
)
from vercel.queue._internal.constants import (
    CLOUD_EVENT_HEADER_VQS_DELIVERY_COUNT,
    CLOUD_EVENT_HEADER_VQS_MESSAGE_ID,
    CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE,
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_MULTIPART_MIXED,
    CONTENT_TYPE_NDJSON,
    CONTENT_TYPE_TEXT,
    HEADER_ACCEPT,
    VQS_HEADER_DELAY_SECONDS,
    VQS_HEADER_DEPLOYMENT_ID,
    VQS_HEADER_MAX_MESSAGES,
    VQS_HEADER_MESSAGE_ID,
    VQS_HEADER_RETENTION_SECONDS,
    VQS_HEADER_VISIBILITY_TIMEOUT_SECONDS,
)
from vercel.queue._internal.embedded import PAYLOAD_SPILL_THRESHOLD_BYTES
from vercel.queue._internal.types import MessageMetadata
from vercel.queue.devserver import EmbeddedQueueDevServer

from .helpers import collect_messages, payloads


def _queue_debug_events(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    return [
        json.loads(record.message) for record in caplog.records if record.name == "vercel.queue"
    ]


@pytest.mark.anyio
async def test_async_send_poll_metadata_and_ack_lifecycle(
    eqs: EmbeddedQueueDevServer,
) -> None:

    message_id = await eqs.client.send("emails", {"subject": "hi"}, retention=60)
    deliveries: list[Delivery[Any]] = []
    delivery: Delivery[Any]
    async for delivery in eqs.client.poll("emails", "test-group", lease_duration=15):
        deliveries.append(delivery)  # noqa: PERF401

    assert message_id == "msg_1"
    assert len(deliveries) == 1
    message = deliveries[0].message
    assert message.metadata.expires_at == datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)
    assert message.metadata.receipt_handle in eqs.state.by_receipt

    await eqs.client.acknowledge(message)
    assert eqs.state.by_id["msg_1"].acknowledged
    assert message.metadata.receipt_handle not in eqs.state.by_receipt
    empty_messages: list[Message[Any]] = await collect_messages(
        eqs.client.poll("emails", "test-group")
    )
    assert empty_messages == []


@pytest.mark.anyio
async def test_async_acknowledgement_is_scoped_to_consumer_group(
    eqs: EmbeddedQueueDevServer,
) -> None:

    await eqs.client.send("fanout", {"subject": "hi"})
    first_group_deliveries: list[Delivery[Any]] = []
    delivery: Delivery[Any]
    async for delivery in eqs.client.poll("fanout", "test-group-a"):
        first_group_deliveries.append(delivery)  # noqa: PERF401
    first_group = [delivery.message for delivery in first_group_deliveries]
    await eqs.client.acknowledge(first_group[0])

    second_group: list[Message[Any]] = await collect_messages(
        eqs.client.poll("fanout", "test-group-b")
    )

    assert payloads(second_group) == [{"subject": "hi"}]
    assert second_group[0].metadata.delivery_count == 1
    assert eqs.state.by_id["msg_1"].acknowledged_for("test-group-a")
    assert not eqs.state.by_id["msg_1"].acknowledged_for("test-group-b")


@pytest.mark.anyio
async def test_async_send_response_includes_message_id_header(
    eqs: EmbeddedQueueDevServer,
) -> None:
    response = await eqs.http.post("/api/v3/topic/emails", headers={}, content=b"{}")

    assert response.status_code == 201
    assert response.json() == {"messageId": "msg_1"}
    assert response.headers[VQS_HEADER_MESSAGE_ID] == "msg_1"


@pytest.mark.anyio
async def test_async_send_rejects_invalid_integer_headers(
    eqs: EmbeddedQueueDevServer,
) -> None:
    float_response = await eqs.http.post(
        "/api/v3/topic/emails",
        headers={VQS_HEADER_RETENTION_SECONDS: "60.5"},
        content=b"{}",
    )
    short_retention_response = await eqs.http.post(
        "/api/v3/topic/emails",
        headers={VQS_HEADER_RETENTION_SECONDS: "59"},
        content=b"{}",
    )
    long_retention_response = await eqs.http.post(
        "/api/v3/topic/emails",
        headers={VQS_HEADER_RETENTION_SECONDS: "604801"},
        content=b"{}",
    )
    excessive_delay_response = await eqs.http.post(
        "/api/v3/topic/emails",
        headers={
            VQS_HEADER_RETENTION_SECONDS: "60",
            VQS_HEADER_DELAY_SECONDS: "61",
        },
        content=b"{}",
    )

    assert float_response.status_code == 400
    assert short_retention_response.status_code == 400
    assert long_retention_response.status_code == 400
    assert excessive_delay_response.status_code == 400


@pytest.mark.anyio
async def test_async_receive_requires_supported_accept(
    eqs: EmbeddedQueueDevServer,
) -> None:
    await eqs.http.post("/api/v3/topic/emails", headers={}, content=b"{}")
    missing_poll_accept = await eqs.http.post(
        "/api/v3/topic/emails/consumer/test-group", headers={}
    )
    unsupported_poll_accept = await eqs.http.post(
        "/api/v3/topic/emails/consumer/test-group",
        headers={HEADER_ACCEPT: "application/json"},
    )
    missing_id_accept = await eqs.http.post(
        "/api/v3/topic/emails/consumer/test-group/id/msg_1", headers={}
    )
    supported_accept = await eqs.http.post(
        "/api/v3/topic/emails/consumer/test-group/id/msg_1",
        headers={HEADER_ACCEPT: CONTENT_TYPE_MULTIPART_MIXED},
    )

    assert missing_poll_accept.status_code == 400
    assert unsupported_poll_accept.status_code == 400
    assert missing_id_accept.status_code == 400
    assert supported_accept.status_code == 200


@pytest.mark.anyio
async def test_async_receive_returns_ndjson(
    eqs: EmbeddedQueueDevServer,
) -> None:
    await eqs.http.post(
        "/api/v3/topic/emails",
        headers={"Content-Type": CONTENT_TYPE_TEXT},
        content=b"hello",
    )
    response = await eqs.http.post(
        "/api/v3/topic/emails/consumer/test-group",
        headers={HEADER_ACCEPT: CONTENT_TYPE_NDJSON},
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == CONTENT_TYPE_NDJSON
    lines = response.text.strip().split("\n")
    assert len(lines) == 1
    body = json.loads(lines[0])
    assert body == {
        "messageId": "msg_1",
        "receiptHandle": body["receiptHandle"],
        "deliveryCount": 1,
        "timestamp": "2026-01-01T00:00:00Z",
        "expiresAt": "2026-01-02T00:00:00Z",
        "contentType": CONTENT_TYPE_TEXT,
        "body": base64.b64encode(b"hello").decode("ascii"),
    }
    assert body["receiptHandle"]


@pytest.mark.anyio
async def test_async_receive_by_id_returns_ndjson(
    eqs: EmbeddedQueueDevServer,
) -> None:
    await eqs.http.post("/api/v3/topic/emails", headers={}, content=b"{}")
    response = await eqs.http.post(
        "/api/v3/topic/emails/consumer/test-group/id/msg_1",
        headers={HEADER_ACCEPT: CONTENT_TYPE_NDJSON},
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == CONTENT_TYPE_NDJSON
    [message] = [json.loads(line) for line in response.text.strip().split("\n")]
    assert message["messageId"] == "msg_1"
    assert message["receiptHandle"]
    assert message["deliveryCount"] == 1
    assert message["body"] == base64.b64encode(b"{}").decode("ascii")


@pytest.mark.anyio
async def test_async_raw_http_receive_uses_protocol_default_visibility_timeout(
    eqs: EmbeddedQueueDevServer,
) -> None:
    await eqs.http.post("/api/v3/topic/emails", headers={}, content=b"{}")
    receive_response = await eqs.http.post(
        "/api/v3/topic/emails/consumer/test-group",
        headers={HEADER_ACCEPT: CONTENT_TYPE_NDJSON},
    )
    await eqs.http.post("/api/v3/topic/emails", headers={}, content=b"{}")
    by_id_response = await eqs.http.post(
        "/api/v3/topic/emails/consumer/test-group/id/msg_2",
        headers={HEADER_ACCEPT: CONTENT_TYPE_NDJSON},
    )

    assert receive_response.status_code == 200
    assert by_id_response.status_code == 200
    assert eqs.state.by_id["msg_1"].lease_deadline_by_consumer[
        "test-group"
    ] == eqs.state.now + timedelta(seconds=60)
    assert eqs.state.by_id["msg_2"].lease_deadline_by_consumer[
        "test-group"
    ] == eqs.state.now + timedelta(seconds=60)


@pytest.mark.anyio
async def test_async_receive_ndjson_empty_queue_returns_204(
    eqs: EmbeddedQueueDevServer,
) -> None:
    response = await eqs.http.post(
        "/api/v3/topic/emails/consumer/test-group",
        headers={HEADER_ACCEPT: CONTENT_TYPE_NDJSON},
    )

    assert response.status_code == 204
    assert not response.text


@pytest.mark.anyio
async def test_debug_logs_empty_receive(
    eqs: EmbeddedQueueDevServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level(logging.INFO, logger="vercel.queue")

    messages: list[Message[Any]] = await collect_messages(eqs.client.poll("emails", "test-group"))

    assert messages == []
    assert any(
        event["event"] == "receive.empty"
        and event["topic"] == "emails"
        and event["consumer_group"] == "test-group"
        and event["status_code"] == 204
        for event in _queue_debug_events(caplog)
    )


@pytest.mark.anyio
async def test_async_receive_prefers_multipart_when_accept_allows_both(
    eqs: EmbeddedQueueDevServer,
) -> None:
    await eqs.http.post("/api/v3/topic/emails", headers={}, content=b"{}")
    response = await eqs.http.post(
        "/api/v3/topic/emails/consumer/test-group",
        headers={HEADER_ACCEPT: f"{CONTENT_TYPE_NDJSON}, {CONTENT_TYPE_MULTIPART_MIXED}"},
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith(CONTENT_TYPE_MULTIPART_MIXED)


@pytest.mark.anyio
async def test_async_large_payload_spills_to_disk_and_round_trips_ndjson(
    eqs: EmbeddedQueueDevServer,
) -> None:
    payload = b"a" * (PAYLOAD_SPILL_THRESHOLD_BYTES + 1)

    send_response = await eqs.http.post(
        "/api/v3/topic/large-payloads",
        headers={"Content-Type": CONTENT_TYPE_TEXT},
        content=payload,
    )

    assert send_response.status_code == 201
    stored_message = eqs.state.by_id["msg_1"]
    assert stored_message.payload.spilled is True
    assert stored_message.payload.path is not None
    spill_path = stored_message.payload.path
    assert spill_path.exists()

    receive_response = await eqs.http.post(
        "/api/v3/topic/large-payloads/consumer/test-group",
        headers={HEADER_ACCEPT: CONTENT_TYPE_NDJSON},
    )

    assert receive_response.status_code == 200
    [message] = [json.loads(line) for line in receive_response.text.strip().split("\n")]
    assert message["messageId"] == "msg_1"
    assert base64.b64decode(message["body"]) == payload

    eqs.reset()
    assert not spill_path.exists()


@pytest.mark.anyio
async def test_async_receive_rejects_invalid_limit_and_visibility_headers(
    eqs: EmbeddedQueueDevServer,
) -> None:
    await eqs.http.post("/api/v3/topic/emails", headers={}, content=b"{}")
    invalid_limit = await eqs.http.post(
        "/api/v3/topic/emails/consumer/test-group",
        headers={
            HEADER_ACCEPT: CONTENT_TYPE_MULTIPART_MIXED,
            VQS_HEADER_MAX_MESSAGES: "11",
        },
    )
    invalid_visibility = await eqs.http.post(
        "/api/v3/topic/emails/consumer/test-group",
        headers={
            HEADER_ACCEPT: CONTENT_TYPE_MULTIPART_MIXED,
            VQS_HEADER_VISIBILITY_TIMEOUT_SECONDS: "3601",
        },
    )
    float_visibility = await eqs.http.post(
        "/api/v3/topic/emails/consumer/test-group/id/msg_1",
        headers={
            HEADER_ACCEPT: CONTENT_TYPE_MULTIPART_MIXED,
            VQS_HEADER_VISIBILITY_TIMEOUT_SECONDS: "1.5",
        },
    )
    infinite_limit = await eqs.http.post(
        "/api/v3/topic/emails/consumer/test-group",
        headers={
            HEADER_ACCEPT: CONTENT_TYPE_MULTIPART_MIXED,
            VQS_HEADER_MAX_MESSAGES: "Infinity",
        },
    )
    scientific_integer_limit = await eqs.http.post(
        "/api/v3/topic/emails/consumer/other-test-group",
        headers={
            HEADER_ACCEPT: CONTENT_TYPE_NDJSON,
            VQS_HEADER_MAX_MESSAGES: "1e1",
        },
    )
    trailing_decimal_limit = await eqs.http.post(
        "/api/v3/topic/emails/consumer/yet-other-test-group",
        headers={
            HEADER_ACCEPT: CONTENT_TYPE_NDJSON,
            VQS_HEADER_MAX_MESSAGES: "5.",
        },
    )

    assert invalid_limit.status_code == 400
    assert invalid_visibility.status_code == 400
    assert float_visibility.status_code == 400
    assert infinite_limit.status_code == 400
    assert scientific_integer_limit.status_code == 200
    assert trailing_decimal_limit.status_code == 200


@pytest.mark.anyio
async def test_async_payload_preservation_for_json_text_empty_and_binary(
    eqs: EmbeddedQueueDevServer,
) -> None:

    binary = uuid4().bytes

    await eqs.client.send("payloads", {"kind": "json"})
    await eqs.client.send(Topic[str]("payloads", transport=TextBufferTransport()), "cafe")
    await eqs.client.send(Topic[bytes]("payloads", transport=ByteBufferTransport()), b"")
    await eqs.client.send(Topic[bytes]("payloads", transport=ByteBufferTransport()), binary)

    json_messages: list[Message[Any]] = await collect_messages(
        eqs.client.poll("payloads", "json", limit=1)
    )
    text_messages: list[Message[str]] = await collect_messages(
        eqs.client.poll(
            Topic[str]("payloads", transport=TextBufferTransport()),
            "text",
            limit=2,
        )
    )
    byte_messages: list[Message[bytes]] = await collect_messages(
        eqs.client.poll(
            Topic[bytes]("payloads", transport=ByteBufferTransport()),
            "bytes",
            limit=4,
        )
    )

    assert json_messages[0].payload == {"kind": "json"}
    assert text_messages[1].payload == "cafe"
    assert payloads(byte_messages) == [
        b'{"kind": "json"}',
        b"cafe",
        b"",
        binary,
    ]
    assert text_messages[1].metadata.content_type == CONTENT_TYPE_TEXT


@pytest.mark.anyio
async def test_async_multiple_messages_limit_and_empty_poll(
    eqs: EmbeddedQueueDevServer,
) -> None:

    for index in range(3):
        await eqs.client.send("batch", {"index": index})

    first_two: list[Message[Any]] = await collect_messages(
        eqs.client.poll("batch", "test-group", limit=2)
    )
    empty_while_locked: list[Message[Any]] = await collect_messages(
        eqs.client.poll("batch", "test-group", limit=3)
    )

    assert payloads(first_two) == [{"index": 0}, {"index": 1}]
    assert [message.metadata.message_id for message in first_two] == ["msg_1", "msg_2"]
    assert payloads(empty_while_locked) == [{"index": 2}]
    missing_messages: list[Message[Any]] = await collect_messages(
        eqs.client.poll("missing", "test-group")
    )
    assert missing_messages == []


@pytest.mark.anyio
async def test_async_receive_by_id_duplicate_response_includes_original_message_id(
    eqs: EmbeddedQueueDevServer,
) -> None:

    original_id = await eqs.client.send("ids", {"value": "original"}, idempotency_key="same")
    duplicate_id = await eqs.client.send("ids", {"value": "original"}, idempotency_key="same")
    assert original_id is not None
    assert duplicate_id is not None

    duplicate_response = await eqs.http.post(
        f"/api/v3/topic/ids/consumer/other-test-group/id/{duplicate_id}",
        headers={HEADER_ACCEPT: CONTENT_TYPE_MULTIPART_MIXED},
    )
    assert duplicate_response.status_code == 409
    assert duplicate_response.json() == {
        "error": "This messageId was a duplicate - use originalMessageId instead",
        "originalMessageId": original_id,
    }


@pytest.mark.anyio
async def test_async_receive_by_id_duplicate_redirect_loop_is_rejected(
    eqs: EmbeddedQueueDevServer,
) -> None:

    await eqs.client.send("ids", {"value": 1}, idempotency_key="same")
    await eqs.client.send("ids", {"value": 1}, idempotency_key="same")
    eqs.state.by_id["msg_1"].duplicate_of = "msg_2"

    with pytest.raises(ProtocolError, match="duplicate redirect loop"):
        await eqs.client._poll_by_id("ids", "test-group", "msg_2")


@pytest.mark.anyio
async def test_async_lease_extension_redelivery_and_ack_failures(
    eqs: EmbeddedQueueDevServer,
) -> None:

    await eqs.client.send("leases", {"task": 1})
    first = (await _poll(eqs.client, "leases", "test-group", lease_duration=10))[0]

    await eqs.client.extend_lease(first, 20)
    eqs.state.shift(10)
    assert await _poll(eqs.client, "leases", "test-group") == []

    success_response = await eqs.http.patch(
        f"/api/v3/topic/leases/consumer/test-group/lease/{first.metadata.receipt_handle}",
        headers={},
        json={"visibilityTimeoutSeconds": 20},
    )
    assert success_response.status_code == 200
    assert success_response.json() == {"success": True}

    await eqs.client.extend_lease(first, 0)
    redelivered = (await _poll(eqs.client, "leases", "test-group"))[0]
    assert redelivered.metadata.message_id == first.metadata.message_id
    assert redelivered.metadata.delivery_count == 2

    with pytest.raises(MessageLockedError):
        await eqs.client.acknowledge(first)

    metadata = redelivered.metadata
    other_consumer = MessageMetadata(
        message_id=metadata.message_id,
        delivery_count=metadata.delivery_count,
        created_at=metadata.created_at,
        expires_at=metadata.expires_at,
        topic=metadata.topic,
        consumer_group=SanitizedName("wrong-test-group"),
        receipt_handle=metadata.receipt_handle,
        content_type=metadata.content_type,
    )
    with pytest.raises(MessageLockedError):
        await eqs.client.acknowledge(other_consumer)

    eqs.state.shift(301)
    with pytest.raises(MessageLockedError):
        await eqs.client.acknowledge(redelivered)


@pytest.mark.anyio
async def test_async_stale_receipt_handle_returns_conflict(
    eqs: EmbeddedQueueDevServer,
) -> None:

    await eqs.client.send("leases", {"task": 1})
    first = (await _poll(eqs.client, "leases", "test-group", lease_duration=1))[0]
    eqs.state.shift(2)
    second = (await _poll(eqs.client, "leases", "test-group", lease_duration=10))[0]

    assert first.metadata.receipt_handle not in eqs.state.by_receipt
    assert second.metadata.receipt_handle in eqs.state.by_receipt

    response = await eqs.http.delete(
        f"/api/v3/topic/leases/consumer/test-group/lease/{first.metadata.receipt_handle}",
        headers={},
    )

    assert second.metadata.delivery_count == 2
    assert response.status_code == 409
    assert response.json() == {"error": "Message is not currently in-flight"}


@pytest.mark.anyio
async def test_async_lease_extension_past_expiration_is_rejected(
    eqs: EmbeddedQueueDevServer,
) -> None:

    await eqs.client.send("leases", {"task": 1}, retention=60)
    message = (await _poll(eqs.client, "leases", "test-group", lease_duration=58))[0]
    eqs.state.shift(55)

    with pytest.raises(BadRequestError, match="expiration"):
        await eqs.client.extend_lease(message, 10)


@pytest.mark.anyio
async def test_async_visibility_route_alias_and_expiration_error_body(
    eqs: EmbeddedQueueDevServer,
) -> None:

    await eqs.client.send("leases", {"task": 1}, retention=60)
    message = (await _poll(eqs.client, "leases", "test-group", lease_duration=30))[0]
    receipt_handle = message.metadata.receipt_handle
    assert receipt_handle is not None

    success_response = await eqs.http.patch(
        f"/api/v3/topic/leases/consumer/test-group/lease/{receipt_handle}/visibility",
        headers={},
        json={"visibilityTimeoutSeconds": 58},
    )
    assert success_response.status_code == 200
    assert success_response.json() == {"success": True}

    eqs.state.shift(55)

    error_response = await eqs.http.patch(
        f"/api/v3/topic/leases/consumer/test-group/lease/{receipt_handle}/visibility",
        headers={},
        json={"visibilityTimeoutSeconds": 10},
    )
    assert error_response.status_code == 400
    assert error_response.json() == {
        "error": "Visibility timeout cannot extend beyond message expiration",
        "messageExpiresAt": "2026-01-01T00:01:00Z",
        "requestedExpiresAt": "2026-01-01T00:01:05Z",
    }


@pytest.mark.anyio
async def test_async_receive_by_id_and_lease_followups_are_deployment_scoped(
    eqs: EmbeddedQueueDevServer,
) -> None:
    partition_client = eqs.get_async_client(
        token="token",
        base_url="http://vqs.test",
        deployment="dpl_a",
    )

    message_id = await partition_client.send("partitions", {"partition": "a"})
    assert message_id is not None

    wrong_receive_response = await eqs.http.post(
        f"/api/v3/topic/partitions/consumer/test-group/id/{message_id}",
        headers={
            HEADER_ACCEPT: CONTENT_TYPE_MULTIPART_MIXED,
            VQS_HEADER_DEPLOYMENT_ID: "dpl_b",
        },
    )
    assert wrong_receive_response.status_code == 404

    message: Message[Any] = await partition_client._poll_by_id(
        "partitions", "test-group", message_id
    )
    receipt_handle = message.metadata.receipt_handle
    assert receipt_handle is not None

    wrong_extend_response = await eqs.http.patch(
        f"/api/v3/topic/partitions/consumer/test-group/lease/{receipt_handle}",
        headers={VQS_HEADER_DEPLOYMENT_ID: "dpl_b"},
        json={"visibilityTimeoutSeconds": 30},
    )
    wrong_ack_response = await eqs.http.delete(
        f"/api/v3/topic/partitions/consumer/test-group/lease/{receipt_handle}",
        headers={VQS_HEADER_DEPLOYMENT_ID: "dpl_b"},
    )
    assert wrong_extend_response.status_code == 404
    assert wrong_ack_response.status_code == 404

    await partition_client.acknowledge(message)


@pytest.mark.anyio
async def test_async_lease_extension_requires_integer_visibility_timeout(
    eqs: EmbeddedQueueDevServer,
) -> None:

    await eqs.client.send("leases", {"task": 1})
    message = (await _poll(eqs.client, "leases", "test-group", lease_duration=30))[0]
    receipt_handle = message.metadata.receipt_handle
    assert receipt_handle is not None

    for payload in (
        {},
        {"visibilityTimeoutSeconds": 30.5},
        {"visibilityTimeoutSeconds": "30"},
        {"visibilityTimeoutSeconds": -1},
    ):
        response = await eqs.http.patch(
            f"/api/v3/topic/leases/consumer/test-group/lease/{receipt_handle}",
            headers={},
            json=payload,
        )
        assert response.status_code == 400


@pytest.mark.anyio
async def test_async_acknowledged_message_lease_routes_return_not_found(
    eqs: EmbeddedQueueDevServer,
) -> None:

    await eqs.client.send("leases", {"task": 1})
    message = (await _poll(eqs.client, "leases", "test-group", lease_duration=30))[0]
    await eqs.client.acknowledge(message)

    with pytest.raises(MessageLockedError):
        await eqs.client.acknowledge(message)
    with pytest.raises(MessageLockedError):
        await eqs.client.extend_lease(message, 30)


@pytest.mark.anyio
async def test_async_deployment_partition_all_deployments_delay_retention_and_idempotency(
    eqs: EmbeddedQueueDevServer,
) -> None:
    partition_client = eqs.get_async_client(
        token="token",
        base_url="http://vqs.test",
        deployment="dpl_a",
    )

    await partition_client.send("partitions", {"partition": "a"})
    await eqs.client.send("partitions", {"partition": "all"})
    await eqs.client.send("partitions", {"delay": True}, delay=65)
    await eqs.client.send("partitions", {"short": True}, retention=60)
    duplicate_original = await eqs.client.send("partitions", {"dedupe": True}, idempotency_key="k")
    duplicate = await eqs.client.send("partitions", {"dedupe": True}, idempotency_key="k")

    assert eqs.state.next_expires_at == datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)

    partition_messages: list[Any] = payloads(
        await collect_messages(partition_client.poll("partitions", "test-group", limit=10))
    )
    visible_to_all: list[Any] = payloads(
        await collect_messages(eqs.client.poll("partitions", "all", limit=10))
    )
    assert partition_messages == [{"partition": "a"}]
    assert visible_to_all == [
        {"partition": "a"},
        {"partition": "all"},
        {"short": True},
        {"dedupe": True},
    ]
    assert duplicate_original == "msg_5"
    assert duplicate == "msg_6"

    eqs.state.shift(61)
    late_messages: list[Any] = payloads(
        await collect_messages(eqs.client.poll("partitions", "late", limit=10))
    )
    assert "msg_4" not in eqs.state.by_id
    assert eqs.state.next_expires_at == datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert late_messages == [
        {"partition": "a"},
        {"partition": "all"},
        {"dedupe": True},
    ]
    eqs.state.shift(4)
    delayed_messages: list[Any] = payloads(
        await collect_messages(eqs.client.poll("partitions", "delayed", limit=10))
    )
    assert delayed_messages == [
        {"partition": "a"},
        {"partition": "all"},
        {"delay": True},
        {"dedupe": True},
    ]

    deliveries = list(eqs.iter_push_deliveries("partitions", "push-test-group", deployment="dpl_a"))
    assert [delivery.body for delivery in deliveries] == [b'{"partition": "a"}']
    assert list(eqs.iter_push_deliveries("partitions", "push-test-group", deployment="dpl_a")) == []


@pytest.mark.anyio
async def test_async_custom_base_url_path_prefix_and_encoded_receipt_handle_are_preserved(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_async_client(base_url="http://vqs.test/proxy")

    await client.send("paths", {"ok": True})
    message = (await _poll(client, "paths", "test-group"))[0]
    assert message.metadata.content_type == CONTENT_TYPE_JSON
    await client.extend_lease(message, timedelta(seconds=1))
    await client.acknowledge(message)


def test_push_delivery_suppresses_idempotent_duplicates(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()

    original_id = client.send("push", {"dedupe": True}, idempotency_key="same")
    duplicate_id = client.send("push", {"dedupe": True}, idempotency_key="same")
    assert original_id is not None
    assert duplicate_id is not None

    deliveries = list(eqs.iter_push_deliveries("push", "test-group"))

    assert [delivery.headers[CLOUD_EVENT_HEADER_VQS_MESSAGE_ID] for delivery in deliveries] == [
        original_id
    ]
    assert [json.loads(delivery.body) for delivery in deliveries] == [{"dedupe": True}]
    assert eqs.state.by_id[duplicate_id].duplicate_of == original_id


def test_push_delivery_without_idempotency_delivers_duplicate_payloads(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()

    first_id = client.send("push", {"dedupe": False})
    second_id = client.send("push", {"dedupe": False})
    assert first_id is not None
    assert second_id is not None

    deliveries = list(eqs.iter_push_deliveries("push", "test-group"))

    assert [delivery.headers[CLOUD_EVENT_HEADER_VQS_MESSAGE_ID] for delivery in deliveries] == [
        first_id,
        second_id,
    ]
    assert [json.loads(delivery.body) for delivery in deliveries] == [
        {"dedupe": False},
        {"dedupe": False},
    ]


def test_push_delivery_fans_out_by_consumer_and_blocks_leased_redelivery(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()

    message_id = client.send("fanout", {"ok": True})
    assert message_id is not None

    [first_delivery] = list(
        eqs.iter_push_deliveries(
            "fanout",
            "test-group-a",
            lease_seconds=30,
        )
    )
    blocked_same_consumer = list(eqs.iter_push_deliveries("fanout", "test-group-a"))
    [other_consumer_delivery] = list(eqs.iter_push_deliveries("fanout", "test-group-b"))

    assert first_delivery.headers[CLOUD_EVENT_HEADER_VQS_MESSAGE_ID] == message_id
    assert blocked_same_consumer == []
    assert other_consumer_delivery.headers[CLOUD_EVENT_HEADER_VQS_MESSAGE_ID] == message_id
    assert other_consumer_delivery.headers[CLOUD_EVENT_HEADER_VQS_DELIVERY_COUNT] == "1"
    assert eqs.state.by_id[message_id].lease_deadline_by_consumer[
        "test-group-b"
    ] == eqs.state.now + timedelta(seconds=300)

    eqs.state.shift(29)
    assert list(eqs.iter_push_deliveries("fanout", "test-group-a")) == []

    eqs.state.shift(1)
    [redelivery] = list(eqs.iter_push_deliveries("fanout", "test-group-a"))
    assert redelivery.headers[CLOUD_EVENT_HEADER_VQS_MESSAGE_ID] == message_id
    assert redelivery.headers[CLOUD_EVENT_HEADER_VQS_DELIVERY_COUNT] == "2"


def test_push_delivery_does_not_rescan_acked_messages(
    eqs: EmbeddedQueueDevServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for index in range(10_000):
        eqs.app.server.put(
            "indexed",
            json.dumps({"index": index}).encode(),
            {"Content-Type": CONTENT_TYPE_JSON},
        )

    for delivery in eqs.iter_push_deliveries("indexed", "test-group", lease_seconds=60):
        message = eqs.state.by_id[delivery.headers[CLOUD_EVENT_HEADER_VQS_MESSAGE_ID]]
        message.acknowledge_for("test-group")
        eqs.state.by_receipt.pop(delivery.headers[CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE], None)

    later_message = eqs.app.server.put(
        "indexed",
        json.dumps({"index": 10_000}).encode(),
        {"Content-Type": CONTENT_TYPE_JSON},
    )
    can_deliver_calls = 0
    original_can_deliver = eqs.app.server._can_deliver

    def track_can_deliver(*args: Any, **kwargs: Any) -> bool:
        nonlocal can_deliver_calls
        can_deliver_calls += 1
        return original_can_deliver(*args, **kwargs)

    monkeypatch.setattr(eqs.app.server, "_can_deliver", track_can_deliver)

    [delivery] = list(eqs.iter_push_deliveries("indexed", "test-group", lease_seconds=60))

    assert delivery.headers[CLOUD_EVENT_HEADER_VQS_MESSAGE_ID] == later_message.message_id
    assert can_deliver_calls == 0


@pytest.mark.anyio
async def test_push_delivery_requeues_visibility_update_at_new_deadline(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    message_id = client.send("visibility", {"ok": True})
    assert message_id is not None
    [delivery] = list(eqs.iter_push_deliveries("visibility", "test-group", lease_seconds=30))
    receipt_handle = quote(delivery.headers[CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE], safe="")

    response = await eqs.http.patch(
        f"/api/v3/topic/visibility/consumer/test-group/lease/{receipt_handle}",
        headers={},
        json={"visibilityTimeoutSeconds": 0},
    )

    assert response.status_code == 200
    [redelivery] = list(eqs.iter_push_deliveries("visibility", "test-group"))
    assert redelivery.headers[CLOUD_EVENT_HEADER_VQS_MESSAGE_ID] == message_id
    assert redelivery.headers[CLOUD_EVENT_HEADER_VQS_DELIVERY_COUNT] == "2"


def test_push_delivery_expiration_cleanup_invalidates_index(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    expiring_id = client.send("expire", {"old": True}, retention=60)
    assert expiring_id is not None
    eqs.state.shift(60)
    live_id = client.send("expire", {"new": True}, retention=60)
    assert live_id is not None

    [delivery] = list(eqs.iter_push_deliveries("expire", "test-group"))

    assert delivery.headers[CLOUD_EVENT_HEADER_VQS_MESSAGE_ID] == live_id
    assert expiring_id not in eqs.state.by_id


async def _poll(
    client: QueueClient,
    topic: str,
    consumer: str,
    *,
    lease_duration: int | None = None,
) -> list[Message[Any]]:
    stream: AsyncIterator[Delivery[Any]] = client.poll(
        topic,
        consumer,
        lease_duration=lease_duration,
    )
    return [delivery.message async for delivery in stream]
