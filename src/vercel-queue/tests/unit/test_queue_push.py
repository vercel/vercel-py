from __future__ import annotations

from typing import Any, ClassVar, cast

import json
import logging
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from vercel.queue import (
    ALL_DEPLOYMENTS,
    ByteBufferTransport,
    Message,
    ProtocolError,
    QueueClient,
    RawJsonTransport,
)
from vercel.queue._internal.constants import (
    CLOUD_EVENT_HEADER_TYPE,
    CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP,
    CLOUD_EVENT_HEADER_VQS_CREATED_AT,
    CLOUD_EVENT_HEADER_VQS_DELIVERY_COUNT,
    CLOUD_EVENT_HEADER_VQS_EXPIRES_AT,
    CLOUD_EVENT_HEADER_VQS_MESSAGE_ID,
    CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE,
    CLOUD_EVENT_HEADER_VQS_REGION,
    CLOUD_EVENT_HEADER_VQS_TOPIC,
    CLOUD_EVENT_HEADER_VQS_VISIBILITY_DEADLINE,
    CLOUD_EVENT_TYPE_V2BETA,
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_OCTET_STREAM,
    HEADER_CONTENT_TYPE,
)
from vercel.queue.devserver import EmbeddedQueueDevServer
from vercel.queue.sync import QueueClient as SyncQueueClient

from .helpers import (
    CREATED_AT,
    CREATED_AT_DT,
    EXPIRES_AT,
    EXPIRES_AT_DT,
    callback_headers,
)


def _queue_debug_events(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    return [
        json.loads(record.message) for record in caplog.records if record.name == "vercel.queue"
    ]


def test_v1_push_delivery_is_rejected() -> None:
    body = json.dumps({"type": "com.vercel.queue.v1beta", "data": {}}).encode()

    with pytest.raises(ValueError, match=r"com\.vercel\.queue\.v2beta"):
        SyncQueueClient(token="token", deployment=ALL_DEPLOYMENTS)._accept_impl(
            body,
            {CLOUD_EVENT_HEADER_TYPE: "com.vercel.queue.v1beta"},
            transport=RawJsonTransport[dict[str, bool]](),
        )


def test_accept_parses_json_push_delivery() -> None:
    body = b'{"ok": true}'
    visibility_deadline = "2026-01-01T00:05:00Z"
    headers = {
        CLOUD_EVENT_HEADER_TYPE: CLOUD_EVENT_TYPE_V2BETA,
        CLOUD_EVENT_HEADER_VQS_TOPIC: "emails",
        CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP: "test-group",
        CLOUD_EVENT_HEADER_VQS_MESSAGE_ID: "msg_1",
        CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE: "rh_1",
        CLOUD_EVENT_HEADER_VQS_DELIVERY_COUNT: "3",
        CLOUD_EVENT_HEADER_VQS_CREATED_AT: CREATED_AT,
        CLOUD_EVENT_HEADER_VQS_EXPIRES_AT: EXPIRES_AT,
        CLOUD_EVENT_HEADER_VQS_REGION: "sfo1",
        CLOUD_EVENT_HEADER_VQS_VISIBILITY_DEADLINE: visibility_deadline,
        HEADER_CONTENT_TYPE: CONTENT_TYPE_JSON,
    }
    message = SyncQueueClient(token="token", deployment=ALL_DEPLOYMENTS)._accept_impl(
        body,
        headers,
        transport=RawJsonTransport[dict[str, bool]](),
    )
    assert message.payload == {"ok": True}
    assert message.metadata.delivery_count == 3
    assert message.metadata.created_at == CREATED_AT_DT
    assert message.metadata.expires_at == EXPIRES_AT_DT
    assert message.metadata.region == "sfo1"
    assert message.metadata.visibility_deadline == datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc)


@pytest.mark.anyio
async def test_debug_logs_push_metadata_with_redacted_sensitive_values(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level(logging.INFO, logger="vercel.queue")
    receipt_handle = "s.msg_1.secret-ticket"
    headers = {
        CLOUD_EVENT_HEADER_TYPE: CLOUD_EVENT_TYPE_V2BETA,
        CLOUD_EVENT_HEADER_VQS_TOPIC: "emails",
        CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP: "test-group",
        CLOUD_EVENT_HEADER_VQS_MESSAGE_ID: "msg_1",
        CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE: receipt_handle,
        CLOUD_EVENT_HEADER_VQS_DELIVERY_COUNT: "3",
        CLOUD_EVENT_HEADER_VQS_CREATED_AT: CREATED_AT,
        CLOUD_EVENT_HEADER_VQS_EXPIRES_AT: EXPIRES_AT,
        CLOUD_EVENT_HEADER_VQS_REGION: "sfo1",
        CLOUD_EVENT_HEADER_VQS_VISIBILITY_DEADLINE: "2026-01-01T00:05:00Z",
        HEADER_CONTENT_TYPE: CONTENT_TYPE_JSON,
        "authorization": "Bearer secret-token",
        "x-vercel-oidc-token": "oidc-token",
    }

    parsed = await QueueClient(token="token", deployment=ALL_DEPLOYMENTS)._accept_impl(
        b'{"ok": true}',
        headers,
        transport=RawJsonTransport[dict[str, bool]](),
    )

    assert parsed.metadata.receipt_handle == receipt_handle
    events = _queue_debug_events(caplog)
    metadata_events = [event for event in events if event["event"] == "push.delivery_metadata"]
    assert len(metadata_events) == 1
    metadata = cast("dict[str, object]", metadata_events[0]["metadata"])
    assert metadata["message_id"] == "msg_1"
    assert metadata["topic"] == "emails"
    assert metadata["consumer_group"] == "test-group"
    assert metadata["receipt_handle"] == "rh_1_REDACTED"

    text = caplog.text
    assert receipt_handle not in text
    assert "secret-token" not in text
    assert "oidc-token" not in text
    assert "7b226f6b223a20747275657d" not in text


def test_accept_parses_mixed_case_push_delivery_headers() -> None:
    body = b'{"ok": true}'
    headers = {
        "Ce-Type": CLOUD_EVENT_TYPE_V2BETA,
        "Ce-Vqsqueuename": "emails",
        "Ce-Vqsconsumergroup": "test-group",
        "Ce-Vqsmessageid": "msg_1",
        "Ce-Vqsreceipthandle": "rh_1",
        "Ce-Vqsdeliverycount": "3",
        "Ce-Vqscreatedat": CREATED_AT,
        "Ce-Vqsexpiresat": EXPIRES_AT,
        "Ce-Vqsregion": "sfo1",
        "Content-Type": CONTENT_TYPE_JSON,
    }

    message = SyncQueueClient(token="token", deployment=ALL_DEPLOYMENTS)._accept_impl(
        body,
        headers,
        transport=RawJsonTransport[dict[str, bool]](),
    )

    assert message.payload == {"ok": True}
    assert message.metadata.delivery_count == 3
    assert message.metadata.expires_at == EXPIRES_AT_DT
    assert message.metadata.region == "sfo1"


def test_accept_v2beta_metadata_optional_defaults() -> None:
    headers = {
        CLOUD_EVENT_HEADER_TYPE: CLOUD_EVENT_TYPE_V2BETA,
        CLOUD_EVENT_HEADER_VQS_TOPIC: "emails",
        CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP: "test-group",
        CLOUD_EVENT_HEADER_VQS_MESSAGE_ID: "msg_1",
        CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE: "rh_1",
        CLOUD_EVENT_HEADER_VQS_DELIVERY_COUNT: "not-an-int",
        CLOUD_EVENT_HEADER_VQS_CREATED_AT: CREATED_AT,
        HEADER_CONTENT_TYPE: CONTENT_TYPE_JSON,
    }

    message = SyncQueueClient(token="token", deployment=ALL_DEPLOYMENTS)._accept_impl(
        b'{"ok": true}',
        headers,
        transport=RawJsonTransport[dict[str, bool]](),
    )

    assert message.metadata.expires_at is None
    assert message.metadata.delivery_count == 1


def test_accept_rejects_invalid_visibility_deadline() -> None:
    with pytest.raises(ValueError, match=CLOUD_EVENT_HEADER_VQS_VISIBILITY_DEADLINE):
        SyncQueueClient(token="token", deployment=ALL_DEPLOYMENTS)._accept_impl(
            b'{"ok": true}',
            {
                CLOUD_EVENT_HEADER_TYPE: CLOUD_EVENT_TYPE_V2BETA,
                CLOUD_EVENT_HEADER_VQS_TOPIC: "emails",
                CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP: "test-group",
                CLOUD_EVENT_HEADER_VQS_MESSAGE_ID: "msg_1",
                CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE: "rh_1",
                CLOUD_EVENT_HEADER_VQS_CREATED_AT: CREATED_AT,
                CLOUD_EVENT_HEADER_VQS_VISIBILITY_DEADLINE: "not-a-date",
                HEADER_CONTENT_TYPE: CONTENT_TYPE_JSON,
            },
        )


def test_accept_parses_iterable_push_delivery() -> None:
    headers = {
        CLOUD_EVENT_HEADER_TYPE: CLOUD_EVENT_TYPE_V2BETA,
        CLOUD_EVENT_HEADER_VQS_TOPIC: "emails",
        CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP: "test-group",
        CLOUD_EVENT_HEADER_VQS_MESSAGE_ID: "msg_1",
        CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE: "rh_1",
        CLOUD_EVENT_HEADER_VQS_CREATED_AT: CREATED_AT,
        HEADER_CONTENT_TYPE: CONTENT_TYPE_JSON,
    }
    message = SyncQueueClient(token="token", deployment=ALL_DEPLOYMENTS)._accept_impl(
        [b'{"ok":', b" true}"],
        headers,
        transport=RawJsonTransport[dict[str, bool]](),
    )
    assert message.payload == {"ok": True}


def test_accept_parses_http_response_push_delivery() -> None:
    class Response:
        headers: ClassVar[dict[str, str]] = {
            CLOUD_EVENT_HEADER_TYPE: CLOUD_EVENT_TYPE_V2BETA,
            CLOUD_EVENT_HEADER_VQS_TOPIC: "emails",
            CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP: "test-group",
            CLOUD_EVENT_HEADER_VQS_MESSAGE_ID: "msg_1",
            CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE: "rh_1",
            CLOUD_EVENT_HEADER_VQS_CREATED_AT: CREATED_AT,
            HEADER_CONTENT_TYPE: CONTENT_TYPE_JSON,
        }

        def iter_bytes(self, chunk_size: int | None = None) -> Iterator[bytes]:
            del chunk_size
            yield b'{"ok":'
            yield b" true}"

    message = SyncQueueClient(token="token", deployment=ALL_DEPLOYMENTS)._accept_impl(
        Response(),
        transport=RawJsonTransport[dict[str, bool]](),
    )
    assert message.payload == {"ok": True}


def test_accept_treats_httpx_response_headers_as_headers() -> None:
    class Response:
        headers = httpx.Headers({
            "Ce-Type": CLOUD_EVENT_TYPE_V2BETA,
            "Ce-Vqsqueuename": "emails",
            "Ce-Vqsconsumergroup": "test-group",
            "Ce-Vqsmessageid": "msg_1",
            "Ce-Vqsreceipthandle": "rh_1",
            "Ce-Vqscreatedat": CREATED_AT,
            "Content-Type": CONTENT_TYPE_JSON,
        })

        def iter_bytes(self, chunk_size: int | None = None) -> Iterator[bytes]:
            del chunk_size
            yield b'{"ok": true}'

    message = SyncQueueClient(token="token", deployment=ALL_DEPLOYMENTS)._accept_impl(
        Response(),
        transport=RawJsonTransport[dict[str, bool]](),
    )

    assert message.payload == {"ok": True}


@pytest.mark.anyio
async def test_async_accept_parses_async_iterable_push_delivery() -> None:
    async def body() -> AsyncIterator[bytes]:
        yield b'{"ok":'
        yield b" true}"

    headers = {
        CLOUD_EVENT_HEADER_TYPE: CLOUD_EVENT_TYPE_V2BETA,
        CLOUD_EVENT_HEADER_VQS_TOPIC: "emails",
        CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP: "test-group",
        CLOUD_EVENT_HEADER_VQS_MESSAGE_ID: "msg_1",
        CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE: "rh_1",
        CLOUD_EVENT_HEADER_VQS_CREATED_AT: CREATED_AT,
        HEADER_CONTENT_TYPE: CONTENT_TYPE_JSON,
    }
    message: Message[dict[str, bool]] = await QueueClient(
        token="token",
        deployment=ALL_DEPLOYMENTS,
    )._accept_impl(
        body(),
        headers,
        transport=RawJsonTransport[dict[str, bool]](),
    )
    assert message.payload == {"ok": True}


@pytest.mark.anyio
async def test_async_accept_parses_async_http_response_push_delivery() -> None:
    class Response:
        headers: ClassVar[dict[str, str]] = {
            CLOUD_EVENT_HEADER_TYPE: CLOUD_EVENT_TYPE_V2BETA,
            CLOUD_EVENT_HEADER_VQS_TOPIC: "emails",
            CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP: "test-group",
            CLOUD_EVENT_HEADER_VQS_MESSAGE_ID: "msg_1",
            CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE: "rh_1",
            CLOUD_EVENT_HEADER_VQS_CREATED_AT: CREATED_AT,
            HEADER_CONTENT_TYPE: CONTENT_TYPE_JSON,
        }

        async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
            del chunk_size
            yield b'{"ok":'
            yield b" true}"

        status_code = 200
        text = ""

        def json(self) -> Any:
            return {}

    message: Message[dict[str, bool]] = await QueueClient(
        token="token",
        deployment=ALL_DEPLOYMENTS,
    )._accept_impl(
        Response(),
        transport=RawJsonTransport[dict[str, bool]](),
    )
    assert message.payload == {"ok": True}


@pytest.mark.anyio
async def test_async_accept_rejects_sync_iterable_push_delivery() -> None:
    with pytest.raises(
        TypeError,
        match=r"async accept\(\) requires bytes or an async byte iterable",
    ):
        await QueueClient(token="token", deployment=ALL_DEPLOYMENTS)._accept_impl(
            cast("Any", [b'{"ok": true}']),
            callback_headers(),
            transport=RawJsonTransport[dict[str, bool]](),
        )


@pytest.mark.anyio
async def test_async_accept_rejects_sync_http_response_push_delivery() -> None:
    class Response:
        headers = callback_headers()

        def iter_bytes(self, chunk_size: int | None = None) -> Iterator[bytes]:
            del chunk_size
            yield b'{"ok": true}'

    with pytest.raises(
        TypeError,
        match=r"async accept\(\) requires headers or an async HTTP response",
    ):
        await QueueClient(token="token", deployment=ALL_DEPLOYMENTS)._accept_impl(
            cast("Any", Response()),
            transport=RawJsonTransport[dict[str, bool]](),
        )


def test_v2_push_delivery_binary_mode() -> None:
    message = SyncQueueClient(token="token", deployment=ALL_DEPLOYMENTS)._accept_impl(
        b"raw",
        {
            CLOUD_EVENT_HEADER_TYPE: CLOUD_EVENT_TYPE_V2BETA,
            CLOUD_EVENT_HEADER_VQS_TOPIC: "emails",
            CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP: "test-group",
            CLOUD_EVENT_HEADER_VQS_MESSAGE_ID: "msg_1",
            CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE: "rh_1",
            CLOUD_EVENT_HEADER_VQS_CREATED_AT: CREATED_AT,
            HEADER_CONTENT_TYPE: CONTENT_TYPE_OCTET_STREAM,
        },
        transport=ByteBufferTransport(),
    )
    assert message.payload == b"raw"
    assert message.metadata.receipt_handle == "rh_1"


def test_accept_metadata_only_requires_consumer_group() -> None:
    headers = callback_headers(receipt_handle="")
    del headers[CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP]

    with pytest.raises(ProtocolError, match="consumer_group"):
        SyncQueueClient(token="token", deployment=ALL_DEPLOYMENTS)._accept_impl(b"", headers)


@pytest.mark.anyio
async def test_async_accept_metadata_only_uses_push_delivery_region(
    eqs: EmbeddedQueueDevServer,
) -> None:
    message_id = await eqs.client.send("emails", {"ok": True})
    assert message_id is not None

    message: Message[dict[str, bool]] = await eqs.client._accept_impl(
        b"",
        {
            CLOUD_EVENT_HEADER_TYPE: CLOUD_EVENT_TYPE_V2BETA,
            CLOUD_EVENT_HEADER_VQS_TOPIC: "emails",
            CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP: "test-group",
            CLOUD_EVENT_HEADER_VQS_MESSAGE_ID: message_id,
            CLOUD_EVENT_HEADER_VQS_REGION: "sfo1",
        },
        lease_duration=20,
    )

    assert message.payload == {"ok": True}
    assert message.metadata.region == "sfo1"
    assert eqs.state.by_id[message_id].lease_deadline_by_consumer["test-group"] == (
        eqs.state.now + timedelta(seconds=20)
    )
    await eqs.client.acknowledge(message)
    assert eqs.state.by_id[message_id].acknowledged


@pytest.mark.anyio
async def test_debug_logs_header_only_push_fetch(
    eqs: EmbeddedQueueDevServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level(logging.INFO, logger="vercel.queue")
    message_id = await eqs.client.send("emails", {"ok": True})
    assert message_id is not None

    await eqs.client._accept_impl(
        b"",
        {
            CLOUD_EVENT_HEADER_TYPE: CLOUD_EVENT_TYPE_V2BETA,
            CLOUD_EVENT_HEADER_VQS_TOPIC: "emails",
            CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP: "test-group",
            CLOUD_EVENT_HEADER_VQS_MESSAGE_ID: message_id,
            CLOUD_EVENT_HEADER_VQS_REGION: "sfo1",
        },
    )

    assert any(
        event["event"] == "push.header_only_fetch"
        and event["topic"] == "emails"
        and event["consumer_group"] == "test-group"
        for event in _queue_debug_events(caplog)
    )


@pytest.mark.anyio
async def test_async_accept_header_only_ignores_streamed_body(
    eqs: EmbeddedQueueDevServer,
) -> None:
    message_id = await eqs.client.send("emails", {"ok": True})
    assert message_id is not None
    consumed = False

    async def body() -> AsyncIterator[bytes]:
        nonlocal consumed
        consumed = True
        yield b""

    message: Message[dict[str, bool]] = await eqs.client._accept_impl(
        body(),
        {
            CLOUD_EVENT_HEADER_TYPE: CLOUD_EVENT_TYPE_V2BETA,
            CLOUD_EVENT_HEADER_VQS_TOPIC: "emails",
            CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP: "test-group",
            CLOUD_EVENT_HEADER_VQS_MESSAGE_ID: message_id,
            CLOUD_EVENT_HEADER_VQS_REGION: "sfo1",
            HEADER_CONTENT_TYPE: CONTENT_TYPE_JSON,
        },
        lease_duration=20,
    )

    assert message.payload == {"ok": True}
    assert eqs.state.by_id[message_id].delivery_count_by_consumer["test-group"] == 1
    assert not consumed
