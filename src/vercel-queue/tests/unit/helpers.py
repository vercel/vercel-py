from __future__ import annotations

from typing import Any, TypeVar

import time
from collections.abc import AsyncIterator, Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial

import anyio
import anyio.lowlevel
import httpx
from anyio import to_thread

from vercel.queue import (
    Delivery,
    Message,
    MessageMetadata,
    QueueClient,
    Topic,
    subscribe,
)
from vercel.queue._internal.constants import (
    CLOUD_EVENT_HEADER_TYPE,
    CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP,
    CLOUD_EVENT_HEADER_VQS_CREATED_AT,
    CLOUD_EVENT_HEADER_VQS_DELIVERY_COUNT,
    CLOUD_EVENT_HEADER_VQS_MESSAGE_ID,
    CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE,
    CLOUD_EVENT_HEADER_VQS_TOPIC,
    CLOUD_EVENT_HEADER_VQS_VISIBILITY_DEADLINE,
    CLOUD_EVENT_TYPE_V2BETA,
    CONTENT_TYPE_JSON,
    HEADER_CONTENT_TYPE,
    VQS_HEADER_EXPIRES_AT,
)
from vercel.queue._internal.streams import AsyncStreamPayload
from vercel.queue._internal.types import Transport
from vercel.queue.devserver import EmbeddedQueueDevServer
from vercel.queue.sync import QueueClient as SyncQueueClient

T = TypeVar("T")

CREATED_AT = "2026-01-01T00:00:00Z"
EXPIRES_AT = "2026-01-02T00:00:00Z"
CREATED_AT_DT = datetime(2026, 1, 1, tzinfo=timezone.utc)
EXPIRES_AT_DT = datetime(2026, 1, 2, tzinfo=timezone.utc)


@dataclass(frozen=True)
class EmbeddedDelivery:
    client: SyncQueueClient | QueueClient
    body: bytes
    headers: dict[str, str]

    @property
    def message_id(self) -> str:
        return self.headers[CLOUD_EVENT_HEADER_VQS_MESSAGE_ID]


def mock_response(*args: Any, **kwargs: Any) -> Any:
    return httpx.Response(*args, **kwargs)


def sync_delivery(
    server: EmbeddedQueueDevServer,
    payload: T,
    *,
    topic: str = "emails",
    consumer_group: str = "tests",
    client: SyncQueueClient | None = None,
    transport: Transport[T] | None = None,
    lease_seconds: int = 300,
) -> EmbeddedDelivery:
    sync_client = client or server.get_sync_client()
    send_topic: str | Topic[T] = (
        topic if transport is None else Topic[T](topic, transport=transport)
    )
    sync_client.send(send_topic, payload)
    delivery = next(server.iter_push_deliveries(topic, consumer_group, lease_seconds=lease_seconds))
    return EmbeddedDelivery(sync_client, delivery.body, delivery.headers)


async def async_delivery(
    server: EmbeddedQueueDevServer,
    payload: T,
    *,
    topic: str = "emails",
    consumer_group: str = "tests",
    client: QueueClient | None = None,
    transport: Transport[T] | None = None,
    lease_seconds: int = 300,
) -> EmbeddedDelivery:
    async_client = client or server.get_async_client(base_url=server.base_url)
    send_topic: str | Topic[T] = (
        topic if transport is None else Topic[T](topic, transport=transport)
    )
    await async_client.send(send_topic, payload)
    delivery = next(server.iter_push_deliveries(topic, consumer_group, lease_seconds=lease_seconds))
    return EmbeddedDelivery(async_client, delivery.body, delivery.headers)


def multipart_body(
    boundary: str,
    payload: bytes = b'{"ok": true}',
    *,
    expires_at: str | None = EXPIRES_AT,
) -> bytes:
    metadata_headers = [
        b"Content-Type: application/json",
        b"Vqs-Message-Id: msg_1",
        b"Vqs-Receipt-Handle: rh_1",
        b"Vqs-Delivery-Count: 2",
        f"Vqs-Timestamp: {CREATED_AT}".encode(),
    ]
    if expires_at is not None:
        metadata_headers.append(f"{VQS_HEADER_EXPIRES_AT}: {expires_at}".encode())
    return b"\r\n".join([
        f"--{boundary}".encode(),
        *metadata_headers,
        b"",
        payload,
        f"--{boundary}--".encode(),
        b"",
    ])


def multipart_messages_body(boundary: str, payloads: list[tuple[str, str, bytes]]) -> bytes:
    parts: list[bytes] = []
    for message_id, receipt_handle, payload in payloads:
        parts.extend([
            f"--{boundary}".encode(),
            b"Content-Type: application/json",
            f"Vqs-Message-Id: {message_id}".encode(),
            f"Vqs-Receipt-Handle: {receipt_handle}".encode(),
            b"Vqs-Delivery-Count: 1",
            f"Vqs-Timestamp: {CREATED_AT}".encode(),
            b"",
            payload,
        ])
    parts.extend([f"--{boundary}--".encode(), b""])
    return b"\r\n".join(parts)


def malformed_multipart_body(boundary: str, headers: list[bytes]) -> bytes:
    return b"\r\n".join([
        f"--{boundary}".encode(),
        *headers,
        b"",
        b'{"ok": true}',
        f"--{boundary}--".encode(),
        b"",
    ])


async def one_chunk(payload: bytes) -> AsyncIterator[bytes]:
    yield payload


async def collect_async_stream(payload: AsyncStreamPayload) -> bytes:
    chunks = bytearray()
    async for chunk in payload:
        chunks.extend(chunk)
    return bytes(chunks)


async def collect_messages(stream: AsyncIterator[Delivery[T]]) -> list[Message[T]]:
    return [delivery.accept() async for delivery in stream]


def payloads(messages: list[Message[T]]) -> list[T]:
    return [message.payload for message in messages]


async def advance_async_time(clock: Any, seconds: float) -> None:
    await anyio.lowlevel.checkpoint()
    clock.shift(seconds + 0.001)
    for _ in range(10):
        await anyio.lowlevel.checkpoint()


async def wait_for_call_count(route: Any, count: int) -> None:
    await to_thread.run_sync(wait_for_call_count_sync, route, count)


def _wait_until_sync(
    condition: Callable[[], bool],
    *,
    max_wait_seconds: float,
    delay_seconds: float,
) -> None:
    deadline = time.monotonic() + max_wait_seconds
    while not condition():
        if time.monotonic() >= deadline:
            raise TimeoutError("condition was not met")
        time.sleep(delay_seconds)


async def wait_until(
    condition: Callable[[], bool],
    *,
    max_wait_seconds: float = 1,
    delay_seconds: float = 0.001,
) -> None:
    await to_thread.run_sync(
        partial(
            _wait_until_sync,
            max_wait_seconds=max_wait_seconds,
            delay_seconds=delay_seconds,
        ),
        condition,
    )


def wait_for_call_count_sync(route: Any, count: int) -> None:
    deadline = time.monotonic() + 1
    while route.call_count < count:
        if time.monotonic() >= deadline:
            raise AssertionError(f"expected {count} calls, saw {route.call_count}")
        time.sleep(0.001)


def run_with_anyio_backend(test: Callable[[], Awaitable[None]], backend: str) -> None:
    def run() -> None:
        anyio.run(test, backend=backend)

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(run).result()


def callback_headers(
    *,
    topic: str = "emails",
    receipt_handle: str = "rh_1",
    visibility_deadline: str | None = None,
) -> dict[str, str]:
    headers = {
        CLOUD_EVENT_HEADER_TYPE: CLOUD_EVENT_TYPE_V2BETA,
        CLOUD_EVENT_HEADER_VQS_TOPIC: topic,
        CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP: "tests",
        CLOUD_EVENT_HEADER_VQS_MESSAGE_ID: "msg_1",
        CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE: receipt_handle,
        CLOUD_EVENT_HEADER_VQS_DELIVERY_COUNT: "1",
        CLOUD_EVENT_HEADER_VQS_CREATED_AT: CREATED_AT,
        HEADER_CONTENT_TYPE: CONTENT_TYPE_JSON,
    }
    if visibility_deadline is not None:
        headers[CLOUD_EVENT_HEADER_VQS_VISIBILITY_DEADLINE] = visibility_deadline
    return headers


def callback_subscribe(
    **kwargs: Any,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    return subscribe(consumer_group="tests", **kwargs)


def sync_push_message(
    server: EmbeddedQueueDevServer,
    client: SyncQueueClient,
    message: T,
    *,
    topic: str = "emails",
    consumer_group: str = "test-group",
) -> Message[T]:
    client.send(topic, message)
    delivery = next(server.iter_push_deliveries(topic, consumer_group))
    return client._accept_impl(delivery.body, delivery.headers)


def make_metadata(
    topic: str,
    *,
    message_id: str = "m1",
    consumer_group: str = "c",
) -> MessageMetadata:
    return MessageMetadata(
        message_id=message_id,
        delivery_count=1,
        created_at=CREATED_AT_DT,
        topic=topic,
        consumer_group=consumer_group,
    )


def make_leased_metadata(topic: str, *, message_id: str = "m1") -> MessageMetadata:
    return MessageMetadata(
        message_id=message_id,
        delivery_count=1,
        created_at=CREATED_AT_DT,
        topic=topic,
        consumer_group="c",
        receipt_handle="rh_1",
    )
