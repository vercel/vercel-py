from __future__ import annotations

from typing import TypeVar, cast

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import datetime, timezone

from .constants import (
    HEADER_CONTENT_TYPE,
    VQS_HEADER_DELIVERY_COUNT,
    VQS_HEADER_EXPIRES_AT,
    VQS_HEADER_MESSAGE_ID,
    VQS_HEADER_RECEIPT_HANDLE,
    VQS_HEADER_TIMESTAMP,
)
from .errors import MessageCorruptedError
from .http import headers_from_raw
from .streams import AsyncStreamPayload, AsyncTextStreamPayload
from .types import (
    Message,
    MessageMetadata,
    RawHeaders,
    ReceiptHandle,
    Transport,
)

T = TypeVar("T")


async def message_from_part_async(
    topic: str,
    consumer_group: str,
    transport: Transport[T],
    headers: RawHeaders,
    payload: AsyncIterator[bytes],
) -> Message[T]:
    metadata = _message_metadata_from_part(topic, consumer_group, headers)
    try:
        decoded = await transport.deserialize(payload, content_type=metadata.content_type or "")
    except Exception as exc:
        raise MessageCorruptedError(
            metadata.message_id,
            f"Failed to parse payload: {exc}",
        ) from exc
    return Message(payload=decoded, metadata=metadata)


def _message_metadata_from_part(
    topic: str,
    consumer_group: str,
    headers: RawHeaders,
) -> MessageMetadata:
    headers = headers_from_raw(headers)
    message_id = headers.get(VQS_HEADER_MESSAGE_ID)
    receipt_handle = headers.get(VQS_HEADER_RECEIPT_HANDLE)
    if not message_id:
        raise MessageCorruptedError("<unknown>", "Missing Vqs-Message-Id multipart header")
    if not receipt_handle:
        raise MessageCorruptedError(message_id, "Missing Vqs-Receipt-Handle multipart header")
    delivery_count_raw = headers.get(VQS_HEADER_DELIVERY_COUNT) or "0"
    try:
        delivery_count = int(delivery_count_raw)
    except ValueError:
        delivery_count = 0
    return MessageMetadata(
        message_id=message_id,
        delivery_count=delivery_count,
        created_at=parse_required_datetime(
            headers.get(VQS_HEADER_TIMESTAMP),
            VQS_HEADER_TIMESTAMP,
        ),
        expires_at=parse_optional_datetime(
            headers.get(VQS_HEADER_EXPIRES_AT),
            VQS_HEADER_EXPIRES_AT,
        ),
        topic=topic,
        consumer_group=consumer_group,
        receipt_handle=ReceiptHandle(receipt_handle),
        content_type=headers.get(HEADER_CONTENT_TYPE, ""),
    )


def message_with_region(message: Message[T], region: str | None) -> Message[T]:
    if region is None:
        return message
    return Message(payload=message.payload, metadata=replace(message.metadata, region=region))


def sync_message_payload(message: Message[T]) -> Message[T]:
    if isinstance(message.payload, AsyncStreamPayload):
        return Message(payload=cast("T", message.payload.to_sync()), metadata=message.metadata)
    if isinstance(message.payload, AsyncTextStreamPayload):
        return Message(payload=cast("T", message.payload.to_sync()), metadata=message.metadata)
    return message


def parse_required_datetime(value: str | None, header_name: str) -> datetime:
    parsed = parse_optional_datetime(value, header_name)
    if parsed is None:
        raise ValueError(f"missing required {header_name} header")
    return parsed


def parse_optional_datetime(value: str | None, header_name: str) -> datetime | None:
    if not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"invalid {header_name} header: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
