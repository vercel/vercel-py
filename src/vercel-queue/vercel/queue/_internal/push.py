from __future__ import annotations

from typing import Generic, TypeGuard, TypeVar, cast

import json
from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timezone

from .asynctools import iter_bytes_async
from .config import validate_region
from .constants import (
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
from .errors import ProtocolError
from .http import (
    AsyncHttpMessage,
    AsyncPushDeliveryBody,
    AsyncPushDeliveryInput,
    HttpResponse,
    PushDeliveryBody,
    PushDeliveryInput,
    headers_from_raw,
    response_headers,
)
from .log import debug_log
from .messages import parse_optional_datetime, parse_required_datetime
from .names import SanitizedName
from .types import (
    Headers,
    Message,
    MessageMetadata,
    RawHeaders,
    ReceiptHandle,
    Transport,
)

T = TypeVar("T")


@dataclass(frozen=True, kw_only=True)
class ParsedPushDelivery(Generic[T]):
    metadata: MessageMetadata
    message: Message[T] | None


async def parse_push_delivery(
    raw_body: PushDeliveryBody,
    headers: RawHeaders | None = None,
    *,
    transport: Transport[T] | None = None,
) -> ParsedPushDelivery[T]:
    headers = headers_from_raw(headers or {})
    metadata = _parse_push_delivery_metadata(headers)
    debug_log(
        "push.delivery_metadata",
        metadata=_debug_metadata(metadata),
    )
    if not metadata.receipt_handle:
        return ParsedPushDelivery(metadata=metadata, message=None)
    # v2beta inline callbacks send the stored payload bytes directly; there is
    # no JSON CloudEvent envelope to unwrap.
    message = Message(
        payload=await _deserialize_push_delivery(
            raw_body,
            metadata.content_type or CONTENT_TYPE_OCTET_STREAM,
            transport,
        ),
        metadata=metadata,
    )
    return ParsedPushDelivery(
        metadata=metadata,
        message=message,
    )


def accept_input_sync(
    raw_body_or_response: PushDeliveryBody | HttpResponse,
    headers: RawHeaders | None,
) -> tuple[PushDeliveryBody, Headers]:
    return _accept_input_from_response(
        raw_body_or_response,
        headers,
        allow_async_response=False,
    )


def accept_input(
    raw_body_or_response: AsyncPushDeliveryInput,
    headers: RawHeaders | None,
) -> tuple[AsyncPushDeliveryBody, Headers]:
    raw_body, resolved_headers = _accept_input_from_response(
        raw_body_or_response,
        headers,
        allow_async_response=True,
    )
    return cast("AsyncPushDeliveryBody", raw_body), resolved_headers


def _parse_push_delivery_metadata(headers: Headers) -> MessageMetadata:
    event_type = headers.get(CLOUD_EVENT_HEADER_TYPE, "")
    if event_type != CLOUD_EVENT_TYPE_V2BETA:
        raise ValueError(
            f"Invalid CloudEvent type: expected {CLOUD_EVENT_TYPE_V2BETA!r}, got {event_type!r}"
        )

    topic = headers.get(CLOUD_EVENT_HEADER_VQS_TOPIC, "")
    consumer_group = headers.get(CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP, "")
    message_id = headers.get(CLOUD_EVENT_HEADER_VQS_MESSAGE_ID, "")
    receipt_handle = headers.get(CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE, "")
    if not consumer_group:
        raise ProtocolError("push delivery metadata must include consumer_group")
    if not topic or not message_id:
        raise ProtocolError("missing required ce-vqs* headers")

    region = validate_region(headers.get(CLOUD_EVENT_HEADER_VQS_REGION))
    if not receipt_handle:
        return MessageMetadata(
            message_id=message_id,
            delivery_count=1,
            created_at=datetime.now(timezone.utc),
            topic=topic,
            consumer_group=SanitizedName(consumer_group),
            receipt_handle=None,
            content_type=headers.get(HEADER_CONTENT_TYPE),
            region=region,
        )

    delivery_count_raw = headers.get(CLOUD_EVENT_HEADER_VQS_DELIVERY_COUNT, "1")
    try:
        delivery_count = int(delivery_count_raw)
    except ValueError:
        delivery_count = 1

    content_type = headers.get(HEADER_CONTENT_TYPE, CONTENT_TYPE_OCTET_STREAM)
    return MessageMetadata(
        message_id=message_id,
        delivery_count=delivery_count,
        created_at=parse_required_datetime(
            headers.get(CLOUD_EVENT_HEADER_VQS_CREATED_AT),
            CLOUD_EVENT_HEADER_VQS_CREATED_AT,
        ),
        expires_at=parse_optional_datetime(
            headers.get(CLOUD_EVENT_HEADER_VQS_EXPIRES_AT),
            CLOUD_EVENT_HEADER_VQS_EXPIRES_AT,
        ),
        topic=topic,
        consumer_group=SanitizedName(consumer_group),
        receipt_handle=ReceiptHandle(receipt_handle),
        content_type=content_type,
        region=region,
        visibility_deadline=parse_optional_datetime(
            headers.get(CLOUD_EVENT_HEADER_VQS_VISIBILITY_DEADLINE),
            CLOUD_EVENT_HEADER_VQS_VISIBILITY_DEADLINE,
        ),
    )


def _debug_metadata(metadata: MessageMetadata) -> dict[str, object]:
    return {
        "message_id": metadata.message_id,
        "delivery_count": metadata.delivery_count,
        "created_at": metadata.created_at,
        "expires_at": metadata.expires_at,
        "topic": metadata.topic,
        "consumer_group": str(metadata.consumer_group),
        "receipt_handle": metadata.receipt_handle,
        "content_type": metadata.content_type,
        "region": metadata.region,
        "visibility_deadline": metadata.visibility_deadline,
    }


def parse_push_delivery_metadata(headers: RawHeaders) -> MessageMetadata:
    return _parse_push_delivery_metadata(headers_from_raw(headers))


async def _deserialize_push_delivery(
    payload: PushDeliveryBody,
    content_type: str,
    transport: Transport[T] | None,
) -> T:
    chunks = _push_delivery_chunks(payload)
    if transport is not None:
        return await transport.deserialize(chunks, content_type=content_type)
    buffered = await _collect_push_delivery_body(chunks)
    if CONTENT_TYPE_JSON in content_type.lower():
        return json.loads(buffered.decode("utf-8"))
    return cast("T", buffered)


def _push_delivery_chunks(
    payload: PushDeliveryBody,
) -> AsyncIterator[bytes]:
    if isinstance(payload, bytes):
        return _one_chunk_async(payload)
    if isinstance(payload, AsyncIterable):
        return aiter(cast("AsyncIterable[bytes]", payload))
    return iter_bytes_async(payload)


async def _collect_push_delivery_body(payload: AsyncIterable[bytes]) -> bytes:
    chunks = bytearray()
    async for chunk in payload:
        chunks.extend(chunk)
    return bytes(chunks)


async def _one_chunk_async(payload: bytes) -> AsyncIterator[bytes]:
    yield payload


def _accept_input_from_response(
    raw_body_or_response: PushDeliveryInput | AsyncPushDeliveryInput,
    headers: RawHeaders | None,
    *,
    allow_async_response: bool,
) -> tuple[PushDeliveryBody, Headers]:
    if headers is not None:
        if allow_async_response and not _is_async_push_delivery_body(raw_body_or_response):
            raise TypeError("async accept() requires bytes or an async byte iterable")
        return cast("PushDeliveryBody", raw_body_or_response), headers_from_raw(headers)
    if allow_async_response and isinstance(raw_body_or_response, AsyncHttpMessage):
        return raw_body_or_response.aiter_bytes(), response_headers(raw_body_or_response)
    if allow_async_response:
        raise TypeError("async accept() requires headers or an async HTTP response")
    if isinstance(raw_body_or_response, HttpResponse):
        return raw_body_or_response.iter_bytes(), response_headers(raw_body_or_response)
    raise TypeError("accept() missing required headers")


def _is_async_push_delivery_body(value: object) -> TypeGuard[AsyncPushDeliveryBody]:
    return isinstance(value, (bytes, AsyncIterable))
