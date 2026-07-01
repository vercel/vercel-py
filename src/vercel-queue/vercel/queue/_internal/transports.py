from __future__ import annotations

from typing import Any, ForwardRef, Generic, Literal, TypeAlias, TypeVar, cast

import inspect
import json
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable
from importlib import import_module

from .constants import CONTENT_TYPE_JSON, CONTENT_TYPE_OCTET_STREAM, CONTENT_TYPE_TEXT
from .errors import SubscriptionError
from .streams import (
    AsyncStreamPayload,
    AsyncTextStreamPayload,
    SyncStreamPayload,
    SyncTextStreamPayload,
)
from .types import (
    RequestContent,
    Topic,
    Transport,
)
from .typeutils import (
    annotation_needs_resolution,
    args,
    is_classvar,
    is_final,
    is_type_var,
    is_union_type,
    origin_is,
    strip_annotated,
)

T = TypeVar("T")

TransportKind: TypeAlias = Literal[
    "json",
    "byte-buffer",
    "byte-stream",
    "text-buffer",
    "text-stream",
]


class ByteBufferTransport:
    content_type = CONTENT_TYPE_OCTET_STREAM

    def serialize(self, value: bytes | bytearray | memoryview) -> bytes:
        if isinstance(value, bytes):
            return value
        return bytes(value)

    async def deserialize(
        self,
        payload: AsyncIterator[bytes],
        *,
        content_type: str,
    ) -> bytes:
        return await _collect_bytes_async(payload)


class TextBufferTransport:
    content_type = CONTENT_TYPE_TEXT

    def serialize(self, value: str) -> bytes:
        return value.encode("utf-8")

    async def deserialize(
        self,
        payload: AsyncIterator[bytes],
        *,
        content_type: str,
    ) -> str:
        return (await _collect_bytes_async(payload)).decode("utf-8")


class ByteStreamTransport:
    """Transport that preserves receive payloads as one-shot byte streams.

    This transport avoids buffering message bytes on receive and passes stream
    payloads through to the HTTP client on send.
    """

    content_type = CONTENT_TYPE_OCTET_STREAM

    def serialize(
        self,
        value: bytes
        | bytearray
        | memoryview
        | Iterable[bytes]
        | AsyncIterable[bytes]
        | SyncStreamPayload
        | AsyncStreamPayload,
    ) -> RequestContent:
        if isinstance(value, bytes):
            return value
        if isinstance(value, (bytearray, memoryview)):
            return bytes(value)
        if isinstance(value, AsyncIterable):
            return value
        return value

    async def deserialize(
        self,
        payload: AsyncIterator[bytes],
        *,
        content_type: str,
    ) -> AsyncStreamPayload:
        return AsyncStreamPayload(payload)


class RawJsonTransport(Generic[T]):
    content_type = CONTENT_TYPE_JSON

    def __init__(
        self,
        *,
        json_encoder: type[json.JSONEncoder] | None = None,
        json_decoder: type[json.JSONDecoder] | None = None,
    ) -> None:
        self.json_encoder = json_encoder
        self.json_decoder = json_decoder

    def serialize(self, value: T) -> bytes:
        if self.json_encoder is None:
            return json.dumps(value).encode("utf-8")
        return json.dumps(value, cls=self.json_encoder).encode("utf-8")

    async def deserialize(
        self,
        payload: AsyncIterator[bytes],
        *,
        content_type: str,
    ) -> T:
        text = (await _collect_bytes_async(payload)).decode("utf-8")
        if self.json_decoder is None:
            return cast("T", json.loads(text))
        return cast("T", json.loads(text, cls=self.json_decoder))


class TypedJsonTransport(Generic[T]):
    content_type = CONTENT_TYPE_JSON

    def __init__(self, model: type[T]) -> None:
        self.model = model
        try:
            type_adapter = import_module("pydantic").TypeAdapter
        except ImportError as exc:  # pragma: no cover - depends on optional extra
            raise RuntimeError("Install vercel-queue[typed] to use TypedJsonTransport") from exc
        self._adapter = type_adapter(model)

    def serialize(self, value: T) -> bytes:
        if hasattr(value, "model_dump_json"):
            return cast("Any", value).model_dump_json().encode("utf-8")
        return self._adapter.dump_json(value)

    async def deserialize(
        self,
        payload: AsyncIterator[bytes],
        *,
        content_type: str,
    ) -> T:
        return self._adapter.validate_json(await _collect_bytes_async(payload))


class _ModelDumpJsonTransport:
    content_type = CONTENT_TYPE_JSON

    def __init__(self, model_dump_json: Callable[[], str]) -> None:
        self._model_dump_json = model_dump_json

    def serialize(self, value: object) -> bytes:
        del value
        return self._model_dump_json().encode("utf-8")

    async def deserialize(
        self,
        payload: AsyncIterator[bytes],
        *,
        content_type: str,
    ) -> object:
        return await RawJsonTransport[Any]().deserialize(payload, content_type=content_type)


class TextStreamTransport:
    """Transport that sends and receives one-shot UTF-8 text streams."""

    content_type = CONTENT_TYPE_TEXT

    def serialize(
        self,
        value: str
        | Iterable[str]
        | AsyncIterable[str]
        | SyncTextStreamPayload
        | AsyncTextStreamPayload,
    ) -> RequestContent:
        if isinstance(value, str):
            return value.encode("utf-8")
        if isinstance(value, AsyncIterable):
            return _encode_text_async(cast("AsyncIterable[str]", value))
        return _encode_text_sync(value)

    async def deserialize(
        self,
        payload: AsyncIterator[bytes],
        *,
        content_type: str,
    ) -> AsyncTextStreamPayload:
        return AsyncTextStreamPayload(payload)


def infer_send_transport(payload: object) -> object:
    if model_dump_json := _model_dump_json(payload):
        return _ModelDumpJsonTransport(model_dump_json)
    if isinstance(payload, (bytes, bytearray, memoryview)):
        return ByteBufferTransport()
    if isinstance(payload, str):
        return TextBufferTransport()
    if isinstance(payload, (SyncTextStreamPayload, AsyncTextStreamPayload)):
        return TextStreamTransport()
    if isinstance(payload, (SyncStreamPayload, AsyncStreamPayload)):
        return ByteStreamTransport()
    return RawJsonTransport[Any]()


def is_untyped_payload_annotation(annotation: Any) -> bool:
    annotation = strip_annotated(annotation)
    return annotation in {inspect.Signature.empty, Any, object}


def reject_invalid_payload_annotation(annotation: Any) -> None:
    annotation = strip_annotated(annotation)
    if is_untyped_payload_annotation(annotation):
        return
    if is_type_var(annotation) or is_classvar(annotation) or is_final(annotation):
        raise SubscriptionError(f"unsupported queue subscriber payload annotation: {annotation!r}")
    if annotation in {list, dict, tuple, set, frozenset}:
        raise SubscriptionError(
            f"unsupported bare queue subscriber payload annotation: {annotation!r}"
        )
    if isinstance(annotation, str | ForwardRef) or origin_is(annotation, Literal):
        return
    if is_union_type(annotation):
        for item in args(annotation):
            reject_invalid_payload_annotation(item)
        return
    if origin_is(annotation, Iterable, AsyncIterable):
        stream_args = args(annotation)
        if len(stream_args) != 1 or stream_args[0] not in {bytes, str}:
            raise SubscriptionError(
                "queue stream subscriber annotation must be Iterable[bytes], "
                "Iterable[str], AsyncIterable[bytes], or AsyncIterable[str]"
            )
        return
    for item in args(annotation):
        if item is not Ellipsis:
            reject_invalid_payload_annotation(item)


def payload_transport_kind(annotation: Any) -> TransportKind:
    annotation = strip_annotated(annotation)
    if annotation is bytes:
        return "byte-buffer"
    if annotation is str:
        return "text-buffer"
    if annotation in {SyncStreamPayload, AsyncStreamPayload}:
        return "byte-stream"
    if annotation in {SyncTextStreamPayload, AsyncTextStreamPayload}:
        return "text-stream"
    if origin_is(annotation, Iterable, AsyncIterable):
        stream_args = args(annotation)
        if stream_args == (bytes,):
            return "byte-stream"
        if stream_args == (str,):
            return "text-stream"
    return "json"


def transport_for_kind(kind: TransportKind) -> Transport[Any]:
    if kind == "byte-buffer":
        return ByteBufferTransport()
    if kind == "byte-stream":
        return ByteStreamTransport()
    if kind == "text-buffer":
        return TextBufferTransport()
    if kind == "text-stream":
        return TextStreamTransport()
    return RawJsonTransport[Any]()


def receive_transport_for_annotation(annotation: Any) -> Transport[Any]:
    annotation = strip_annotated(annotation)
    if is_untyped_payload_annotation(annotation):
        return RawJsonTransport[Any]()
    if annotation_needs_resolution(annotation):
        raise SubscriptionError(f"unsupported queue subscriber payload annotation: {annotation!r}")
    reject_invalid_payload_annotation(annotation)
    kind = payload_transport_kind(annotation)
    if kind == "json":
        return TypedJsonTransport[Any](annotation)
    return transport_for_kind(kind)


def receive_transport_for_topic(topic: object) -> Transport[Any]:
    if not isinstance(topic, Topic):
        return RawJsonTransport[Any]()
    if topic.transport is not None:
        return topic.transport
    if getattr(type(topic), "__topic_origin__", None) is not Topic:
        return RawJsonTransport[Any]()
    return receive_transport_for_annotation(type(topic).__topic_payload_type__)


def send_transport_for_topic(topic: object) -> Transport[Any] | None:
    if isinstance(topic, Topic):
        return topic.transport
    return None


def _model_dump_json(value: object) -> Callable[[], str] | None:
    model_dump_json = getattr(value, "model_dump_json", None)
    if callable(model_dump_json):
        return cast("Callable[[], str]", model_dump_json)
    return None


async def _collect_bytes_async(payload: AsyncIterator[bytes]) -> bytes:
    chunks = bytearray()
    async for chunk in payload:
        chunks.extend(chunk)
    return bytes(chunks)


def _encode_text_sync(payload: Iterable[str]) -> Iterable[bytes]:
    for chunk in payload:
        yield chunk.encode("utf-8")


async def _encode_text_async(payload: AsyncIterable[str]) -> AsyncIterator[bytes]:
    async for chunk in payload:
        yield chunk.encode("utf-8")


# Only add public symbols to __all__; internal helpers must stay unexported.
__all__ = (
    "ByteBufferTransport",
    "ByteStreamTransport",
    "RawJsonTransport",
    "TextBufferTransport",
    "TextStreamTransport",
    "TransportKind",
    "TypedJsonTransport",
    "infer_send_transport",
    "is_untyped_payload_annotation",
    "payload_transport_kind",
    "receive_transport_for_annotation",
    "receive_transport_for_topic",
    "reject_invalid_payload_annotation",
    "send_transport_for_topic",
    "transport_for_kind",
)
