"""The v4 event-log transport, distinct from user workflow stream framing."""

from collections.abc import AsyncIterable
from typing import Any, Literal

import cbor2
import pydantic

from .. import serialization as ser, world as w

CONTENT_TYPE = "application/vnd.workflow.v4-frames"
MAX_META_SIZE = 1_000_000
MAX_BODY_SIZE = 100_000_000

PAYLOAD_FIELDS = {
    "run_created": "input",
    "run_started": "input",
    "run_completed": "output",
    "run_failed": "error",
    "step_created": "input",
    "step_completed": "result",
    "step_failed": "error",
    "step_retrying": "error",
    "hook_created": "metadata",
    "hook_received": "payload",
}


class IncompleteFrameError(Exception):
    """A transport ended before the required frame or sentinel arrived."""


class EventStreamEnd(w.BaseModel):
    end: Literal[1] = pydantic.Field(alias="_end")
    next: str | None = None
    has_more: bool = pydantic.Field(alias="hasMore", strict=True)


def _invalid(message: str) -> w.WorkflowWorldError:
    return w.WorkflowWorldError(message, status=0, code="SCHEMA_VALIDATION")


def encode_frame(meta: dict[str, Any], body: bytes = b"") -> bytes:
    encoded = cbor2.dumps(meta)
    return len(encoded).to_bytes(4, "big") + encoded + len(body).to_bytes(4, "big") + body


def decode_event(meta: dict[str, Any], body: bytes) -> w.Event:
    if body:
        field = PAYLOAD_FIELDS.get(meta.get("eventType", ""))
        if field is None or not isinstance(meta.get("eventData"), dict):
            raise _invalid("Event frame cannot carry a payload body")
        payload: Any = body
        if meta["eventType"] in {"run_failed", "step_failed", "step_retrying"} and body[:4] not in {
            ser.DEVALUE_V1,
            ser.ENCRYPTED,
            ser.SEALED,
            ser.GZIP,
            ser.ZSTD,
        }:
            # Stable runtimes stored structured errors as CBOR objects.
            try:
                decoded = cbor2.loads(body)
            except cbor2.CBORDecodeError:
                pass
            else:
                if isinstance(decoded, dict) and isinstance(decoded.get("message"), str):
                    payload = decoded
        meta = meta | {"eventData": meta["eventData"] | {field: payload}}
    try:
        event = w.EventAdaptor.from_wire(meta)
    except pydantic.ValidationError as error:
        raise _invalid("Invalid event frame") from error
    if event.server_props is None:
        raise _invalid("Event frame missing server properties")
    return event


async def consume_event_stream(
    chunks: AsyncIterable[bytes], events: list[w.Event]
) -> EventStreamEnd:
    """Append validated events so an interrupted read can retain its prefix."""
    source = aiter(chunks)
    buffer = bytearray()
    offset = 0

    async def read_exact(size: int) -> bytes:
        nonlocal offset
        if len(buffer) - offset < size:
            del buffer[:offset]
            offset = 0
            while len(buffer) < size:
                try:
                    chunk = await anext(source)
                except StopAsyncIteration:
                    raise IncompleteFrameError("Event stream ended before its sentinel") from None
                buffer.extend(chunk)
        result = bytes(buffer[offset : offset + size])
        offset += size
        return result

    while True:
        meta_size = int.from_bytes(await read_exact(4), "big")
        if meta_size > MAX_META_SIZE:
            raise _invalid("Event frame metadata is too large")
        try:
            meta = cbor2.loads(await read_exact(meta_size))
        except cbor2.CBORDecodeError as error:
            raise _invalid("Invalid event frame CBOR metadata") from error
        if not isinstance(meta, dict) or not all(isinstance(key, str) for key in meta):
            raise _invalid("Event frame metadata must be a map")
        body_size = int.from_bytes(await read_exact(4), "big")
        if body_size > MAX_BODY_SIZE:
            raise _invalid("Event frame body is too large")
        body = await read_exact(body_size)
        if meta.get("_end") == 1:
            if type(meta["_end"]) is not int:
                raise _invalid("Invalid event stream sentinel")
            try:
                end = EventStreamEnd.from_wire(meta)
            except pydantic.ValidationError as error:
                raise _invalid("Invalid event stream sentinel") from error
            if body or (end.has_more and not end.next) or (events and not end.next):
                raise _invalid("Invalid event stream continuation")
            return end
        if any(key.startswith("_") for key in meta):
            raise w.WorkflowWorldError(
                f"Event stream error or unexpected control frame: {meta.get('code', 'unknown')}",
                status=0,
                code="WORLD_CONTRACT_ERROR",
            )
        events.append(decode_event(meta, body))
