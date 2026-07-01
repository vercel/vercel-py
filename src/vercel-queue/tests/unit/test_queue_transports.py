from __future__ import annotations

from typing import cast

from collections.abc import AsyncIterator, Iterator

import pytest
from pydantic import BaseModel

from vercel.queue import (
    ByteBufferTransport,
    ByteStreamTransport,
    RawJsonTransport,
    TextBufferTransport,
    TextStreamTransport,
    TypedJsonTransport,
)
from vercel.queue._internal.constants import (
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_OCTET_STREAM,
)
from vercel.queue._internal.streams import (
    AsyncStreamPayload,
    AsyncTextStreamPayload,
    SyncTextStreamPayload,
)
from vercel.queue._internal.transports import infer_send_transport

from .helpers import (
    one_chunk,
)


def test_bytes_stream_transport_send_preserves_sync_iterable() -> None:
    consumed = False

    def chunks() -> Iterator[bytes]:
        nonlocal consumed
        consumed = True
        yield b"raw"

    payload = chunks()
    serialized = ByteStreamTransport().serialize(payload)

    assert serialized is payload
    assert not consumed
    assert b"".join(serialized) == b"raw"


@pytest.mark.anyio
async def test_byte_buffer_transport_deserializes_bytes() -> None:
    async def chunks() -> AsyncIterator[bytes]:
        yield b"ra"
        yield b"w"

    assert (
        await ByteBufferTransport().deserialize(
            chunks(),
            content_type=CONTENT_TYPE_OCTET_STREAM,
        )
        == b"raw"
    )


@pytest.mark.anyio
async def test_text_buffer_transport_deserializes_string() -> None:
    async def chunks() -> AsyncIterator[bytes]:
        yield b"caf\xc3"
        yield b"\xa9"

    assert (
        await TextBufferTransport().deserialize(
            chunks(),
            content_type=TextBufferTransport.content_type,
        )
        == "caf\u00e9"
    )


@pytest.mark.anyio
async def test_bytes_stream_transport_send_preserves_async_iterable() -> None:
    consumed = False

    async def chunks() -> AsyncIterator[bytes]:
        nonlocal consumed
        consumed = True
        yield b"raw"

    payload = chunks()
    serialized = ByteStreamTransport().serialize(payload)

    assert serialized is payload
    assert not consumed
    collected = bytearray()
    async for chunk in serialized:
        collected.extend(chunk)
    assert bytes(collected) == b"raw"


def test_send_inference_does_not_peek_at_sync_iterables() -> None:
    consumed = False

    def chunks() -> Iterator[bytes]:
        nonlocal consumed
        consumed = True
        yield b"raw"

    payload = chunks()
    transport = infer_send_transport(payload)

    assert isinstance(transport, RawJsonTransport)
    assert not consumed


def test_send_inference_does_not_peek_at_async_iterables() -> None:
    consumed = False

    async def chunks() -> AsyncIterator[bytes]:
        nonlocal consumed
        consumed = True
        yield b"raw"

    payload = chunks()
    transport = infer_send_transport(payload)

    assert isinstance(transport, RawJsonTransport)
    assert not consumed


def test_text_stream_transport_send_serializes_string() -> None:
    serialized = TextStreamTransport().serialize("caf\u00e9")

    assert serialized == "caf\u00e9".encode()


def test_send_inference_uses_text_stream_for_text_stream_payload_wrappers() -> None:
    sync_payload = SyncTextStreamPayload(iter(["raw"]))
    async_payload = AsyncTextStreamPayload(one_chunk(b"raw"))

    assert isinstance(infer_send_transport(sync_payload), TextStreamTransport)
    assert isinstance(infer_send_transport(async_payload), TextStreamTransport)


def test_text_stream_transport_send_encodes_sync_iterable() -> None:
    consumed = False

    def chunks() -> Iterator[str]:
        nonlocal consumed
        consumed = True
        yield "caf"
        yield "\u00e9"

    serialized = TextStreamTransport().serialize(chunks())

    assert not consumed
    assert b"".join(cast("Iterator[bytes]", serialized)) == "caf\u00e9".encode()


@pytest.mark.anyio
async def test_text_stream_transport_send_encodes_async_iterable() -> None:
    consumed = False

    async def chunks() -> AsyncIterator[str]:
        nonlocal consumed
        consumed = True
        yield "caf"
        yield "\u00e9"

    serialized = TextStreamTransport().serialize(chunks())

    assert not consumed
    collected = bytearray()
    async for chunk in cast("AsyncIterator[bytes]", serialized):
        collected.extend(chunk)
    assert bytes(collected) == "caf\u00e9".encode()


@pytest.mark.anyio
async def test_text_stream_payload_decodes_split_multibyte_sequences() -> None:
    async def chunks() -> AsyncIterator[bytes]:
        yield b"caf\xc3"
        yield b"\xa9"

    payload = await TextStreamTransport().deserialize(chunks(), content_type="")

    assert isinstance(payload, AsyncTextStreamPayload)
    assert [chunk async for chunk in payload] == ["caf", "\u00e9"]


@pytest.mark.anyio
async def test_text_stream_payload_read_decodes_split_multibyte_sequences() -> None:
    async def chunks() -> AsyncIterator[bytes]:
        yield b"caf\xc3"
        yield b"\xa9 noir"

    payload = AsyncTextStreamPayload(chunks())

    assert await payload.read(4) == "caf\u00e9"
    assert await payload.read() == " noir"


@pytest.mark.anyio
async def test_text_stream_payload_can_wrap_byte_stream_payload() -> None:
    byte_payload = AsyncStreamPayload(one_chunk("caf\u00e9".encode()))
    text_payload = AsyncTextStreamPayload(byte_payload)

    assert await text_payload.read() == "caf\u00e9"
    assert await byte_payload.read() == b""


@pytest.mark.anyio
async def test_text_stream_payload_finalize_delegates_to_byte_payload_once() -> None:
    consumed: list[bytes] = []
    closed = 0

    async def body() -> AsyncIterator[bytes]:
        for chunk in [b"a", b"b"]:
            consumed.append(chunk)
            yield chunk

    async def close() -> None:
        nonlocal closed
        closed += 1

    payload = AsyncTextStreamPayload(body(), on_close=close)

    await payload.afinalize()
    await payload.afinalize()

    assert consumed == [b"a", b"b"]
    assert closed == 1


@pytest.mark.anyio
async def test_typed_json_transport_validates_pydantic_model() -> None:
    class Payload(BaseModel):
        count: int

    transport = TypedJsonTransport(Payload)
    serialized = transport.serialize(Payload(count=3))
    payload = await transport.deserialize(one_chunk(serialized), content_type=CONTENT_TYPE_JSON)

    assert serialized == b'{"count":3}'
    assert payload == Payload(count=3)
