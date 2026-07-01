from __future__ import annotations

from typing import Any, ClassVar

from collections.abc import AsyncIterator

import anyio
import anyio.lowlevel
import pytest

from vercel.queue import ProtocolError
from vercel.queue._internal.multipart import (
    DEFAULT_MAX_BOUNDARY_BUFFER_SIZE,
    DEFAULT_MAX_HEADER_BUFFER_SIZE,
    DEFAULT_MAX_HEADER_COUNT,
    parse_multipart_messages,
)


class Response:
    status_code = 200
    text = ""

    def __init__(self, content: bytes | list[bytes], content_type: str) -> None:
        self.headers = {"Content-Type": content_type}
        self._chunks = content if isinstance(content, list) else [content]

    async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk

    def json(self) -> Any:
        raise NotImplementedError


async def collect(
    content: bytes | str | list[bytes],
    content_type: str,
) -> list[tuple[dict[str, str], bytes]]:
    if isinstance(content, str):
        content = content.encode()
    messages: list[tuple[dict[str, str], bytes]] = []
    async for headers, body in parse_multipart_messages(Response(content, content_type)):
        messages.append((dict(headers), await collect_payload(body)))
    return messages


@pytest.mark.anyio
async def test_extracts_quoted_unquoted_and_case_insensitive_boundary() -> None:
    content = b"--boundary\r\nContent-Type: text/plain\r\n\r\nok\r\n--boundary--"
    for content_type in (
        'multipart/mixed; boundary="boundary"',
        "multipart/mixed; boundary=boundary",
        'multipart/mixed; BOUNDARY="boundary"',
        'multipart/mixed; charset=utf-8; boundary="boundary"',
    ):
        assert await collect(content, content_type) == [({"Content-Type": "text/plain"}, b"ok")]


@pytest.mark.anyio
async def test_rejects_missing_or_invalid_boundary() -> None:
    with pytest.raises(ProtocolError, match="Content-Type"):
        await collect("test", "text/plain")
    with pytest.raises(ProtocolError, match="valid multipart boundary"):
        await collect("test", "multipart/mixed")
    with pytest.raises(ProtocolError, match="valid multipart boundary"):
        await collect("test", 'multipart/mixed; boundary=""')


@pytest.mark.anyio
async def test_rejects_boundary_over_default_limit() -> None:
    boundary = "b" * (DEFAULT_MAX_BOUNDARY_BUFFER_SIZE + 1)
    response = Response(
        b"test",
        f"multipart/mixed; boundary={boundary}",
    )

    with pytest.raises(ProtocolError, match="boundary buffer exceeded"):
        async for _headers, _body in parse_multipart_messages(response):
            pass


@pytest.mark.anyio
async def test_rejects_headers_over_default_limit() -> None:
    header_value = b"x" * (DEFAULT_MAX_HEADER_BUFFER_SIZE + 1)
    response = Response(
        b"\r\n".join([
            b"--boundary",
            b"Content-Type: text/plain",
            b"X-Test: " + header_value,
            b"",
            b"ok",
            b"--boundary--",
        ]),
        "multipart/mixed; boundary=boundary",
    )

    with pytest.raises(ProtocolError, match="header buffer exceeded"):
        async for _headers, _body in parse_multipart_messages(response):
            pass


@pytest.mark.anyio
async def test_rejects_header_count_over_default_limit() -> None:
    headers = [f"X-Test-{index}: ok".encode() for index in range(DEFAULT_MAX_HEADER_COUNT + 1)]
    response = Response(
        b"\r\n".join([
            b"--boundary",
            *headers,
            b"",
            b"ok",
            b"--boundary--",
        ]),
        "multipart/mixed; boundary=boundary",
    )

    with pytest.raises(ProtocolError, match="header count exceeded"):
        async for _headers, _body in parse_multipart_messages(response):
            pass


@pytest.mark.anyio
async def test_parses_multiple_messages_and_binary_payloads() -> None:
    binary = b"\x00\x01\x02\x03\xff\xfe\xfd"
    content = b"\r\n".join([
        b"--boundary",
        b"Content-Type: application/json",
        b"X-Message-Id: msg-1",
        b"",
        b'{"message": 1}',
        b"--boundary",
        b"Content-Type: application/octet-stream",
        b"X-Message-Id: msg-2",
        b"",
        binary,
        b"--boundary--",
    ])

    assert await collect(content, "multipart/mixed; boundary=boundary") == [
        (
            {"Content-Type": "application/json", "X-Message-Id": "msg-1"},
            b'{"message": 1}',
        ),
        ({"Content-Type": "application/octet-stream", "X-Message-Id": "msg-2"}, binary),
    ]


@pytest.mark.anyio
async def test_handles_boundary_spanning_chunks() -> None:
    chunks = [
        b"--boundary\r\nContent-Type: text/plain\r\nX-Test: chunked\r\n\r\nHello",
        b" Wor",
        b"ld\r\n--boun",
        b"dary--",
    ]

    assert await collect(chunks, "multipart/mixed; boundary=boundary") == [
        ({"Content-Type": "text/plain", "X-Test": "chunked"}, b"Hello World")
    ]


@pytest.mark.anyio
async def test_rejects_malformed_and_incomplete_streams() -> None:
    with pytest.raises(ProtocolError, match="invalid multipart stream"):
        await collect(
            "Content-Type: text/plain\r\n\r\nNo boundary here",
            "multipart/mixed; boundary=boundary",
        )

    with pytest.raises(ProtocolError, match="unexpected end of multipart stream"):
        await collect(
            "--boundary\r\nContent-Type: text/plain\r\n\r\nIncomplete data",
            "multipart/mixed; boundary=boundary",
        )


@pytest.mark.anyio
async def test_stream_yields_part_before_full_payload_arrives() -> None:
    gate = anyio.Event()
    requested_chunks = 0

    class StreamingResponse:
        status_code = 200
        headers: ClassVar[dict[str, str]] = {"Content-Type": "multipart/mixed; boundary=boundary"}
        text = ""

        async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
            nonlocal requested_chunks
            requested_chunks += 1
            yield b"--boundary\r\nContent-Type: text/plain\r\nX-Test: streaming\r\n\r\nhel"
            await gate.wait()
            requested_chunks += 1
            yield b"lo\r\n--boundary--"

        def json(self) -> Any:
            raise NotImplementedError

    stream = parse_multipart_messages(StreamingResponse())
    headers, body = await anext(stream)

    assert headers == {"Content-Type": "text/plain", "X-Test": "streaming"}
    assert requested_chunks == 1

    result: bytes | None = None
    async with anyio.create_task_group() as task_group:

        async def collect_body() -> None:
            nonlocal result
            result = await collect_payload(body)

        task_group.start_soon(collect_body)
        await anyio.lowlevel.checkpoint()
        assert result is None
        gate.set()
    assert result == b"hello"


@pytest.mark.anyio
async def test_preserves_full_range_payload_chunk_without_copy() -> None:
    payload = b"hello"

    class StreamingResponse:
        status_code = 200
        headers: ClassVar[dict[str, str]] = {"Content-Type": "multipart/mixed; boundary=boundary"}
        text = ""

        async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
            del chunk_size
            yield b"--boundary\r\nContent-Type: text/plain\r\n\r\n"
            yield payload
            yield b"\r\n--boundary--"

        def json(self) -> Any:
            raise NotImplementedError

    stream = parse_multipart_messages(StreamingResponse())
    _headers, body = await anext(stream)
    chunk = await anext(body)

    assert chunk is payload


async def collect_payload(payload: AsyncIterator[bytes]) -> bytes:
    chunks = bytearray()
    async for chunk in payload:
        chunks.extend(chunk)
    return bytes(chunks)
