from __future__ import annotations

from typing import TYPE_CHECKING
from typing_extensions import Self

from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass

from python_multipart.exceptions import MultipartParseError
from python_multipart.multipart import (
    MultipartParser,
    parse_options_header,
)

from .errors import ProtocolError
from .http import AsyncHttpResponse

if TYPE_CHECKING:
    from collections.abc import Mapping


DEFAULT_MAX_HEADER_BUFFER_SIZE = 64 * 1024
DEFAULT_MAX_BOUNDARY_BUFFER_SIZE = 8 * 1024
DEFAULT_MAX_HEADER_COUNT = 64
MULTIPART_MIXED_MEDIA_TYPE = b"multipart/mixed"


class _PartBody:
    """Async iterator over a single multipart part body.

    The parser feeds this object as bytes arrive. If the consumer asks for more
    bytes before the current part has ended, ``PartBody`` pumps the parent
    parser by one input chunk.
    """

    def __init__(self, pump: Callable[[], Awaitable[None]]) -> None:
        self._pump = pump
        self._chunks: deque[bytes] = deque()
        self._done = False

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> bytes:
        while not self._chunks and not self._done:
            await self._pump()
        if self._chunks:
            return self._chunks.popleft()
        raise StopAsyncIteration

    async def drain(self) -> None:
        """Consume and discard unread bytes for this part."""
        async for _ in self:
            pass

    def feed(self, chunk: bytes) -> None:
        if chunk:
            self._chunks.append(chunk)

    def feed_slice(self, data: bytes, start: int, end: int) -> None:
        if start == end:
            return
        if start == 0 and end == len(data):
            self._chunks.append(data)
            return
        self._chunks.append(data[start:end])

    def close(self) -> None:
        self._done = True


@dataclass(frozen=True, kw_only=True)
class _Part:
    """A streamed multipart part with parsed headers and body iterator."""

    headers: dict[str, str]
    body: _PartBody


class _MultipartParser:
    """Incremental multipart/mixed parser.

    python-multipart is callback-based and synchronous. This wrapper turns it
    into an async iterator of parts while preserving streaming behavior: a part
    is yielded as soon as its headers are complete, and its body continues to be
    filled as the caller iterates it.
    """

    def __init__(
        self,
        data: AsyncIterator[bytes],
        /,
        *,
        content_type: str,
    ) -> None:
        self._input = data
        self._max_header_buffer_size = DEFAULT_MAX_HEADER_BUFFER_SIZE

        self._parts: deque[_Part] = deque()

        self._headers: dict[str, str] = {}
        self._field = bytearray()
        self._value = bytearray()
        self._header_bytes = 0
        self._body: _PartBody | None = None
        self._yielded_body: _PartBody | None = None

        self._ended = False
        self._exhausted = False

        self._parser = MultipartParser(
            _multipart_boundary(content_type),
            max_size=float("inf"),
            max_header_count=DEFAULT_MAX_HEADER_COUNT,
            max_header_size=self._max_header_buffer_size,
            callbacks={
                "on_header_begin": self._on_header_begin,
                "on_header_field": self._on_header_field,
                "on_header_value": self._on_header_value,
                "on_header_end": self._on_header_end,
                "on_headers_finished": self._on_headers_finished,
                "on_part_data": self._on_part_data,
                "on_part_end": self._on_part_end,
                "on_end": self._on_end,
            },
        )

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> tuple[Mapping[str, str], AsyncIterator[bytes]]:
        if self._yielded_body is not None:
            await self._yielded_body.drain()
            self._yielded_body = None

        while not self._parts:
            if self._ended:
                raise StopAsyncIteration
            await self._advance()
        part = self._parts.popleft()
        self._yielded_body = part.body
        return part.headers, part.body

    async def _advance(self) -> None:
        if self._exhausted:
            if self._ended:
                return
            raise ProtocolError("unexpected end of multipart stream")
        try:
            chunk = await anext(self._input)
        except StopAsyncIteration:
            self._exhausted = True
            if not self._ended:
                raise ProtocolError("unexpected end of multipart stream") from None
            return
        try:
            self._parser.write(chunk)
        except ProtocolError:
            raise
        except MultipartParseError as exc:
            if "Maximum segment count exceeded" in str(exc):
                raise ProtocolError(
                    f"multipart header count exceeded {DEFAULT_MAX_HEADER_COUNT}"
                ) from exc
            if "Maximum header size exceeded" in str(exc):
                raise ProtocolError(
                    f"multipart header buffer exceeded {self._max_header_buffer_size} bytes"
                ) from exc
            raise ProtocolError(f"invalid multipart stream: {exc}") from exc

    def _on_header_begin(self) -> None:
        self._field.clear()
        self._value.clear()

    def _on_header_field(self, data: bytes, start: int, end: int) -> None:
        self._add_header_bytes(end - start)
        self._field.extend(data[start:end])

    def _on_header_value(self, data: bytes, start: int, end: int) -> None:
        self._add_header_bytes(end - start)
        self._value.extend(data[start:end])

    def _on_header_end(self) -> None:
        name = self._field.decode("latin1").strip()
        if name:
            self._headers[name] = self._value.decode("latin1").strip()

    def _on_headers_finished(self) -> None:
        self._body = _PartBody(self._advance)
        part = _Part(headers=dict(self._headers), body=self._body)
        self._headers.clear()
        self._header_bytes = 0
        self._parts.append(part)

    def _on_part_data(self, data: bytes, start: int, end: int) -> None:
        if self._body is None:
            raise RuntimeError("MultipartParser.on_part_data called out of order")
        self._body.feed_slice(data, start, end)

    def _on_part_end(self) -> None:
        if self._body is None:
            raise RuntimeError("MultipartParser.on_part_end called out of order")
        self._body.close()
        self._body = None

    def _on_end(self) -> None:
        self._ended = True

    def _add_header_bytes(self, size: int) -> None:
        self._header_bytes += size
        if self._header_bytes > self._max_header_buffer_size:
            raise ProtocolError(
                f"multipart header buffer exceeded {self._max_header_buffer_size} bytes"
            )


def parse_multipart_messages(
    response: AsyncHttpResponse,
) -> AsyncIterator[tuple[Mapping[str, str], AsyncIterator[bytes]]]:
    """Stream multipart parts from an HTTP response-like.

    The response must have a ``multipart/mixed`` Content-Type with a boundary.
    Each yielded part exposes its headers and an async byte iterator for the
    body. If a caller advances to the next part without consuming the current
    body, the parser drains the current body first.

    Raises:
        ProtocolError on malformed headers or other response deformity.

    """
    content_type = response.headers.get("Content-Type", "")
    return _MultipartParser(
        response.aiter_bytes(),
        content_type=content_type,
    )


def _multipart_boundary(content_type: str) -> bytes:
    media_type, options = parse_options_header(content_type)
    if media_type.lower() != MULTIPART_MIXED_MEDIA_TYPE:
        raise ProtocolError(
            f"expected a multipart/mixed response, got Content-Type={content_type!r}"
        )
    value = options.get(b"boundary")
    if not value:
        raise ProtocolError("response did not specify a valid multipart boundary")
    value = value.strip()
    if not value:
        raise ProtocolError("response did not specify a valid multipart boundary")
    if len(value) > DEFAULT_MAX_BOUNDARY_BUFFER_SIZE:
        raise ProtocolError(
            f"multipart boundary buffer exceeded {DEFAULT_MAX_BOUNDARY_BUFFER_SIZE} bytes"
        )
    return value
