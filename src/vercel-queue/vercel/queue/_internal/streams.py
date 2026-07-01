from __future__ import annotations

from typing import Any, Concatenate, Generic, ParamSpec, TypeVar

import codecs
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping
from functools import wraps

import anyio
from anyio.abc import ByteReceiveStream
from anyio.streams.buffered import BufferedByteReceiveStream

from .asynctools import iter_async_iterator

_DEFAULT_READ_SIZE = 64 * 1024
_DEFAULT_DELIMITER_LIMIT = 64 * 1024

_P = ParamSpec("_P")
_R = TypeVar("_R")
_T = TypeVar("_T")


class AsyncIteratorByteReceiveStream(ByteReceiveStream):
    """Adapt an async byte iterator to AnyIO's byte receive stream protocol."""

    def __init__(self, stream: AsyncIterator[bytes]) -> None:
        self._stream = stream
        self._buffer = bytearray()
        self._closed = False

    @property
    def extra_attributes(self) -> Mapping[Any, Any]:
        return {}

    async def receive(self, max_bytes: int = 65536) -> bytes:
        if self._closed:
            raise anyio.ClosedResourceError
        if max_bytes < 1:
            raise ValueError("max_bytes must be greater than zero")
        if self._buffer:
            chunk = bytes(self._buffer[:max_bytes])
            del self._buffer[:max_bytes]
            return chunk
        try:
            chunk = await anext(self._stream)
        except StopAsyncIteration:
            raise anyio.EndOfStream from None
        if not chunk:
            return await self.receive(max_bytes)
        if len(chunk) > max_bytes:
            self._buffer.extend(chunk[max_bytes:])
            return chunk[:max_bytes]
        return chunk

    async def aclose(self) -> None:
        self._closed = True
        close = getattr(self._stream, "aclose", None)
        if close is not None:
            await close()


def buffered_byte_receive_stream(
    stream: AsyncIterator[bytes],
) -> BufferedByteReceiveStream:
    return BufferedByteReceiveStream(AsyncIteratorByteReceiveStream(stream))


class _SyncStreamPayloadBase(Generic[_T]):
    def __init__(self, stream: Iterator[_T]) -> None:
        self._stream = stream
        self._consumed = False

    def __iter__(self) -> Iterator[_T]:
        if self._consumed:
            return iter(())
        self._consumed = True
        return self._stream

    def finalize(self) -> None:
        for _ in self._stream:
            pass
        self._consumed = True


class SyncStreamPayload(_SyncStreamPayloadBase[bytes]):
    """One-shot synchronous byte stream for a message body.

    Multipart receive responses expose part bodies as streams. The sync client
    wraps the async body iterator so callers can process large payloads without
    requiring the SDK to buffer them first.
    """


class _ReadGuard:
    def __init__(self) -> None:
        self._reading = False


_S = TypeVar("_S", bound=_ReadGuard)


def _guarded_read(
    func: Callable[Concatenate[_S, _P], Awaitable[_R]],
) -> Callable[Concatenate[_S, _P], Awaitable[_R]]:
    @wraps(func)
    async def wrapper(self: _S, *args: _P.args, **kwargs: _P.kwargs) -> _R:
        if self._reading:
            raise anyio.BusyResourceError(f"reading from {type(self).__name__}")
        self._reading = True
        try:
            return await func(self, *args, **kwargs)
        finally:
            self._reading = False

    return wrapper


def _guarded_iter(
    func: Callable[Concatenate[_S, _P], AsyncIterator[_T]],
) -> Callable[Concatenate[_S, _P], AsyncIterator[_T]]:
    @wraps(func)
    async def wrapper(
        self: _S,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> AsyncIterator[_T]:
        if self._reading:
            raise anyio.BusyResourceError(f"reading from {type(self).__name__}")
        self._reading = True
        try:
            async for item in func(self, *args, **kwargs):
                yield item
        finally:
            self._reading = False

    return wrapper


class AsyncStreamPayload(_ReadGuard):
    """One-shot asynchronous byte stream for a message body.

    Iteration and reader methods share a single cursor over the underlying
    stream. Concurrent reads or iteration are unsupported.
    """

    def __init__(
        self,
        stream: AsyncIterator[bytes],
        *,
        on_close: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        super().__init__()
        self._stream = buffered_byte_receive_stream(stream)
        self._on_close = on_close
        self._closed = False

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self._iterate()

    def to_sync(self) -> SyncStreamPayload:
        return SyncStreamPayload(iter_async_iterator(aiter(self)))

    @_guarded_read
    async def read(self, n: int = -1) -> bytes:
        try:
            if n < 0:
                return await self._read_all()
            if n == 0:
                return b""
            return await self._stream.receive(n)
        except anyio.EndOfStream:
            return b""

    @_guarded_read
    async def readline(self) -> bytes:
        try:
            return await self._stream.receive_until(b"\n", _DEFAULT_DELIMITER_LIMIT) + b"\n"
        except anyio.IncompleteRead:
            partial = self._stream.buffer
            self._stream.feed_data(b"")
            await self._discard_buffer()
            return partial

    async def _read_all(self) -> bytes:
        chunks: list[bytes] = []
        while True:
            try:
                chunk = await self._stream.receive(_DEFAULT_READ_SIZE)
            except anyio.EndOfStream:
                break
            chunks.append(chunk)
        return b"".join(chunks)

    @_guarded_read
    async def readexactly(self, n: int) -> bytes:
        if n < 0:
            raise ValueError("readexactly size can not be less than zero")
        return await self._stream.receive_exactly(n)

    @_guarded_read
    async def readuntil(self, separator: bytes = b"\n") -> bytes:
        if not separator:
            raise ValueError("Separator should be at least one-byte string")
        return await self._stream.receive_until(separator, _DEFAULT_DELIMITER_LIMIT) + separator

    @_guarded_read
    async def afinalize(self) -> None:
        if self._closed:
            return
        try:
            while True:
                try:
                    await self._stream.receive(_DEFAULT_READ_SIZE)
                except anyio.EndOfStream:
                    break
        finally:
            self._closed = True
            if self._on_close is not None:
                await self._on_close()

    @_guarded_iter
    async def _iterate(self) -> AsyncIterator[bytes]:
        while True:
            try:
                yield await self._stream.receive(_DEFAULT_READ_SIZE)
            except anyio.EndOfStream:
                break

    async def _discard_buffer(self) -> None:
        if not self._stream.buffer:
            return
        await self._stream.receive(len(self._stream.buffer))


class SyncTextStreamPayload(_SyncStreamPayloadBase[str]):
    """One-shot synchronous text stream for a UTF-8 message body."""


class AsyncTextStreamPayload(_ReadGuard):
    """One-shot asynchronous text stream for a UTF-8 message body."""

    def __init__(
        self,
        stream: AsyncIterator[bytes] | AsyncStreamPayload,
        *,
        on_close: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        if isinstance(stream, AsyncStreamPayload):
            self._stream = stream
        else:
            self._stream = AsyncStreamPayload(stream, on_close=on_close)
        super().__init__()
        self._decoder = codecs.getincrementaldecoder("utf-8")()
        self._buffer = ""
        self._eof = False
        self._closed = False

    def __aiter__(self) -> AsyncIterator[str]:
        return self._iterate()

    def to_sync(self) -> SyncTextStreamPayload:
        return SyncTextStreamPayload(iter_async_iterator(aiter(self)))

    @_guarded_read
    async def read(self, n: int = -1) -> str:
        if n == 0:
            return ""
        if n < 0:
            return await self._read_all()
        while len(self._buffer) < n:
            text = await self._read_next_text()
            if not text:
                break
            self._buffer += text
        data = self._buffer[:n]
        self._buffer = self._buffer[n:]
        return data

    async def afinalize(self) -> None:
        if self._closed:
            return
        try:
            await self._stream.afinalize()
        finally:
            self._closed = True

    @_guarded_iter
    async def _iterate(self) -> AsyncIterator[str]:
        if self._buffer:
            yield self._read_buffered_text()
        while text := await self._read_next_text():
            yield text

    async def _read_all(self) -> str:
        chunks = [self._read_buffered_text()] if self._buffer else []
        while text := await self._read_next_text():
            chunks.append(text)
        return "".join(chunks)

    def _read_buffered_text(self) -> str:
        text = self._buffer
        self._buffer = ""
        return text

    async def _read_next_text(self) -> str:
        if self._eof:
            return ""
        while chunk := await self._stream.read(_DEFAULT_READ_SIZE):
            text = self._decoder.decode(chunk)
            if text:
                return text
        self._eof = True
        return self._decoder.decode(b"", final=True)
