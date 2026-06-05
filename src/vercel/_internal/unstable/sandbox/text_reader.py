"""Text reader contracts and private process log stream implementations."""

from abc import ABC, abstractmethod
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from types import TracebackType

import anyio
import httpx

from vercel._internal.unstable.sandbox.log_stream import _parse_command_log_record
from vercel._internal.unstable.sandbox.models import ProcessLogStream


class _TextBuffer:
    __slots__ = ("_chunks", "_head", "_size", "eof")

    def __init__(self) -> None:
        self._chunks: deque[str] = deque()
        self._head = 0
        self._size = 0
        self.eof = False

    def __len__(self) -> int:
        return self._size

    def append(self, value: str) -> None:
        if value:
            self._chunks.append(value)
            self._size += len(value)

    def clear(self) -> None:
        self._chunks.clear()
        self._head = 0
        self._size = 0

    def _prefix(self, size: int) -> str:
        remaining = size
        parts: list[str] = []
        for index, chunk in enumerate(self._chunks):
            start = self._head if index == 0 else 0
            part = chunk[start : start + remaining]
            parts.append(part)
            remaining -= len(part)
            if remaining == 0:
                break
        return "".join(parts)

    def take(self, size: int) -> str:
        size = self._size if size < 0 else min(size, self._size)
        value = self._prefix(size)
        remaining = size
        while remaining:
            chunk = self._chunks[0]
            available = len(chunk) - self._head
            if remaining < available:
                self._head += remaining
                remaining = 0
            else:
                remaining -= available
                self._chunks.popleft()
                self._head = 0
        self._size -= size
        return value

    def take_line(self) -> str | None:
        seen = 0
        for index, chunk in enumerate(self._chunks):
            start = self._head if index == 0 else 0
            newline = chunk.find("\n", start)
            if newline >= 0:
                return self.take(seen + newline - start + 1)
            seen += len(chunk) - start
        if self.eof:
            return self.take(-1)
        return None


class TextReader(anyio.abc.ObjectReceiveStream[str], ABC):
    """A one-shot asynchronous reader for one process text stream."""

    @property
    @abstractmethod
    def closed(self) -> bool: ...

    @abstractmethod
    async def read(self, size: int = -1) -> str: ...

    @abstractmethod
    async def readline(self) -> str: ...


class SyncTextReader(ABC):
    """A one-shot synchronous reader for one process text stream."""

    @property
    @abstractmethod
    def closed(self) -> bool: ...

    @abstractmethod
    def read(self, size: int = -1) -> str: ...

    @abstractmethod
    def readline(self) -> str: ...

    @abstractmethod
    def close(self) -> None: ...

    def __iter__(self) -> Iterator[str]:
        while line := self.readline():
            yield line

    def __enter__(self) -> "SyncTextReader":
        if self.closed:
            raise anyio.ClosedResourceError
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


class _AsyncTextTransport:
    __slots__ = ("_broken", "_buffers", "_closed", "_lines", "_lock", "_open_response", "_response")

    def __init__(self, open_response: Callable[[], Awaitable[httpx.Response]]) -> None:
        self._open_response = open_response
        self._response: httpx.Response | None = None
        self._lines: AsyncIterator[str] | None = None
        self._buffers = {
            ProcessLogStream.STDOUT: _TextBuffer(),
            ProcessLogStream.STDERR: _TextBuffer(),
        }
        self._closed: set[ProcessLogStream] = set()
        self._broken = False
        self._lock = anyio.Lock()

    async def _cleanup(self) -> None:
        response, self._response = self._response, None
        self._lines = None
        if response is not None:
            with anyio.CancelScope(shield=True):
                await response.aclose()

    async def pump(self) -> None:
        # Fail fast without acquiring a lock
        if self._broken:
            raise anyio.BrokenResourceError
        async with self._lock:
            if self._broken:
                raise anyio.BrokenResourceError
            try:
                if self._response is None:
                    self._response = await self._open_response()
                    self._lines = self._response.aiter_lines()
                assert self._lines is not None
                while True:
                    try:
                        line = await anext(self._lines)
                    except StopAsyncIteration:
                        for buffer in self._buffers.values():
                            buffer.eof = True
                        await self._cleanup()
                        return
                    if not line:
                        continue
                    event = _parse_command_log_record(line)
                    if event is not None:
                        if event.stream not in self._closed:
                            self._buffers[event.stream].append(event.data)
                        return
            except BaseException:
                self._broken = True
                for buffer in self._buffers.values():
                    buffer.eof = True
                await self._cleanup()
                raise

    async def close(self, stream: ProcessLogStream) -> None:
        self._closed.add(stream)
        self._buffers[stream].clear()
        self._buffers[stream].eof = True
        if len(self._closed) == 2:
            await self._cleanup()


class _TextReader(TextReader):
    __slots__ = ("_buffer", "_closed", "_guard", "_stream", "_transport")

    def __init__(self, transport: _AsyncTextTransport, stream: ProcessLogStream) -> None:
        self._transport = transport
        self._stream = stream
        self._buffer = transport._buffers[stream]
        self._closed = False
        self._guard = anyio.ResourceGuard("reading from")

    @property
    def closed(self) -> bool:
        return self._closed

    def _ensure_open(self) -> None:
        if self._closed:
            raise anyio.ClosedResourceError
        if self._transport._broken:
            raise anyio.BrokenResourceError

    async def read(self, size: int = -1) -> str:
        self._ensure_open()
        if size < -1:
            raise ValueError("size must be -1 or non-negative")
        if size == 0:
            return ""
        with self._guard:
            while not self._buffer.eof and (size < 0 or len(self._buffer) < size):
                await self._transport.pump()
            return self._buffer.take(size)

    async def readline(self) -> str:
        self._ensure_open()
        with self._guard:
            while True:
                line = self._buffer.take_line()
                if line is not None:
                    return line
                await self._transport.pump()

    async def receive(self) -> str:
        line = await self.readline()
        if not line:
            raise anyio.EndOfStream
        return line

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            await self._transport.close(self._stream)


class _SyncTextTransport:
    __slots__ = ("_broken", "_buffers", "_closed", "_lines", "_open_response", "_response")

    def __init__(self, open_response: Callable[[], httpx.Response]) -> None:
        self._open_response = open_response
        self._response: httpx.Response | None = None
        self._lines: Iterator[str] | None = None
        self._buffers = {
            ProcessLogStream.STDOUT: _TextBuffer(),
            ProcessLogStream.STDERR: _TextBuffer(),
        }
        self._closed: set[ProcessLogStream] = set()
        self._broken = False

    def _cleanup(self) -> None:
        response, self._response = self._response, None
        self._lines = None
        if response is not None:
            response.close()

    def pump(self) -> None:
        if self._broken:
            raise anyio.BrokenResourceError
        try:
            if self._response is None:
                self._response = self._open_response()
                self._lines = self._response.iter_lines()
            assert self._lines is not None
            while True:
                try:
                    line = next(self._lines)
                except StopIteration:
                    for buffer in self._buffers.values():
                        buffer.eof = True
                    self._cleanup()
                    return
                if not line:
                    continue
                event = _parse_command_log_record(line)
                if event is not None:
                    if event.stream not in self._closed:
                        self._buffers[event.stream].append(event.data)
                    return
        except BaseException:
            self._broken = True
            for buffer in self._buffers.values():
                buffer.eof = True
            self._cleanup()
            raise

    def close(self, stream: ProcessLogStream) -> None:
        self._closed.add(stream)
        self._buffers[stream].clear()
        self._buffers[stream].eof = True
        if len(self._closed) == 2:
            self._cleanup()


class _SyncTextReader(SyncTextReader):
    __slots__ = ("_buffer", "_closed", "_guard", "_stream", "_transport")

    def __init__(self, transport: _SyncTextTransport, stream: ProcessLogStream) -> None:
        self._transport = transport
        self._stream = stream
        self._buffer = transport._buffers[stream]
        self._closed = False
        self._guard = anyio.ResourceGuard("reading from")

    @property
    def closed(self) -> bool:
        return self._closed

    def _ensure_open(self) -> None:
        if self._closed:
            raise anyio.ClosedResourceError
        if self._transport._broken:
            raise anyio.BrokenResourceError

    def read(self, size: int = -1) -> str:
        self._ensure_open()
        if size < -1:
            raise ValueError("size must be -1 or non-negative")
        if size == 0:
            return ""
        with self._guard:
            while not self._buffer.eof and (size < 0 or len(self._buffer) < size):
                self._transport.pump()
            return self._buffer.take(size)

    def readline(self) -> str:
        self._ensure_open()
        with self._guard:
            while True:
                line = self._buffer.take_line()
                if line is not None:
                    return line
                self._transport.pump()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._transport.close(self._stream)


def _text_readers(
    open_response: Callable[[], Awaitable[httpx.Response]],
) -> tuple[TextReader, TextReader]:
    transport = _AsyncTextTransport(open_response)
    return (
        _TextReader(transport, ProcessLogStream.STDOUT),
        _TextReader(transport, ProcessLogStream.STDERR),
    )


def _sync_text_readers(
    open_response: Callable[[], httpx.Response],
) -> tuple[SyncTextReader, SyncTextReader]:
    transport = _SyncTextTransport(open_response)
    return (
        _SyncTextReader(transport, ProcessLogStream.STDOUT),
        _SyncTextReader(transport, ProcessLogStream.STDERR),
    )
