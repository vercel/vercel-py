"""Process log readers backed by one async-shaped streaming core."""

import inspect
import subprocess
import sys
import threading
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping
from types import TracebackType
from typing import Protocol, TextIO, TypeAlias

import anyio

from vercel._internal.core.http import StreamingResponse
from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel.sandbox._internal.log_stream import _parse_command_log_record
from vercel.sandbox._internal.models import ProcessLogStream

_OpenResponse: TypeAlias = Callable[[], StreamingResponse | Awaitable[StreamingResponse]]


class _TextBuffer:
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

    def take(self, size: int) -> str:
        size = self._size if size < 0 else min(size, self._size)
        remaining = size
        parts: list[str] = []
        while remaining:
            chunk = self._chunks[0]
            available = len(chunk) - self._head
            count = min(remaining, available)
            parts.append(chunk[self._head : self._head + count])
            self._head += count
            remaining -= count
            if self._head == len(chunk):
                self._chunks.popleft()
                self._head = 0
        self._size -= size
        return "".join(parts)

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
    @property
    @abstractmethod
    def closed(self) -> bool: ...

    @abstractmethod
    async def read(self, size: int = -1) -> str: ...

    @abstractmethod
    async def readline(self) -> str: ...


class SyncTextReader(ABC):
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


_TextDestination: TypeAlias = _TextBuffer | TextIO | None


def _distinct_buffers(routes: Mapping[ProcessLogStream, _TextDestination]) -> list[_TextBuffer]:
    return list(
        {
            id(destination): destination
            for destination in routes.values()
            if isinstance(destination, _TextBuffer)
        }.values()
    )


class _PumpLock(Protocol):
    async def acquire(self) -> None: ...

    def release(self) -> None: ...


class _SyncPumpLock:
    def __init__(self) -> None:
        self._lock = threading.Lock()

    async def acquire(self) -> None:
        self._lock.acquire()

    def release(self) -> None:
        self._lock.release()


class _AsyncPumpLock:
    def __init__(self) -> None:
        self._lock = anyio.Lock()

    async def acquire(self) -> None:
        await self._lock.acquire()

    def release(self) -> None:
        self._lock.release()


def _normalize_open_response(
    open_response: _OpenResponse,
) -> Callable[[], Awaitable[StreamingResponse]]:
    async def open_stream() -> StreamingResponse:
        result = open_response()
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, StreamingResponse):
            return result
        raise TypeError("open_response must return an HTTP streaming response")

    return open_stream


class _TextTransportCore:
    def __init__(
        self,
        open_response: Callable[[], Awaitable[StreamingResponse]],
        routes: Mapping[ProcessLogStream, _TextDestination],
        lock: _PumpLock,
    ) -> None:
        self._open_response = open_response
        self._response: StreamingResponse | None = None
        self._lines: AsyncIterator[str] | None = None
        self._routes = dict(routes)
        self._broken = False
        self._eof = False
        self._lock = lock

    async def _cleanup(self) -> None:
        response, self._response = self._response, None
        self._lines = None
        if response is not None:
            await response.aclose()

    async def pump(self) -> None:
        if self._broken:
            raise anyio.BrokenResourceError
        if self._eof:
            return
        await self._lock.acquire()
        try:
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
                        for buffer in _distinct_buffers(self._routes):
                            buffer.eof = True
                        self._eof = True
                        await self._cleanup()
                        return
                    if not line:
                        continue
                    event = _parse_command_log_record(line)
                    if event is not None:
                        target = self._routes[event.stream]
                        if isinstance(target, _TextBuffer):
                            target.append(event.data)
                        elif target is not None:
                            target.write(event.data)
                            target.flush()
                        return
            except BaseException:
                self._broken = True
                for buffer in _distinct_buffers(self._routes):
                    buffer.eof = True
                await self._cleanup()
                raise
        finally:
            self._lock.release()

    async def drain(self) -> None:
        while not self._eof:
            await self.pump()

    async def close(self, buffer: _TextBuffer) -> None:
        await self._lock.acquire()
        try:
            buffer.clear()
            buffer.eof = True
            for stream, target in self._routes.items():
                if target is buffer:
                    self._routes[stream] = None
            if all(target is None for target in self._routes.values()):
                self._eof = True
                await self._cleanup()
        finally:
            self._lock.release()


class _ReaderCore:
    def __init__(self, transport: _TextTransportCore, buffer: _TextBuffer) -> None:
        self.transport = transport
        self.buffer = buffer
        self.closed = False

    def ensure_open(self) -> None:
        if self.closed:
            raise anyio.ClosedResourceError
        if self.transport._broken:
            raise anyio.BrokenResourceError

    async def read(self, size: int = -1) -> str:
        self.ensure_open()
        if size < -1:
            raise ValueError("size must be -1 or non-negative")
        if size == 0:
            return ""
        while not self.buffer.eof and (size < 0 or len(self.buffer) < size):
            await self.transport.pump()
        return self.buffer.take(size)

    async def readline(self) -> str:
        self.ensure_open()
        while True:
            line = self.buffer.take_line()
            if line is not None:
                return line
            await self.transport.pump()

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            await self.transport.close(self.buffer)


class _TextReader(TextReader):
    def __init__(self, core: _ReaderCore) -> None:
        self._core = core
        self._guard = anyio.ResourceGuard("reading from")

    @property
    def closed(self) -> bool:
        return self._core.closed

    async def read(self, size: int = -1) -> str:
        with self._guard:
            return await self._core.read(size)

    async def readline(self) -> str:
        with self._guard:
            return await self._core.readline()

    async def receive(self) -> str:
        line = await self.readline()
        if not line:
            raise anyio.EndOfStream
        return line

    async def aclose(self) -> None:
        with anyio.CancelScope(shield=True):
            await self._core.close()


class _SyncTextReader(SyncTextReader):
    def __init__(self, core: _ReaderCore) -> None:
        self._core = core

    @property
    def closed(self) -> bool:
        return self._core.closed

    def read(self, size: int = -1) -> str:
        return iter_coroutine(self._core.read(size))

    def readline(self) -> str:
        return iter_coroutine(self._core.readline())

    def close(self) -> None:
        iter_coroutine(self._core.close())


def _destination(value: TextIO | int | None, inherited: TextIO) -> _TextDestination:
    if value is None:
        return inherited
    if value == subprocess.PIPE:
        return _TextBuffer()
    if value == subprocess.DEVNULL:
        return None
    if isinstance(value, int):
        raise ValueError(f"unsupported output destination: {value}")
    return value


def _cores(
    open_response: Callable[[], Awaitable[StreamingResponse]],
    stdout: TextIO | int | None,
    stderr: TextIO | int | None,
    lock: _PumpLock,
) -> tuple[_TextTransportCore | None, _ReaderCore | None, _ReaderCore | None]:
    stdout_destination = _destination(stdout, sys.stdout)
    stderr_destination = (
        stdout_destination if stderr == subprocess.STDOUT else _destination(stderr, sys.stderr)
    )
    if stdout_destination is None and stderr_destination is None:
        return None, None, None
    transport = _TextTransportCore(
        open_response,
        {
            ProcessLogStream.STDOUT: stdout_destination,
            ProcessLogStream.STDERR: stderr_destination,
        },
        lock,
    )
    stdout_core = (
        _ReaderCore(transport, stdout_destination)
        if isinstance(stdout_destination, _TextBuffer)
        else None
    )
    stderr_core = (
        _ReaderCore(transport, stderr_destination)
        if isinstance(stderr_destination, _TextBuffer)
        and stderr_destination is not stdout_destination
        else None
    )
    return transport, stdout_core, stderr_core


def _text_transport_and_readers(
    open_response: _OpenResponse,
    *,
    stdout: TextIO | int | None,
    stderr: TextIO | int | None,
) -> tuple[_TextTransportCore | None, TextReader | None, TextReader | None]:
    transport, stdout_core, stderr_core = _cores(
        _normalize_open_response(open_response), stdout, stderr, _AsyncPumpLock()
    )
    return (
        transport,
        None if stdout_core is None else _TextReader(stdout_core),
        None if stderr_core is None else _TextReader(stderr_core),
    )


def _text_readers(
    open_response: _OpenResponse,
    *,
    stdout: TextIO | int | None = subprocess.PIPE,
    stderr: TextIO | int | None = subprocess.PIPE,
) -> tuple[TextReader | None, TextReader | None]:
    _, stdout_reader, stderr_reader = _text_transport_and_readers(
        open_response, stdout=stdout, stderr=stderr
    )
    return stdout_reader, stderr_reader


def _sync_text_transport_and_readers(
    open_response: _OpenResponse,
    *,
    stdout: TextIO | int | None,
    stderr: TextIO | int | None,
) -> tuple[_TextTransportCore | None, SyncTextReader | None, SyncTextReader | None]:
    transport, stdout_core, stderr_core = _cores(
        _normalize_open_response(open_response), stdout, stderr, _SyncPumpLock()
    )
    return (
        transport,
        None if stdout_core is None else _SyncTextReader(stdout_core),
        None if stderr_core is None else _SyncTextReader(stderr_core),
    )


def _sync_text_readers(
    open_response: _OpenResponse,
    *,
    stdout: TextIO | int | None = subprocess.PIPE,
    stderr: TextIO | int | None = subprocess.PIPE,
) -> tuple[SyncTextReader | None, SyncTextReader | None]:
    _, stdout_reader, stderr_reader = _sync_text_transport_and_readers(
        open_response, stdout=stdout, stderr=stderr
    )
    return stdout_reader, stderr_reader
