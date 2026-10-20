"""Process log readers backed by one async-shaped streaming core."""

import inspect
import subprocess
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterator, Mapping
from types import TracebackType
from typing import Protocol, TypeAlias

import anyio

from vercel._internal.core.http import StreamingResponse
from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel.sandbox._internal.log_stream import AsyncSleep, _iter_command_logs
from vercel.sandbox._internal.models import ProcessLog, ProcessLogStream

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


def _distinct_buffers(routes: Mapping[ProcessLogStream, _TextBuffer | None]) -> list[_TextBuffer]:
    return list({id(buffer): buffer for buffer in routes.values() if buffer is not None}.values())


class _PumpCondition(Protocol):
    """A lock over shared reader state plus a wait for that state to change."""

    async def acquire(self) -> None: ...

    def release(self) -> None: ...

    async def wait(self) -> None: ...

    def notify_all(self) -> None: ...


class _SyncPumpCondition:
    def __init__(self) -> None:
        self._condition = threading.Condition()

    async def acquire(self) -> None:
        self._condition.acquire()

    def release(self) -> None:
        self._condition.release()

    async def wait(self) -> None:
        self._condition.wait()

    def notify_all(self) -> None:
        self._condition.notify_all()


class _AsyncPumpCondition:
    def __init__(self) -> None:
        self._condition = anyio.Condition()

    async def acquire(self) -> None:
        # The lock is never held across stream reads, so acquiring it is brief,
        # and shielding lets a cancelled pump take it back to record the break.
        with anyio.CancelScope(shield=True):
            await self._condition.acquire()

    def release(self) -> None:
        self._condition.release()

    async def wait(self) -> None:
        await self._condition.wait()

    def notify_all(self) -> None:
        self._condition.notify_all()


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
    """Route one shared log stream into per-reader buffers.

    One reader at a time pumps the stream, reading records without holding the
    condition. Other readers wait on the condition and recheck their own buffer
    after each record is routed, so a record routed on a reader's behalf is
    returned to it without waiting for further output.
    """

    def __init__(
        self,
        open_response: Callable[[], Awaitable[StreamingResponse]],
        routes: Mapping[ProcessLogStream, _TextBuffer | None],
        condition: _PumpCondition,
        sleep: AsyncSleep,
    ) -> None:
        self._open_response = open_response
        self._sleep = sleep
        self._events: AsyncGenerator[ProcessLog, None] | None = None
        self._routes = dict(routes)
        self._live = len(_distinct_buffers(routes))
        self._broken = False
        self._pumping = False
        self._condition = condition

    async def _cleanup(self) -> None:
        events, self._events = self._events, None
        if events is not None:
            await events.aclose()

    def _mark_eof(self) -> None:
        for buffer in _distinct_buffers(self._routes):
            buffer.eof = True

    async def receive(self, take: Callable[[], str | None]) -> str:
        """Return the first non-``None`` result of ``take``, pumping as needed."""
        await self._condition.acquire()
        try:
            while True:
                if self._broken:
                    raise anyio.BrokenResourceError
                result = take()
                if result is not None:
                    return result
                if self._pumping:
                    await self._condition.wait()
                else:
                    await self._pump()
        finally:
            self._condition.release()

    async def _pump(self) -> None:
        # Entered and exited holding the condition, which is released while
        # waiting on the stream so other readers can take what is routed.
        self._pumping = True
        if self._events is None:
            self._events = _iter_command_logs(self._open_response, sleep=self._sleep)
        events = self._events
        self._condition.release()
        try:
            try:
                event: ProcessLog | None = await anext(events)
            except StopAsyncIteration:
                event = None
        except BaseException:
            await self._condition.acquire()
            self._broken = True
            self._mark_eof()
            await self._cleanup()
            raise
        else:
            await self._condition.acquire()
            if event is None:
                self._mark_eof()
                await self._cleanup()
            elif (target := self._routes[event.stream]) is not None:
                target.append(event.data)
        finally:
            self._pumping = False
            self._condition.notify_all()

    async def close(self, buffer: _TextBuffer) -> None:
        await self._condition.acquire()
        try:
            buffer.clear()
            buffer.eof = True
            for stream, target in self._routes.items():
                if target is buffer:
                    self._routes[stream] = None
            self._live -= 1
            if self._live == 0:
                while self._pumping:
                    await self._condition.wait()
                await self._cleanup()
        finally:
            self._condition.release()


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
        buffer = self.buffer

        def take() -> str | None:
            if buffer.eof or (size >= 0 and len(buffer) >= size):
                return buffer.take(size)
            return None

        return await self.transport.receive(take)

    async def readline(self) -> str:
        self.ensure_open()
        return await self.transport.receive(self.buffer.take_line)

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


async def _blocking_sleep(seconds: float) -> None:
    time.sleep(seconds)


def _reader_buffers(stdout: int, stderr: int) -> tuple[_TextBuffer | None, _TextBuffer | None]:
    stdout_buffer = _TextBuffer() if stdout == subprocess.PIPE else None
    if stderr == subprocess.STDOUT:
        stderr_buffer = stdout_buffer
    elif stderr == subprocess.PIPE:
        stderr_buffer = _TextBuffer()
    else:
        stderr_buffer = None
    return stdout_buffer, stderr_buffer


def _cores(
    open_response: Callable[[], Awaitable[StreamingResponse]],
    stdout: int,
    stderr: int,
    condition: _PumpCondition,
    sleep: AsyncSleep,
) -> tuple[_ReaderCore | None, _ReaderCore | None]:
    stdout_buffer, stderr_buffer = _reader_buffers(stdout, stderr)
    if stdout_buffer is None and stderr_buffer is None:
        return None, None
    transport = _TextTransportCore(
        open_response,
        {ProcessLogStream.STDOUT: stdout_buffer, ProcessLogStream.STDERR: stderr_buffer},
        condition,
        sleep,
    )
    return (
        None if stdout_buffer is None else _ReaderCore(transport, stdout_buffer),
        None
        if stderr_buffer is None or stderr_buffer is stdout_buffer
        else _ReaderCore(transport, stderr_buffer),
    )


def _text_readers(
    open_response: _OpenResponse,
    *,
    stdout: int = subprocess.PIPE,
    stderr: int = subprocess.PIPE,
    sleep: AsyncSleep = anyio.sleep,
) -> tuple[TextReader | None, TextReader | None]:
    stdout_core, stderr_core = _cores(
        _normalize_open_response(open_response), stdout, stderr, _AsyncPumpCondition(), sleep
    )
    return (
        None if stdout_core is None else _TextReader(stdout_core),
        None if stderr_core is None else _TextReader(stderr_core),
    )


def _sync_text_readers(
    open_response: _OpenResponse,
    *,
    stdout: int = subprocess.PIPE,
    stderr: int = subprocess.PIPE,
    sleep: AsyncSleep = _blocking_sleep,
) -> tuple[SyncTextReader | None, SyncTextReader | None]:
    stdout_core, stderr_core = _cores(
        _normalize_open_response(open_response), stdout, stderr, _SyncPumpCondition(), sleep
    )
    return (
        None if stdout_core is None else _SyncTextReader(stdout_core),
        None if stderr_core is None else _SyncTextReader(stderr_core),
    )
