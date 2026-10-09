"""Shared command stream decoding and connection recovery for Sandbox streams.

Command output reaches the SDK over long-lived NDJSON responses that the
network can cut off at any point. The logs endpoint replays a command's output
from the beginning on every connection, so an interrupted stream is resumed by
reconnecting and skipping the output that was already delivered.
"""

import codecs
import json
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from typing import TypeVar

import httpx2 as httpx

from vercel._internal.core.http import StreamingResponse
from vercel.sandbox._internal.errors import (
    SandboxApiError,
    SandboxResponseError,
    SandboxStreamError,
)
from vercel.sandbox._internal.models import ProcessLog, ProcessLogStream

AsyncSleep = Callable[[float], Awaitable[None]]

# Retries after the first attempt, with exponential backoff between attempts.
_RETRIES = 2
_RETRY_BACKOFF_SECONDS = 0.2

_ResultT = TypeVar("_ResultT")


class _TruncatedStreamError(SandboxResponseError):
    """A stream ended before a complete record or its final metadata arrived."""


def _is_transient_error(error: BaseException) -> bool:
    """Return whether retrying the failed request may succeed.

    Network failures and streams that end mid-record are connection problems
    rather than answers from the API, and rate limits and server errors are
    expected to clear up. Cancellation is never an ``Exception`` and so is
    never retried.
    """
    if isinstance(error, httpx.TransportError | _TruncatedStreamError):
        return True
    return isinstance(error, SandboxApiError) and (
        error.status_code == 429 or error.status_code >= 500
    )


def _retry_delay(failures: int) -> float:
    return _RETRY_BACKOFF_SECONDS * 2**failures


async def _with_transient_retries(
    operation: Callable[[], Awaitable[_ResultT]],
    *,
    sleep: AsyncSleep,
) -> _ResultT:
    """Run an idempotent request, retrying transient failures a bounded number of times."""
    failures = 0
    while True:
        try:
            return await operation()
        except Exception as error:
            if failures >= _RETRIES or not _is_transient_error(error):
                raise
        await sleep(_retry_delay(failures))
        failures += 1


async def _ndjson_lines(response: StreamingResponse) -> AsyncIterator[tuple[str, bool]]:
    """Yield decoded NDJSON lines and whether each one was newline-terminated.

    Only a line cut short by the end of the response can be unterminated, which
    lets callers tell a complete final record from a truncated one.
    """
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    pending: list[str] = []
    async for chunk in response:
        *lines, rest = decoder.decode(chunk).split("\n")
        for line in lines:
            pending.append(line)
            yield "".join(pending), True
            pending.clear()
        if rest:
            pending.append(rest)
    pending.append(decoder.decode(b"", final=True))
    if tail := "".join(pending):
        yield tail, False


def _parse_command_log_record(line: str, *, terminated: bool = True) -> ProcessLog | None:
    """Decode one wire log record, skipping unsupported or malformed input.

    Raises:
        SandboxStreamError: If the record reports an in-band stream failure.
        _TruncatedStreamError: If an unterminated final record is incomplete.
    """
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        if not terminated:
            raise _TruncatedStreamError(
                "Sandbox log stream ended in the middle of a record", data=line
            ) from None
        return None
    if not isinstance(record, dict):
        return None

    stream = record.get("stream")
    data = record.get("data")
    if stream in {"stdout", "stderr"} and isinstance(data, str):
        return ProcessLog(stream=stream, data=data)
    if stream != "error" or not isinstance(data, dict):
        return None

    code = data.get("code")
    message = data.get("message")
    if isinstance(code, str) and isinstance(message, str):
        raise SandboxStreamError(message, code=code)
    return None


class _OutputOffsets:
    """Count the characters of each output stream delivered to a consumer."""

    __slots__ = ("_counts",)

    def __init__(self, counts: dict[ProcessLogStream, int] | None = None) -> None:
        self._counts = dict.fromkeys(ProcessLogStream, 0) if counts is None else dict(counts)

    def copy(self) -> "_OutputOffsets":
        return _OutputOffsets(self._counts)

    def advance(self, stream: ProcessLogStream, count: int) -> None:
        self._counts[stream] += count

    def skip(self, event: ProcessLog) -> str:
        """Consume replayed output from ``event`` and return the data past it."""
        skipped = min(self._counts[event.stream], len(event.data))
        self._counts[event.stream] -= skipped
        return event.data[skipped:]


async def _iter_command_logs(
    open_response: Callable[[], Awaitable[StreamingResponse]],
    *,
    sleep: AsyncSleep,
    delivered: _OutputOffsets | None = None,
) -> AsyncGenerator[ProcessLog, None]:
    """Yield a command's logs, reconnecting when the connection is interrupted.

    Each reconnection replays the logs from the beginning and skips what
    ``delivered`` has already counted, so consumers see every character once.
    Passing the offsets of output delivered by a different stream continues
    from where that stream stopped. Retries are bounded per interruption: a
    connection that delivers new output resets the retry budget.
    """
    delivered = _OutputOffsets() if delivered is None else delivered
    failures = 0
    while True:
        replay = delivered.copy()
        progressed = False
        try:
            response = await open_response()
            try:
                async for line, terminated in _ndjson_lines(response):
                    if not line.strip():
                        continue
                    event = _parse_command_log_record(line, terminated=terminated)
                    if event is None:
                        continue
                    data = replay.skip(event)
                    if not data and event.data:
                        continue
                    delivered.advance(event.stream, len(data))
                    progressed = progressed or bool(data)
                    yield (
                        event if data == event.data else ProcessLog(stream=event.stream, data=data)
                    )
            finally:
                await response.aclose()
            return
        except Exception as error:
            if progressed:
                failures = 0
            if failures >= _RETRIES or not _is_transient_error(error):
                raise
        await sleep(_retry_delay(failures))
        failures += 1
