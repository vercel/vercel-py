"""Shared command stream decoding and connection recovery for Sandbox streams.

Command output reaches the SDK over long-lived CBOR Sequence responses that the
network can cut off at any point. The logs endpoint replays a command's output
from the beginning on every connection, so an interrupted stream is resumed by
reconnecting and skipping the output that was already delivered.
"""

import io
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from enum import IntEnum
from typing import TypeVar

import cbor2

from vercel._internal.core.http import StreamingResponse
from vercel._internal.core.http._compat import transport_errors
from vercel.sandbox._internal.errors import (
    SandboxApiError,
    SandboxResponseError,
    SandboxStreamError,
)
from vercel.sandbox._internal.models import ProcessLog, ProcessLogStream
from vercel.sandbox._internal.retry import _RETRIES, AsyncSleep, _retry_delay, _with_request_retries

_ResultT = TypeVar("_ResultT")
_RETRY_BACKOFF_SECONDS = 0.2

# Reported when the API loses its connection to the sandbox, whether or not
# the sandbox itself stopped. It arrives as an in-band stream record, or as an
# HTTP 410 when it happens before the first record.
STREAM_CLOSED_ERROR_CODE = "sandbox_stream_closed"

# A CBOR Sequence (RFC 8742) of ``[type, payload]`` arrays. Output payloads are
# byte strings, so output arrives exactly as the command wrote it.
COMMAND_STREAM_MEDIA_TYPE = "application/vnd.vercel.sandbox.command-stream+cbor-seq"


class _ItemType(IntEnum):
    STDOUT = 1
    STDERR = 2
    ERROR = 3
    COMMAND = 0x80


_OUTPUT_STREAMS: dict[int, ProcessLogStream] = {
    _ItemType.STDOUT: ProcessLogStream.STDOUT,
    _ItemType.STDERR: ProcessLogStream.STDERR,
}


class _TruncatedStreamError(SandboxResponseError):
    """A stream ended before a complete record or its final metadata arrived."""


def _is_transient_error(error: BaseException) -> bool:
    """Return whether retrying the failed request may succeed.

    Network failures, streams that end mid-record, and the API losing its own
    connection to the sandbox are connection problems rather than answers
    from the API, and rate limits and server errors are expected to clear up.
    If the sandbox did stop, the retried request reports that instead.
    Cancellation is never an ``Exception`` and so is never retried.
    """
    if isinstance(error, (*transport_errors(), _TruncatedStreamError)):
        return True
    if (
        isinstance(error, SandboxApiError | SandboxStreamError)
        and error.code == STREAM_CLOSED_ERROR_CODE
    ):
        return True
    return isinstance(error, SandboxApiError) and (
        error.status_code == 429 or 500 <= error.status_code < 600
    )


async def _with_transient_retries(
    operation: Callable[[], Awaitable[_ResultT]], *, sleep: AsyncSleep
) -> _ResultT:
    """Retry command reads, including the service losing its sandbox connection."""
    return await _with_request_retries(operation, sleep=sleep, is_retryable=_is_transient_error)


async def _command_stream_items(
    response: StreamingResponse,
) -> AsyncIterator[tuple[int, object]]:
    """Yield the ``(type, payload)`` items of a command stream response.

    Raises:
        SandboxResponseError: If the response is not a well-formed command stream.
        _TruncatedStreamError: If the response ends in the middle of an item.
    """
    content_type = response.response.headers.get("content-type", "")
    if content_type.partition(";")[0].strip().lower() != COMMAND_STREAM_MEDIA_TYPE:
        raise SandboxResponseError(
            "Sandbox command stream response has an unexpected content type",
            data=content_type,
        )
    buffer = bytearray()
    async for chunk in response:
        buffer += chunk
        reader = io.BytesIO(buffer)
        decoder = cbor2.CBORDecoder(reader)
        consumed = 0
        while consumed < len(buffer):
            try:
                item = decoder.decode()
            except cbor2.CBORDecodeEOF:
                break
            except cbor2.CBORDecodeError as exc:
                raise SandboxResponseError(
                    "Sandbox command stream included malformed CBOR", data=bytes(buffer)
                ) from exc
            consumed = reader.tell()
            if not isinstance(item, list) or len(item) != 2 or type(item[0]) is not int:
                raise SandboxResponseError(
                    "Sandbox command stream included a malformed item", data=item
                )
            yield item[0], item[1]
        del buffer[:consumed]
    if buffer:
        raise _TruncatedStreamError(
            "Sandbox command stream ended in the middle of an item", data=bytes(buffer)
        )


def _command_output(item_type: int, payload: object) -> ProcessLog | None:
    """Decode an output or stream error item, or return ``None`` for other items.

    Raises:
        SandboxStreamError: If the item reports an in-band stream failure.
        SandboxResponseError: If an output or error item has a malformed payload.
    """
    stream = _OUTPUT_STREAMS.get(item_type)
    if stream is not None:
        if not isinstance(payload, bytes):
            raise SandboxResponseError(
                "Sandbox command stream output is not a byte string", data=payload
            )
        return ProcessLog(stream=stream, data=payload)
    if item_type != _ItemType.ERROR:
        return None
    if isinstance(payload, dict):
        code = payload.get("code")
        message = payload.get("message")
        if isinstance(code, str) and isinstance(message, str):
            raise SandboxStreamError(message, code=code)
    raise SandboxResponseError("Sandbox command stream included a malformed error", data=payload)


class _OutputOffsets:
    """Count the bytes of each output stream delivered to a consumer."""

    __slots__ = ("_counts",)

    def __init__(self, counts: dict[ProcessLogStream, int] | None = None) -> None:
        self._counts = dict.fromkeys(ProcessLogStream, 0) if counts is None else dict(counts)

    def copy(self) -> "_OutputOffsets":
        return _OutputOffsets(self._counts)

    def advance(self, stream: ProcessLogStream, count: int) -> None:
        self._counts[stream] += count

    def skip(self, event: ProcessLog) -> bytes:
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
    ``delivered`` has already counted, so consumers see every byte once.
    Passing the offsets of output delivered by a different stream continues
    from where that stream stopped. Retries are bounded per interruption: a
    connection that delivers new output resets the retry budget.
    """
    delivered = _OutputOffsets() if delivered is None else delivered
    failures = 0
    while True:
        replay = delivered.copy()
        progressed = False
        opened = False
        try:
            response = await open_response()
            opened = True
            try:
                async for item_type, payload in _command_stream_items(response):
                    event = _command_output(item_type, payload)
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
            # HTTP opening failures follow request policy; interrupted bodies
            # retain the TypeScript stream reconnection delay.
            delay = (
                _RETRY_BACKOFF_SECONDS * 2**failures if opened else _retry_delay(error, failures)
            )
            if delay is None:
                raise
        await sleep(delay)
        failures += 1
