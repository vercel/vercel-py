"""Run-scoped streams: the wire framing, and the writer steps use.

A run's stream is an append-only, indexed log of chunks that a reader can tail
live and resume at an index. Steps write to it with
:func:`vercel.workflow.get_writable`; anything holding the run id reads it back
through the world's ``streams_get``.

Each user write becomes exactly one length-prefixed frame, and each frame is
stored under exactly one chunk index::

    [4-byte big-endian length][format-prefixed payload]

That one-write-one-frame-one-index correspondence is what makes
``start_index + frames_consumed`` a correct resume position, so nothing along
this path may coalesce or split frames. The payload is whatever
:mod:`.serialization` writes -- a ``devl``-prefixed devalue payload -- which is
the format `@workflow/core`'s ``getDeserializeStream`` reads, so a stream
written here is readable by the TypeScript SDK, the ``workflow`` CLI and the
dashboard. The length header stays outside the payload so a reader can find
frame boundaries without understanding the payload format at all.
"""

from __future__ import annotations

import abc
import asyncio
import base64
import contextlib
import dataclasses
import os
from collections.abc import AsyncGenerator, AsyncIterable, Iterator, Sequence
from typing import TYPE_CHECKING, Any

from . import serialization as ser

if TYPE_CHECKING:
    from . import world as w

FRAME_HEADER_SIZE = 4
"""Bytes of big-endian length prefix in front of every frame."""

MAX_FRAME_SIZE = 100_000_000
"""Largest single frame payload, matching `@workflow/core`'s ``MAX_FRAME_SIZE``.

A length header advertising more than this is refused rather than allocated:
past a certain size the far more likely explanation is a misframed wire than a
100 MB chunk.
"""


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    """Read a positive-integer knob, falling back to *default* when unusable.

    Mirrors `@workflow/world`'s ``envNumber``: an unset, non-numeric or
    out-of-range value is ignored rather than fatal, since these are
    operational overrides and a typo should not take a deployment down.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value >= minimum else default


def workflow_run_stream_id(run_id: str, namespace: str | None = None) -> str:
    """The name of a run's default user stream.

    Mirrors `@workflow/core`'s ``getWorkflowRunStreamId``. A namespace is
    base64url-encoded because it reaches Redis keys on the Vercel world, where
    arbitrary user text is not safe as a key segment.
    """
    # JS `String.replace` with a string pattern replaces the first match only.
    name = run_id.replace("wrun_", "strm_", 1) + "_user"
    if not namespace:
        return name
    encoded = base64.urlsafe_b64encode(namespace.encode()).decode("ascii").rstrip("=")
    return f"{name}_{encoded}"


def encode_frame(payload: bytes) -> bytes:
    """Wrap *payload* in its length header."""
    if len(payload) > MAX_FRAME_SIZE:
        raise ser.SerializationError(
            f"Stream chunk of {len(payload)} bytes exceeds the maximum frame size "
            f"({MAX_FRAME_SIZE}); split the data into smaller chunks before writing"
        )
    return len(payload).to_bytes(FRAME_HEADER_SIZE, "big") + payload


def _reduce_uint8array(value: Any) -> Any:
    """devalue reducer sending a chunk's ``bytes`` as a view. Falsy declines.

    Only the emitted *form* differs: devalue writes a typed array as a view
    onto its own buffer entry, ``["Uint8Array", <buffer>]``, where the payload
    boundary writes the bare ``["ArrayBuffer", …]``. Both carry the same bytes
    and both read back here as ``bytes``.

    It matters because of what reads a stream. The pattern the TypeScript docs
    lead with pipes a run's readable straight into a `Response`, and a body
    stream takes `Uint8Array` chunks only -- an `ArrayBuffer` chunk raises
    ``Received non-Uint8Array chunk``. So a step writing ``b"..."`` has to
    frame it as a view or every consumer doing that breaks.

    Chunk-local on purpose. Python has one bytes type, so whichever form the
    encoder picks the other becomes a one-way landing; the payload boundary
    already chose ``ArrayBuffer`` and has tests pinning it. Nothing downstream
    of a payload cares which form a ``bytes`` *field* took, so there is no
    reason to move that.
    """
    if type(value) is not bytes:
        return False
    # A `memoryview` rather than the `bytes` itself, and not for speed: devalue
    # indexes by identity, so returning the same object would make the view
    # reference its own slot instead of a buffer. A distinct object flattens to
    # the `["ArrayBuffer", …]` entry the view is supposed to point at -- and a
    # view over the same memory copies nothing to get one.
    return memoryview(value)


CHUNK_REDUCERS: dict[str, Any] = {**ser.REDUCERS, "Uint8Array": _reduce_uint8array}
"""Reducers for a stream chunk, as opposed to a run/step/hook payload."""


def encode_value(value: Any) -> bytes:
    """The frame a single user write becomes."""
    return encode_frame(ser.dehydrate(value, reducers=CHUNK_REDUCERS))


class FrameDecoder:
    """Reassembles frames from transport reads of arbitrary size.

    The transport decides where its own read boundaries fall: one frame may
    arrive split across three reads, and three frames may arrive in one. Feed
    every read in and take whatever whole frames come out.
    """

    __slots__ = ("_buffer",)

    def __init__(self) -> None:
        self._buffer = bytearray()

    @property
    def pending(self) -> int:
        """Bytes buffered as part of a frame that has not fully arrived."""
        return len(self._buffer)

    def feed(self, data: bytes) -> Iterator[bytes]:
        """Yield the payload of every frame completed by *data*."""
        self._buffer += data
        while len(self._buffer) >= FRAME_HEADER_SIZE:
            length = int.from_bytes(self._buffer[:FRAME_HEADER_SIZE], "big")
            if length > MAX_FRAME_SIZE:
                raise ser.SerializationError(
                    f"Stream frame length {length} exceeds the maximum ({MAX_FRAME_SIZE}); "
                    f"this usually means a non-framed stream is being read as framed"
                )
            end = FRAME_HEADER_SIZE + length
            if len(self._buffer) < end:
                return
            yield bytes(self._buffer[FRAME_HEADER_SIZE:end])
            del self._buffer[:end]

    def finish(self) -> None:
        """Assert the stream ended on a frame boundary.

        Leftover bytes mean the stream was truncated mid-frame. Reporting that
        beats silently dropping the tail, which would look like a short stream.
        """
        if self._buffer:
            raise ser.SerializationError(
                f"Stream ended with {len(self._buffer)} bytes of incomplete frame data; "
                f"it was truncated mid-frame"
            )


# ═══════════════════════════════════════════════════════════════════════════
# reader
# ═══════════════════════════════════════════════════════════════════════════

MAX_RECONNECTS = 50
"""Consecutive reconnects allowed before a read gives up.

Reset by any reconnect that delivers a frame, so this bounds *consecutive*
failures rather than the lifetime total: a long-lived stream may reconnect far
more than this and stay healthy, as long as it keeps making progress.
"""

MAX_TOTAL_RECONNECTS = 1000
"""Absolute backstop, independent of progress.

The consecutive cap resets on forward progress, which is correct against a
backend that honours ``start_index``. One that ignored it would re-deliver
earlier frames, look like progress every time, and never trip that cap -- so
this guarantees the loop terminates regardless.
"""


async def reconnecting_frames(
    world: w.World, run_id: str, name: str, start_index: int | None = None
) -> AsyncGenerator[bytes, None]:
    """Read a stream's frames, reopening the connection when it breaks.

    A live read is long-lived and the transport under it is not: the Vercel
    world's read endpoint errors the response body when the server's max
    duration expires, which is what tells this loop apart from the end of the
    stream. A clean end of iteration means the stream is closed and the read is
    done; an error means reopen at the next unread frame and carry on.

    Resuming is exact because one frame is one stored chunk, so the position is
    ``start_index`` plus the frames already handed to the caller. Bytes of a
    frame that had not fully arrived are dropped -- the reopened connection
    re-sends that chunk whole.

    A negative *start_index* (last-N) cannot be resumed: its meaning depends on
    where the tail was at connect time, and re-resolving it after a break would
    silently move the window. Such a read is single-shot.
    """
    resumable = start_index is None or start_index >= 0
    position = start_index or 0
    consumed = 0
    consecutive = 0
    total = 0
    max_reconnects = _env_int("WORKFLOW_FRAMED_STREAM_MAX_RECONNECTS", MAX_RECONNECTS)
    max_total = _env_int("WORKFLOW_FRAMED_STREAM_MAX_TOTAL_RECONNECTS", MAX_TOTAL_RECONNECTS)

    while True:
        # A fresh decoder per connection: whatever partial frame the broken
        # connection left behind is not continued, it is re-sent.
        decoder = FrameDecoder()
        effective = position + consumed if resumable else start_index
        try:
            source = world.streams_get(run_id, name, effective)
            async with contextlib.aclosing(source):
                async for data in source:
                    for payload in decoder.feed(data):
                        consumed += 1
                        consecutive = 0
                        yield payload
        except Exception as error:
            if not resumable:
                raise
            consecutive += 1
            total += 1
            if consecutive > max_reconnects:
                raise ser.SerializationError(
                    f"Stream {name!r} exceeded {max_reconnects} consecutive reconnection attempts"
                ) from error
            if total > max_total:
                raise ser.SerializationError(
                    f"Stream {name!r} exceeded {max_total} total reconnection attempts"
                ) from error
            position += consumed
            consumed = 0
            continue

        # Iteration ended without error: the stream is closed and complete.
        decoder.finish()
        return


# ═══════════════════════════════════════════════════════════════════════════
# writer
# ═══════════════════════════════════════════════════════════════════════════
#
# Ported from `@workflow/core`'s `WorkflowServerWritableStream`, whose shape is
# not obvious and is worth restating.
#
# `write()` returns once the chunk is *buffered*, not once it is durable. That
# early acknowledgement is what makes batching work at all: the producer keeps
# handing over chunks while a request is in flight, so they accumulate and the
# next request carries the whole group. If `write()` waited for the server, a
# producer writing in a loop would serialize into one request per chunk.
#
# Durability gets its own barrier, `drain()`, which the step handler awaits
# before recording the step as complete -- so "the step finished" still implies
# "its chunks are durable", and a caller who needs that guarantee mid-step can
# ask for it.

MAX_INFLIGHT_CHUNKS = 1000
"""Chunks that may sit buffered-but-not-durable before ``write()`` blocks."""

MAX_BUFFERED_BYTES = 8 * 1024 * 1024
"""Byte-denominated counterpart of :data:`MAX_INFLIGHT_CHUNKS`.

Count alone is not a bound: 1,000 small chunks are ~100 KB, 1,000 file-sized
ones are hundreds of MB.
"""

MAX_CHUNKS_PER_BATCH = 1000
"""Chunks in one write request, matching the server's per-batch cap."""

MAX_BYTES_PER_BATCH = 1024 * 1024
"""Cumulative bytes in one write request, under platform body limits."""


class WorkflowWritable(abc.ABC):
    """One of a run's streams, as :func:`vercel.workflow.get_writable` hands it out.

    Two things wear this interface. In a step it is a
    :class:`WorkflowStreamWriter` and every method works. In a workflow body it
    is a :class:`WorkflowStreamHandle`, which knows which stream it is but
    refuses to write to it.

    One interface rather than two so that a workflow can take a writable and
    pass it to a step without the type changing on the way, which is also how
    `@workflow/core` does it -- its workflow-side `getWritable()` returns
    something with `WritableStream`'s prototype whose methods throw.
    """

    @property
    @abc.abstractmethod
    def run_id(self) -> str:
        """The run that owns the stream."""

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """The stream this writes to."""

    @abc.abstractmethod
    async def write(self, value: Any) -> None:
        """Append *value* as one chunk."""

    @abc.abstractmethod
    async def write_from(self, source: AsyncIterable[Any]) -> None:
        """Append every item *source* yields, in order."""

    @abc.abstractmethod
    async def drain(self) -> None:
        """Wait until every accepted chunk is durably written."""

    @abc.abstractmethod
    async def close(self) -> None:
        """Mark the stream complete, ending its readers."""


@dataclasses.dataclass(frozen=True)
class WorkflowStreamHandle(WorkflowWritable):
    """A stream a workflow body can name but not write to.

    The body re-executes on every replay and its sandbox has no network, so
    writing from there is not on offer -- but naming the stream is, and that is
    what a step needs. Pass the handle into a step and it arrives as a
    :class:`WorkflowStreamWriter`.

    Deterministic by construction: the name comes from the run id and the
    namespace, so a replay produces the same handle rather than pointing a
    later attempt at a different stream.
    """

    # Underscored because the interface declares `run_id` and `name` as
    # properties, and a dataclass field only annotates -- it puts nothing in the
    # class body for `abc` to see as the implementation. Constructed
    # positionally, so the names stay out of the way.
    _run_id: str
    _name: str

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def name(self) -> str:
        return self._name

    def _refuse(self) -> RuntimeError:
        return RuntimeError(
            f"cannot write to stream {self._name!r} from a workflow body: it re-runs "
            f"on every replay and cannot reach the network. Pass this to a step "
            f"and write to it there."
        )

    async def write(self, value: Any) -> None:
        raise self._refuse()

    async def write_from(self, source: AsyncIterable[Any]) -> None:
        raise self._refuse()

    async def drain(self) -> None:
        raise self._refuse()

    async def close(self) -> None:
        raise self._refuse()


class WorkflowStreamWriter(WorkflowWritable):
    """A group-committing writer for one ``(run_id, name)`` stream.

    Not safe to use from more than one event loop; within one loop, concurrent
    writers are fine and their chunks land in call order.
    """

    def __init__(self, world: w.World, run_id: str, name: str) -> None:
        if not name:
            raise ValueError(f'"name" is required, got {name!r}')
        self._world = world
        self._run_id = run_id
        self._name = name

        self._buffer: list[bytes] = []
        self._buffered_bytes = 0
        # Counted against the buffer bound as well, so the bound keeps its
        # documented meaning -- a cap on everything read but not yet durable,
        # not just on the queued follow-up group.
        self._inflight_chunks = 0
        self._inflight_bytes = 0
        self._dispatch: asyncio.Task[None] | None = None
        # Sticky: once a request fails, its group stays at the head of the
        # buffer and every later call raises the original error. Chunks whose
        # `write()` already returned surface their failure at the barrier --
        # that is the contract of an early-acknowledging sink.
        self._sink_error: BaseException | None = None
        self._closed = False
        self._condition = asyncio.Condition()

        self._max_inflight_chunks = _env_int(
            "WORKFLOW_STREAM_MAX_INFLIGHT_CHUNKS", MAX_INFLIGHT_CHUNKS
        )
        self._max_buffered_bytes = _env_int(
            "WORKFLOW_STREAM_MAX_BUFFERED_BYTES", MAX_BUFFERED_BYTES
        )
        self._max_chunks_per_batch = _env_int(
            "WORKFLOW_STREAM_MAX_CHUNKS_PER_BATCH", MAX_CHUNKS_PER_BATCH
        )
        self._max_bytes_per_batch = _env_int(
            "WORKFLOW_STREAM_MAX_BYTES_PER_BATCH", MAX_BYTES_PER_BATCH
        )

    @property
    def name(self) -> str:
        """The stream this writer appends to."""
        return self._name

    @property
    def run_id(self) -> str:
        """The run that owns the stream."""
        return self._run_id

    async def write(self, value: Any) -> None:
        """Append *value* as one chunk.

        Returns once the chunk is buffered and ordered, which is not yet
        durable; await :meth:`drain` or :meth:`close` for that. Blocks while
        the buffer is full, so a fast producer cannot grow it without bound.
        """
        await self._enqueue(encode_value(value))

    async def write_from(self, source: AsyncIterable[Any]) -> None:
        """Append every item *source* yields, in order.

        The counterpart of piping a stream into this one: a step forwarding an
        upstream async iterator (LLM tokens, an HTTP body) wants this rather
        than a hand-rolled loop, and it keeps the group-commit buffer fed
        without the caller thinking about batching.
        """
        async for value in source:
            await self.write(value)

    async def _enqueue(self, frame: bytes) -> None:
        async with self._condition:
            self._raise_if_unusable()
            while self._at_capacity():
                await self._condition.wait()
                self._raise_if_unusable()
            self._buffer.append(frame)
            self._buffered_bytes += len(frame)
            self._start_dispatch()

    def _raise_if_unusable(self) -> None:
        if self._sink_error is not None:
            raise self._sink_error
        if self._closed:
            raise ser.SerializationError(f"Stream {self._name!r} is closed")

    def _at_capacity(self) -> bool:
        chunks = len(self._buffer) + self._inflight_chunks
        octets = self._buffered_bytes + self._inflight_bytes
        # A single chunk over the byte bound would deadlock against it, so an
        # empty buffer always accepts one.
        if chunks == 0:
            return False
        return chunks >= self._max_inflight_chunks or octets >= self._max_buffered_bytes

    def _start_dispatch(self) -> None:
        """Start the dispatch loop unless one is already running.

        Exactly one runs at a time, which is what keeps groups reaching the
        server in write order. Called with the condition held.
        """
        if self._dispatch is None and self._sink_error is None and self._buffer:
            self._dispatch = asyncio.ensure_future(self._dispatch_loop())

    def _take_group(self) -> tuple[list[bytes], int]:
        """The largest leading group that fits one request.

        A single chunk larger than the byte cap still goes out alone -- the
        alternative is refusing to send it at all.
        """
        count = 0
        octets = 0
        for frame in self._buffer:
            if count >= self._max_chunks_per_batch:
                break
            if count > 0 and octets + len(frame) > self._max_bytes_per_batch:
                break
            count += 1
            octets += len(frame)
        group = self._buffer[:count]
        del self._buffer[:count]
        self._buffered_bytes -= octets
        return group, octets

    async def _dispatch_loop(self) -> None:
        try:
            while True:
                async with self._condition:
                    if not self._buffer:
                        self._dispatch = None
                        # Nothing buffered and nothing in flight: the
                        # durability barrier is satisfied.
                        self._condition.notify_all()
                        return
                    group, octets = self._take_group()
                    self._inflight_chunks = len(group)
                    self._inflight_bytes = octets

                try:
                    await self._send(group)
                except BaseException as error:
                    cancelled = isinstance(error, asyncio.CancelledError)
                    async with self._condition:
                        # The group did not land, so it goes back at the head
                        # either way: the buffer holds everything not known to
                        # be durable, and dropping it here would lose chunks
                        # whose `write()` had already returned.
                        self._buffer[:0] = group
                        self._buffered_bytes += octets
                        self._inflight_chunks = 0
                        self._inflight_bytes = 0
                        self._dispatch = None
                        # Cancellation is this task being torn down, not the
                        # stream failing. Recording it as the sink error would
                        # make a later `write()` raise `CancelledError` inside
                        # a caller that was never cancelled.
                        if not cancelled:
                            self._sink_error = error
                        self._condition.notify_all()
                    if cancelled:
                        raise
                    return

                async with self._condition:
                    self._inflight_chunks = 0
                    self._inflight_bytes = 0
                    # This group is durable: release writers blocked on the
                    # bound. They re-check it and may block again.
                    self._condition.notify_all()
        except BaseException as error:
            # Cancellation, or a bug in the loop itself. Either way no one is
            # driving this buffer any more, so waiters have to be released --
            # and for anything that is not cancellation the sink is poisoned
            # too, because `drain()` would otherwise see an idle buffer, start
            # a fresh loop, and hit the same bug forever.
            async with self._condition:
                self._dispatch = None
                if not isinstance(error, asyncio.CancelledError):
                    self._sink_error = error
                self._condition.notify_all()
            raise

    async def _send(self, group: Sequence[bytes]) -> None:
        if len(group) == 1:
            await self._world.streams_write(self._run_id, self._name, group[0])
        else:
            await self._world.streams_write_multi(self._run_id, self._name, group)

    async def drain(self) -> None:
        """Wait until every accepted chunk is durably written.

        Raises the sink's error if any request failed -- including one whose
        ``write()`` had already returned.
        """
        async with self._condition:
            while True:
                if self._sink_error is not None:
                    raise self._sink_error
                if not self._buffer and self._inflight_chunks == 0 and self._dispatch is None:
                    return
                self._start_dispatch()
                await self._condition.wait()

    async def close(self) -> None:
        """Drain, then mark the stream complete so readers see the end.

        Idempotent. Nothing closes a run's stream implicitly -- not the end of
        a step, not the end of the run -- so a stream a workflow never closes
        leaves its readers waiting until the run expires.
        """
        if self._closed and self._sink_error is None:
            return
        await self.drain()
        await self._world.streams_close(self._run_id, self._name)
        self._closed = True

    async def __aenter__(self) -> WorkflowStreamWriter:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        if exc_type is None:
            await self.close()
