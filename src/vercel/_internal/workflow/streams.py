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
import base64
import contextlib
import dataclasses
import os
from collections.abc import AsyncGenerator, AsyncIterable, Iterator, Sequence
from typing import TYPE_CHECKING, Any

import anyio

from . import serialization as ser

if TYPE_CHECKING:
    from anyio.abc import TaskGroup

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

    Every caller reads the knob where it uses it, rather than into a module
    global once at import, so an override still applies when the environment
    is populated after this module loads -- which is how the tests set these,
    and how the JS side reads its own (``envNumber`` behind a getter, called
    per use).
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


def encode_value(value: Any) -> bytes:
    """The frame a single user write becomes."""
    return encode_frame(ser.dehydrate(value))


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
"""How many reconnects in a row may deliver nothing before the read gives up.

Any reconnect that delivers a frame resets this to zero, so it measures being
stuck, not how often the connection dropped -- a read that runs for hours may
reconnect far more than 50 times and never come close. Same value as
`@workflow/core`'s ``FRAMED_STREAM_MAX_RECONNECTS``.
"""

MAX_TOTAL_RECONNECTS = 1000
"""How many reconnects in total. Never resets.

The cap above resets whenever a frame arrives, which works as long as a new
frame really means the read moved forward. A backend that ignored
``start_index`` would re-send the same old frames after every break, so it
would reset forever and the read would never end. This one cannot reset.
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
# Ported from `@workflow/core`'s `WorkflowServerWritableStream`.
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

    Two classes implement it. In a step it is a :class:`WorkflowStreamWriter`
    and every method works. In a workflow body it is a
    :class:`WorkflowStreamHandle`, which refers to the stream but refuses to
    write to it.

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
    """A reference to a stream, writable only once it reaches a step.

    A workflow body re-executes on every replay and its sandbox has no network,
    so writing from there is not on offer -- but referring to the stream is,
    and that is what a step needs. Pass the handle into a step and it arrives
    as a :class:`WorkflowStreamWriter`. Hydrating a payload outside a step
    yields a handle for the same reason: a writer sends from a task the step
    handler owns, and there is no such owner out there.

    Deterministic by construction: the stream name is derived from the run id
    and the namespace, so a replay produces the same handle rather than
    pointing a later attempt at a different stream.
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
            f"cannot write to stream {self._name!r} from here: a workflow body re-runs "
            f"on every replay and cannot reach the network, and outside a run there is "
            f"nothing to carry the sends. Pass this to a step and write to it there."
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
    """A stream that workflow steps can write to.

    Sending happens in a task of *task_group*, not in ``write()``, which is
    what lets a producer keep filling the buffer while a request is in flight.
    The group therefore has to outlive every write and the final
    :meth:`drain` -- the step handler owns one for exactly that span.

    Not safe to use from more than one event loop; within one loop, concurrent
    writers are fine and their chunks land in call order.
    """

    def __init__(
        self,
        *,
        world: w.World,
        run_id: str,
        name: str,
        task_group: TaskGroup,
        reentrant_ctx_on_err: bool = True,
    ) -> None:
        if not name:
            raise ValueError(f'"name" is required, got {name!r}')
        self._world = world
        self._run_id = run_id
        self._name = name
        self._task_group = task_group

        self._buffer: list[bytes] = []
        self._buffered_bytes = 0
        # Counted against the buffer bound as well, so the bound keeps its
        # documented meaning -- a cap on everything read but not yet durable,
        # not just on the queued follow-up group.
        self._inflight_chunks = 0
        self._inflight_bytes = 0
        self._dispatching = False
        # Sticky: once a request fails, its group stays at the head of the
        # buffer and every later call raises the original error. Chunks whose
        # `write()` already returned surface their failure at the barrier --
        # that is the contract of an early-acknowledging sink.
        self._sink_error: BaseException | None = None
        self._closed = False
        # Every critical section under this condition is await-free -- the only
        # `await` taken while holding it is `wait()`, which releases it. That
        # keeps the restore in `_dispatch_loop`'s cancellation handler short
        # enough to run under a shield without holding cancellation off for
        # anything that can block.
        self._condition = anyio.Condition()

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

        self._reentrant_ctx_on_err = reentrant_ctx_on_err

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

        This is simply a shortcut for a manual loop + :meth:`write` for
        forwarding an upstream async iterator (e.g. LLM tokens, HTTP body).
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
        if not self._dispatching and self._sink_error is None and self._buffer:
            self._dispatching = True
            self._task_group.start_soon(self._dispatch_loop)

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

    def _put_back_group(self, group: Sequence[bytes], octets: int) -> None:
        self._buffer[:0] = group
        self._buffered_bytes += octets
        self._inflight_chunks = 0
        self._inflight_bytes = 0
        self._dispatching = False

    async def _dispatch_loop(self) -> None:
        """Send buffered groups until the buffer runs dry.

        Runs as a task in the owner's group, so raising here would tear down
        the step that is writing. It doesn't: a send failure is reported by
        poisoning the sink, which is where ``write()`` and ``drain()`` already
        look. Cancellation still propagates -- that is the group taking the
        loop down rather than the loop failing.
        """
        try:
            while True:
                async with self._condition:
                    if not self._buffer:
                        self._dispatching = False
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
                    cancelled = isinstance(error, anyio.get_cancelled_exc_class())
                    # Shielded because acquiring the condition is a checkpoint:
                    # in a cancelled task it would raise instead, leaving the
                    # group booked as in flight forever.
                    with anyio.CancelScope(shield=True):
                        async with self._condition:
                            self._put_back_group(group, octets)
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
            # Cancellation outside `_send`, or a bug in the loop itself. Either
            # way no one is driving this buffer any more, so waiters have to be
            # released -- and for anything that is not cancellation the sink is
            # poisoned too, because `drain()` would otherwise see an idle
            # buffer, start a fresh loop, and hit the same bug forever.
            cancelled = isinstance(error, anyio.get_cancelled_exc_class())
            with anyio.CancelScope(shield=True):
                async with self._condition:
                    self._dispatching = False
                    if not cancelled:
                        self._sink_error = error
                    self._condition.notify_all()
            if cancelled:
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
                if not self._buffer and self._inflight_chunks == 0 and not self._dispatching:
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
        if exc_type is None or not self._reentrant_ctx_on_err:
            await self.close()
