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
import contextvars
import dataclasses
import os
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterator,
    Sequence,
)
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, cast, overload

import anyio
import pydantic

from vercel._internal.core.byte_stream import buffer_to_bytes
from vercel._internal.core.polyfills import Buffer

from . import serialization as ser, ulid

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

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class ReadableStream(AsyncIterable[T_co], Generic[T_co], abc.ABC):
    """A serializable asynchronous stream of values.

    The same public type represents a local source, an opaque workflow-side
    reference and a live World-backed reader.  Execution context determines
    which operations are available after a descriptor is revived.
    """

    @abc.abstractmethod
    def __aiter__(self) -> AsyncIterator[T_co]: ...

    @abc.abstractmethod
    async def aclose(self) -> None:
        """Stop this reader and cancel its locally owned producer when possible."""


ReadableMode = Literal["objects", "bytes"]
_new_stream_ulid = ulid.monotonic_factory()


@dataclasses.dataclass(frozen=True)
class _ReadableDescriptor:
    data: dict[str, Any]

    @classmethod
    def parse(cls, value: Any) -> _ReadableDescriptor:
        if not isinstance(value, dict) or not isinstance(value.get("name"), str):
            raise ValueError(f"malformed ReadableStream payload: {value!r}")
        stream_type = value.get("type")
        framing = value.get("framing")
        if stream_type is not None and stream_type != "bytes":
            raise ValueError(f"unknown ReadableStream type: {stream_type!r}")
        if framing is not None and framing != "framed-v1":
            raise ValueError(f"unknown ReadableStream framing: {framing!r}")
        if framing is not None and stream_type != "bytes":
            raise ValueError("ReadableStream framing requires type='bytes'")
        return cls(dict(value))

    @property
    def name(self) -> str:
        return cast(str, self.data["name"])

    @property
    def start_index(self) -> int | None:
        value = self.data.get("startIndex")
        if value is not None and not isinstance(value, int):
            raise ValueError(f"malformed ReadableStream startIndex: {value!r}")
        return value


class _LocalReadableStream(ReadableStream[T]):
    def __init__(self, source: AsyncIterable[T], mode: ReadableMode) -> None:
        self.source = source
        self.mode = mode
        descriptor: dict[str, Any] = {"name": f"strm_{_new_stream_ulid()}"}
        if mode == "bytes":
            descriptor.update(type="bytes", framing="framed-v1")
        self.descriptor = _ReadableDescriptor(descriptor)
        self.bound_run_id: str | None = None
        self.pump_registered = False
        self.owner: _ReadablePump | None = None

    def __aiter__(self) -> AsyncIterator[T]:
        if self.pump_registered:
            raise RuntimeError("cannot consume a readable stream after its World pump has started")
        return aiter(self.source)

    async def aclose(self) -> None:
        if self.owner is not None:
            self.owner.cancel()
        close = getattr(self.source, "aclose", None)
        if close is not None:
            await close()


class _OpaqueReadableStream(ReadableStream[Any]):
    def __init__(self, descriptor: _ReadableDescriptor) -> None:
        self.descriptor = descriptor

    def _refuse(self) -> RuntimeError:
        return RuntimeError(
            f"cannot consume stream {self.descriptor.name!r} inside a workflow: "
            "workflow bodies replay in an I/O-free sandbox; forward or return the stream instead"
        )

    def __aiter__(self) -> AsyncIterator[Any]:
        raise self._refuse()

    async def aclose(self) -> None:
        raise self._refuse()


class _WorldReadableStream(ReadableStream[Any]):
    def __init__(self, *, world: w.World, run_id: str, descriptor: _ReadableDescriptor) -> None:
        self._world = world
        self._run_id = run_id
        self.descriptor = descriptor
        self._iterator: AsyncGenerator[Any, None] | None = None

    def __aiter__(self) -> AsyncIterator[Any]:
        if self._iterator is not None:
            raise RuntimeError("a ReadableStream can only be iterated once")

        async def values() -> AsyncGenerator[Any, None]:
            stream_type = self.descriptor.data.get("type")
            framing = self.descriptor.data.get("framing")
            if stream_type == "bytes" and framing is None:
                # Legacy descriptors name an unframed raw byte stream. The
                # transport's reads are the only boundaries available, and a
                # frame-index reconnect would be unsafe.
                source = self._world.streams_get(
                    self._run_id,
                    self.descriptor.name,
                    self.descriptor.start_index,
                )
                async with contextlib.aclosing(source):
                    async for chunk in source:
                        yield bytes(chunk)
            else:
                frames = reconnecting_frames(
                    self._world,
                    self._run_id,
                    self.descriptor.name,
                    self.descriptor.start_index,
                )
                async with contextlib.aclosing(frames):
                    index = self.descriptor.start_index or 0
                    async for payload in frames:
                        if stream_type == "bytes":
                            yield payload
                        else:
                            with reviving_readable_streams(
                                self._run_id, world=self._world, live=True
                            ):
                                yield ser.hydrate(
                                    payload,
                                    what=f"chunk {index} of stream {self.descriptor.name}",
                                )
                        index += 1
            owner = _readable_pump(self._world, self._run_id, self.descriptor.name)
            if owner is not None and owner.error is not None:
                _readable_pumps.pop((self._world, self._run_id, self.descriptor.name), None)
                raise owner.error

        self._iterator = values()
        return self._iterator

    async def aclose(self) -> None:
        if self._iterator is not None:
            await self._iterator.aclose()
        owner = _readable_pumps.get((self._world, self._run_id, self.descriptor.name))
        if owner is not None:
            owner.cancel()
            if owner.done:
                _readable_pumps.pop((self._world, self._run_id, self.descriptor.name), None)


@overload
def readable_stream(
    source: AsyncIterable[T], *, mode: Literal["objects"] = "objects"
) -> ReadableStream[T]: ...


@overload
def readable_stream(
    source: AsyncIterable[Buffer], *, mode: Literal["bytes"]
) -> ReadableStream[bytes]: ...


def readable_stream(
    source: AsyncIterable[Any], *, mode: ReadableMode = "objects"
) -> ReadableStream[Any]:
    """Make an asynchronous source serializable as a native ReadableStream."""
    if mode not in ("objects", "bytes"):
        raise ValueError(f"unknown readable stream mode: {mode!r}")
    if not isinstance(source, AsyncIterable):
        raise TypeError(
            f"readable stream source must be an AsyncIterable, got {type(source).__name__}"
        )
    return _LocalReadableStream(source, mode)


@dataclasses.dataclass
class _ReadableSerialization:
    allow_local: bool
    local_sources: list[_LocalReadableStream[Any]] = dataclasses.field(default_factory=list)
    _seen: set[int] = dataclasses.field(default_factory=set)

    def add(self, stream: _LocalReadableStream[Any]) -> None:
        if not self.allow_local:
            raise RuntimeError(
                "cannot create a readable stream inside a workflow: local sources need I/O; "
                "create it in a caller, step, or hook resumer and forward its handle"
            )
        if id(stream) not in self._seen:
            self._seen.add(id(stream))
            self.local_sources.append(stream)


_serialization_ctx: contextvars.ContextVar[_ReadableSerialization | None] = contextvars.ContextVar(
    "WorkflowReadableSerialization", default=None
)


@contextlib.contextmanager
def collecting_readable_sources(*, allow_local: bool = True) -> Iterator[_ReadableSerialization]:
    state = _ReadableSerialization(allow_local=allow_local)
    token = _serialization_ctx.set(state)
    try:
        yield state
    finally:
        _serialization_ctx.reset(token)


@dataclasses.dataclass(frozen=True)
class _ReadableRevival:
    run_id: str
    world: w.World | None = None
    live: bool = False


_revival_ctx: contextvars.ContextVar[_ReadableRevival | None] = contextvars.ContextVar(
    "WorkflowReadableRevival", default=None
)


@contextlib.contextmanager
def reviving_readable_streams(
    run_id: str, *, world: w.World | None = None, live: bool = False
) -> Iterator[None]:
    token = _revival_ctx.set(_ReadableRevival(run_id=run_id, world=world, live=live))
    try:
        yield
    finally:
        _revival_ctx.reset(token)


def reduce_readable_stream(value: Any) -> Any:
    if isinstance(value, _LocalReadableStream):
        state = _serialization_ctx.get()
        if state is None:
            raise RuntimeError(
                "a local readable stream can only be serialized at a workflow execution boundary"
            )
        state.add(value)
        return value.descriptor.data
    if isinstance(value, _OpaqueReadableStream | _WorldReadableStream):
        return value.descriptor.data
    return False


def revive_readable_stream(value: Any) -> ReadableStream[Any]:
    descriptor = _ReadableDescriptor.parse(value)
    context = _revival_ctx.get()
    if context is not None and context.live:
        if context.world is None:
            raise RuntimeError("a live ReadableStream revival requires a World")
        return _WorldReadableStream(
            world=context.world,
            run_id=context.run_id,
            descriptor=descriptor,
        )
    return _OpaqueReadableStream(descriptor)


_background_tasks_ctx: contextvars.ContextVar[Callable[[Awaitable[object]], None] | None] = (
    contextvars.ContextVar("WorkflowReadableBackgroundTasks", default=None)
)


@dataclasses.dataclass
class _ReadablePump:
    cancel_scope: anyio.CancelScope | None = None
    cancel_requested: bool = False
    done: bool = False
    error: BaseException | None = None

    def cancel(self) -> None:
        self.cancel_requested = True
        if self.cancel_scope is not None:
            self.cancel_scope.cancel()


_readable_pumps: dict[tuple[w.World, str, str], _ReadablePump] = {}


def _readable_pump(world: w.World, run_id: str, name: str) -> _ReadablePump | None:
    return _readable_pumps.get((world, run_id, name))


@contextlib.contextmanager
def readable_background_tasks(
    register: Callable[[Awaitable[object]], None],
) -> Iterator[None]:
    """Override invocation-owned background registration, primarily for hosts."""
    token = _background_tasks_ctx.set(register)
    try:
        yield
    finally:
        _background_tasks_ctx.reset(token)


def _background_task_registrar() -> Callable[[Awaitable[object]], None]:
    register = _background_tasks_ctx.get()
    if register is not None:
        return register

    # Vercel's Python Functions runtime installs this callback for the active
    # invocation. Import lazily so `vercel-workflow` can still be imported as a
    # standalone namespace package when the umbrella SDK is not installed.
    try:
        from vercel.cache.context import get_context
    except ImportError:
        pass
    else:
        register = get_context().wait_until
        if register is not None:
            return register

    raise RuntimeError(
        "serializing a local readable stream requires an invocation-owned background-task "
        "facility (Vercel Functions wait_until is not available in this context)"
    )


async def _pump_local_stream(
    stream: _LocalReadableStream[Any],
    *,
    world: w.World,
    run_id: str,
    owner: _ReadablePump,
) -> None:
    cancelled = False
    try:
        with anyio.CancelScope() as cancel_scope:
            owner.cancel_scope = cancel_scope
            if owner.cancel_requested:
                cancel_scope.cancel()
            async with anyio.create_task_group() as task_group:
                writer = WorkflowStreamWriter(
                    world=world,
                    run_id=run_id,
                    name=stream.descriptor.name,
                    task_group=task_group,
                )
                source_error: Exception | None = None
                try:
                    async for value in stream.source:
                        if stream.mode == "objects":
                            await writer.write(value)
                        else:
                            try:
                                frame = encode_framed_bytes(value)
                            except TypeError:
                                raise TypeError(
                                    "byte stream chunks must implement the buffer protocol; "
                                    f"got {type(value).__name__}"
                                ) from None
                            await writer.write_encoded(frame)
                except Exception as error:
                    source_error = error
                    await writer.drain()
                else:
                    await writer.close()
            if source_error is not None:
                raise source_error
        cancelled = cancel_scope.cancel_called
    except BaseException as error:
        owner.error = error
        # The World API has no failed-stream operation. Closing is the only way
        # to guarantee a reader is not left hanging; the background owner still
        # observes and reports the producer exception.
        with anyio.CancelScope(shield=True):
            await world.streams_close(run_id, stream.descriptor.name)
        raise
    finally:
        owner.done = True
        owner.cancel_scope = None
        close = getattr(stream.source, "aclose", None)
        if close is not None:
            with anyio.CancelScope(shield=True):
                await close()

    if cancelled:
        with anyio.CancelScope(shield=True):
            await world.streams_close(run_id, stream.descriptor.name)
    if owner.error is None:
        _readable_pumps.pop((world, run_id, stream.descriptor.name), None)


def bind_readable_sources(state: _ReadableSerialization, *, world: w.World, run_id: str) -> None:
    """Bind newly serialized local sources to *run_id* and register their pumps."""
    if not state.local_sources:
        return
    register = _background_task_registrar()
    for stream in state.local_sources:
        if stream.bound_run_id is not None and stream.bound_run_id != run_id:
            raise RuntimeError(
                f"readable stream {stream.descriptor.name!r} is already bound to "
                f"run {stream.bound_run_id!r}"
            )
        if stream.pump_registered:
            continue
        stream.bound_run_id = run_id
        owner = _ReadablePump()
        stream.owner = owner
        key = (world, run_id, stream.descriptor.name)
        _readable_pumps[key] = owner
        pump = _pump_local_stream(stream, world=world, run_id=run_id, owner=owner)
        try:
            register(pump)
        except BaseException:
            pump.close()
            _readable_pumps.pop(key, None)
            stream.owner = None
            stream.bound_run_id = None
            raise
        stream.pump_registered = True


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


def encode_framed_bytes(value: Buffer, /) -> bytes:
    """Snapshot and frame one user-provided byte-stream chunk."""
    return encode_frame(buffer_to_bytes(value))


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


@pydantic.dataclasses.dataclass(frozen=True)
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

    @pydantic.model_serializer(mode="plain")
    def _identity(self) -> Any:
        """Leave the handle for the lower layer's ``WritableStream`` reducer."""
        return self

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

    async def write_encoded(self, frame: bytes) -> None:
        """Enqueue one already encoded frame through the shared batching sink."""
        await self._enqueue(frame)

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
