"""`WorkflowStreamWriter`: group commit, its bounds, and its failure mode.

The writer acknowledges a `write()` as soon as the chunk is buffered, which is
what lets a producer keep filling while a request is in flight -- and is also
what makes the failure behavior worth pinning: a chunk whose `write()` already
returned can still fail, and it has to surface at the durability barrier rather
than vanish.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Sequence
from typing import Any

import anyio
import pytest

from vercel._internal.workflow import serialization as ser, streams, world as w
from vercel.tests.world_stubs import NoStreams

RUN_ID = "wrun_test"
NAME = "strm_test_user"


class RecordingWorld(NoStreams, w.World):
    """Records stream traffic and can be told to stall or fail a request."""

    def __init__(self, *, fail_on: int | None = None) -> None:
        self.batches: list[list[bytes]] = []
        self.closes: list[str] = []
        self.fail_on = fail_on
        self.calls = 0
        # Set to block the next request until the test releases it, so a
        # "while a request is in flight" state can be observed.
        self.gate: asyncio.Event | None = None

    # -- the streams surface under test ---------------------------------
    async def streams_write(self, run_id: str, name: str, chunk: bytes) -> None:
        await self._record(name, [chunk])

    async def streams_write_multi(self, run_id: str, name: str, chunks: Sequence[bytes]) -> None:
        await self._record(name, list(chunks))

    async def streams_close(self, run_id: str, name: str) -> None:
        self.closes.append(name)

    async def _record(self, name: str, chunks: list[bytes]) -> None:
        self.calls += 1
        if self.gate is not None:
            await self.gate.wait()
        if self.fail_on == self.calls:
            raise w.WorkflowWorldError("server said no", status=500)
        self.batches.append(chunks)

    # -- unused World surface ------------------------------------------
    async def get_deployment_id(self) -> str:
        return ""

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
        raise NotImplementedError

    def create_queue_handler(
        self, queue_name_prefix: w.QueuePrefix, handler: w.QueueHandler
    ) -> w.HTTPHandler:
        raise NotImplementedError

    async def runs_get(self, run_id: str) -> w.WorkflowRun:
        raise NotImplementedError

    async def steps_get(self, run_id: str, step_id: str) -> w.WorkflowStep:
        raise NotImplementedError

    async def hooks_get_by_token(self, token: str) -> w.Hook:
        raise NotImplementedError

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        raise NotImplementedError

    async def events_list(self, run_id: str, *, pagination: Any = None) -> Any:
        raise NotImplementedError


@contextlib.asynccontextmanager
async def _writing(world: w.World) -> AsyncIterator[streams.WorkflowStreamWriter]:
    """A writer plus the task group its dispatch runs in.

    Stands in for the step handler, which owns that group for the length of an
    invocation. Leaving cancels rather than waits: a test that parks a request
    on a gate it never opens should fail on its assertions, not hang here.
    """
    async with anyio.create_task_group() as task_group:
        yield streams.WorkflowStreamWriter(
            world=world, run_id=RUN_ID, name=NAME, task_group=task_group
        )
        task_group.cancel_scope.cancel()


async def _settle() -> None:
    """Let every runnable task reach its next blocking point.

    The dispatch task starts at a checkpoint and takes several more to reach
    the world, so a single `sleep(0)` does not mean "it got there".
    """
    for _ in range(10):
        await asyncio.sleep(0)


def _values(batches: list[list[bytes]]) -> list[Any]:
    """Every chunk sent, decoded, flattened back into write order."""
    decoder = streams.FrameDecoder()
    out = []
    for batch in batches:
        for chunk in batch:
            for payload in decoder.feed(chunk):
                out.append(ser.hydrate(payload, what="chunk"))
    decoder.finish()
    return out


async def test_writes_reach_the_world_in_order() -> None:
    world = RecordingWorld()
    async with _writing(world) as writer:
        for i in range(5):
            await writer.write(i)
        await writer.drain()

    assert _values(world.batches) == [0, 1, 2, 3, 4]


async def test_close_drains_then_marks_the_stream_complete() -> None:
    world = RecordingWorld()
    async with _writing(world) as writer:
        await writer.write("last")
        await writer.close()

    assert _values(world.batches) == ["last"]
    assert world.closes == [NAME]


async def test_close_is_idempotent() -> None:
    world = RecordingWorld()
    async with _writing(world) as writer:
        await writer.close()
        await writer.close()
    assert world.closes == [NAME]


async def test_writing_after_close_is_refused() -> None:
    world = RecordingWorld()
    async with _writing(world) as writer:
        await writer.close()
        with pytest.raises(ser.SerializationError, match="is closed"):
            await writer.write("late")


async def test_chunks_arriving_during_a_request_leave_as_one_group() -> None:
    """The point of the early acknowledgement.

    While the first chunk's request is in flight the producer keeps writing;
    those chunks accumulate and go out together, so a producer writing in a
    loop does not turn into one request per chunk.
    """
    world = RecordingWorld()
    world.gate = asyncio.Event()
    async with _writing(world) as writer:
        await writer.write("first")
        # The dispatch task has taken the first chunk and is blocked on the gate.
        await _settle()
        for i in range(4):
            await writer.write(i)

        world.gate.set()
        await writer.drain()

    assert [len(batch) for batch in world.batches] == [1, 4]
    assert _values(world.batches) == ["first", 0, 1, 2, 3]


async def test_a_group_is_capped_by_chunk_count(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_STREAM_MAX_CHUNKS_PER_BATCH", "2")
    world = RecordingWorld()
    world.gate = asyncio.Event()
    async with _writing(world) as writer:
        for i in range(5):
            await writer.write(i)
        world.gate.set()
        await writer.drain()

    assert [len(batch) for batch in world.batches] == [2, 2, 1]
    assert _values(world.batches) == [0, 1, 2, 3, 4]


async def test_a_group_is_capped_by_bytes(monkeypatch) -> None:
    # Small enough that two frames cannot share a request.
    monkeypatch.setenv("WORKFLOW_STREAM_MAX_BYTES_PER_BATCH", "40")
    world = RecordingWorld()
    world.gate = asyncio.Event()
    async with _writing(world) as writer:
        for _ in range(3):
            await writer.write("x" * 20)
        world.gate.set()
        await writer.drain()

    assert [len(batch) for batch in world.batches] == [1, 1, 1]


async def test_a_chunk_larger_than_the_byte_cap_still_goes_out(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_STREAM_MAX_BYTES_PER_BATCH", "10")
    world = RecordingWorld()
    async with _writing(world) as writer:
        await writer.write("y" * 500)
        await writer.drain()

    assert _values(world.batches) == ["y" * 500]


async def test_write_blocks_at_the_buffer_bound_and_resumes(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_STREAM_MAX_INFLIGHT_CHUNKS", "2")
    monkeypatch.setenv("WORKFLOW_STREAM_MAX_CHUNKS_PER_BATCH", "1")
    world = RecordingWorld()
    world.gate = asyncio.Event()
    async with _writing(world) as writer:
        await writer.write(0)
        await _settle()  # chunk 0 is in flight, blocked on the gate
        await writer.write(1)

        blocked = asyncio.ensure_future(writer.write(2))
        await _settle()
        assert not blocked.done(), "write() should block once the bound is reached"

        world.gate.set()
        await blocked
        await writer.drain()

    assert _values(world.batches) == [0, 1, 2]


async def test_a_failed_request_poisons_the_writer_with_its_own_error() -> None:
    world = RecordingWorld(fail_on=1)
    async with _writing(world) as writer:
        await writer.write("doomed")

        with pytest.raises(w.WorkflowWorldError, match="server said no") as first:
            await writer.drain()
        # Every later call reports the original failure, not a derived one: the
        # chunk is still unsent, and a fresh error would hide why.
        with pytest.raises(w.WorkflowWorldError, match="server said no") as second:
            await writer.write("after")
        with pytest.raises(w.WorkflowWorldError, match="server said no"):
            await writer.close()
        assert first.value is second.value
    assert world.closes == []


async def test_a_failed_group_is_retained_not_dropped() -> None:
    world = RecordingWorld(fail_on=1)
    async with _writing(world) as writer:
        await writer.write("kept")
        with pytest.raises(w.WorkflowWorldError):
            await writer.drain()

    # Nothing landed, and the chunk is still buffered rather than discarded --
    # so a caller that can recover has something to recover.
    assert world.batches == []


async def test_a_failure_reaches_a_writer_already_blocked_on_capacity(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_STREAM_MAX_INFLIGHT_CHUNKS", "1")
    world = RecordingWorld(fail_on=1)
    world.gate = asyncio.Event()
    async with _writing(world) as writer:
        await writer.write(0)
        await _settle()
        blocked = asyncio.ensure_future(writer.write(1))
        await _settle()
        assert not blocked.done()

        world.gate.set()
        with pytest.raises(w.WorkflowWorldError, match="server said no"):
            await blocked


async def test_drain_waits_for_the_request_in_flight() -> None:
    world = RecordingWorld()
    world.gate = asyncio.Event()
    async with _writing(world) as writer:
        await writer.write("pending")
        await _settle()
        draining = asyncio.ensure_future(writer.drain())
        await _settle()
        assert not draining.done(), "drain() must not resolve while a write is in flight"

        world.gate.set()
        await draining
    assert _values(world.batches) == ["pending"]


async def test_cancelling_the_dispatch_does_not_poison_the_writer() -> None:
    """A torn-down dispatch task is not a stream failure.

    `CancelledError` is a `BaseException`, so recording it alongside real send
    failures would make a later `write()` raise it inside a caller that was
    never cancelled -- which reads as that caller being torn down. The group
    the dispatch was carrying goes back on the buffer instead, unsent rather
    than lost.
    """
    world = RecordingWorld()
    world.gate = asyncio.Event()

    async with anyio.create_task_group() as task_group:
        writer = streams.WorkflowStreamWriter(
            world=world, run_id=RUN_ID, name=NAME, task_group=task_group
        )
        await writer.write("first")
        await _settle()  # the dispatch task is now blocked on the gate
        assert writer._dispatching
        task_group.cancel_scope.cancel()

    assert writer._sink_error is None
    assert not writer._dispatching
    assert world.batches == []
    assert _values([writer._buffer]) == ["first"]


async def test_drain_on_an_untouched_writer_is_a_no_op() -> None:
    world = RecordingWorld()
    async with _writing(world) as writer:
        await writer.drain()
    assert world.batches == []


async def test_write_from_forwards_an_async_iterable_in_order() -> None:
    async def source() -> AsyncIterator[str]:
        for token in ("a", "b", "c"):
            yield token

    world = RecordingWorld()
    async with _writing(world) as writer:
        await writer.write_from(source())
        await writer.drain()

    assert _values(world.batches) == ["a", "b", "c"]


async def test_concurrent_writers_do_not_interleave_a_single_chunk() -> None:
    """Chunks from concurrent producers stay whole and stay ordered per producer."""
    world = RecordingWorld()
    async with _writing(world) as writer:

        async def produce(tag: str) -> None:
            for i in range(10):
                await writer.write(f"{tag}{i}")

        await asyncio.gather(produce("a"), produce("b"))
        await writer.drain()

    sent = _values(world.batches)
    assert len(sent) == 20
    assert [v for v in sent if v.startswith("a")] == [f"a{i}" for i in range(10)]
    assert [v for v in sent if v.startswith("b")] == [f"b{i}" for i in range(10)]


async def test_context_manager_closes_on_success_only() -> None:
    world = RecordingWorld()
    async with _writing(world) as writer:
        async with writer:
            await writer.write("done")
    assert world.closes == [NAME]

    # A stream a step failed midway through is left open on purpose: the run may
    # retry the step or write more from a later one, and a closed stream cannot
    # be reopened.
    world2 = RecordingWorld()
    async with _writing(world2) as writer:
        with pytest.raises(RuntimeError):
            async with writer:
                await writer.write("partial")
                raise RuntimeError("boom")
    assert world2.closes == []


async def test_an_empty_stream_name_is_refused() -> None:
    async with anyio.create_task_group() as task_group:
        with pytest.raises(ValueError, match='"name" is required'):
            streams.WorkflowStreamWriter(
                world=RecordingWorld(), run_id=RUN_ID, name="", task_group=task_group
            )
