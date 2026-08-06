"""Reading a stream back, and surviving the connection breaking underneath.

A live read outlives its transport: the Vercel world's read endpoint errors the
response body when the server's max duration expires. Telling that apart from
the end of the stream, and resuming at exactly the right chunk, is the whole
job of the reader -- so most of this file is about the break.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, Sequence
from typing import Any

import pytest

from vercel._internal.workflow import runtime, serialization as ser, streams, world as w
from vercel.tests.world_stubs import NoStreams

RUN_ID = "wrun_test"
NAME = "strm_test_user"


class ReplayWorld(NoStreams, w.World):
    """Serves a fixed list of frames, optionally breaking part-way through.

    ``breaks`` is how many times a read should fail before one is allowed to
    run to the end, and ``deliver_before_break`` how many frames each of those
    reads hands over first. ``reads`` records the ``start_index`` of every
    connection, which is what the resume assertions are about.
    """

    def __init__(
        self,
        values: Sequence[Any],
        *,
        breaks: int = 0,
        deliver_before_break: int = 1,
        split_at: int | None = None,
    ) -> None:
        self.frames = [streams.encode_value(value) for value in values]
        self.breaks = breaks
        self.deliver_before_break = deliver_before_break
        # Byte offset to cut the transport read at, so a frame is delivered in
        # two pieces -- and, on a breaking read, cut mid-frame.
        self.split_at = split_at
        self.reads: list[int | None] = []

    def streams_get(
        self, run_id: str, name: str, start_index: int | None = None
    ) -> AsyncGenerator[bytes, None]:
        self.reads.append(start_index)
        breaking = self.breaks > 0
        if breaking:
            self.breaks -= 1
        return self._serve(start_index or 0, breaking)

    async def _serve(self, start: int, breaking: bool) -> AsyncGenerator[bytes, None]:
        wire = b"".join(self.frames[start:])
        if breaking:
            # Hand over whole frames, then a partial one, then die.
            whole = b"".join(self.frames[start : start + self.deliver_before_break])
            trailing = self.frames[start + self.deliver_before_break :]
            yield whole + (trailing[0][:3] if trailing else b"")
            raise w.WorkflowWorldError("connection reset", status=500)
        if self.split_at is not None:
            yield wire[: self.split_at]
            yield wire[self.split_at :]
        else:
            yield wire

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


async def _read(world: w.World, start_index: int | None = None) -> list[Any]:
    out = []
    frames = streams.reconnecting_frames(world, RUN_ID, NAME, start_index)
    async with contextlib.aclosing(frames):
        async for payload in frames:
            out.append(ser.hydrate(payload, what="chunk"))
    return out


class TestReading:
    async def test_reads_every_frame_in_order(self) -> None:
        world = ReplayWorld(["a", {"b": 1}, 2])
        assert await _read(world) == ["a", {"b": 1}, 2]
        # An absent start index resolves to 0 on the way out, as it does in
        # `createReconnectingFramedStream`, so resume arithmetic has a base.
        assert world.reads == [0]

    async def test_a_clean_end_does_not_reconnect(self) -> None:
        # EOF is the stream being closed, not a failure -- reconnecting here
        # would reopen a finished stream forever.
        world = ReplayWorld(["only"])
        await _read(world)
        assert len(world.reads) == 1

    async def test_frames_split_across_transport_reads_are_reassembled(self) -> None:
        world = ReplayWorld(["first", "second"], split_at=7)
        assert await _read(world) == ["first", "second"]

    async def test_start_index_is_passed_through(self) -> None:
        world = ReplayWorld(["a", "b", "c"])
        assert await _read(world, 1) == ["b", "c"]
        assert world.reads == [1]

    async def test_an_empty_stream_yields_nothing(self) -> None:
        assert await _read(ReplayWorld([])) == []


class TestReconnect:
    async def test_resumes_at_the_frame_after_the_last_one_delivered(self) -> None:
        world = ReplayWorld(["a", "b", "c", "d"], breaks=1, deliver_before_break=2)

        assert await _read(world) == ["a", "b", "c", "d"]
        # Two frames arrived before the break, so the reopen asks for the third.
        assert world.reads == [0, 2]

    async def test_a_frame_cut_in_half_by_the_break_arrives_whole(self) -> None:
        """The partial frame is dropped, not stitched onto the new connection.

        The reopened read starts at a frame boundary and re-sends that chunk in
        full, so keeping the fragment would corrupt it.
        """
        world = ReplayWorld(["a", "bbbbbbbb", "c"], breaks=1, deliver_before_break=1)

        assert await _read(world) == ["a", "bbbbbbbb", "c"]
        assert world.reads == [0, 1]

    async def test_resume_is_relative_to_the_caller_start_index(self) -> None:
        world = ReplayWorld(["a", "b", "c", "d"], breaks=1, deliver_before_break=1)

        assert await _read(world, 1) == ["b", "c", "d"]
        # Started at 1, delivered one frame, so the reopen is at 2 -- not at 1.
        assert world.reads == [1, 2]

    async def test_repeated_breaks_keep_making_progress(self) -> None:
        world = ReplayWorld(list("abcde"), breaks=3, deliver_before_break=1)

        assert await _read(world) == list("abcde")
        assert world.reads == [0, 1, 2, 3]

    async def test_a_negative_start_index_is_never_resumed(self) -> None:
        """Last-N resolves against the tail at connect time.

        Reopening would re-resolve it against a tail that has since moved, so
        the window would shift under the reader; single-shot is the honest
        behaviour.
        """
        world = ReplayWorld(["a", "b"], breaks=1)

        with pytest.raises(w.WorkflowWorldError, match="connection reset"):
            await _read(world, -2)
        assert len(world.reads) == 1

    async def test_giving_up_after_too_many_consecutive_failures(self, monkeypatch) -> None:
        monkeypatch.setenv("WORKFLOW_FRAMED_STREAM_MAX_RECONNECTS", "3")
        # Never delivers anything, so no reconnect ever counts as progress.
        world = ReplayWorld(["a"], breaks=100, deliver_before_break=0)

        with pytest.raises(ser.SerializationError, match="3 consecutive reconnection"):
            await _read(world)
        assert len(world.reads) == 4

    async def test_progress_resets_the_consecutive_budget(self, monkeypatch) -> None:
        # Two failures, each delivering a frame, under a budget of two: the
        # budget must not accumulate across a reconnect that made progress.
        monkeypatch.setenv("WORKFLOW_FRAMED_STREAM_MAX_RECONNECTS", "2")
        world = ReplayWorld(list("abcd"), breaks=2, deliver_before_break=1)

        assert await _read(world) == list("abcd")

    async def test_the_absolute_backstop_stops_a_backend_that_ignores_resume(
        self, monkeypatch
    ) -> None:
        """A world that re-sends from the start looks like progress forever.

        The consecutive cap resets every time, so only the total cap can end
        this -- without it the loop would never terminate.
        """
        monkeypatch.setenv("WORKFLOW_FRAMED_STREAM_MAX_TOTAL_RECONNECTS", "5")

        class IgnoresStartIndex(ReplayWorld):
            def streams_get(self, run_id, name, start_index=None):
                self.reads.append(start_index)
                return self._serve(0, True)  # always from 0, always breaks

        world = IgnoresStartIndex(["a", "b"], deliver_before_break=1)

        with pytest.raises(ser.SerializationError, match="5 total reconnection"):
            await _read(world)


class TestRunApi:
    @pytest.fixture(autouse=True)
    def _reset_world(self):
        yield
        w.set_world(None)

    async def test_readable_hydrates_the_run_default_stream(self) -> None:
        world = ReplayWorld(["hello", {"n": 1}])
        w.set_world(world)

        assert [chunk async for chunk in runtime.Run(RUN_ID).readable()] == ["hello", {"n": 1}]

    async def test_readable_bytes_yields_only_bytes(self) -> None:
        world = ReplayWorld([b"one", b"two"])
        w.set_world(world)

        assert [b async for b in runtime.Run(RUN_ID).readable_bytes()] == [b"one", b"two"]

    async def test_readable_bytes_refuses_a_stream_of_values(self) -> None:
        # An HTTP body cannot carry a dict, and failing here names the problem
        # better than whatever the response layer would say.
        world = ReplayWorld([{"not": "bytes"}])
        w.set_world(world)

        with pytest.raises(ser.SerializationError, match="not bytes"):
            [b async for b in runtime.Run(RUN_ID).readable_bytes()]

    async def test_a_namespace_reads_its_own_stream(self) -> None:
        world = ReplayWorld(["logged"])
        w.set_world(world)

        run = runtime.Run(RUN_ID)
        async with contextlib.aclosing(run.readable(namespace="logs")) as chunks:
            assert [c async for c in chunks] == ["logged"]

    async def test_abandoning_a_read_closes_the_underlying_one(self) -> None:
        closed = False

        class Watcher(ReplayWorld):
            def streams_get(self, run_id, name, start_index=None):
                self.reads.append(start_index)
                return self._watched()

            async def _watched(self):
                nonlocal closed
                try:
                    for frame in self.frames:
                        yield frame
                finally:
                    closed = True

        w.set_world(Watcher(["a", "b", "c"]))

        chunks = runtime.Run(RUN_ID).readable()
        async with contextlib.aclosing(chunks):
            assert await chunks.__anext__() == "a"

        assert closed, "closing the reader has to release the transport read"

    async def test_stream_info_and_list_reach_the_world(self) -> None:
        class Meta(ReplayWorld):
            async def streams_get_info(self, run_id: str, name: str) -> w.StreamInfo:
                return w.StreamInfo(tailIndex=4, done=True)

            async def streams_list(self, run_id: str) -> list[str]:
                return [NAME]

        w.set_world(Meta([]))
        run = runtime.Run(RUN_ID)

        assert await run.stream_info() == w.StreamInfo(tailIndex=4, done=True)
        assert await run.list_streams() == [NAME]

    async def test_read_stream_takes_a_name_run_cannot_derive(self) -> None:
        world = ReplayWorld(["x"])
        w.set_world(world)

        assert [c async for c in runtime.read_stream(RUN_ID, "strm_someone_else")] == ["x"]
