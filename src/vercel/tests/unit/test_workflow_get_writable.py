"""`get_writable()` inside a step, and what the step handler owes its chunks.

A stream write returns as soon as it is buffered, so the guarantee that makes
streaming usable is placed here rather than in the writer: by the time a step is
recorded complete, everything it streamed is durable. These tests drive the real
step path of `workflow_handler` to pin that, plus the caching that keeps chunk
order sane.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

import anyio
import pytest

from vercel._internal.core.polyfills import UTC
from vercel._internal.workflow import core, runtime, serialization as ser, streams, world as w
from vercel.tests.world_stubs import NoStreams

NOW = datetime(2026, 1, 1, tzinfo=UTC)
RUN_ID = "wrun_test"
STEP_ID = "step_test"
WORKFLOW_NAME = "workflow//tests.wf"
STREAM = "strm_test_user"


class FakeWorld(NoStreams, w.World):
    """Records the event log and the stream traffic in one ordered list.

    One list, deliberately: the property under test is the *relative order* of
    a chunk landing and the step being recorded complete.
    """

    def __init__(self, *, fail_writes: bool = False, step_input: bytes | None = None) -> None:
        self.log: list[tuple[str, Any]] = []
        self.fail_writes = fail_writes
        self.step_input = step_input or ser.dehydrate(ser.step_arguments((), {}))

    async def get_deployment_id(self) -> str:
        return ""

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
        return "msg_fake"

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

    async def events_list(self, run_id: str, *, pagination: Any = None) -> Any:
        raise NotImplementedError

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        if data.event_type == "step_started":
            return w.EventResult(
                step=w.NonFinalWorkflowStep(
                    runId=RUN_ID,
                    stepId=STEP_ID,
                    stepName=data.correlation_id or "",
                    status="running",
                    attempt=1,
                    createdAt=NOW,
                    updatedAt=NOW,
                    startedAt=NOW,
                    input=self.step_input,
                )
            )
        self.log.append((data.event_type, data))
        return w.EventResult()

    # -- streams --------------------------------------------------------
    async def streams_write(self, run_id: str, name: str, chunk: bytes) -> None:
        await self.streams_write_multi(run_id, name, [chunk])

    async def streams_write_multi(self, run_id: str, name: str, chunks: Sequence[bytes]) -> None:
        if self.fail_writes:
            raise w.WorkflowWorldError("stream unavailable", status=503)
        self.log.append(("chunk", (name, list(chunks))))

    async def streams_close(self, run_id: str, name: str) -> None:
        self.log.append(("close", name))


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)


@pytest.fixture
def registry() -> core.Workflows:
    return core.Workflows(as_vercel_job=False)


async def _invoke(registry: core.Workflows, step_name: str) -> w.QueueContinuation | None:
    payload = w.WorkflowInvokePayload(
        runId=RUN_ID,
        stepId=STEP_ID,
        stepName=step_name,
    )
    return await runtime.workflow_handler(
        payload.model_dump(by_alias=True),
        attempt=1,
        queue_name=f"__wkf_workflow_{WORKFLOW_NAME}",
        message_id="msg_1",
        registry=registry,
    )


def _kinds(fake: FakeWorld) -> list[str]:
    return [kind for kind, _ in fake.log]


def _chunk_values(fake: FakeWorld) -> list[Any]:
    decoder = streams.FrameDecoder()
    out = []
    for kind, entry in fake.log:
        if kind != "chunk":
            continue
        for chunk in entry[1]:
            for payload in decoder.feed(chunk):
                out.append(ser.hydrate(payload, what="chunk"))
    decoder.finish()
    return out


async def test_a_step_streams_and_the_chunks_land_before_it_completes(registry) -> None:
    """The guarantee: "the step finished" implies "its chunks are readable".

    Without the handler's drain, `step_completed` would be recorded while the
    chunks were still sitting in the writer's buffer, and a reader could see the
    run move on past output it never received.
    """

    @registry.step
    async def emit() -> str:
        writable = runtime.get_writable()
        await writable.write("progress 1")
        await writable.write("progress 2")
        return "ok"

    fake = FakeWorld()
    w.set_world(fake)

    await _invoke(registry, emit.name)

    assert _kinds(fake) == ["chunk", "step_completed"]
    assert _chunk_values(fake) == ["progress 1", "progress 2"]


async def test_the_default_stream_is_named_after_the_run(registry) -> None:
    @registry.step
    async def emit() -> None:
        await runtime.get_writable().write("x")

    fake = FakeWorld()
    w.set_world(fake)
    await _invoke(registry, emit.name)

    ((name, _),) = [entry for kind, entry in fake.log if kind == "chunk"]
    assert name == STREAM


async def test_two_calls_in_one_step_share_a_writer(registry) -> None:
    """One writer per stream per step, so writes go through one serial sink.

    A fresh writer per call would give each its own buffer over the same
    stream, and two buffers flushing independently interleave by whichever
    request wins -- scrambling the order the step wrote in.
    """
    seen: list[streams.WorkflowWritable] = []

    @registry.step
    async def emit() -> None:
        for i in range(3):
            writable = runtime.get_writable()
            seen.append(writable)
            await writable.write(i)

    fake = FakeWorld()
    w.set_world(fake)
    await _invoke(registry, emit.name)

    assert seen[0] is seen[1] is seen[2]
    assert _chunk_values(fake) == [0, 1, 2]


async def test_a_namespace_gets_its_own_stream(registry) -> None:
    @registry.step
    async def emit() -> None:
        await runtime.get_writable().write("default")
        await runtime.get_writable(namespace="logs").write("logged")

    fake = FakeWorld()
    w.set_world(fake)
    await _invoke(registry, emit.name)

    names = {entry[0] for kind, entry in fake.log if kind == "chunk"}
    assert names == {STREAM, streams.workflow_run_stream_id(RUN_ID, "logs")}


async def test_closing_from_a_step_marks_the_stream_complete(registry) -> None:
    @registry.step
    async def finish() -> None:
        writable = runtime.get_writable()
        await writable.write("last")
        await writable.close()

    fake = FakeWorld()
    w.set_world(fake)
    await _invoke(registry, finish.name)

    assert _kinds(fake) == ["chunk", "close", "step_completed"]


async def test_a_stream_left_open_is_not_closed_by_the_step_ending(registry) -> None:
    """Deliberate: the run's stream spans steps.

    Closing at step end would end the stream after the first step that touched
    it, and a closed stream cannot be reopened. The cost is that a workflow
    which never closes leaves readers waiting until the run expires.
    """

    @registry.step
    async def emit() -> None:
        await runtime.get_writable().write("more to come")

    fake = FakeWorld()
    w.set_world(fake)
    await _invoke(registry, emit.name)

    assert "close" not in _kinds(fake)


async def test_a_stream_write_failure_fails_the_step(registry) -> None:
    # Surfacing at the drain is what turns a lost chunk into a retryable step
    # rather than output that silently never arrives.
    @registry.step
    async def emit() -> str:
        await runtime.get_writable().write("doomed")
        return "unreachable"

    fake = FakeWorld(fail_writes=True)
    w.set_world(fake)

    result = await _invoke(registry, emit.name)

    assert _kinds(fake) == ["step_retrying"]
    assert result == w.QueueContinuation(delay_seconds=1.0)
    ((_, event),) = fake.log
    assert "stream unavailable" in event.event_data.error


async def test_a_failing_step_still_flushes_what_it_wrote(registry) -> None:
    """A reader tailing the run should see the progress that led to the failure."""

    @registry.step
    async def emit() -> None:
        await runtime.get_writable().write("got this far")
        raise RuntimeError("boom")

    fake = FakeWorld()
    w.set_world(fake)
    await _invoke(registry, emit.name)

    assert _kinds(fake) == ["chunk", "step_retrying"]
    assert _chunk_values(fake) == ["got this far"]


async def test_get_writable_outside_a_run_is_refused() -> None:
    # Without a run there is no stream to refer to, so this cannot answer.
    with pytest.raises(RuntimeError, match="inside a workflow or a step"):
        runtime.get_writable()


class TestHandOff:
    """A workflow body picking out a stream and a step writing to it.

    The workflow cannot write -- it replays, and its sandbox has no network --
    but it can say *which* stream, which is what lets one workflow fan the same
    stream out to several steps.
    """

    async def test_a_workflow_body_gets_a_handle_it_cannot_write_to(self, registry) -> None:
        writable: list[Any] = []

        @registry.workflow
        async def wf() -> None:
            writable.append(runtime.get_writable())

        fake = FakeWorld()
        w.set_world(fake)
        ctx = runtime.WorkflowOrchestratorContext(
            [], run_id=RUN_ID, seed=RUN_ID, started_at=0, registry=registry
        )
        token = ctx._ctx.set(ctx)
        try:
            await wf.func()
        finally:
            ctx._ctx.reset(token)

        (handle,) = writable
        assert isinstance(handle, streams.WorkflowStreamHandle)
        assert handle.name == STREAM
        assert handle.run_id == RUN_ID
        with pytest.raises(RuntimeError, match="Pass this to a step"):
            await handle.write("nope")

    async def test_the_handle_is_the_same_on_every_replay(self, registry) -> None:
        # Derived from the run id, not minted, so a replay cannot point a later
        # attempt at a different stream.
        ctx = runtime.WorkflowOrchestratorContext(
            [], run_id=RUN_ID, seed=RUN_ID, started_at=0, registry=registry
        )
        again = runtime.WorkflowOrchestratorContext(
            [], run_id=RUN_ID, seed=RUN_ID, started_at=0, registry=registry
        )
        assert ctx.stream_handle(None) == again.stream_handle(None)
        assert ctx.stream_handle("logs") != ctx.stream_handle(None)

    async def test_a_handle_survives_the_trip_into_a_step(self, registry) -> None:
        """The round trip that makes the feature work.

        The handle is dehydrated into the step's arguments as `@workflow/core`'s
        `WritableStream` tag and revived on the other side as a live writer, so
        the step writes to the stream the workflow chose.
        """
        handle = streams.WorkflowStreamHandle(RUN_ID, STREAM)
        payload = ser.dehydrate(ser.step_arguments((), {"out": handle}))

        # The tag is the one a TypeScript peer already understands.
        assert b'"WritableStream"' in payload

        fake = FakeWorld()
        w.set_world(fake)
        state = runtime._StepStreams(run_id=RUN_ID)
        token = runtime._step_streams_ctx.set(state)
        try:
            # Inside `dispatching()`, as the real step path is: reviving the tag
            # makes a writer, and a writer needs the group its sends run in.
            async with state.dispatching():
                _, kwargs = ser.step_call_arguments(
                    ser.hydrate(payload, what="input"), what="input"
                )
        finally:
            runtime._step_streams_ctx.reset(token)

        revived = kwargs["out"]
        assert isinstance(revived, streams.WorkflowStreamWriter)
        assert (revived.run_id, revived.name) == (RUN_ID, STREAM)

    async def test_a_handle_hydrated_outside_a_step_stays_a_handle(self) -> None:
        """Nothing out here can carry the sends.

        A writer dispatches from a task in the group the step handler owns, so
        a client reading a payload that happens to name a stream gets the
        handle back rather than a writer with nowhere to send from.
        """
        payload = ser.dehydrate(
            ser.step_arguments((), {"out": streams.WorkflowStreamHandle(RUN_ID, STREAM)})
        )

        _, kwargs = ser.step_call_arguments(ser.hydrate(payload, what="input"), what="input")

        revived = kwargs["out"]
        assert isinstance(revived, streams.WorkflowStreamHandle)
        assert (revived.run_id, revived.name) == (RUN_ID, STREAM)
        with pytest.raises(RuntimeError, match="Pass this to a step"):
            await revived.write("nope")

    async def test_a_revived_handle_is_the_step_own_writer(self, registry) -> None:
        """Not a second writer over the same stream.

        Two writers would each buffer independently and interleave by whichever
        request wins, so a step that both received a handle and called
        `get_writable()` would scramble its own chunk order.
        """
        seen: list[Any] = []

        @registry.step
        async def emit(*, out: Any) -> None:
            seen.append(out)
            seen.append(runtime.get_writable())
            await out.write("via handle")
            await seen[1].write("via get_writable")

        fake = FakeWorld(
            step_input=ser.dehydrate(
                ser.step_arguments((), {"out": streams.WorkflowStreamHandle(RUN_ID, STREAM)})
            )
        )
        w.set_world(fake)
        await _invoke(registry, emit.name)

        assert seen[0] is seen[1], "the revived handle has to be the step's own writer"
        assert _chunk_values(fake) == ["via handle", "via get_writable"]
        assert _kinds(fake)[-1] == "step_completed"

    async def test_a_writer_reduces_back_to_the_same_reference(self) -> None:
        # A step forwarding its own writable onward has to produce the same
        # descriptor the workflow would have.
        world = FakeWorld()
        async with anyio.create_task_group() as task_group:
            writer = streams.WorkflowStreamWriter(
                world=world, run_id=RUN_ID, name=STREAM, task_group=task_group
            )
            handle = streams.WorkflowStreamHandle(RUN_ID, STREAM)
            assert ser.dehydrate(writer) == ser.dehydrate(handle)
