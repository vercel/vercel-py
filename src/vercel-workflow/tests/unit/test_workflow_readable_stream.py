from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Sequence
from datetime import datetime
from typing import Any

import anyio
import pytest

from vercel._internal.core.polyfills import UTC, Buffer
from vercel.workflow import ReadableStream, readable_stream
from vercel.workflow._internal import core, runtime, serialization as ser, streams, world as w
from vercel.workflow._internal.worlds.local import LocalWorld

from ..world_stubs import NoStreams

RUN_ID = "wrun_readable"
STEP_ID = "step_readable"
NOW = datetime(2026, 9, 2, tzinfo=UTC)


class ReadableWorld(NoStreams, w.World):
    def __init__(self, *, step_input: bytes | None = None) -> None:
        self.step_input = step_input or ser.dehydrate(ser.step_arguments((), {}))
        self.events: list[w.Event] = []
        self.chunks: dict[str, list[bytes]] = {}
        self.closed: set[str] = set()
        self.queues: list[w.QueuePayload] = []
        self.run: w.WorkflowRun | None = None

    async def get_deployment_id(self) -> str:
        return "dpl_test"

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
        self.queues.append(message)
        return "msg_test"

    def create_queue_handler(
        self, queue_name_prefix: w.QueuePrefix, handler: w.QueueHandler
    ) -> w.HTTPHandler:
        raise NotImplementedError

    async def runs_get(self, run_id: str) -> w.WorkflowRun:
        if self.run is None:
            raise NotImplementedError
        return self.run

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
                    run_id=RUN_ID,
                    step_id=STEP_ID,
                    step_name=data.correlation_id or "",
                    status="running",
                    attempt=1,
                    created_at=NOW,
                    updated_at=NOW,
                    started_at=NOW,
                    input=self.step_input,
                )
            )
        self.events.append(data)
        return w.EventResult()

    async def streams_write(self, run_id: str, name: str, chunk: bytes) -> None:
        self.chunks.setdefault(name, []).append(chunk)

    async def streams_write_multi(self, run_id: str, name: str, chunks: Sequence[bytes]) -> None:
        self.chunks.setdefault(name, []).extend(chunks)

    async def streams_close(self, run_id: str, name: str) -> None:
        self.closed.add(name)

    def streams_get(
        self, run_id: str, name: str, start_index: int | None = None
    ) -> AsyncGenerator[bytes, None]:
        async def read() -> AsyncGenerator[bytes, None]:
            start = start_index or 0
            for chunk in self.chunks.get(name, [])[start:]:
                yield chunk

        return read()


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)


async def _invoke(registry: core.Workflows, step_name: str) -> None:
    payload = w.WorkflowInvokePayload(
        run_id=RUN_ID,
        step_id=STEP_ID,
        step_name=step_name,
    )
    await runtime.workflow_handler(
        payload.model_dump(by_alias=True),
        attempt=1,
        queue_name="__wkf_workflow_test",
        message_id="msg_test",
        registry=registry,
    )


async def test_step_returned_object_stream_is_recorded_before_its_pump_runs() -> None:
    registry = core.Workflows(as_vercel_job=False)
    started = False

    async def values() -> AsyncIterator[Any]:
        nonlocal started
        started = True
        yield {"token": "hello"}
        yield b"world"

    @registry.step
    async def produce() -> ReadableStream[object]:
        return readable_stream(values())

    world = ReadableWorld()
    w.set_world(world)
    background: list[Awaitable[object]] = []

    with streams.readable_background_tasks(background.append):
        await _invoke(registry, produce.name)

    assert not started
    completed = [event for event in world.events if event.event_type == "step_completed"]
    assert len(completed) == 1
    payload = completed[0].event_data.result
    assert b'"ReadableStream"' in payload

    with streams.reviving_readable_streams(RUN_ID):
        workflow_value = ser.hydrate(payload, what="step result")
    assert isinstance(workflow_value, ReadableStream)
    with pytest.raises(RuntimeError, match="inside a workflow"):
        workflow_value.__aiter__()

    # Forwarding an existing reference preserves its descriptor and does not
    # register a second source pump.
    assert ser.dehydrate(workflow_value) == payload
    assert len(background) == 1

    await background[0]
    assert world.closed == set(world.chunks)

    with streams.reviving_readable_streams(RUN_ID, world=world, live=True):
        caller_value = ser.hydrate(payload, what="run output")
    assert [item async for item in caller_value] == [
        {"token": "hello"},
        b"world",
    ]


async def test_local_sources_need_an_execution_boundary_owner() -> None:
    async def values() -> AsyncIterator[int]:
        yield 1

    stream = readable_stream(values())
    with pytest.raises(RuntimeError, match="execution boundary"):
        ser.dehydrate(stream)
    await stream.aclose()


async def test_unknown_readable_descriptor_metadata_is_rejected() -> None:
    payload = ser.DEVALUE_V1 + b'[["ReadableStream",1],{"name":2,"type":3},"s","other"]'
    with pytest.raises(ser.SerializationError, match="unknown ReadableStream type"):
        ser.hydrate(payload, what="stream")


async def test_object_stream_can_flow_from_step_a_into_step_b() -> None:
    registry = core.Workflows(as_vercel_job=False)

    async def values() -> AsyncIterator[str]:
        yield "one"
        yield "two"

    @registry.step
    async def consume(source: ReadableStream[str]) -> list[str]:
        return [value async for value in source]

    source = readable_stream(values())
    background: list[Awaitable[object]] = []
    with streams.collecting_readable_sources() as pending:
        reference = ser.dehydrate(source)

    world = ReadableWorld(
        step_input=ser.dehydrate(
            ser.step_arguments((), {"source": ser.hydrate(reference, what="reference")})
        )
    )
    w.set_world(world)
    with streams.readable_background_tasks(background.append):
        streams.bind_readable_sources(pending, world=world, run_id=RUN_ID)
    await background[0]

    await _invoke(registry, consume.name)
    completed = [event for event in world.events if event.event_type == "step_completed"]
    assert ser.hydrate(completed[-1].event_data.result, what="result") == ["one", "two"]


async def test_nested_references_preserve_identity_without_repumping() -> None:
    async def values() -> AsyncIterator[int]:
        yield 1

    source = readable_stream(values())
    with streams.collecting_readable_sources() as pending:
        payload = ser.dehydrate({"direct": source, "nested": [source]})

    with streams.reviving_readable_streams(RUN_ID):
        revived = ser.hydrate(payload, what="nested stream")
    assert revived["direct"] is revived["nested"][0]
    assert len(pending.local_sources) == 1
    await source.aclose()


async def test_start_defers_binding_until_the_run_exists(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    registry = core.Workflows(as_vercel_job=False)

    @registry.workflow
    async def relay(source: ReadableStream[str]) -> ReadableStream[str]:
        return source

    async def values() -> AsyncIterator[str]:
        yield "started"

    class World(LocalWorld):
        async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
            return "msg_start"

    world = World()
    w.set_world(world)
    background: list[Awaitable[object]] = []
    with streams.readable_background_tasks(background.append):
        run = await runtime.start(relay, readable_stream(values()))

    stored = await world.runs_get(run.run_id)
    assert isinstance(stored.input, bytes)
    assert b'"ReadableStream"' in stored.input
    assert len(background) == 1
    await background[0]

    with streams.reviving_readable_streams(run.run_id, world=world, live=True):
        _, kwargs = ser.call_arguments(
            ser.hydrate(stored.input, what="run input"), what="run input"
        )
    assert [value async for value in kwargs["source"]] == ["started"]


async def test_hook_resumer_can_own_an_object_stream() -> None:
    async def values() -> AsyncIterator[str]:
        yield "resumed"

    world = ReadableWorld()
    world.run = w.NonFinalWorkflowRun(
        run_id=RUN_ID,
        status="running",
        deployment_id="dpl_test",
        workflow_name="workflow//test",
        created_at=NOW,
        updated_at=NOW,
        started_at=NOW,
    )
    w.set_world(world)
    hook = core.Hook(
        token="hook_token",
        hook_id="hook_test",
        run_id=RUN_ID,
        created_at=NOW,
    )
    background: list[Awaitable[object]] = []
    with streams.readable_background_tasks(background.append):
        await runtime.resume_hook(hook, readable_stream(values()))

    received = [event for event in world.events if event.event_type == "hook_received"]
    assert len(received) == 1
    assert b'"ReadableStream"' in received[0].event_data.payload
    await background[0]

    with streams.reviving_readable_streams(RUN_ID, world=world, live=True):
        value = ser.hydrate(received[0].event_data.payload, what="hook payload")
    assert [item async for item in value] == ["resumed"]


async def test_producer_error_is_reported_by_a_local_reader(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))

    async def values() -> AsyncIterator[str]:
        yield "before"
        raise RuntimeError("producer failed")

    world = LocalWorld()
    source = readable_stream(values())
    background: list[Awaitable[object]] = []
    with streams.collecting_readable_sources() as pending:
        payload = ser.dehydrate(source)
    with streams.readable_background_tasks(background.append):
        streams.bind_readable_sources(pending, world=world, run_id=RUN_ID)

    with pytest.raises(RuntimeError, match="producer failed"):
        await background[0]

    with streams.reviving_readable_streams(RUN_ID, world=world, live=True):
        reader = ser.hydrate(payload, what="stream")
    iterator = aiter(reader)
    assert await anext(iterator) == "before"
    with pytest.raises(RuntimeError, match="producer failed"):
        await anext(iterator)


async def test_aclose_cancels_a_locally_owned_pump(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    producing = anyio.Event()
    source_closed = anyio.Event()

    async def values() -> AsyncIterator[str]:
        try:
            yield "first"
            producing.set()
            await anyio.sleep_forever()
        finally:
            source_closed.set()

    world = LocalWorld()
    source = readable_stream(values())
    background: list[Awaitable[object]] = []
    with streams.collecting_readable_sources() as pending:
        payload = ser.dehydrate(source)
    with streams.readable_background_tasks(background.append):
        streams.bind_readable_sources(pending, world=world, run_id=RUN_ID)

    async def run_pump() -> None:
        await background[0]

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(run_pump)
        await producing.wait()
        with streams.reviving_readable_streams(RUN_ID, world=world, live=True):
            reader = ser.hydrate(payload, what="stream")
        iterator = aiter(reader)
        assert await anext(iterator) == "first"
        await reader.aclose()
        await source_closed.wait()

    info = await world.streams_get_info(RUN_ID, next(iter(await world.streams_list(RUN_ID))))
    assert info.done


async def test_step_returned_framed_byte_stream_preserves_user_chunks() -> None:
    registry = core.Workflows(as_vercel_job=False)
    mutable = bytearray(b"mutable")

    async def values() -> AsyncIterator[Buffer]:
        yield mutable
        mutable[:] = b"changed"
        yield memoryview(b"view")
        yield b""

    @registry.step
    async def produce() -> ReadableStream[bytes]:
        return readable_stream(values(), mode="bytes")

    world = ReadableWorld()
    w.set_world(world)
    background: list[Awaitable[object]] = []
    with streams.readable_background_tasks(background.append):
        await _invoke(registry, produce.name)

    completed = [event for event in world.events if event.event_type == "step_completed"]
    payload = completed[0].event_data.result
    assert b'"type"' in payload and b'"bytes"' in payload
    assert b'"framing"' in payload and b'"framed-v1"' in payload
    await background[0]

    stored = next(iter(world.chunks.values()))
    assert stored == [
        streams.encode_frame(b"mutable"),
        streams.encode_frame(b"view"),
        streams.encode_frame(b""),
    ]
    assert all(ser.DEVALUE_V1 not in frame for frame in stored)

    with streams.reviving_readable_streams(RUN_ID, world=world, live=True):
        reader = ser.hydrate(payload, what="byte stream")
    assert [chunk async for chunk in reader] == [b"mutable", b"view", b""]


async def test_byte_stream_rejects_a_non_buffer_chunk() -> None:
    async def values() -> AsyncIterator[Any]:
        yield "not bytes"

    source = readable_stream(values(), mode="bytes")
    world = ReadableWorld()
    background: list[Awaitable[object]] = []
    with streams.collecting_readable_sources() as pending:
        ser.dehydrate(source)
    with streams.readable_background_tasks(background.append):
        streams.bind_readable_sources(pending, world=world, run_id=RUN_ID)

    with pytest.raises(
        TypeError,
        match="byte stream chunks must implement the buffer protocol; got str",
    ):
        await background[0]


async def test_legacy_raw_byte_descriptor_reads_transport_chunks_without_framing() -> None:
    payload = ser.DEVALUE_V1 + (b'[["ReadableStream",1],{"name":2,"type":3},"legacy","bytes"]')
    world = ReadableWorld()
    world.chunks["legacy"] = [b"raw ", b"transport"]
    world.closed.add("legacy")

    with streams.reviving_readable_streams(RUN_ID, world=world, live=True):
        reader = ser.hydrate(payload, what="legacy stream")
    assert [chunk async for chunk in reader] == [b"raw ", b"transport"]
