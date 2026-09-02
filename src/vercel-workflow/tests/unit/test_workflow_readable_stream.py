from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Sequence
from datetime import datetime
from typing import Any

import pytest

from vercel._internal.core.polyfills import UTC
from vercel.workflow import ReadableStream, readable_stream
from vercel.workflow._internal import core, runtime, serialization as ser, streams, world as w

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

    async def get_deployment_id(self) -> str:
        return "dpl_test"

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
        return "msg_test"

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

    async def values() -> AsyncIterator[object]:
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
