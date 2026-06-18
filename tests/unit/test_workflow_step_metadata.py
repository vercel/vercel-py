"""Tests for step metadata exposed to step bodies via ``get_step_metadata()``.

Mirrors the JS SDK's ``getStepMetadata()``: a step body can read the stable
``step_id`` (plus run id, name, attempt) of the step it is executing, e.g. to use
as an idempotency key for non-idempotent side effects.
"""

from __future__ import annotations

import json

import pytest

from vercel._internal.workflow import runtime, world as w
from vercel._internal.workflow.worlds.local import LocalWorld
from vercel.workflow import StepInfo, Workflows, get_step_metadata


def _encode(args: list, kwargs: dict) -> list[bytes]:
    return [b"json" + json.dumps([args, kwargs]).encode()]


class _RecordingLocalWorld(LocalWorld):
    """Real LocalWorld for storage; only the outbound (networked) queue is stubbed."""

    def __init__(self, data_dir) -> None:
        super().__init__()
        self.data_dir = data_dir
        self.queued: list[tuple[str, w.QueuePayload]] = []

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs) -> str:
        self.queued.append((queue_name, message))
        return "msg_test"


def test_get_step_metadata_outside_step_raises() -> None:
    with pytest.raises(RuntimeError, match="inside a step"):
        get_step_metadata()


async def test_step_metadata_available_inside_step(tmp_path, monkeypatch) -> None:
    world = _RecordingLocalWorld(tmp_path)
    monkeypatch.setattr(w, "the_world", world)

    registry = Workflows(as_vercel_job=False)
    captured: list[StepInfo] = []

    @registry.step
    async def greet(name: str) -> str:
        captured.append(get_step_metadata())
        return f"hi {name}"

    # Stand up a running run with a single pending step.
    run_result = await world.events_create(
        None,
        w.RunCreatedEventData(
            deploymentId="",
            workflowName="test-wf",
            input=_encode(["world"], {}),
        ).into_event(),
    )
    assert run_result.run is not None
    run_id = run_result.run.run_id
    await world.events_create(run_id, w.RunStartedEvent())

    step_id = "step_testid"
    await world.events_create(
        run_id,
        w.StepCreatedEventData(stepName=greet.name, input=_encode(["world"], {})).into_event(
            step_id
        ),
    )

    payload = w.StepInvokePayload(
        workflowName="test-wf",
        workflowRunId=run_id,
        workflowStartedAt=0.0,
        stepId=step_id,
    )
    await runtime.step_handler(
        payload.model_dump(by_alias=True),
        attempt=1,
        queue_name=f"__wkf_step_{greet.name}",
        message_id="msg_1",
        registry=registry,
    )

    # The body saw its own metadata.
    assert len(captured) == 1
    info = captured[0]
    assert isinstance(info, StepInfo)
    assert info.run_id == run_id
    assert info.step_id == step_id
    assert info.step_name == greet.name
    assert info.attempt == 1

    # The step actually ran to completion, and the parent workflow was re-enqueued.
    step_run = await world.steps_get(run_id, step_id)
    assert step_run.status == "completed"
    assert any(qn.startswith("__wkf_workflow_") for qn, _ in world.queued)

    # The context var is cleared once the step body returns.
    with pytest.raises(RuntimeError, match="inside a step"):
        get_step_metadata()
