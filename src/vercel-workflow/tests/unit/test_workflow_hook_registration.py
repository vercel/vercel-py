"""When a hook is registered, and what happens to a payload nobody awaits yet.

A hook is registered (its ``hook_created`` written) at the run's next
suspension, whatever the body is blocked on, not at the first ``await`` of the
hook. That matches the JS SDK, and it means a token is claimed, and resumable
from outside, as soon as the body has created the hook and yielded.

Registering early means a payload can arrive while the body is doing something
else. Such a ``hook_received`` must be held until the body awaits the hook, not
dropped. The same hazard already existed via ``get_conflict()``, which left a
registered hook with no awaiter.
"""

from __future__ import annotations

from collections.abc import Iterator

import pydantic
import pytest

from tests.payloads import PLAIN_ENCODER
from vercel.workflow._internal import core, runtime, serialization as ser, world as w
from vercel.workflow._internal.worlds import local as local_mod

TOKEN = "order:42"

registry = core.Workflows(as_vercel_job=False)


class Approval(core.BaseHook, pydantic.BaseModel):
    approved: bool


@registry.step
async def pending_step() -> str:
    return "done"


@registry.workflow
async def create_then_step_then_await() -> dict[str, object]:
    approval = Approval.wait(token=TOKEN)
    step_result = await pending_step()
    return {"approved": (await approval).approved, "step_result": step_result}


@registry.workflow
async def confirm_then_step_then_await() -> dict[str, object]:
    approval = Approval.wait(token=TOKEN)
    assert await approval.get_conflict() is None
    step_result = await pending_step()
    return {"approved": (await approval).approved, "step_result": step_result}


class RecordingLocalWorld(local_mod.LocalWorld):
    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: object) -> str:
        return "msg_test"


@pytest.fixture
def world(tmp_path, monkeypatch) -> Iterator[RecordingLocalWorld]:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = RecordingLocalWorld()
    w.set_world(world)
    yield world
    w.set_world(None)


async def _create_run(world: local_mod.LocalWorld, workflow_name: str) -> str:
    result = await world.events_create(
        None,
        w.RunCreatedEventData(
            deployment_id="",
            workflow_name=workflow_name,
            input=PLAIN_ENCODER.encode(ser.argument_array((), {})),
        ).into_event(),
    )
    assert result.run is not None
    return result.run.run_id


async def _invoke(run_id: str, workflow_name: str) -> w.QueueContinuation | None:
    return await runtime.workflow_handler(
        w.WorkflowInvokePayload(run_id=run_id).model_dump(by_alias=True),
        attempt=1,
        queue_name=w.get_queue_name(workflow_name),
        message_id="msg_1",
        registry=registry,
    )


async def _correlation_id(world: local_mod.LocalWorld, run_id: str, event_type: type) -> str:
    event = next(e for e in (await world.events_list(run_id)).data if isinstance(e, event_type))
    assert event.correlation_id is not None
    return event.correlation_id


async def test_hook_is_registered_when_the_run_first_suspends(world) -> None:
    run_id = await _create_run(world, create_then_step_then_await.workflow_id)

    await _invoke(run_id, create_then_step_then_await.workflow_id)

    events = (await world.events_list(run_id)).data
    assert sorted(event.event_type for event in events) == sorted(
        ["run_created", "run_started", "step_created", "hook_created"]
    )
    assert (await world.hooks_get_by_token(TOKEN)).run_id == run_id


@pytest.mark.parametrize(
    "workflow",
    [create_then_step_then_await, confirm_then_step_then_await],
    ids=["awaited later", "confirmed with get_conflict"],
)
async def test_payload_received_before_the_body_awaits_is_kept(world, workflow) -> None:
    run_id = await _create_run(world, workflow.workflow_id)
    await _invoke(run_id, workflow.workflow_id)
    hook_id = await _correlation_id(world, run_id, w.HookCreatedEvent)
    step_id = await _correlation_id(world, run_id, w.StepCreatedEvent)

    # The payload lands while the body is still blocked on the step.
    await world.events_create(
        run_id,
        w.HookReceivedEventData(
            payload=PLAIN_ENCODER.encode({"approved": True}), token=TOKEN
        ).into_event(hook_id),
    )
    await _invoke(run_id, workflow.workflow_id)
    assert (await world.runs_get(run_id)).status == "running"

    await world.events_create(
        run_id, w.StepCompletedEventData(result=PLAIN_ENCODER.encode("done")).into_event(step_id)
    )
    await _invoke(run_id, workflow.workflow_id)

    assert (await world.runs_get(run_id)).status == "completed"
    assert await runtime.Run(run_id).return_value() == {"approved": True, "step_result": "done"}


class HookCheckingLocalWorld(RecordingLocalWorld):
    """Records whether the run's hook already existed when each step was enqueued."""

    def __init__(self) -> None:
        super().__init__()
        self.hook_existed_at_enqueue: list[bool] = []

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: object) -> str:
        if isinstance(message, w.WorkflowInvokePayload) and message.step_id is not None:
            try:
                await self.hooks_get_by_token(TOKEN)
            except w.HookNotFoundError:
                self.hook_existed_at_enqueue.append(False)
            else:
                self.hook_existed_at_enqueue.append(True)
        return "msg_test"


async def test_hooks_are_created_before_steps_are_enqueued(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = HookCheckingLocalWorld()
    w.set_world(world)
    try:
        run_id = await _create_run(world, create_then_step_then_await.workflow_id)
        await _invoke(run_id, create_then_step_then_await.workflow_id)
    finally:
        w.set_world(None)

    assert world.hook_existed_at_enqueue == [True]
