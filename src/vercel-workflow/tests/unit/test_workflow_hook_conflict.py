"""LocalWorld hook-token conflict semantics.

A hook's token is claimed exclusively the first time its ``hook_created`` event
is issued. A second issue can mean two different things, and the world must tell
them apart:

- the *same* hook (same correlation id) re-claiming its token -- a replay
  re-issue or an overlapping/retried invocation of the same run. This is
  idempotent, so the world raises ``EntityConflictError`` (which the runtime
  swallows), mirroring the backend's hookId-keyed idempotency.
- a *different* hook claiming a token already in use -- a genuine cross-workflow
  conflict, surfaced as a ``HookConflictEvent``.

The handler tests also pin the replay boundary around that conflict: a returned
``hook_conflict`` replays immediately, while a successful registration does not.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from vercel.workflow import HookConflictError, WorkflowRunFailedError, sleep
from vercel.workflow._internal import core, runtime, serialization as ser, world as w
from vercel.workflow._internal.worlds import local as local_mod

RUN_ID = "wrun_test"
TOKEN = "shared-token"

registry = core.Workflows(as_vercel_job=False)


class Claim(core.BaseHook):
    pass


@registry.workflow
async def wait_for_claim() -> str:
    await Claim.wait(token=TOKEN)
    return "received"


@registry.step
async def pending_step() -> str:
    return "done"


@registry.workflow
async def wait_for_step() -> str:
    return await pending_step()


@registry.workflow
async def wait_for_claim_and_sleep() -> None:
    await asyncio.gather(Claim.wait(token=TOKEN), sleep("1h"))


class RecordingLocalWorld(local_mod.LocalWorld):
    def __init__(self) -> None:
        super().__init__()
        self.event_list_calls = 0
        self.run_started_calls = 0
        self.queued: list[tuple[str, w.QueuePayload]] = []

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        if isinstance(data, w.RunStartedEvent):
            self.run_started_calls += 1
        return await super().events_create(run_id, data)

    async def events_list(
        self,
        run_id: str,
        *,
        pagination: w.PaginationOptions | None = None,
    ) -> w.PaginatedResult[w.Event]:
        self.event_list_calls += 1
        return await super().events_list(run_id, pagination=pagination)

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
        self.queued.append((queue_name, message))
        return "msg_test"


def _world(tmp_path, monkeypatch) -> RecordingLocalWorld:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    return RecordingLocalWorld()


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)


async def _create_run(
    world: local_mod.LocalWorld, workflow_name: str = wait_for_claim.workflow_id
) -> str:
    result = await world.events_create(
        None,
        w.RunCreatedEventData(
            deployment_id="",
            workflow_name=workflow_name,
            input=ser.dehydrate(ser.argument_array((), {})),
        ).into_event(),
    )
    assert result.run is not None
    return result.run.run_id


async def _invoke(
    run_id: str, workflow_name: str = wait_for_claim.workflow_id
) -> w.QueueContinuation | None:
    return await runtime.workflow_handler(
        w.WorkflowInvokePayload(run_id=run_id).model_dump(by_alias=True),
        attempt=1,
        queue_name=w.get_queue_name(workflow_name),
        message_id="msg_1",
        registry=registry,
    )


async def test_same_hook_reclaim_raises_entity_conflict(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    event = w.HookCreatedEventData(token=TOKEN).into_event("hook_1")

    await world.events_create(RUN_ID, event)

    try:
        await world.events_create(RUN_ID, event)
    except w.EntityConflictError:
        pass
    else:
        raise AssertionError("re-claiming the same hook's token should raise EntityConflictError")


async def test_different_hook_same_token_conflicts(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)

    await world.events_create(RUN_ID, w.HookCreatedEventData(token=TOKEN).into_event("hook_1"))
    result = await world.events_create(
        RUN_ID, w.HookCreatedEventData(token=TOKEN).into_event("hook_2")
    )

    assert isinstance(result.event, w.HookConflictEvent)
    assert result.event.event_data.token == TOKEN


async def test_hook_conflict_replays_and_fails_the_losing_run(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    w.set_world(world)
    winner = await _create_run(world)
    loser = await _create_run(world)

    await _invoke(winner)
    await _invoke(loser)

    run = await world.runs_get(loser)
    events = (await world.events_list(loser)).data

    assert run.status == "failed"
    # One setup write per run; the loser's in-process replay does not repeat it.
    assert world.run_started_calls == 2
    assert [event.event_type for event in events] == [
        "run_created",
        "run_started",
        "hook_conflict",
        "run_failed",
    ]
    with pytest.raises(WorkflowRunFailedError) as exc_info:
        await runtime.Run(loser).return_value()
    cause = exc_info.value.__cause__
    assert isinstance(cause, HookConflictError)
    assert cause.token == TOKEN
    assert cause.conflicting_run_id is None


async def test_hook_conflict_replays_even_with_a_pending_wait(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    w.set_world(world)
    winner = await _create_run(world)
    loser = await _create_run(world, wait_for_claim_and_sleep.workflow_id)

    await _invoke(winner)
    continuation = await _invoke(loser, wait_for_claim_and_sleep.workflow_id)

    assert continuation is None
    assert (await world.runs_get(loser)).status == "failed"


async def test_successful_hook_creation_does_not_replay(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    w.set_world(world)
    run_id = await _create_run(world)

    await _invoke(run_id)

    assert world.event_list_calls == 1
    assert (await world.runs_get(run_id)).status == "running"


async def test_step_creation_waits_for_its_result_instead_of_replaying(
    tmp_path, monkeypatch
) -> None:
    world = _world(tmp_path, monkeypatch)
    w.set_world(world)
    run_id = await _create_run(world, wait_for_step.workflow_id)

    await _invoke(run_id, wait_for_step.workflow_id)

    assert world.event_list_calls == 1
    assert len(world.queued) == 1
    queued = world.queued[0][1]
    assert isinstance(queued, w.WorkflowInvokePayload)
    assert queued.step_id is not None
