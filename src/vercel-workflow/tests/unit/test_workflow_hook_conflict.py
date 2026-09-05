"""Hook-token conflict behavior across worlds and workflow replay.

In LocalWorld, a hook's token is claimed exclusively the first time its
``hook_created`` event is issued. A second issue can mean two different things,
and the world must tell them apart:

- the *same* hook (same correlation id) re-claiming its token -- a replay
  re-issue or an overlapping/retried invocation of the same run. This is
  idempotent, so the world raises ``EntityConflictError`` (which the runtime
  swallows), mirroring the backend's hookId-keyed idempotency.
- a *different* hook claiming a token already in use -- a genuine cross-workflow
  conflict, surfaced as a ``HookConflictEvent``.

The handler tests also pin the replay boundary around that conflict: a returned
``hook_conflict`` replays immediately, while a successful registration does not.
The VercelWorld tests pin the corresponding lazy event-result boundary: a hook
conflict must be materialized for the runtime to inspect, while successful lazy
results stay opaque.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pydantic
import pytest

from tests.payloads import PLAIN_ENCODER
from vercel.workflow import HookConflictError, WorkflowRunFailedError, sleep
from vercel.workflow._internal import core, runtime, serialization as ser, world as w
from vercel.workflow._internal.worlds import local as local_mod

if TYPE_CHECKING:
    from vercel.workflow._internal.worlds.vercel import VercelWorld

RUN_ID = "wrun_test"
VERCEL_RUN_ID = "wrun_loser"
OWNER_RUN_ID = "wrun_owner"
TOKEN = "shared-token"

registry = core.Workflows(as_vercel_job=False)


class Claim(core.BaseHook, pydantic.BaseModel):
    pass


@registry.workflow
async def wait_for_claim() -> str:
    await Claim.wait(token=TOKEN)
    return "received"


@registry.workflow
async def reuse_claim_after_dispose() -> str:
    first = Claim.wait(token=TOKEN)
    await first
    first.dispose()
    conflict = await Claim.wait(token=TOKEN).get_conflict()
    return "available" if conflict is None else "conflict"


@registry.workflow
async def register_claim() -> dict[str, str | None]:
    conflict = await Claim.wait(token=TOKEN).get_conflict()
    return {"conflicting_run_id": None if conflict is None else conflict.run_id}


@registry.workflow
async def inspect_claim_conflict() -> dict[str, str | bool]:
    hook = Claim.wait(token=TOKEN)
    first = await hook.get_conflict()
    second = await hook.get_conflict()
    assert first is not None

    try:
        await hook
    except HookConflictError as error:
        return {
            "conflicting_run_id": first.run_id,
            "same_run": first is second,
            "error_run_id": error.conflicting_run_id or "",
        }
    raise AssertionError("awaiting a conflicting hook should fail")


@registry.step
async def pending_step() -> str:
    return "done"


@registry.workflow
async def wait_for_step() -> str:
    return await pending_step()


@registry.workflow
async def register_claim_with_step() -> dict[str, str | None]:
    conflict, step_result = await asyncio.gather(
        Claim.wait(token=TOKEN).get_conflict(),
        pending_step(),
    )
    return {
        "conflicting_run_id": None if conflict is None else conflict.run_id,
        "step_result": step_result,
    }


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


def _vercel_route(world: VercelWorld, response: object):
    import cbor2
    import httpx2 as httpx

    import vendor.respx as respx

    return respx.post(f"{world._base_url}/v3/runs/{VERCEL_RUN_ID}/events").mock(
        return_value=httpx.Response(
            200,
            content=cbor2.dumps(response),
            headers={"content-type": "application/cbor"},
        )
    )


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
            input=PLAIN_ENCODER.encode(ser.argument_array((), {})),
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
    assert result.event.event_data.conflicting_run_id == RUN_ID


async def test_disposal_is_flushed_before_reusing_a_hook_token(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    w.set_world(world)
    run_id = await _create_run(world, reuse_claim_after_dispose.workflow_id)

    await _invoke(run_id, reuse_claim_after_dispose.workflow_id)
    events = (await world.events_list(run_id)).data
    first_hook = next(event for event in events if isinstance(event, w.HookCreatedEvent))
    assert first_hook.correlation_id is not None
    await world.events_create(
        run_id,
        w.HookReceivedEventData(payload=PLAIN_ENCODER.encode({}), token=TOKEN).into_event(
            first_hook.correlation_id
        ),
    )

    await _invoke(run_id, reuse_claim_after_dispose.workflow_id)

    events = (await world.events_list(run_id)).data
    assert [event.event_type for event in events] == [
        "run_created",
        "run_started",
        "hook_created",
        "hook_received",
        "hook_disposed",
        "hook_created",
        "run_completed",
    ]
    assert await runtime.Run(run_id).return_value() == "available"


def test_hook_conflict_owner_id_round_trips_on_the_wire() -> None:
    data = w.HookConflictEventData.from_wire({"token": TOKEN, "conflictingRunId": "wrun_owner"})

    assert data.conflicting_run_id == "wrun_owner"
    assert data.model_dump() == {
        "token": TOKEN,
        "conflictingRunId": "wrun_owner",
    }


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
    assert cause.conflicting_run_id == winner


async def test_get_conflict_registers_without_waiting_for_payload(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    w.set_world(world)
    run_id = await _create_run(world, register_claim.workflow_id)

    await _invoke(run_id, register_claim.workflow_id)

    assert world.event_list_calls == 2
    assert (await world.runs_get(run_id)).status == "completed"
    assert [event.event_type for event in (await world.events_list(run_id)).data] == [
        "run_created",
        "run_started",
        "hook_created",
        "run_completed",
    ]
    assert await runtime.Run(run_id).return_value() == {"conflicting_run_id": None}


async def test_get_conflict_returns_the_owner_but_awaiting_payload_fails(
    tmp_path, monkeypatch
) -> None:
    world = _world(tmp_path, monkeypatch)
    w.set_world(world)
    winner = await _create_run(world)
    loser = await _create_run(world, inspect_claim_conflict.workflow_id)

    await _invoke(winner)
    await _invoke(loser, inspect_claim_conflict.workflow_id)

    assert (await world.runs_get(loser)).status == "completed"
    assert await runtime.Run(loser).return_value() == {
        "conflicting_run_id": winner,
        "same_run": True,
        "error_run_id": winner,
    }
    assert [event.event_type for event in (await world.events_list(loser)).data] == [
        "run_created",
        "run_started",
        "hook_conflict",
        "run_completed",
    ]


async def test_get_conflict_rejects_an_old_event_without_an_owner() -> None:
    context = runtime.WorkflowOrchestratorContext(
        [],
        run_id=RUN_ID,
        seed="seed",
        started_at=0,
        registry=registry,
    )
    token = context._ctx.set(context)
    try:
        hook_event = context.create_hook(TOKEN, Claim)
        context.events.append(
            w.HookConflictEvent(
                correlation_id=hook_event._correlation_id,
                event_data=w.HookConflictEventData(token=TOKEN),
            )
        )
        pending = asyncio.create_task(hook_event.get_conflict())
        await asyncio.sleep(0)
        context.resume()

        with pytest.raises(HookConflictError) as exc_info:
            await pending
        assert exc_info.value.conflicting_run_id is None
        with pytest.raises(HookConflictError):
            await hook_event.get_conflict()
        with pytest.raises(HookConflictError):
            await hook_event
    finally:
        context._ctx.reset(token)


async def test_get_conflict_rejects_a_disposed_unregistered_hook() -> None:
    context = runtime.WorkflowOrchestratorContext(
        [],
        run_id=RUN_ID,
        seed="seed",
        started_at=0,
        registry=registry,
    )
    token = context._ctx.set(context)
    try:
        hook_event = context.create_hook(TOKEN, Claim)
        hook_event.dispose()

        with pytest.raises(RuntimeError, match="disposed hook"):
            await hook_event.get_conflict()
        assert not context.suspensions
    finally:
        context._ctx.reset(token)


async def test_get_conflict_continues_while_a_parallel_step_runs(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    w.set_world(world)
    run_id = await _create_run(world, register_claim_with_step.workflow_id)

    await _invoke(run_id, register_claim_with_step.workflow_id)

    assert world.event_list_calls == 2
    assert (await world.runs_get(run_id)).status == "running"
    assert len(world.queued) == 1
    queued = world.queued[0][1]
    assert isinstance(queued, w.WorkflowInvokePayload)
    await runtime._execute_step(
        queued,
        run=await world.runs_get(run_id),
        queue_name=w.get_queue_name(register_claim_with_step.workflow_id),
        registry=registry,
    )
    await _invoke(run_id, register_claim_with_step.workflow_id)

    assert await runtime.Run(run_id).return_value() == {
        "conflicting_run_id": None,
        "step_result": "done",
    }


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


async def test_vercel_lazy_hook_conflict_result_is_returned_as_typed_event() -> None:
    import cbor2

    import vendor.respx as respx
    from vercel.workflow._internal.worlds.vercel import VercelWorld

    with respx.mock:
        world = VercelWorld(token="test-token")
        route = _vercel_route(
            world,
            {
                "event": {
                    "eventType": "hook_conflict",
                    "specVersion": 7,
                    "correlationId": "hook_loser",
                    "eventData": {
                        "token": TOKEN,
                        "conflictingRunId": OWNER_RUN_ID,
                    },
                }
            },
        )

        result = await world.events_create(
            VERCEL_RUN_ID,
            w.HookCreatedEventData(token=TOKEN).into_event("hook_loser"),
        )

        request = cbor2.loads(route.calls.last.request.content)
        assert request["remoteRefBehavior"] == "lazy"
        assert isinstance(result.event, w.HookConflictEvent)
        assert result.event.event_data.conflicting_run_id == OWNER_RUN_ID


async def test_vercel_successful_lazy_hook_result_stays_opaque() -> None:
    import vendor.respx as respx
    from vercel.workflow._internal.worlds.vercel import VercelWorld

    with respx.mock:
        world = VercelWorld(token="test-token")
        event = {
            "eventType": "hook_created",
            "specVersion": 2,
            "correlationId": "hook_loser",
            "eventData": {
                "token": TOKEN,
                "metadata": {"_type": "RemoteRef", "_ref": "s3rf:payload"},
            },
        }
        _vercel_route(world, {"event": event})

        result = await world.events_create(
            VERCEL_RUN_ID,
            w.HookCreatedEventData(token=TOKEN).into_event("hook_loser"),
        )

        assert result.event == event
