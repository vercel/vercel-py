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
import multiprocessing
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import anyio
import pydantic
import pytest

from tests.payloads import PLAIN_ENCODER
from tests.workflow_context import bind_context
from vercel._internal.core.polyfills import UTC
from vercel.workflow import (
    HookConflictError,
    Run,
    WorkflowRunFailedError,
    get_hook_by_token,
    sleep,
    start,
)
from vercel.workflow._internal import core, runtime, serialization as ser, world as w
from vercel.workflow._internal.worlds import local as local_mod

if TYPE_CHECKING:
    from vercel.workflow._internal.worlds.vercel import VercelWorld

RUN_ID = "wrun_test"
VERCEL_RUN_ID = "wrun_loser"
OWNER_RUN_ID = "wrun_owner"
TOKEN = "shared-token"
RETENTION_NOW = datetime(2026, 9, 22, tzinfo=UTC)

registry = core.Workflows(as_vercel_job=False)


class Claim(core.BaseHook, pydantic.BaseModel):
    pass


@registry.workflow
async def wait_for_claim(retention: Any = None, fail: bool = False) -> str:
    await Claim.wait(token=TOKEN, experimental_min_retention=retention)
    if fail:
        raise RuntimeError("failed with retained hook")
    return "received"


@registry.workflow
async def reuse_claim_after_dispose() -> str:
    first = Claim.wait(token=TOKEN)
    await first
    first.dispose()
    conflict = await Claim.wait(token=TOKEN).get_conflict()
    return "available" if conflict is None else "conflict"


@registry.workflow
async def reuse_uncommitted_claims() -> str:
    first = Claim.wait(token=TOKEN)
    first.dispose()
    second = Claim.wait(token=TOKEN)
    second.dispose()
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
    assert await Run(run_id).return_value() == "available"


async def test_uncommitted_hooks_are_created_disposed_and_replayed_in_order(
    tmp_path, monkeypatch
) -> None:
    world = _world(tmp_path, monkeypatch)
    w.set_world(world)
    run_id = await _create_run(world, reuse_uncommitted_claims.workflow_id)
    await _invoke(run_id, reuse_uncommitted_claims.workflow_id)
    events = (await world.events_list(run_id)).data
    hooks = [e for e in events if e.event_type.startswith("hook_")]
    assert [e.event_type for e in hooks] == [
        "hook_created",
        "hook_disposed",
        "hook_created",
        "hook_disposed",
        "hook_created",
    ]
    assert hooks[0].correlation_id == hooks[1].correlation_id
    assert hooks[2].correlation_id == hooks[3].correlation_id
    assert len({e.correlation_id for e in hooks}) == 3
    assert await Run(run_id).return_value() == "available"


def _invoke_overlapping_lifecycle(
    name, data_dir, run_id, first_hook, both_read, first_disposed, second_finished
) -> None:
    os.environ["WORKFLOW_LOCAL_DATA_DIR"] = str(data_dir)

    class OverlappingWorld(RecordingLocalWorld):
        initial_read = True

        async def events_list(self, run_id, *, pagination=None):
            page = await super().events_list(run_id, pagination=pagination)
            if self.initial_read:
                self.initial_read = False
                both_read.wait(timeout=15)
            return page

        async def events_create(self, run_id, data):
            if (
                name == "second"
                and isinstance(data, w.HookCreatedEvent)
                and data.correlation_id == first_hook
            ):
                assert first_disposed.wait(timeout=15)
            result = await super().events_create(run_id, data)
            if (
                name == "first"
                and isinstance(data, w.HookDisposedEvent)
                and data.correlation_id == first_hook
            ):
                first_disposed.set()
                assert second_finished.wait(timeout=15)
            return result

    w.set_world(OverlappingWorld())
    try:
        asyncio.run(_invoke(run_id, reuse_uncommitted_claims.workflow_id))
    finally:
        if name == "second":
            second_finished.set()
        w.set_world(None)


def _run_overlapping_lifecycles(data_dir, run_id, first_hook) -> None:
    process_context = multiprocessing.get_context("spawn")
    signals = (process_context.Barrier(2), process_context.Event(), process_context.Event())
    processes = [
        process_context.Process(
            target=_invoke_overlapping_lifecycle,
            args=(name, data_dir, run_id, first_hook, *signals),
        )
        for name in ("first", "second")
    ]
    # Both processes read the same events. The first creates and disposes a
    # hook, then waits while the second tries to create it from the old log.
    # This must not recreate the disposed hook or block reuse of its token.
    try:
        for process in processes:
            process.start()
        for process in processes:
            process.join(timeout=20)
            assert process.exitcode == 0
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            if process.pid is not None:
                process.join(timeout=5)


async def test_overlapping_invocations_do_not_recreate_disposed_hooks(tmp_path, monkeypatch):
    world = _world(tmp_path, monkeypatch)
    w.set_world(world)
    run_id = await _create_run(world, reuse_uncommitted_claims.workflow_id)
    started = await world.events_create(run_id, w.RunStartedEvent())
    assert started.run is not None and started.run.started_at is not None
    probe = runtime.WorkflowOrchestratorContext(
        [],
        run_id=run_id,
        seed=run_id,
        started_at=int(started.run.started_at.timestamp() * 1000),
        registry=registry,
    )
    first_hook = f"hook_{probe.generate_ulid()}"
    # ggt uses daemon processes, which cannot spawn multiprocessing workers.
    # Start a separate interpreter to launch this test's two processes.
    with anyio.fail_after(50):
        result = await anyio.run_process(
            [
                sys.executable,
                "-c",
                "import sys\n"
                "from tests.unit.test_workflow_hook_conflict import _run_overlapping_lifecycles\n"
                "_run_overlapping_lifecycles(*sys.argv[1:])\n",
                str(world.data_dir),
                run_id,
                first_hook,
            ],
            cwd=Path(__file__).resolve().parents[2],
            check=False,
        )
    assert result.returncode == 0, result.stderr.decode()
    assert await Run(run_id).return_value() == "available"
    hooks = [e for e in (await world.events_list(run_id)).data if e.event_type.startswith("hook_")]
    assert [e.event_type for e in hooks] == [
        "hook_created",
        "hook_disposed",
        "hook_created",
        "hook_disposed",
        "hook_created",
    ]
    assert hooks[0].correlation_id == hooks[1].correlation_id == first_hook
    assert hooks[2].correlation_id == hooks[3].correlation_id
    assert len({e.correlation_id for e in hooks}) == 3


@pytest.mark.parametrize("lost_ack", ["hook_created", "hook_disposed"])
async def test_partial_hook_flush_retries_without_recreating_prior_hooks(
    tmp_path, monkeypatch, lost_ack
) -> None:
    world = _world(tmp_path, monkeypatch)
    w.set_world(world)
    run_id = await _create_run(world, reuse_uncommitted_claims.workflow_id)
    create = world.events_create

    async def lose_ack(run_id, data):
        result = await create(run_id, data)
        if data.event_type == lost_ack:
            raise RuntimeError("lost hook acknowledgement")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(world, "events_create", lose_ack)
        with pytest.raises(RuntimeError, match="lost hook acknowledgement"):
            await _invoke(run_id, reuse_uncommitted_claims.workflow_id)
    assert (await world.runs_get(run_id)).status == "running"
    await _invoke(run_id, reuse_uncommitted_claims.workflow_id)
    events = (await world.events_list(run_id)).data
    assert [e.event_type for e in events if e.event_type.startswith("hook_")] == [
        "hook_created",
        "hook_disposed",
        "hook_created",
        "hook_disposed",
        "hook_created",
    ]
    assert await Run(run_id).return_value() == "available"


async def test_retry_of_disposed_hook_does_not_reclaim_a_successors_token(
    tmp_path, monkeypatch
) -> None:
    world = _world(tmp_path, monkeypatch)
    w.set_world(world)
    run_id = await _create_run(world, reuse_uncommitted_claims.workflow_id)
    create = world.events_create

    async def lose_dispose_ack(run_id, data):
        result = await create(run_id, data)
        if isinstance(data, w.HookDisposedEvent):
            raise RuntimeError("lost dispose acknowledgement")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(world, "events_create", lose_dispose_ack)
        with pytest.raises(RuntimeError, match="lost dispose acknowledgement"):
            await _invoke(run_id, reuse_uncommitted_claims.workflow_id)
    owner = await _create_run(world)
    await _invoke(owner)
    replacement = await world.hooks_get_by_token(TOKEN)

    await _invoke(run_id, reuse_uncommitted_claims.workflow_id)
    assert await Run(run_id).return_value() == "conflict"
    assert (await world.hooks_get_by_token(TOKEN)).hook_id == replacement.hook_id
    events = (await world.events_list(run_id)).data
    assert [e.event_type for e in events if e.event_type.startswith("hook_")] == [
        "hook_created",
        "hook_disposed",
        "hook_conflict",
        "hook_conflict",
    ]


@pytest.mark.parametrize("fail_first", [False, True])
async def test_token_groups_are_concurrent_and_settle_before_a_flush_returns(
    tmp_path, monkeypatch, fail_first
) -> None:
    world = _world(tmp_path, monkeypatch)
    w.set_world(world)
    run_id = await _create_run(world)
    context = runtime.WorkflowOrchestratorContext(
        [], run_id=run_id, seed=run_id, started_at=0, registry=registry
    )
    with bind_context(context):
        for token in ("first-token", "second-token"):
            handle = context.create_hook(token, Claim)
            context.dispose_hook(correlation_id=handle._correlation_id)

    second_started = anyio.Event()
    create = world.events_create

    async def delay_create(run_id, data):
        if isinstance(data, w.HookCreatedEvent):
            if data.event_data.token == "first-token":
                await second_started.wait()
                if fail_first:
                    raise RuntimeError("first group failed")
            else:
                second_started.set()
                await anyio.sleep(0)
        return await create(run_id, data)

    monkeypatch.setattr(world, "events_create", delay_create)
    with anyio.fail_after(5):
        if fail_first:
            with pytest.raises(RuntimeError, match="first group failed"):
                await runtime._flush_hook_lifecycle_events(context)
        else:
            await runtime._flush_hook_lifecycle_events(context)
    events = (await world.events_list(run_id)).data
    for hook in context.hooks.values():
        kinds = [e.event_type for e in events if e.correlation_id == hook.correlation_id]
        if fail_first and hook.token == "first-token":
            assert kinds == []
        else:
            assert kinds == ["hook_created", "hook_disposed"]


async def test_token_groups_report_first_failure_after_other_groups_finish(
    tmp_path, monkeypatch
) -> None:
    world = _world(tmp_path, monkeypatch)
    w.set_world(world)
    run_id = await _create_run(world)
    context = runtime.WorkflowOrchestratorContext(
        [], run_id=run_id, seed=run_id, started_at=0, registry=registry
    )
    with bind_context(context):
        for token in ("first-token", "second-token", "third-token"):
            handle = context.create_hook(token, Claim)
            context.dispose_hook(correlation_id=handle._correlation_id)

    failures = {
        "first-token": RuntimeError("first group failed"),
        "second-token": ValueError("second group failed"),
    }
    raised_failures: list[Exception] = []
    third_started = anyio.Event()
    create = world.events_create

    async def fail_create(run_id, data):
        if isinstance(data, w.HookCreatedEvent):
            if data.event_data.token in failures:
                await third_started.wait()
                error = failures[data.event_data.token]
                raised_failures.append(error)
                raise error
            third_started.set()
            await anyio.sleep(0)
        return await create(run_id, data)

    monkeypatch.setattr(world, "events_create", fail_create)
    with anyio.fail_after(5):
        with pytest.raises((RuntimeError, ValueError)) as caught:
            await runtime._flush_hook_lifecycle_events(context)
    assert set(raised_failures) == set(failures.values())
    assert caught.value is raised_failures[0]

    events = (await world.events_list(run_id)).data
    for hook in context.hooks.values():
        kinds = [e.event_type for e in events if e.correlation_id == hook.correlation_id]
        assert kinds == ([] if hook.token in failures else ["hook_created", "hook_disposed"])


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
        await Run(loser).return_value()
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
    assert await Run(run_id).return_value() == {"conflicting_run_id": None}


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
    assert await Run(loser).return_value() == {
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

    assert await Run(run_id).return_value() == {
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


@pytest.fixture
def retained_world(tmp_path, monkeypatch):
    monkeypatch.setattr(local_mod, "js_now", lambda: RETENTION_NOW)
    world = _world(tmp_path, monkeypatch)
    w.set_world(world)
    return world


async def _create_retained_claim(
    world: local_mod.LocalWorld, deadline: datetime = RETENTION_NOW + timedelta(days=1)
) -> w.Hook:
    run_id = await _create_run(world)
    await world.events_create(
        run_id,
        w.HookCreatedEventData(token=TOKEN, token_retention_until=deadline).into_event(
            f"hook_{run_id}"
        ),
    )
    return await world.hooks_get_by_token(TOKEN)


async def _assert_retained_token_conflicts(world: local_mod.LocalWorld, owner_id: str) -> None:
    duplicate = await _create_run(world, register_claim.workflow_id)
    await _invoke(duplicate, register_claim.workflow_id)
    assert await Run(duplicate).return_value() == {"conflicting_run_id": owner_id}


@pytest.mark.parametrize("mode", ["plain", "failure", "cancelled"])
async def test_terminal_run_keeps_token_but_cannot_be_resumed(retained_world, mode) -> None:
    run = await start(wait_for_claim, "1d", fail=mode == "failure")
    await _invoke(run.run_id)
    hook = await get_hook_by_token(TOKEN)
    if mode == "cancelled":
        await run.terminate()
    else:
        await Claim().resume(TOKEN)
        await _invoke(run.run_id)
    assert (await get_hook_by_token(TOKEN)).hook_id == hook.hook_id

    before = len((await retained_world.events_list(hook.run_id)).data)
    queued = len(retained_world.queued)
    for target in (TOKEN, hook):
        with pytest.raises(w.EntityConflictError, match="terminal state"):
            await Claim().resume(target)
    assert len((await retained_world.events_list(hook.run_id)).data) == before
    assert len(retained_world.queued) == queued
    await _assert_retained_token_conflicts(retained_world, hook.run_id)


@pytest.mark.parametrize("lookup_before_reclaim", [False, True])
async def test_expired_token_can_be_reclaimed(
    retained_world, monkeypatch, lookup_before_reclaim
) -> None:
    old_hook = await _create_retained_claim(retained_world)
    await retained_world.events_create(
        old_hook.run_id, w.RunCompletedEventData(output=PLAIN_ENCODER.encode("done")).into_event()
    )
    monkeypatch.setattr(local_mod, "js_now", lambda: RETENTION_NOW + timedelta(days=1))
    if lookup_before_reclaim:
        with pytest.raises(w.HookNotFoundError):
            await get_hook_by_token(TOKEN)
    replacement = await _create_retained_claim(retained_world, RETENTION_NOW + timedelta(days=2))
    assert (await get_hook_by_token(TOKEN)).run_id == replacement.run_id
    assert not (retained_world.data_dir / "hooks" / f"{old_hook.hook_id}.json").exists()
    # Delayed cleanup from the old owner must not release the new claim.
    await retained_world.delete_all_hooks_for_run(old_hook.run_id)
    await _assert_retained_token_conflicts(retained_world, replacement.run_id)


@pytest.mark.parametrize("param", [0, RETENTION_NOW - timedelta(days=1), "1d"])
async def test_active_run_keeps_token_after_deadline(retained_world, monkeypatch, param) -> None:
    run = await start(wait_for_claim, param)
    await _invoke(run.run_id)
    monkeypatch.setattr(local_mod, "js_now", lambda: RETENTION_NOW + timedelta(days=2))
    assert (await get_hook_by_token(TOKEN)).run_id == run.run_id
    await _assert_retained_token_conflicts(retained_world, run.run_id)
    await Claim().resume(TOKEN)
    await _invoke(run.run_id)
    with pytest.raises(w.HookNotFoundError):
        await get_hook_by_token(TOKEN)


async def test_local_retention_limit_is_enforced_before_claim(retained_world) -> None:
    run_id = await _create_run(retained_world)
    with pytest.raises(w.WorkflowWorldError, match="cannot exceed 30 days"):
        await retained_world.events_create(
            run_id,
            w.HookCreatedEventData(
                token=TOKEN, token_retention_until=RETENTION_NOW + timedelta(days=31)
            ).into_event("hook_too_long"),
        )
    with pytest.raises(w.HookNotFoundError):
        await get_hook_by_token(TOKEN)
    assert not retained_world._hook_token_path(TOKEN).exists()


async def test_local_retention_limit_is_configurable(retained_world, monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_LOCAL_HOOK_RETENTION_LIMIT_DAYS", "60")
    configured = local_mod.LocalWorld()
    run_id = await _create_run(configured)
    deadline = RETENTION_NOW + timedelta(days=31)
    await configured.events_create(
        run_id,
        w.HookCreatedEventData(token=TOKEN, token_retention_until=deadline).into_event("hook_long"),
    )
    assert (await configured.hooks_get_by_token(TOKEN)).token_retention_until == deadline


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf"])
def test_invalid_local_retention_limit(monkeypatch, value) -> None:
    monkeypatch.setenv("WORKFLOW_LOCAL_HOOK_RETENTION_LIMIT_DAYS", value)
    with pytest.raises(w.WorkflowWorldError, match="positive number"):
        local_mod.LocalWorld()


def _claim_in_process(data_dir: str, run_id: str, hook_id: str) -> str:
    os.environ["WORKFLOW_LOCAL_DATA_DIR"] = data_dir

    async def claim() -> str:
        world = local_mod.LocalWorld()
        await world.events_create(
            run_id,
            w.RunCreatedEventData(
                deployment_id="", workflow_name="claim", input=PLAIN_ENCODER.encode([])
            ).into_event(),
        )
        ready = world.data_dir / "racers"
        (ready / hook_id).touch()
        while len(list(ready.iterdir())) < 4:
            await asyncio.sleep(0.01)
        result = await world.events_create(
            run_id, w.HookCreatedEventData(token=TOKEN).into_event(hook_id)
        )
        assert result.event is not None
        return result.event.event_type

    return asyncio.run(claim())


async def test_expired_token_has_only_one_winner_across_processes(
    retained_world, monkeypatch
) -> None:
    import subprocess

    # Use a deadline in the past relative to the workers' real clocks.
    hook = await _create_retained_claim(retained_world, RETENTION_NOW - timedelta(days=365))

    async def skip_cleanup(run_id: str) -> None:
        pass

    # Leave the expired entity and claim as a process interrupted during
    # terminal cleanup would. New claimants must serialize their handoff.
    with monkeypatch.context() as patch:
        patch.setattr(retained_world, "delete_all_hooks_for_run", skip_cleanup)
        await retained_world.events_create(
            hook.run_id, w.RunCompletedEventData(output=PLAIN_ENCODER.encode("done")).into_event()
        )
    (retained_world.data_dir / "racers").mkdir()

    async def race(i: int) -> str:
        result = await asyncio.to_thread(
            subprocess.run,
            [
                sys.executable,
                "-c",
                "import sys; from tests.unit.test_workflow_hook_conflict "
                "import _claim_in_process; print(_claim_in_process(*sys.argv[1:]))",
                str(retained_world.data_dir),
                f"wrun_racer_{i}",
                f"hook_racer_{i}",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
        return result.stdout.strip()

    results = await asyncio.gather(*(race(i) for i in range(4)))
    assert results.count("hook_created") == 1
    assert results.count("hook_conflict") == 3
    assert (await retained_world.hooks_get_by_token(TOKEN)).hook_id != hook.hook_id
