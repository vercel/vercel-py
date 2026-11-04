"""Atomic admission and immediate resumability through the public Python API."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from tests.payloads import PLAIN_ENCODER
from vercel.workflow import HookConflictError, WorkflowStartError, get_hook_by_token, start
from vercel.workflow._internal import (
    core,
    deployment,
    runtime,
    serialization as ser,
    start_hooks,
    world as w,
)
from vercel.workflow._internal.worlds import local as local_mod

TOKEN = "start:order"
registry = core.Workflows(as_vercel_job=False)


@dataclasses.dataclass
class Approval(core.BaseHook):
    value: int


@registry.workflow
async def receive_two() -> list[int]:
    hook = Approval.wait(token=TOKEN)
    assert await hook.get_conflict() is None
    return [(await hook).value, (await hook).value]


@registry.workflow
async def finish_without_opening() -> str:
    return "done"


@registry.workflow
async def dispose_start_hook() -> str:
    hook = Approval.wait(token=TOKEN)
    hook.dispose()
    return "done"


class World(local_mod.LocalWorld):
    def __init__(self):
        super().__init__()
        self.messages: list[w.WorkflowInvokePayload] = []
        self.deliver = False

    async def queue(self, queue_name, message, **kwargs):
        assert isinstance(message, w.WorkflowInvokePayload)
        self.messages.append(message)
        if self.deliver and message.run_input is not None:
            await invoke(message)
        return "msg_test"


@pytest.fixture
def world(tmp_path, monkeypatch):
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    instance = World()
    w.set_world(instance)
    yield instance
    w.set_world(None)


async def invoke(message: w.WorkflowInvokePayload):
    return await runtime.workflow_handler(
        message.model_dump(),
        attempt=1,
        queue_name=w.get_queue_name(receive_two.workflow_id),
        message_id="msg_test",
        registry=registry,
    )


def created(token=TOKEN, **kwargs):
    return w.RunCreatedEventData(
        deployment_id="local",
        workflow_name=receive_two.workflow_id,
        input=PLAIN_ENCODER.encode([]),
        start_hook=w.StartHook(token=token, **kwargs),
    ).into_event()


@pytest.mark.parametrize("queue_wins", [False, True])
async def test_immediate_resume_before_execution_and_no_duplicate_registration(world, queue_wins):
    world.deliver = queue_wins
    configured = receive_two.with_experimental_hook(token=TOKEN, metadata={"order": 42})
    run = await start(configured)
    hook = await get_hook_by_token(TOKEN)
    assert hook.run_id == run.run_id
    assert hook.hook_id == w.start_hook_id(run.run_id)
    assert hook.metadata == {"order": 42}
    await Approval(1).resume(TOKEN)
    await Approval(2).resume(TOKEN)
    await invoke(world.messages[0])
    assert await run.return_value() == [1, 2]
    events = (await world.events_list(run.run_id)).data
    assert not any(isinstance(event, w.HookCreatedEvent) for event in events)
    with pytest.raises(w.HookNotFoundError):
        await get_hook_by_token(TOKEN)


async def test_rejected_candidate_never_runs_and_rejection_survives_release(world):
    owner = await start(receive_two.with_experimental_hook(token=TOKEN))
    with pytest.raises(HookConflictError) as caught:
        await start(receive_two.with_experimental_hook(token=TOKEN))
    assert caught.value.kind == "start"
    assert caught.value.conflicting_run_id == owner.run_id
    candidate = world.messages[1]
    assert await invoke(candidate) is None
    assert not (world.data_dir / "runs" / f"{candidate.run_id}.json").exists()
    await world.events_create(
        owner.run_id, w.HookDisposedEvent(correlation_id=w.start_hook_id(owner.run_id))
    )
    # A queued loser stays a loser even when the old owner releases its token.
    assert await invoke(candidate) is None
    with pytest.raises(RuntimeError, match="not found"):
        await world.runs_get(candidate.run_id)


@pytest.mark.parametrize("dispose", [False, True])
async def test_retention_and_dispose_reuse_existing_lifecycle(world, dispose):
    wf = dispose_start_hook if dispose else finish_without_opening
    run = await start(wf.with_experimental_hook(token=TOKEN, experimental_min_retention="1d"))
    await invoke(world.messages[0])
    assert await run.return_value() == "done"
    if dispose:
        await start(wf.with_experimental_hook(token=TOKEN))
    else:
        hook = await get_hook_by_token(TOKEN)
        assert hook.token_retention_until is not None
        with pytest.raises(HookConflictError):
            await start(wf.with_experimental_hook(token=TOKEN))
        with pytest.raises(w.HookNotFoundError):
            await Approval(1).resume(TOKEN)


async def test_start_conflicts_with_an_ordinary_hook(world):
    await world.events_create(
        "wrun_ordinary", w.HookCreatedEventData(token=TOKEN).into_event("hook_ordinary")
    )
    with pytest.raises(HookConflictError) as caught:
        await start(receive_two.with_experimental_hook(token=TOKEN))
    assert caught.value.conflicting_run_id == "wrun_ordinary"


async def test_admission_replays_and_does_not_resurrect_disposed_hook(world):
    req = created()
    run_id = f"wrun_{world.create_run_id()}"
    first = await world.events_create(run_id, req)
    assert first.run is not None
    second = await world.events_create(run_id, req)
    assert second.run == first.run
    await world.events_create(run_id, w.HookDisposedEvent(correlation_id=w.start_hook_id(run_id)))
    await world.events_create(run_id, req)
    with pytest.raises(w.HookNotFoundError):
        await world.hooks_get_by_token(TOKEN)


async def test_crash_after_run_publication_repairs_hook_and_event(world, monkeypatch):
    original = world._ensure_start_hook
    monkeypatch.setattr(
        world, "_ensure_start_hook", lambda *args: (_ for _ in ()).throw(RuntimeError("crash"))
    )
    run_id = f"wrun_{world.create_run_id()}"
    with pytest.raises(RuntimeError, match="crash"):
        await world.events_create(run_id, created())
    monkeypatch.setattr(world, "_ensure_start_hook", original)
    recovered = await world.hooks_get_by_token(TOKEN)
    assert recovered.run_id == run_id
    assert len((await world.events_list(run_id)).data) == 1
    assert (await world.events_create(run_id, created())).run is not None


async def test_confirmation_after_committed_response_is_lost(world, monkeypatch):
    original = world.events_create
    calls = []

    async def lose_response(run_id, data):
        result = await original(run_id, data)
        if isinstance(data, w.RunCreatedEvent):
            calls.append(run_id)
            if len(calls) == 1:
                raise w.WorkflowWorldError("lost response", status=503)
        return result

    monkeypatch.setattr(world, "events_create", lose_response)
    run = await start(receive_two.with_experimental_hook(token=TOKEN))
    assert calls == [run.run_id, run.run_id]
    assert len(list((world.data_dir / "runs").glob("*.json"))) == 1


async def test_transient_queue_failure_is_republished_with_same_candidate(world, monkeypatch):
    original = world.queue

    async def queue(name, message, **kwargs):
        result = await original(name, message, **kwargs)
        if len(world.messages) == 1:
            raise w.WorkflowWorldError("lost queue response", status=503)
        return result

    monkeypatch.setattr(world, "queue", queue)
    run = await start(receive_two.with_experimental_hook(token=TOKEN))
    assert [message.run_id for message in world.messages] == [run.run_id, run.run_id]


async def test_uncertain_start_identifies_candidate_and_does_not_claim_failure(world, monkeypatch):
    monkeypatch.setattr(start_hooks, "CONFIRM_DELAYS", ())
    monkeypatch.setattr(
        world,
        "events_create",
        AsyncMock(side_effect=w.WorkflowWorldError("unavailable", status=503)),
    )
    with pytest.raises(WorkflowStartError) as caught:
        await start(receive_two.with_experimental_hook(token=TOKEN))
    assert caught.value.stage == "admission"
    assert caught.value.run_id == world.messages[0].run_id


@pytest.mark.parametrize("capable", [False, True])
async def test_cross_deployment_requires_positive_capability_proof(world, monkeypatch, capable):
    target = deployment.Target(
        "other",
        5,
        "none",
        {},
        probe={"capabilities": {"atomicStartHook": {"active": capable}}},
        cross_deployment=True,
    )
    monkeypatch.setattr(deployment, "resolve", AsyncMock(return_value=target))
    if capable:
        await start(receive_two.with_deployment_id("other").with_experimental_hook(token=TOKEN))
        assert len(world.messages) == 1
    else:
        with pytest.raises(w.WorkflowWorldError, match="does not support"):
            await start(receive_two.with_deployment_id("other").with_experimental_hook(token=TOKEN))
        assert not world.messages


async def test_relative_retention_is_resolved_per_start_and_configuration_is_immutable(world):
    configured = receive_two.with_experimental_hook(token=TOKEN, experimental_min_retention="1d")
    branch = configured.with_experimental_hook(token="other")
    assert configured.start_hook is not None and branch.start_hook is not None
    assert configured.start_hook.token == TOKEN
    assert branch.start_hook.token == "other"
    run = await start(configured)
    assert (await world.runs_get(run.run_id)).start_hook.token_retention_until > datetime.now(
        timezone.utc
    )


@pytest.mark.parametrize("duration", [-1, "-1s", datetime(2020, 1, 1, tzinfo=timezone.utc)])
async def test_invalid_retention_prevents_publication(world, duration):
    with pytest.raises((ValueError, RuntimeError)):
        await start(
            receive_two.with_experimental_hook(token=TOKEN, experimental_min_retention=duration)
        )
    assert not world.messages


async def test_errors_round_trip_with_start_identity():
    for error in (
        HookConflictError(TOKEN, "wrun_owner", kind="start"),
        WorkflowStartError("wrun_candidate", "queue", ValueError("lost")),
    ):
        revived = ser.hydrate(PLAIN_ENCODER.encode(error), what="start error")
        assert type(revived) is type(error)
        if isinstance(error, HookConflictError):
            assert isinstance(revived, HookConflictError)
            assert revived.kind == "start"
            assert revived.conflicting_run_id == error.conflicting_run_id
        else:
            assert isinstance(revived, WorkflowStartError)
            assert revived.run_id == error.run_id
            assert revived.stage == error.stage


async def test_cancel_uncreated_candidate_fences_late_queue_copy(world):
    candidate_id = f"wrun_{world.create_run_id()}"
    await world.events_create(candidate_id, w.RunCancelledEvent())
    with pytest.raises(w.WorkflowWorldError) as caught:
        await world.events_create(candidate_id, created())
    assert caught.value.code == start_hooks.ADMISSION_REJECTED
    assert not (world.data_dir / "runs" / f"{candidate_id}.json").exists()


async def test_orphan_claim_recovery_fences_the_original_candidate(world, monkeypatch):
    original = local_mod.write_exclusive
    candidate_id = f"wrun_{world.create_run_id()}"

    def crash(path, data):
        if path.parent.name == "runs":
            raise RuntimeError("crash before acceptance")
        return original(path, data)

    monkeypatch.setattr(local_mod, "write_exclusive", crash)
    with pytest.raises(RuntimeError, match="before acceptance"):
        await world.events_create(candidate_id, created())
    monkeypatch.setattr(local_mod, "write_exclusive", original)
    claim_path = world._hook_token_path(TOKEN)
    claim = local_mod.read_json(claim_path, local_mod.HookTokenClaim)
    assert claim is not None
    # World holds orphan claims for 24h; after that, a successor fences the
    # old candidate before reclaiming, preventing its delayed writer winning.
    claim.reserved_at = datetime.now(timezone.utc).timestamp() * 1000 - 86401000
    local_mod.write_json(claim_path, claim, overwrite=True)
    successor_id = f"wrun_{world.create_run_id()}"
    await world.events_create(successor_id, created())
    with pytest.raises(w.WorkflowWorldError) as caught:
        await world.events_create(candidate_id, created())
    assert caught.value.reason == "expired"
    assert (await world.hooks_get_by_token(TOKEN)).run_id == successor_id


async def test_four_processes_admit_exactly_one_owner(world):
    import sys

    import anyio

    script = """
import sys
import anyio
from vercel.workflow._internal import world as w, errors
from vercel.workflow._internal.worlds.local import LocalWorld
async def main():
    world = LocalWorld()
    try:
        await world.events_create(sys.argv[1], w.RunCreatedEventData(
            deployment_id="local", workflow_name="test", input=b"[]",
            start_hook=w.StartHook(token="start:order")).into_event())
        print("accepted:" + sys.argv[1])
    except errors.HookConflictError as error:
        print("rejected:" + error.conflicting_run_id)
anyio.run(main)
"""
    answers = []

    async def compete():
        candidate = f"wrun_{world.create_run_id()}"
        result = await anyio.run_process([sys.executable, "-c", script, candidate])
        answers.append(result.stdout.decode().strip())

    async with anyio.create_task_group() as group:
        for _ in range(4):
            group.start_soon(compete)
    winners = [
        answer.removeprefix("accepted:") for answer in answers if answer.startswith("accepted:")
    ]
    assert len(winners) == 1
    assert answers.count(f"rejected:{winners[0]}") == 3
    assert len(list((world.data_dir / "runs").glob("*.json"))) == 1
    assert (await world.hooks_get_by_token(TOKEN)).run_id == winners[0]


@pytest.mark.parametrize(
    "probe", [None, {"format": "text"}, {"capabilities": {"atomicStartHook": {"active": True}}}]
)
async def test_retention_requires_explicit_target_support(world, monkeypatch, probe):
    target = deployment.Target("other", 5, "none", {}, probe=probe, cross_deployment=True)
    monkeypatch.setattr(deployment, "resolve", AsyncMock(return_value=target))
    with pytest.raises(w.WorkflowWorldError):
        await start(
            receive_two.with_deployment_id("other").with_experimental_hook(
                token=TOKEN, experimental_min_retention="1d"
            )
        )
    assert not world.messages


@pytest.mark.parametrize("field", ["error", "code"])
def test_vercel_start_hook_conflict_uses_top_level_identity(field):
    import httpx2 as httpx

    from vercel.workflow._internal.worlds.vercel import VercelWorld

    world = VercelWorld(token="test-token")
    response = httpx.Response(
        409,
        json={
            field: "start-hook-conflict",
            "token": TOKEN,
            "conflictingRunId": "wrun_owner",
            "message": "conflict",
        },
    )
    error = world._error_for_response("POST", "/v3/runs/test/events", response)
    assert isinstance(error, HookConflictError)
    assert error.token == TOKEN
    assert error.conflicting_run_id == "wrun_owner"


async def test_vercel_serializes_start_hook_seed_as_cbor(respx_mock):
    import cbor2
    import httpx2 as httpx

    from vercel.workflow._internal.worlds.vercel import VercelWorld

    world = VercelWorld(token="test-token")
    route = respx_mock.post(f"{world._base_url}/v3/runs/wrun_candidate/events").mock(
        return_value=httpx.Response(
            200, content=cbor2.dumps({}), headers={"content-type": "application/cbor"}
        )
    )
    deadline = datetime(2027, 1, 1, tzinfo=timezone.utc)
    await world.events_create(
        "wrun_candidate", created(metadata=b"encoded", token_retention_until=deadline)
    )
    seed = cbor2.loads(route.calls.last.request.content)["eventData"]["startHook"]
    assert seed == {"token": TOKEN, "metadata": b"encoded", "tokenRetentionUntil": deadline}


@registry.step
async def start_duplicate() -> str:
    await start(receive_two.with_experimental_hook(token=TOKEN))
    return "unexpected"


@registry.workflow
async def catch_duplicate() -> str:
    try:
        return await start_duplicate()
    except HookConflictError as error:
        assert error.kind == "start"
        assert error.conflicting_run_id is not None
        return error.conflicting_run_id


async def test_start_conflict_in_step_is_not_retried_or_wrapped(world):
    owner = await start(receive_two.with_experimental_hook(token=TOKEN))
    parent = await start(catch_duplicate)
    await invoke(world.messages[1])
    step_message = next(message for message in world.messages if message.step_id is not None)
    await invoke(step_message)
    await invoke(world.messages[-1])
    assert await parent.return_value() == owner.run_id
    events = (await world.events_list(parent.run_id)).data
    assert sum(isinstance(event, w.StepFailedEvent) for event in events) == 1
    assert not any(isinstance(event, w.StepRetryingEvent) for event in events)


async def test_world_dropping_reservation_never_executes_candidate(world, monkeypatch):
    original = world.events_create

    async def ignore_hook(run_id, data):
        if isinstance(data, w.RunCreatedEvent | w.RunStartedEvent) and data.event_data is not None:
            seed = data.event_data.model_copy(update={"start_hook": None})
            data = data.model_copy(update={"event_data": seed})
        return await original(run_id, data)

    monkeypatch.setattr(world, "events_create", ignore_hook)
    with pytest.raises(w.WorkflowWorldError, match="did not confirm"):
        await start(receive_two.with_experimental_hook(token=TOKEN))
    candidate = world.messages[0]
    with pytest.raises(w.WorkflowWorldError, match="no code was executed"):
        await invoke(candidate)
    run = await world.runs_get(candidate.run_id)
    assert run.status == "failed"
    assert run.error_code == "WORLD_CONTRACT_ERROR"
    events = (await world.events_list(candidate.run_id)).data
    assert not any(isinstance(event, w.HookCreatedEvent | w.StepCreatedEvent) for event in events)
