import asyncio
import dataclasses
from datetime import datetime, timezone

import pydantic
import pytest

from tests.payloads import PLAIN_ENCODER
from vercel.workflow import SandboxPolicy, WorkflowRunFailedError, start
from vercel.workflow._internal import core, errors, runtime, serialization as ser, world as w
from vercel.workflow._internal.worlds.local import LocalWorld

TOKEN = "disposed-hook"
registry = core.Workflows(as_vercel_job=False)


@dataclasses.dataclass
class Payload(core.BaseHook):
    value: str


class Approval(core.BaseHook, pydantic.BaseModel):
    approved: bool


def _context() -> runtime.WorkflowOrchestratorContext:
    return runtime.WorkflowOrchestratorContext(
        [],
        run_id="wrun_test",
        seed="seed",
        started_at=0,
        registry=registry,
    )


@pytest.mark.parametrize("outcome", ["created", "conflict", "legacy_conflict"])
async def test_disposal_does_not_abandon_a_pending_registration(outcome: str) -> None:
    context = _context()
    hook_id = context.create_hook(TOKEN, Payload)._correlation_id
    hook = context.hooks[hook_id]
    registration = asyncio.create_task(context.run_hook_conflict(correlation_id=hook_id))
    await asyncio.sleep(0)
    assert not registration.done()

    try:
        context.dispose_hook(correlation_id=hook_id)
        assert not registration.done()

        context.events.append(
            w.HookCreatedEventData(token=TOKEN).into_event(hook_id)
            if outcome == "created"
            else w.HookConflictEvent(
                correlation_id=hook_id,
                event_data=w.HookConflictEventData(
                    token=TOKEN,
                    conflicting_run_id="wrun_owner" if outcome == "conflict" else None,
                ),
            )
        )
        context.resume()
        await asyncio.sleep(0)
        assert registration.done()
        if outcome == "created":
            assert await registration is None
            assert hook.has_created_event
            assert hook.conflict_error is None
        elif outcome == "conflict":
            owner = await registration
            assert owner is not None
            assert owner.run_id == "wrun_owner"
            assert isinstance(hook.conflict_error, errors.HookConflictError)
        else:
            with pytest.raises(errors.HookConflictError):
                await registration

        assert hook.disposed
        assert hook_id not in context.suspensions
        assert context.replay_index == len(context.events)
    finally:
        registration.cancel()
        await asyncio.gather(registration, return_exceptions=True)


async def test_disposed_hook_still_discards_recorded_payloads() -> None:
    context = _context()
    hook_id = context.create_hook(TOKEN, Payload)._correlation_id
    context.dispose_hook(correlation_id=hook_id)
    context.events.extend(
        [
            w.HookCreatedEventData(token=TOKEN).into_event(hook_id),
            w.HookReceivedEventData(
                token=TOKEN, payload=PLAIN_ENCODER.encode({"value": "discarded"})
            ).into_event(hook_id),
            w.HookDisposedEvent(correlation_id=hook_id),
        ]
    )

    for _ in context.events:
        context.resume()

    assert not context.hooks[hook_id].buffered_results
    assert context.hooks[hook_id].has_dispose_event
    assert hook_id not in context.suspensions
    with pytest.raises(StopAsyncIteration):
        await context.run_hook(correlation_id=hook_id)


@pytest.mark.parametrize("recorded_kind", ["step", "wait"])
async def test_disposed_hook_still_occupies_its_recorded_position(recorded_kind: str) -> None:
    context = _context()
    hook_id = context.create_hook(TOKEN, Payload)._correlation_id
    position = hook_id.split("_", 1)[1]
    event = (
        w.StepCreatedEventData(
            step_name="previous_step", input=PLAIN_ENCODER.encode([])
        ).into_event(f"step_{position}")
        if recorded_kind == "step"
        else w.WaitCreatedEventData(resume_at=datetime(2026, 1, 1, tzinfo=timezone.utc)).into_event(
            f"wait_{position}"
        )
    )
    context.events.append(event)
    context.dispose_hook(correlation_id=hook_id)

    async def replay() -> None:
        context.resume()

    try:
        runtime._run_isolated(replay(), loop_factory=asyncio.new_event_loop)
    except asyncio.CancelledError:
        pass

    assert isinstance(context.resume_exception, runtime.NondeterminismError)
    assert str(context.resume_exception) == (
        f"workflow replay diverged at position {position}: recorded a {recorded_kind!r} call, "
        "but the body now issues a 'hook' call. The workflow body is non-deterministic."
    )
    assert context.suspended


async def test_cancellation_hook_replays_without_a_user_hook() -> None:
    context = _context()
    cancellation = runtime.Cancellation(
        correlation_id="hook_1", token="abrt_1", step_id="step_1", requested=True
    )
    context.suspensions[cancellation.correlation_id] = cancellation
    context.events.extend(
        [
            w.HookCreatedEventData(token=cancellation.token).into_event(
                cancellation.correlation_id
            ),
            w.HookReceivedEventData(payload=PLAIN_ENCODER.encode(None)).into_event(
                cancellation.correlation_id
            ),
        ]
    )

    context.resume()
    assert cancellation.has_created_event
    assert not context.hooks
    context.resume()
    assert not context.suspensions


@pytest.mark.parametrize("dispose_before_second_payload", [False, True])
async def test_recorded_disposal_preserves_unclaimed_payloads(
    dispose_before_second_payload: bool,
) -> None:
    context = _context()
    hook_id = context.create_hook("approval", Approval)._correlation_id
    context.events.extend(
        [
            w.HookCreatedEventData(token="approval").into_event(hook_id),
            w.HookReceivedEventData(payload=PLAIN_ENCODER.encode({"approved": True})).into_event(
                hook_id
            ),
            w.HookReceivedEventData(payload=PLAIN_ENCODER.encode({"approved": False})).into_event(
                hook_id
            ),
            w.HookDisposedEvent(correlation_id=hook_id),
        ]
    )
    for _ in context.events:
        context.resume()

    assert (await context.run_hook(correlation_id=hook_id)).approved is True
    if not dispose_before_second_payload:
        assert (await context.run_hook(correlation_id=hook_id)).approved is False
        with pytest.raises(StopAsyncIteration):
            await context.run_hook(correlation_id=hook_id)

    context.dispose_hook(correlation_id=hook_id)
    assert not context.hooks[hook_id].buffered_results
    with pytest.raises(StopAsyncIteration):
        await context.run_hook(correlation_id=hook_id)


@pytest.mark.parametrize(
    ("payload", "error_type"),
    [
        (PLAIN_ENCODER.encode({"wrong": True}), pydantic.ValidationError),
        (b"dev", ser.SerializationError),
    ],
    ids=["model validation", "hydration"],
)
async def test_recorded_disposal_preserves_buffered_errors(
    payload: bytes, error_type: type[Exception]
) -> None:
    context = _context()
    hook_id = context.create_hook("approval", Approval)._correlation_id
    context.events.extend(
        [
            w.HookCreatedEventData(token="approval").into_event(hook_id),
            w.HookReceivedEventData(payload=payload).into_event(hook_id),
            w.HookReceivedEventData(payload=PLAIN_ENCODER.encode({"approved": True})).into_event(
                hook_id
            ),
            w.HookDisposedEvent(correlation_id=hook_id),
        ]
    )
    for _ in context.events:
        context.resume()

    with pytest.raises(error_type):
        await context.run_hook(correlation_id=hook_id)
    assert (await context.run_hook(correlation_id=hook_id)).approved is True
    with pytest.raises(StopAsyncIteration):
        await context.run_hook(correlation_id=hook_id)


async def test_recorded_disposal_ends_pending_waits() -> None:
    context = _context()
    hook_id = context.create_hook("approval", Approval)._correlation_id
    context.events.append(w.HookCreatedEventData(token="approval").into_event(hook_id))
    context.resume()
    pending: list[asyncio.Task[Approval]] = [
        asyncio.create_task(context.run_hook(correlation_id=hook_id)) for _ in range(3)
    ]
    await asyncio.sleep(0)
    pending[0].cancel()
    try:
        context.events.append(w.HookDisposedEvent(correlation_id=hook_id))
        context.resume()
        await asyncio.sleep(0)

        assert all(task.done() for task in pending)
        for task in pending[1:]:
            with pytest.raises(StopAsyncIteration):
                await task
        with pytest.raises(StopAsyncIteration):
            await context.run_hook(correlation_id=hook_id)
    finally:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


# Share this module so the test can deliberately change the workflow branch
# between invocations, without changing its input or recorded events.
replay_registry = core.Workflows(
    as_vercel_job=False,
    sandbox_policy=SandboxPolicy(passthrough_modules=frozenset({__name__})),
)
_consume_after_step = False


@replay_registry.step
async def checkpoint() -> None:
    pass


@replay_registry.workflow
async def consume_across_disposal(read_mode: str) -> list[bool]:
    approval = Approval.wait(token="approval")
    await checkpoint()

    values = []
    if not _consume_after_step:
        values = [(await approval).approved for _ in range(2)]
        approval.dispose()

    await checkpoint()

    if _consume_after_step:
        if read_mode == "iterate":
            values = [message.approved async for message in approval]
        else:
            count = 3 if read_mode == "extra" else 2
            values = [(await approval).approved for _ in range(count)]
        approval.dispose()
    return values


class RecordingLocalWorld(LocalWorld):
    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: object) -> str:
        return "msg_test"


async def _invoke(run_id: str, step: w.StepCreatedEvent | None = None) -> None:
    payload = w.WorkflowInvokePayload(run_id=run_id)
    if step is not None:
        payload = w.WorkflowInvokePayload(
            run_id=run_id,
            step_id=step.correlation_id,
            step_name=step.event_data.step_name,
        )
    await runtime.workflow_handler(
        payload.model_dump(by_alias=True),
        attempt=1,
        queue_name=w.get_queue_name(consume_across_disposal.workflow_id),
        message_id="msg_test",
        registry=replay_registry,
    )


@pytest.mark.parametrize("read_mode", ["await", "iterate", "extra"])
async def test_replay_reads_buffer_then_ends_closed_hook(tmp_path, monkeypatch, read_mode) -> None:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = RecordingLocalWorld()
    w.set_world(world)
    try:
        run = await start(consume_across_disposal, read_mode)
        await _invoke(run.run_id)
        await Approval(approved=True).resume("approval")
        await Approval(approved=False).resume("approval")
        events = (await world.events_list(run.run_id)).data
        first_step = next(e for e in events if isinstance(e, w.StepCreatedEvent))
        await _invoke(run.run_id, first_step)
        await _invoke(run.run_id)

        events = (await world.events_list(run.run_id)).data
        assert any(isinstance(e, w.HookDisposedEvent) for e in events)
        last_step = next(
            e
            for e in events
            if isinstance(e, w.StepCreatedEvent) and e.correlation_id != first_step.correlation_id
        )
        await _invoke(run.run_id, last_step)

        # Replay now waits for the second step before consuming either message.
        # Its historical disposal is reached while both messages are buffered.
        monkeypatch.setattr(f"{__name__}._consume_after_step", True)
        await _invoke(run.run_id)

        status = (await world.runs_get(run.run_id)).status
        if read_mode == "extra":
            assert status == "failed"
            with pytest.raises(WorkflowRunFailedError, match="HookDisposedError"):
                await run.return_value()
        else:
            assert status == "completed"
            assert await run.return_value() == [True, False]
    finally:
        w.set_world(None)
