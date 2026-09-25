import asyncio
import dataclasses
from datetime import datetime, timezone

import pytest

from tests.payloads import PLAIN_ENCODER
from vercel.workflow._internal import core, errors, runtime, world as w

TOKEN = "disposed-hook"
registry = core.Workflows(as_vercel_job=False)


@dataclasses.dataclass
class Payload(core.BaseHook):
    value: str


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
