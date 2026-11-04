"""Runtime-only support for hooks admitted before the workflow body starts."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import pytest

from tests.payloads import PLAIN_ENCODER
from tests.workflow_context import bind_context
from vercel.workflow._internal import core, errors, runtime, serialization as ser, world as w

TOKEN = "order:42"
HOOK_ID = "hook_admitted"
START_HOOK = runtime.start_hooks.RunStartHook(token=TOKEN, hook_id=HOOK_ID)
registry = core.Workflows(as_vercel_job=False)


@dataclasses.dataclass
class Message(core.BaseHook):
    value: int


@registry.step
async def gate() -> None:
    pass


@registry.workflow
async def delayed_open() -> list[int]:
    await gate()
    hook = Message.wait(token=TOKEN)
    assert await hook.get_conflict() is None
    return [(await hook).value, (await hook).value]


def context(events: list[w.Event] | None = None, *, admitted: bool = True):
    return runtime.WorkflowOrchestratorContext(
        events or [],
        run_id="wrun_test",
        seed="wrun_test",
        started_at=0,
        registry=registry,
        start_hook=START_HOOK if admitted else None,
    )


def received(value: int, *, token: str = TOKEN, hook_id: str = HOOK_ID):
    return w.HookReceivedEventData(
        token=token, payload=PLAIN_ENCODER.encode({"value": value})
    ).into_event(hook_id)


# @lat: [[vercel-workflow#Hooks and streams#Pre-admitted hook replay]]
async def test_payloads_before_open_survive_real_workflow_replay() -> None:
    # The hook receives two payloads before the first step finishes, so replay
    # must buffer them before the body reaches wait(), on every invocation.
    probe = context()
    step_id = f"step_{probe.generate_ulid()}"
    events = [
        received(1),
        w.StepCreatedEventData(
            step_name=gate.name, input=PLAIN_ENCODER.encode(ser.step_arguments((), {}))
        ).into_event(step_id),
        received(2),
        w.StepCompletedEventData(result=PLAIN_ENCODER.encode(None)).into_event(step_id),
    ]
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    run = w.NonFinalWorkflowRun(
        run_id="wrun_test",
        status="running",
        deployment_id="test",
        workflow_name=delayed_open.workflow_id,
        input=PLAIN_ENCODER.encode(ser.argument_array((), {})),
        created_at=now,
        updated_at=now,
        started_at=now,
    )
    for _ in range(2):
        ctx = context(events)
        assert ctx.run_workflow(run) == PLAIN_ENCODER.encode([1, 2])
        assert ctx.hooks[HOOK_ID].has_created_event


@pytest.mark.parametrize("early", [False, True])
async def test_binding_never_registers_and_explicit_disposal_is_flushed(early, monkeypatch):
    ctx = context([received(7)])
    create = AsyncMock()
    monkeypatch.setattr(w, "get_world", lambda: Mock(events_create=create))
    with bind_context(ctx):
        if early:
            ctx._replay_next_event()
        hook = Message.wait(token=TOKEN)
        if not early:
            ctx._replay_next_event()
        assert (await hook).value == 7
        assert await hook.get_conflict() is None
        assert await runtime._flush_hook_lifecycle_events(ctx) == (False, set())
        create.assert_not_awaited()
        hook.dispose()
        await runtime._flush_hook_lifecycle_events(ctx)
    assert create.await_count == 1
    assert isinstance(create.call_args.args[1], w.HookDisposedEvent)
    assert create.call_args.args[1].correlation_id == HOOK_ID


async def test_disposal_before_open_discards_buffered_and_later_payloads():
    ctx = context([received(1), w.HookDisposedEvent(correlation_id=HOOK_ID), received(2)])
    with bind_context(ctx):
        for _ in ctx.events:
            ctx._replay_next_event()
        hook = Message.wait(token=TOKEN)
        with pytest.raises(errors.HookDisposedError):
            await hook
    assert ctx.hooks[HOOK_ID].has_dispose_event
    assert HOOK_ID not in ctx.suspensions


async def test_first_matching_open_binds_and_preserves_following_ids():
    plain = context(admitted=False)
    bound = context()
    with bind_context(plain):
        ordinary_first = Message.wait(token=TOKEN)
        ordinary_second = Message.wait(token="other")
    with bind_context(bound):
        first = Message.wait(token=TOKEN)
        second = Message.wait(token="other")
        duplicate = Message.wait(token=TOKEN)
    assert first._correlation_id == HOOK_ID
    assert ordinary_first._correlation_id != HOOK_ID
    assert second._correlation_id == ordinary_second._correlation_id
    assert duplicate._correlation_id != HOOK_ID
    assert not bound.hooks[duplicate._correlation_id].has_created_event


async def test_binding_rejects_metadata_without_consuming_the_binding():
    ctx = context()
    with bind_context(ctx):
        with pytest.raises(ValueError, match="already exists"):
            Message.wait(token=TOKEN, metadata={"ignored": True})
        assert Message.wait(token=TOKEN)._correlation_id == HOOK_ID


async def test_minted_hook_ids_skip_the_reserved_id(monkeypatch):
    ctx = context()
    monkeypatch.setattr(ctx, "generate_ulid", Mock(side_effect=["admitted", "next"]))
    with bind_context(ctx):
        assert Message.wait(token="other")._correlation_id == "hook_next"


async def test_unopened_start_hook_checks_recorded_token(monkeypatch):
    ctx = context([received(1, token="wrong")])
    # Inspect divergence without suspend() cancelling the test runner's tasks.
    monkeypatch.setattr(ctx, "suspend", Mock())
    with bind_context(ctx):
        ctx._replay_next_event()
    assert isinstance(ctx.resume_exception, runtime.NondeterminismError)


async def test_unknown_hook_payloads_are_not_buffered():
    ctx = context([received(1, hook_id="hook_unknown")])
    with bind_context(ctx):
        ctx._replay_next_event()
        Message.wait(token=TOKEN)
    assert not ctx.hooks[HOOK_ID].buffered_results


async def test_buffered_payload_validation_error_preserves_following_payload():
    ctx = context(
        [
            w.HookReceivedEventData(token=TOKEN, payload=PLAIN_ENCODER.encode({})).into_event(
                HOOK_ID
            ),
            received(2),
        ]
    )
    with bind_context(ctx):
        for _ in ctx.events:
            ctx._replay_next_event()
        hook = Message.wait(token=TOKEN)
        with pytest.raises(TypeError):
            await hook
        assert (await hook).value == 2


async def test_cancellation_hook_avoids_reserved_id(monkeypatch):
    ctx = context()
    monkeypatch.setattr(ctx, "generate_ulid", Mock(side_effect=["step", "admitted", "next"]))
    cancellable = core.Step(gate.func, cancellable=True)
    with bind_context(ctx):
        future = ctx.run_step(cancellable)
        # Cancelling registers the internal hook even before the step is dispatched.
        future.cancel()
    assert HOOK_ID not in ctx.suspensions
    assert "hook_next" in ctx.suspensions
