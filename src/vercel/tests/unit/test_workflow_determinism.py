"""Replay determinism detection.

Correlation IDs are assigned positionally, so a body that issues steps in a
different order or with different arguments on replay would have recorded
results matched onto the wrong calls. ``resume()`` must detect this and fail
loudly rather than silently returning the wrong value.
"""

from datetime import datetime, timezone
from typing import Any

import pytest

from vercel._internal.workflow import (
    core,
    loop as workflow_loop,
    runtime,
    serialization as ser,
    world as w,
)


async def _greet(*, name: str) -> str:
    return name


def _context(
    events: list[w.Event], *, seed: str = "wrun_test"
) -> runtime.WorkflowOrchestratorContext:
    ctx = runtime.WorkflowOrchestratorContext(
        events,
        run_id=seed,
        seed=seed,
        started_at=0,
        registry=core.Workflows(as_vercel_job=False),
    )
    return ctx


def _args(**kwargs: Any) -> bytes:
    """A step input payload, encoded the way the runtime encodes one."""
    return ser.dehydrate(ser.step_arguments((), kwargs))


def _suspension(correlation_id: str, args: bytes) -> runtime.Suspension:
    return runtime.Suspension(correlation_id=correlation_id, step=core.Step(_greet), input=args)


async def test_reordered_step_args_raise_nondeterminism() -> None:
    """Recorded step input "a" but the body now calls the same step with "b"
    on replay -> NondeterminismError."""
    step = core.Step(_greet)
    cid = "step_1"
    events: list[w.Event] = [
        w.StepCreatedEventData(stepName=step.name, input=_args(name="a")).into_event(cid)
    ]
    ctx = _context(events)
    sus = _suspension(cid, _args(name="b"))
    ctx.suspensions[cid] = sus

    ctx.resume()

    assert sus.future.done()
    assert isinstance(sus.future.exception(), runtime.NondeterminismError)


async def test_matching_step_parks_without_nondeterminism() -> None:
    """Same step + same input -> suspension is replayed, then the run parks."""
    step = core.Step(_greet)
    cid = "step_1"
    events: list[w.Event] = [
        w.StepCreatedEventData(stepName=step.name, input=_args(name="a")).into_event(cid)
    ]
    ctx = _context(events)
    sus = _suspension(cid, _args(name="a"))
    ctx.suspensions[cid] = sus

    with pytest.raises(runtime._SuspendException):
        ctx.resume()

    assert not sus.future.done()
    assert sus.has_created_event


async def test_wait_step_swap_raises_nondeterminism() -> None:
    """Recorded a step at this positional slot, but the body now issues a wait
    with the same positional ULID -> NondeterminismError (not a silent stall).

    The kind prefixes differ, so the recorded ``step_1`` never matches the
    body's ``wait_1`` by full correlation ID; the positional ULID does, which is
    how the swap is caught.
    """
    step = core.Step(_greet)
    events: list[w.Event] = [
        w.StepCreatedEventData(stepName=step.name, input=_args(name="a")).into_event("step_1")
    ]
    ctx = _context(events)
    wait = runtime.Wait(
        correlation_id="wait_1", resume_at=datetime(2026, 1, 1, tzinfo=timezone.utc)
    )
    ctx.suspensions["wait_1"] = wait

    ctx.resume()

    assert wait.future.done()
    assert isinstance(wait.future.exception(), runtime.NondeterminismError)


# --- concurrent delivery: the loop idle hook + resume single-step ----------------
#
# When a body issues several calls from concurrent coroutines, recorded
# completions must be delivered ONE AT A TIME, each only once the body has fully
# reacted to the previous one. Otherwise a woken coroutine interleaves with a
# still-running one and the two issue their next calls in a different order than
# at record time -> the positional correlation IDs no longer line up ->
# NondeterminismError. Two pieces cooperate:
#   * WorkflowLoop calls resume() only when its ready queue is empty (quiescent).
#   * resume() applies at most one recorded event (single-step), or parks.

_ARGS = _args(name="a")


def _created(step: "core.Step[Any, Any]", cid: str) -> w.Event:
    return w.StepCreatedEventData(stepName=step.name, input=_ARGS).into_event(cid)


def _completed(cid: str, result: Any) -> w.Event:
    return w.StepCompletedEventData(result=ser.dehydrate(result)).into_event(cid)


async def test_cancelled_step_ignores_later_completion() -> None:
    """A launched step can be cancelled before its completion is replayed.

    The completion event still needs to be consumed, but setting a result on
    the cancelled future would raise ``InvalidStateError``.
    """
    step = core.Step(_greet)
    events: list[w.Event] = [_created(step, "step_1")]
    ctx = _context(events)
    sus = _suspension("step_1", _ARGS)
    ctx.suspensions["step_1"] = sus

    # Replay the launch, then model the workflow task being cancelled while
    # the step is in flight. The completion arrives in a later event batch.
    with pytest.raises(runtime._SuspendException):
        ctx.resume()
    assert sus.has_created_event
    assert sus.future.cancel()

    events.append(_completed("step_1", "one"))
    ctx.resume()

    assert sus.future.cancelled()
    assert "step_1" not in ctx.suspensions


async def test_single_step_delivers_one_completion_per_pass() -> None:
    """Two concurrently-issued steps both have results in the log, but a single
    resume() pass resolves only the first; the rest is left for the next pass."""
    step = core.Step(_greet)
    events: list[w.Event] = [
        _created(step, "step_1"),
        _created(step, "step_2"),
        _completed("step_1", "one"),
        _completed("step_2", "two"),
    ]
    ctx = _context(events)
    sus1 = _suspension("step_1", _ARGS)
    sus2 = _suspension("step_2", _ARGS)
    ctx.suspensions["step_1"] = sus1
    ctx.suspensions["step_2"] = sus2

    ctx.resume()

    # exactly one completion delivered; its suspension consumed...
    assert sus1.future.done() and sus1.future.result() == "one"
    assert "step_1" not in ctx.suspensions
    # ...the second still pending, and resume() returned normally so the loop
    # can deliver it on a later idle pass.
    assert not sus2.future.done()
    assert "step_2" in ctx.suspensions


async def test_workflow_loop_runs_pending_work_before_resume() -> None:
    """The idle hook runs only after the body's pending callbacks are drained."""
    step = core.Step(_greet)
    events: list[w.Event] = [
        _created(step, "step_1"),
        _completed("step_1", "one"),
    ]
    ctx = _context(events)
    sus1 = _suspension("step_1", _ARGS)
    ctx.suspensions["step_1"] = sus1

    pending_ran = False

    def pending() -> None:
        nonlocal pending_ran
        pending_ran = True

    def resume() -> None:
        assert pending_ran
        ctx.resume()

    loop = workflow_loop.WorkflowLoop(idle_hook=resume)
    try:
        loop.call_soon(pending)
        loop._run_once()

        assert sus1.future.done() and sus1.future.result() == "one"
    finally:
        loop.close()


async def test_workflow_loop_runs_resume_when_quiescent() -> None:
    """With the ready queue empty, the idle hook delivers one completion."""
    step = core.Step(_greet)
    events: list[w.Event] = [
        _created(step, "step_1"),
        _completed("step_1", "one"),
    ]
    ctx = _context(events)
    sus1 = _suspension("step_1", _ARGS)
    ctx.suspensions["step_1"] = sus1

    loop = workflow_loop.WorkflowLoop(idle_hook=ctx.resume)
    try:
        loop._run_once()
        assert sus1.future.done() and sus1.future.result() == "one"
    finally:
        loop.close()


async def test_idle_resume_parks_when_nothing_to_deliver() -> None:
    """Suspension registered and its create replayed, no completion yet -> the
    run suspends by cancelling its future."""
    step = core.Step(_greet)
    events: list[w.Event] = [_created(step, "step_1")]
    ctx = _context(events)
    sus1 = _suspension("step_1", _ARGS)
    ctx.suspensions["step_1"] = sus1

    loop = workflow_loop.WorkflowLoop(idle_hook=ctx.resume)
    try:
        with pytest.raises(runtime._SuspendException):
            loop._run_once()

        assert sus1.has_created_event
        assert not sus1.future.done()
    finally:
        loop.close()


# --- now(): deterministic clock anchored to replay progress, not list tail ------


def _stamp(event: w.Event, ts: datetime, *, event_id: str) -> w.Event:
    return event.model_copy(
        update={"server_props": w.ServerProps(runId="wrun_test", eventId=event_id, createdAt=ts)}
    )


async def test_now_uses_first_event_before_any_replay() -> None:
    """Before any suspension has been created/consumed, now() must fall back to
    the first event in the log, not the last. The log already contains
    everything from prior invocations, so `events[-1]` would leak a later
    invocation's timestamp into a call site reached before any of it happened.
    """
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 2, tzinfo=timezone.utc)
    step = core.Step(_greet)
    events: list[w.Event] = [
        _stamp(_created(step, "step_1"), t0, event_id="evt_1"),
        _stamp(_created(step, "step_2"), t1, event_id="evt_2"),
    ]
    ctx = _context(events)

    assert ctx.now() == t0


async def test_now_advances_with_replay_index() -> None:
    """Once resume() has delivered a completion, now() reflects that event's
    timestamp, not a later, not-yet-consumed event further down the log."""
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 2, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 3, tzinfo=timezone.utc)
    step = core.Step(_greet)
    events: list[w.Event] = [
        _stamp(_created(step, "step_1"), t0, event_id="evt_1"),
        _stamp(_completed("step_1", "one"), t1, event_id="evt_2"),
        _stamp(_created(step, "step_2"), t2, event_id="evt_3"),
    ]
    ctx = _context(events)
    sus1 = _suspension("step_1", _ARGS)
    ctx.suspensions["step_1"] = sus1

    ctx.resume()

    assert sus1.future.done() and sus1.future.result() == "one"
    assert ctx.now() == t1


async def test_ctx_now_raises_when_events_empty() -> None:
    ctx = _context([])
    with pytest.raises(RuntimeError):
        ctx.now()


async def test_core_now_raises_outside_workflow() -> None:
    with pytest.raises(RuntimeError):
        core.now()


async def test_time_ns_matches_now_as_nanoseconds() -> None:
    t0 = datetime(2026, 1, 1, 12, 30, 45, 123456, tzinfo=timezone.utc)
    events: list[w.Event] = [_stamp(_created(core.Step(_greet), "step_1"), t0, event_id="evt_1")]
    ctx = _context(events)

    assert ctx.time_ns() == int(t0.timestamp()) * 1_000_000_000 + t0.microsecond * 1_000


async def test_core_time_ns_raises_outside_workflow() -> None:
    with pytest.raises(RuntimeError):
        core.time_ns()


# --- random(): per-run deterministic Random, decoupled from the ambient module ---


async def test_random_same_seed_same_sequence() -> None:
    ctx1 = _context([], seed="wrun_a")
    ctx2 = _context([], seed="wrun_a")

    assert [ctx1.random().random() for _ in range(5)] == [ctx2.random().random() for _ in range(5)]


async def test_random_different_seed_different_sequence() -> None:
    ctx1 = _context([], seed="wrun_a")
    ctx2 = _context([], seed="wrun_b")

    assert [ctx1.random().random() for _ in range(5)] != [ctx2.random().random() for _ in range(5)]


async def test_random_returns_memoized_instance() -> None:
    ctx = _context([], seed="wrun_a")

    assert ctx.random() is ctx.random()


async def test_core_random_raises_outside_workflow() -> None:
    with pytest.raises(RuntimeError):
        core.random()
