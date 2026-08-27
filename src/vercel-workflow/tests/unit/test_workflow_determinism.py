"""Replay determinism detection.

Correlation IDs are assigned positionally, so a body that issues steps in a
different order or with different arguments on replay would have recorded
results matched onto the wrong calls. ``resume()`` must detect this and fail
loudly rather than silently returning the wrong value.
"""

import asyncio
import traceback
from datetime import datetime, timezone
from typing import Any

import pytest

from vercel.workflow._internal import (
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


def _resume_isolated(ctx: runtime.WorkflowOrchestratorContext) -> None:
    """One resume() pass in a throwaway loop.

    A nondeterminism failure suspends the run, which cancels every task in the
    running loop -- including the test's own task, were resume() called in it.
    """

    async def body() -> None:
        ctx.resume()

    try:
        runtime._run_isolated(body(), loop_factory=asyncio.new_event_loop)
    except asyncio.CancelledError:
        pass


async def test_reordered_step_args_raise_nondeterminism() -> None:
    """Recorded step input "a" but the body now calls the same step with "b"
    on replay -> NondeterminismError."""
    step = core.Step(_greet)
    cid = "step_1"
    events: list[w.Event] = [
        w.StepCreatedEventData(step_name=step.name, input=_args(name="a")).into_event(cid)
    ]
    ctx = _context(events)
    sus = _suspension(cid, _args(name="b"))
    ctx.suspensions[cid] = sus

    _resume_isolated(ctx)

    assert sus.future.done()
    assert isinstance(sus.future.exception(), runtime.NondeterminismError)
    # Stashed for run_workflow() to raise, and the run is suspended, so the
    # failure holds even if the body never awaits (or catches) the future.
    assert ctx.resume_exception is sus.future.exception()
    assert ctx.suspended


async def test_wait_step_swap_raises_nondeterminism() -> None:
    """Recorded a step at this positional slot, but the body now issues a wait
    with the same positional ULID -> NondeterminismError (not a silent stall).

    The kind prefixes differ, so the recorded ``step_1`` never matches the
    body's ``wait_1`` by full correlation ID; the positional ULID does, which is
    how the swap is caught.
    """
    step = core.Step(_greet)
    events: list[w.Event] = [
        w.StepCreatedEventData(step_name=step.name, input=_args(name="a")).into_event("step_1")
    ]
    ctx = _context(events)
    wait = runtime.Wait(
        correlation_id="wait_1", resume_at=datetime(2026, 1, 1, tzinfo=timezone.utc)
    )
    ctx.suspensions["wait_1"] = wait

    _resume_isolated(ctx)

    assert wait.future.done()
    assert isinstance(wait.future.exception(), runtime.NondeterminismError)
    assert ctx.resume_exception is wait.future.exception()
    assert ctx.suspended


async def test_created_event_without_suspension_raises_runtime_error() -> None:
    """A creation event cannot precede the body's matching suspension."""
    step = core.Step(_greet)
    ctx = _context([_created(step, "step_1")])

    with pytest.raises(RuntimeError, match="has not registered its suspension"):
        ctx.resume()


# --- concurrent delivery: the loop workflow + resume single-step -----------------
#
# When a body issues several calls from concurrent coroutines, recorded
# completions must be delivered ONE AT A TIME, each only once the body has fully
# reacted to the previous one. Otherwise a woken coroutine interleaves with a
# still-running one and the two issue their next calls in a different order than
# at record time -> the positional correlation IDs no longer line up ->
# NondeterminismError. Two pieces cooperate:
#   * WorkflowLoop calls workflow.resume() only when its ready queue is empty
#     (quiescent).
#   * resume() applies at most one recorded event (single-step), or parks.

_ARGS = _args(name="a")


def _created(step: "core.Step[Any, Any]", cid: str) -> w.Event:
    return w.StepCreatedEventData(step_name=step.name, input=_ARGS).into_event(cid)


def _completed(cid: str, result: Any) -> w.Event:
    return w.StepCompletedEventData(result=ser.dehydrate(result)).into_event(cid)


async def test_cancelled_step_ignores_later_completion() -> None:
    """A step can be cancelled before its completion is replayed.

    The completion event still needs to be consumed, but setting a result on
    the cancelled future would raise ``InvalidStateError``.
    """
    events: list[w.Event] = [_completed("step_1", "one")]
    ctx = _context(events)
    sus = _suspension("step_1", _ARGS)
    ctx.suspensions["step_1"] = sus

    assert sus.future.cancel()
    ctx.resume()

    assert sus.future.cancelled()
    assert "step_1" not in ctx.suspensions


async def test_single_step_delivers_one_completion_per_pass() -> None:
    """Two concurrently-issued steps both have results in the log, but a single
    resume() pass resolves only the first; the rest is left for the next pass."""
    events: list[w.Event] = [
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
    events: list[w.Event] = [
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

    class Workflow:
        def resume(self) -> None:
            resume()

        def time(self) -> float:
            raise NotImplementedError

        def check_suspended(self) -> None:
            pass

        def run_wait(self, param: Any) -> asyncio.Future[None]:
            raise NotImplementedError

    loop = workflow_loop.WorkflowLoop(workflow=Workflow())
    try:
        loop.call_soon(pending)
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

    async def wait_forever() -> None:
        await asyncio.Future()

    with pytest.raises(asyncio.CancelledError):
        runtime._run_isolated(
            wait_forever(),
            loop_factory=lambda: workflow_loop.WorkflowLoop(workflow=ctx),
        )

    assert ctx.suspended
    assert sus1.has_created_event
    assert not sus1.future.done()


# --- nondeterminism fails the run whatever the body does -------------------------
#
# Failing the diverged suspension's future is not enough on its own: the body
# may not be blocked on that future, and even when it is, user code can catch
# the error. run_workflow() must fail the run either way.

_run_registry = core.Workflows(as_vercel_job=False)


@_run_registry.step
async def _record(*, name: str) -> str:
    return name


@_run_registry.workflow
async def _diverging() -> str:
    return await _record(name="b")


@_run_registry.workflow
async def _suppressing() -> str:
    try:
        return await _record(name="b")
    except BaseException:  # noqa: B036
        return "swallowed"


@_run_registry.workflow
async def _reraising() -> str:
    try:
        return await _record(name="b")
    except BaseException:  # noqa: B036
        raise runtime.NondeterminismError("surfaced by the body") from None


def _running_run(workflow_id: str) -> w.WorkflowRun:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return w.NonFinalWorkflowRun(
        run_id="wrun_test",
        status="running",
        deployment_id="",
        workflow_name=workflow_id,
        input=ser.dehydrate(ser.argument_array((), {})),
        created_at=now,
        updated_at=now,
        started_at=now,
    )


def _diverged_log() -> list[w.Event]:
    """A recorded first step whose input differs from what the body issues.

    The probe context shares the run's seed, so its first ULID is the one the
    body's first call will be assigned.
    """
    cid = f"step_{_context([]).generate_ulid()}"
    return [w.StepCreatedEventData(step_name=_record.name, input=_args(name="a")).into_event(cid)]


async def test_nondeterminism_fails_the_run() -> None:
    ctx = runtime.WorkflowOrchestratorContext(
        _diverged_log(), run_id="wrun_test", seed="wrun_test", started_at=0, registry=_run_registry
    )

    with pytest.raises(runtime.NondeterminismError) as excinfo:
        ctx.run_workflow(_running_run(_diverging.workflow_id))

    # The error is raised out of the body's own await, so its traceback shows
    # where in the workflow the divergence happened.
    frames = traceback.extract_tb(excinfo.value.__traceback__)
    assert any(frame.name == "_diverging" for frame in frames)


async def test_nondeterminism_cannot_be_suppressed_by_the_body() -> None:
    ctx = runtime.WorkflowOrchestratorContext(
        _diverged_log(), run_id="wrun_test", seed="wrun_test", started_at=0, registry=_run_registry
    )

    with pytest.raises(runtime.NondeterminismError):
        ctx.run_workflow(_running_run(_suppressing.workflow_id))


async def test_nondeterminism_surfaced_by_the_body_wins() -> None:
    """A body that lets (or re-raises) the divergence error out keeps its own
    instance -- and with it the traceback -- over the stashed one."""
    ctx = runtime.WorkflowOrchestratorContext(
        _diverged_log(), run_id="wrun_test", seed="wrun_test", started_at=0, registry=_run_registry
    )

    with pytest.raises(runtime.NondeterminismError, match="surfaced by the body"):
        ctx.run_workflow(_running_run(_reraising.workflow_id))


# --- now(): deterministic clock anchored to replay progress, not list tail ------


def _stamp(event: w.Event, ts: datetime, *, event_id: str) -> w.Event:
    return event.model_copy(
        update={"server_props": w.ServerProps(run_id="wrun_test", event_id=event_id, created_at=ts)}
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

    for _ in events:
        ctx.resume()
        if sus1.future.done():
            break

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
