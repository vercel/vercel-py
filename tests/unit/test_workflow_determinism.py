"""Replay determinism detection.

Correlation IDs are assigned positionally, so a body that issues steps in a
different order or with different arguments on replay would have recorded
results matched onto the wrong calls. ``resume()`` must detect this and fail
loudly rather than silently returning the wrong value.
"""

import asyncio

from vercel._internal.workflow import core, runtime, world as w


async def _greet(name: str) -> str:
    return name


def _context(events: list[w.Event]) -> runtime.WorkflowOrchestratorContext:
    ctx = runtime.WorkflowOrchestratorContext(
        events,
        seed="wrun_test",
        started_at=0,
        registry=core.Workflows(as_vercel_job=False),
    )
    # resume() short-circuits unless a workflow future is in flight.
    ctx._fut = asyncio.get_event_loop().create_future()
    return ctx


def _suspension(correlation_id: str, args_json: bytes) -> runtime.Suspension:
    return runtime.Suspension(
        correlation_id=correlation_id, step=core.Step(_greet), input=args_json
    )


async def test_reordered_step_args_raise_nondeterminism() -> None:
    """Recorded step input "a" but the body now calls the same step with "b"
    on replay -> NondeterminismError."""
    step = core.Step(_greet)
    cid = "step_1"
    events: list[w.Event] = [
        w.StepCreatedEventData(stepName=step.name, input=[b'json[["a"], {}]']).into_event(cid)
    ]
    ctx = _context(events)
    sus = _suspension(cid, b'json[["b"], {}]')
    ctx.suspensions[cid] = sus

    ctx.resume()

    assert sus.future.done()
    assert isinstance(sus.future.exception(), runtime.NondeterminismError)


async def test_matching_step_does_not_raise() -> None:
    """Same step + same input -> no error, suspension is marked replayed."""
    step = core.Step(_greet)
    cid = "step_1"
    events: list[w.Event] = [
        w.StepCreatedEventData(stepName=step.name, input=[b'json[["a"], {}]']).into_event(cid)
    ]
    ctx = _context(events)
    sus = _suspension(cid, b'json[["a"], {}]')
    ctx.suspensions[cid] = sus

    ctx.resume()

    assert not sus.future.done()
    assert sus.has_created_event
