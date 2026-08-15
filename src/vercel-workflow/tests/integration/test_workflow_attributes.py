"""`set_attributes()` driven through real runs.

Nothing is faked: `LocalWorld` runs its embedded queue service in-process and
`workflow_entrypoint` subscribes the real handler to it, so each workflow below
executes the way `vercel dev` would execute it -- replay, suspension flush and
all. That is the point of testing this here rather than against a fake: a
workflow-body write resolves through a replay, which a unit test driving one
`resume()` cannot show.

Each body mirrors one of the `setAttributes*` fixtures in the TypeScript e2e
suite, so what passes here is what that suite expects to see.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from vercel.queue.testing import clear_subscriptions
from vercel.workflow import FatalError, remove_attributes, set_attributes
from vercel.workflow._internal import core, runtime, world as w
from vercel.workflow._internal.worlds import local as local_mod

# How long a run gets before the test fails instead of hanging. Every run here
# finishes in well under a second.
RUN_DEADLINE_SECONDS = 30

# Module level, not function level: replay re-imports the workflow's defining
# module by name inside the sandbox, so the bodies have to live somewhere
# importable.
registry = core.Workflows(as_vercel_job=False)


@registry.workflow
async def sets_attributes(value: int, /) -> int:
    await set_attributes(phase="init", source="workflow-body")
    tripled = value * 3
    await set_attributes({"phase": "done"})
    await remove_attributes("source")
    return tripled


@registry.step
async def set_attributes_from_step(value: int, /) -> int:
    await set_attributes(phase="step-started", source="step-body", input=str(value))
    await set_attributes(phase="step-done")
    return value * 4


@registry.workflow
async def sets_attributes_inside_a_step(value: int, /) -> int:
    return await set_attributes_from_step(value)


@registry.workflow
async def sets_attributes_in_parallel() -> str:
    await asyncio.gather(
        set_attributes(a="1"),
        set_attributes(b="2"),
        set_attributes(c="3"),
    )
    return "done"


@registry.workflow
async def throws_after_setting_attributes() -> str:
    await set_attributes(phase="about-to-fail", reason="intentional")
    raise RuntimeError("intentional failure to test attribute persistence")


@registry.workflow
async def catches_invalid_attributes() -> dict[str, str]:
    outcomes: dict[str, str] = {}

    async def attempt(label: str, attributes: Any) -> None:
        try:
            await set_attributes(attributes)
            outcomes[label] = "no-error"
        except Exception as e:
            outcomes[label] = f"{type(e).__name__}: {e}"

    await attempt("reserved", {"$system": "nope"})
    await attempt("emptyKey", {"": "v"})
    await attempt("keyTooLong", {"k" * 257: "v"})
    await attempt("valueTooLong", {"note": "v" * 257})
    await attempt("valueTooManyBytes", {"note": "é" * 200})
    await attempt("overCap", {f"k{i}": "v" for i in range(65)})
    await attempt("nonObject", "phase=init")

    # The run has to stay healthy after every rejected call.
    await set_attributes(phase="validated")
    return outcomes


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)
    # Queue subscriptions are process-global and refuse to be registered twice
    # for the same topic pattern, so each test hands its own back.
    clear_subscriptions()


@pytest.fixture
async def world(tmp_path: Path, monkeypatch) -> Any:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = local_mod.LocalWorld()
    w.set_world(world)
    runtime.workflow_entrypoint(registry)
    yield world
    await world.aclose()


async def _run(world: local_mod.LocalWorld, wf: Any, *args: Any) -> tuple[str, Any]:
    """Run a workflow to completion and return its run id and return value."""
    run = await runtime.start(wf, *args)
    result = await asyncio.wait_for(run.return_value(), RUN_DEADLINE_SECONDS)
    return run.run_id, result


async def test_a_body_write_lands_on_the_run(world) -> None:
    run_id, result = await _run(world, sets_attributes, 7)

    assert result == 21
    # The first call sets phase and source, the second overwrites phase, the
    # third removes source.
    run = await world.runs_get(run_id)
    assert run.attributes == {"phase": "done"}


async def test_a_client_reads_them_back_off_the_run(world) -> None:
    """The read side of the API: what a caller holding a `Run` can see."""
    run = await runtime.start(sets_attributes, 7)
    await asyncio.wait_for(run.return_value(), RUN_DEADLINE_SECONDS)

    assert await run.attributes() == {"phase": "done"}


async def test_each_body_write_appends_one_event(world) -> None:
    run_id, _ = await _run(world, sets_attributes, 7)

    events = (await world.events_list(run_id)).data
    attr_events = [e for e in events if e.event_type == "attr_set"]
    assert len(attr_events) == 3
    assert all(isinstance(e.event_data.writer, w.WorkflowAttributeWriter) for e in attr_events)
    # Correlated, so a replay can tell which call each one answers.
    assert all(e.correlation_id and e.correlation_id.startswith("attr_") for e in attr_events)


async def test_a_step_write_is_attributed_to_the_step(world) -> None:
    run_id, result = await _run(world, sets_attributes_inside_a_step, 9)

    assert result == 36
    run = await world.runs_get(run_id)
    assert run.attributes == {"phase": "step-done", "source": "step-body", "input": "9"}

    events = (await world.events_list(run_id)).data
    writers = [e.event_data.writer for e in events if e.event_type == "attr_set"]
    assert len(writers) == 2
    assert all(isinstance(writer, w.StepAttributeWriter) for writer in writers)
    assert all(writer.attempt == 1 for writer in writers)


async def test_parallel_writes_of_disjoint_keys_all_land(world) -> None:
    run_id, result = await _run(world, sets_attributes_in_parallel)

    assert result == "done"
    run = await world.runs_get(run_id)
    assert run.attributes == {"a": "1", "b": "2", "c": "3"}


async def test_a_run_that_fails_keeps_what_it_wrote(world) -> None:
    run = await runtime.start(throws_after_setting_attributes)
    # "workflow failed" and nothing else: the thrown value does not survive the
    # event log yet (gap 13). What matters here is the attributes, below.
    with pytest.raises(RuntimeError, match="workflow failed"):
        await asyncio.wait_for(run.return_value(), RUN_DEADLINE_SECONDS)

    stored = await world.runs_get(run.run_id)
    assert stored.status == "failed"
    assert stored.error is not None and "intentional failure" in stored.error.message
    # The write was awaited, so it landed before the throw -- and the
    # run_failed write has to carry the attribute snapshot forward.
    assert stored.attributes == {"phase": "about-to-fail", "reason": "intentional"}


async def test_a_rejected_write_does_not_wedge_the_run(world) -> None:
    run_id, outcomes = await _run(world, catches_invalid_attributes)

    assert all(outcome.startswith("FatalError: ") for outcome in outcomes.values()), outcomes
    assert "reserved prefix" in outcomes["reserved"]
    assert "must not be empty" in outcomes["emptyKey"]
    assert "key length 257 exceeds limit 256" in outcomes["keyTooLong"]
    assert "byte length 257 exceeds limit 256" in outcomes["valueTooLong"]
    assert "byte length 400 exceeds limit 256" in outcomes["valueTooManyBytes"]
    assert "exceed limit: 65 > 64" in outcomes["overCap"]
    assert "requires a mapping" in outcomes["nonObject"]

    # No invalid write reached the run, and the valid one after them did.
    run = await world.runs_get(run_id)
    assert run.status == "completed"
    assert run.attributes == {"phase": "validated"}


async def test_the_error_type_is_catchable_in_a_body() -> None:
    """The type the bodies above catch by name, pinned here so a change to it
    shows up as a failure rather than as `Exception: ...` in an outcome map."""
    assert issubclass(FatalError, Exception)
