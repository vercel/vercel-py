"""Tests for LocalWorld dev-restart recovery."""

import pytest

from vercel._internal.workflow import world as w

# LocalWorld imports vercel.workers, which is only installed on Python >= 3.12.
pytest.importorskip("vercel.workers")

from vercel._internal.workflow.worlds import local as local_mod  # noqa: E402


@pytest.fixture
def world(tmp_path, monkeypatch) -> local_mod.LocalWorld:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    return local_mod.LocalWorld()


async def _create_run(world: local_mod.LocalWorld) -> str:
    """Create a run and move it to 'running'; returns the run id."""
    created = await world.events_create(
        None,
        w.RunCreatedEventData(
            deploymentId="", workflowName="workflow//tests.wf", input=[b"json[[], {}]"]
        ).into_event(),
    )
    assert created.run is not None
    run_id = created.run.run_id
    await world.events_create(run_id, w.RunStartedEvent())
    return run_id


async def test_resume_pending_runs_reenqueues_only_nonterminal(world) -> None:
    """Recovery sweep re-enqueues runs left 'running' (e.g. a dev restart lost a
    sleep's wake-up), but leaves terminal runs alone."""
    captured: list[tuple[str, str]] = []

    async def fake_queue(queue_name, message, **kwargs):  # type: ignore[no-untyped-def]
        captured.append((queue_name, message.run_id))
        return "msg_x"

    world.queue = fake_queue  # type: ignore[method-assign]

    running_id = await _create_run(world)

    completed_id = await _create_run(world)
    await world.events_create(completed_id, w.RunCompletedEventData(output=[b"json1"]).into_event())

    await world._resume_pending_runs()

    assert captured == [("__wkf_workflow_workflow//tests.wf", running_id)]
