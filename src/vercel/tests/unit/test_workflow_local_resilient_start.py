"""LocalWorld creating a run from a ``run_started`` whose row never landed.

Mirrors ``@workflow/world-local``'s ``events-storage.ts`` resilient-start branch.
"""

from __future__ import annotations

import json

import pytest

from vercel._internal.workflow import serialization as ser, world as w
from vercel._internal.workflow.worlds import local as local_mod

RUN_ID = "wrun_resilient"


def _world(tmp_path, monkeypatch) -> local_mod.LocalWorld:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    return local_mod.LocalWorld()


def _run_input(**overrides) -> w.RunInput:
    return w.RunInput.model_validate(
        {
            "input": ser.dehydrate([7]),
            "deploymentId": "dpl_1",
            "workflowName": "add_ten",
            "specVersion": 6,
            "executionContext": {"workflowCoreVersion": "5.0.0"},
        }
        | overrides
    )


def _resilient_event(run_input: w.RunInput | None = None) -> w.RunStartedEvent:
    run_input = run_input or _run_input()
    return w.RunStartedEventData.from_run_input(run_input).into_event(
        spec_version=run_input.spec_version
    )


async def test_run_started_creates_the_missing_run(tmp_path, monkeypatch) -> None:
    """The whole point: no row, and the run still starts."""
    world = _world(tmp_path, monkeypatch)

    result = await world.events_create(RUN_ID, _resilient_event())

    assert result.run is not None
    assert result.run.run_id == RUN_ID
    # It went through pending and out the other side in the one call.
    assert result.run.status == "running"
    assert result.run.started_at is not None
    assert (await world.runs_get(RUN_ID)).status == "running"


async def test_recreated_run_carries_the_creating_client_data(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    run_input = _run_input(attributes={"tier": "pro"}, encryptionPublicKey="cHVibGljLWtleQ==")

    await world.events_create(RUN_ID, _resilient_event(run_input))

    run = await world.runs_get(RUN_ID)
    assert run.deployment_id == "dpl_1"
    assert run.workflow_name == "add_ten"
    assert run.input == run_input.input
    assert run.execution_context == {"workflowCoreVersion": "5.0.0"}
    assert run.attributes == {"tier": "pro"}
    # Neither is readable through this SDK, but a run that lost them could never
    # get them back.
    assert run.encryption_public_key == "cHVibGljLWtleQ=="


async def test_recreated_run_keeps_the_creators_spec_version(tmp_path, monkeypatch) -> None:
    """Relabelling to our own CURRENT (2) would misreport the run's capabilities."""
    world = _world(tmp_path, monkeypatch)

    await world.events_create(RUN_ID, _resilient_event(_run_input(specVersion=6)))

    assert (await world.runs_get(RUN_ID)).spec_version == 6


async def test_synthetic_run_created_replays_first(tmp_path, monkeypatch) -> None:
    """Replay reads the log in order, so the `run_created` we invent has to sort
    below the `run_started` that caused it."""
    world = _world(tmp_path, monkeypatch)

    await world.events_create(RUN_ID, _resilient_event())

    events = (await world.events_list(RUN_ID)).data
    assert [e.event_type for e in events] == ["run_created", "run_started"]
    assert events[0].server_props is not None and events[1].server_props is not None
    assert events[0].server_props.event_id < events[1].server_props.event_id


async def test_synthetic_run_created_carries_the_input(tmp_path, monkeypatch) -> None:
    """It is the event replay reads the arguments from."""
    world = _world(tmp_path, monkeypatch)
    run_input = _run_input()

    await world.events_create(RUN_ID, _resilient_event(run_input))

    run_created = (await world.events_list(RUN_ID)).data[0]
    assert isinstance(run_created, w.RunCreatedEvent)
    assert run_created.event_data.input == run_input.input
    assert run_created.event_data.deployment_id == "dpl_1"
    assert run_created.event_data.workflow_name == "add_ten"
    assert run_created.spec_version == 6


async def test_stored_run_started_has_no_event_data(tmp_path, monkeypatch) -> None:
    """`eventData` is queue transport, not part of the event. Leaving it on the
    row would put the run's input in the log twice."""
    world = _world(tmp_path, monkeypatch)

    await world.events_create(RUN_ID, _resilient_event())

    rows = [json.loads(p.read_text()) for p in sorted((tmp_path / "events").glob("*.json"))]
    run_started = next(r for r in rows if r["eventType"] == "run_started")
    assert "eventData" not in run_started


async def test_existing_run_is_untouched_by_event_data(tmp_path, monkeypatch) -> None:
    """When the row is there, the carried data is ignored entirely — it must not
    overwrite what `run_created` actually recorded."""
    world = _world(tmp_path, monkeypatch)
    created = await world.events_create(
        None,
        w.RunCreatedEventData(
            deploymentId="dpl_real",
            workflowName="add_ten",
            input=ser.dehydrate([1]),
        ).into_event(),
    )
    assert created.run is not None
    run_id = created.run.run_id

    await world.events_create(
        run_id, _resilient_event(_run_input(deploymentId="dpl_bogus", input=ser.dehydrate([999])))
    )

    run = await world.runs_get(run_id)
    assert run.status == "running"
    assert run.deployment_id == "dpl_real"
    assert run.input == ser.dehydrate([1])
    # No second run_created invented on top of the real one.
    events = (await world.events_list(run_id)).data
    assert [e.event_type for e in events] == ["run_created", "run_started"]


async def test_concurrent_run_created_wins_the_row(tmp_path, monkeypatch) -> None:
    """`start()`'s own write may land between our read and our create, so the
    exclusive write has to leave it alone.

    Simulated by having the racing writer land *inside* our attempt: absent when
    we look, present when we write, which is the whole window.
    """
    world = _world(tmp_path, monkeypatch)
    real_input = ser.dehydrate([1])
    real_row = local_mod.dumps_js(
        w.NonFinalWorkflowRun(
            runId=RUN_ID,
            deploymentId="dpl_real",
            status="pending",
            workflowName="add_ten",
            specVersion=6,
            input=real_input,
            createdAt=local_mod.js_now(),
            updatedAt=local_mod.js_now(),
        ).model_dump(exclude_none=True)
    ).decode()
    real = local_mod.write_exclusive
    raced = False

    def racing_write_exclusive(path, data):
        nonlocal raced
        if path.name == f"{RUN_ID}.json" and not raced:
            raced = True
            # The other writer gets there first, so ours loses the file.
            assert real(path, real_row)
            return real(path, data)
        return real(path, data)

    monkeypatch.setattr(local_mod, "write_exclusive", racing_write_exclusive)

    result = await world.events_create(
        RUN_ID, _resilient_event(_run_input(deploymentId="dpl_bogus"))
    )

    assert raced, "the race path was never taken"
    assert result.run is not None
    assert result.run.status == "running"
    # Theirs, not the one our queue message described.
    assert result.run.deployment_id == "dpl_real"
    assert result.run.input == real_input
    # We lost, so we wrote no synthetic run_created either.
    assert [e.event_type for e in (await world.events_list(RUN_ID)).data] == ["run_started"]


async def test_bare_run_started_on_a_missing_run_still_fails(tmp_path, monkeypatch) -> None:
    """A re-enqueue carries no `runInput`, so there is nothing to recover from
    and the missing row stays an error, as the Vercel world's 404 does."""
    world = _world(tmp_path, monkeypatch)

    with pytest.raises(RuntimeError, match="not found"):
        await world.events_create(RUN_ID, w.RunStartedEvent())

    # No row and no event invented out of nothing.
    with pytest.raises(RuntimeError, match="not found"):
        await world.runs_get(RUN_ID)
    assert not list((tmp_path / "events").glob("*.json"))


@pytest.mark.parametrize("field", ["deploymentId", "workflowName", "input"])
@pytest.mark.parametrize("how", ["missing", "empty"])
async def test_incomplete_event_data_creates_nothing(tmp_path, monkeypatch, field, how) -> None:
    """All three are needed to open a row; a partial one would be worse than none.

    Empty counts as absent for the two names, matching the truthiness guard in
    `events-storage.ts` — a run row with an empty `deploymentId` is garbage.
    """
    world = _world(tmp_path, monkeypatch)
    data = {
        "deploymentId": "dpl_1",
        "workflowName": "add_ten",
        "input": ser.dehydrate([7]),
    }
    if how == "missing":
        del data[field]
    else:
        data[field] = b"" if field == "input" else ""
        if field == "input":
            pytest.skip("empty bytes is a legal payload, unlike an empty name")

    with pytest.raises(RuntimeError, match="not found"):
        await world.events_create(
            RUN_ID, w.RunStartedEvent(eventData=w.RunStartedEventData.model_validate(data))
        )

    with pytest.raises(RuntimeError, match="not found"):
        await world.runs_get(RUN_ID)


async def test_terminal_run_still_conflicts(tmp_path, monkeypatch) -> None:
    """Resilient start must not resurrect a run that already finished."""
    world = _world(tmp_path, monkeypatch)
    created = await world.events_create(
        None,
        w.RunCreatedEventData(
            deploymentId="dpl_1", workflowName="add_ten", input=ser.dehydrate([1])
        ).into_event(),
    )
    assert created.run is not None
    run_id = created.run.run_id
    await world.events_create(run_id, w.RunStartedEvent())
    await world.events_create(
        run_id, w.RunCompletedEventData(output=ser.dehydrate(11)).into_event()
    )

    with pytest.raises(w.EntityConflictError):
        await world.events_create(run_id, _resilient_event())


async def test_run_started_on_a_running_run_appends_no_event(tmp_path, monkeypatch) -> None:
    """The runtime issues `run_started` on *every* delivery, so a log that grew
    an entry per replay would be unbounded."""
    world = _world(tmp_path, monkeypatch)
    first = await world.events_create(RUN_ID, _resilient_event())
    assert first.run is not None

    second = await world.events_create(RUN_ID, _resilient_event())

    assert second.run is not None
    assert second.run.status == "running"
    assert second.run.started_at == first.run.started_at
    assert [e.event_type for e in (await world.events_list(RUN_ID)).data] == [
        "run_created",
        "run_started",
    ]


async def test_bare_run_started_on_a_running_run_appends_no_event(tmp_path, monkeypatch) -> None:
    """Same for a re-enqueue, which carries no `runInput` at all."""
    world = _world(tmp_path, monkeypatch)
    created = await world.events_create(
        None,
        w.RunCreatedEventData(
            deploymentId="dpl_1", workflowName="add_ten", input=ser.dehydrate([1])
        ).into_event(),
    )
    assert created.run is not None
    run_id = created.run.run_id
    await world.events_create(run_id, w.RunStartedEvent())

    result = await world.events_create(run_id, w.RunStartedEvent())

    assert result.run is not None and result.run.status == "running"
    assert [e.event_type for e in (await world.events_list(run_id)).data] == [
        "run_created",
        "run_started",
    ]


async def test_encryption_public_key_survives_the_lifecycle(tmp_path, monkeypatch) -> None:
    """Every transition rewrites the whole row, so a field the model does not
    carry is erased on the next event."""
    world = _world(tmp_path, monkeypatch)
    await world.events_create(
        RUN_ID, _resilient_event(_run_input(encryptionPublicKey="cHVibGljLWtleQ=="))
    )

    await world.events_create(
        RUN_ID, w.RunCompletedEventData(output=ser.dehydrate(17)).into_event()
    )

    run = await world.runs_get(RUN_ID)
    assert run.status == "completed"
    assert run.encryption_public_key == "cHVibGljLWtleQ=="
    # And on disk, where a TS reader looks for it.
    row = json.loads((tmp_path / "runs" / f"{RUN_ID}.json").read_text())
    assert row["encryptionPublicKey"] == "cHVibGljLWtleQ=="


async def test_no_encryption_key_is_absent_not_null(tmp_path, monkeypatch) -> None:
    """The TS run schema types it `undefined`; an explicit null is rejected."""
    world = _world(tmp_path, monkeypatch)

    await world.events_create(RUN_ID, _resilient_event())

    row = json.loads((tmp_path / "runs" / f"{RUN_ID}.json").read_text())
    assert "encryptionPublicKey" not in row
