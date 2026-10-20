"""LocalWorld stores event and step files one directory per run.

Mirrors ``@workflow/world-local``: ``events/<runId>/<runId>-<eventId>.json`` and
``steps/<runId>/<runId>-<stepId>.json``, and a data directory still holding the
old flat files is deleted on startup rather than read, while one that mixes
layouts or holds unknown ``.json`` files is refused and left alone.
"""

from __future__ import annotations

import json
import os

import pytest

from tests.payloads import PLAIN_ENCODER
from vercel.workflow._internal import world as w
from vercel.workflow._internal.worlds import local as local_mod


def _world(tmp_path, monkeypatch) -> local_mod.LocalWorld:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    return local_mod.LocalWorld()


async def _start_run(world: local_mod.LocalWorld) -> str:
    result = await world.events_create(
        None,
        w.RunCreatedEvent(
            event_data=w.RunCreatedEventData(
                deployment_id="dpl_1",
                workflow_name="add_ten",
                input=PLAIN_ENCODER.encode([7]),
            )
        ),
    )
    assert result.run is not None
    return result.run.run_id


async def test_events_and_steps_live_in_the_runs_directory(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    run_id = await _start_run(world)
    await world.events_create(
        run_id,
        w.StepCreatedEvent(
            correlation_id="step_1",
            event_data=w.StepCreatedEventData(step_name="add", input=PLAIN_ENCODER.encode([1])),
        ),
    )

    assert not [p for p in (tmp_path / "events").iterdir() if p.is_file()]
    assert not [p for p in (tmp_path / "steps").iterdir() if p.is_file()]
    events = sorted(p.name for p in (tmp_path / "events" / run_id).iterdir())
    assert len(events) == 2
    assert all(name.startswith(f"{run_id}-evnt_") for name in events)
    assert (tmp_path / "steps" / run_id / f"{run_id}-step_1.json").is_file()

    listed = await world.events_list(run_id)
    assert [e.event_type for e in listed.data] == ["run_created", "step_created"]
    assert (await world.steps_get(run_id, "step_1")).step_name == "add"


async def test_listing_a_run_without_events_is_empty(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    assert (await world.events_list("wrun_missing")).data == []


async def test_listing_reads_only_that_run(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    first, second = await _start_run(world), await _start_run(world)

    for run_id in (first, second):
        listed = await world.events_list(run_id)
        on_disk = [p.name for p in (tmp_path / "events" / run_id).iterdir()]
        assert [
            f"{run_id}-{e.server_props.event_id}.json"
            for e in listed.data
            if e.server_props is not None
        ] == on_disk


def test_a_flat_data_directory_is_deleted_on_startup(tmp_path, monkeypatch, capsys) -> None:
    (tmp_path / "events").mkdir()
    (tmp_path / "events" / "wrun_old-evnt_1.json").write_text(json.dumps({}))
    (tmp_path / "runs").mkdir()
    (tmp_path / "runs" / "wrun_old.json").write_text(json.dumps({}))

    _world(tmp_path, monkeypatch)

    assert not (tmp_path / "events").exists()
    assert not (tmp_path / "runs").exists()
    assert "incompatible storage layout" in capsys.readouterr().err


async def test_a_run_scoped_data_directory_is_kept(tmp_path, monkeypatch, capsys) -> None:
    run_id = await _start_run(_world(tmp_path, monkeypatch))

    world = _world(tmp_path, monkeypatch)

    assert (await world.runs_get(run_id)).run_id == run_id
    assert len((await world.events_list(run_id)).data) == 1
    assert "incompatible" not in capsys.readouterr().err


@pytest.mark.parametrize(
    ("label", "stray"),
    [("flat file next to per-run dirs", "wrun_A-evnt_2.json"), ("unknown json", "notes.json")],
)
def test_a_mixed_or_unrecognized_data_directory_is_refused(
    tmp_path, monkeypatch, label, stray
) -> None:
    (tmp_path / "events" / "wrun_A").mkdir(parents=True)
    (tmp_path / "events" / "wrun_A" / "wrun_A-evnt_1.json").write_text("{}")
    (tmp_path / "events" / stray).write_text("{}")

    with pytest.raises(local_mod.DataDirLayoutError, match=stray):
        _world(tmp_path, monkeypatch)

    assert (tmp_path / "events" / "wrun_A" / "wrun_A-evnt_1.json").is_file()
    assert (tmp_path / "events" / stray).is_file()


async def test_a_symlinked_run_directory_is_refused(tmp_path, monkeypatch) -> None:
    data, outside = tmp_path / "data", tmp_path / "outside"
    outside.mkdir()
    (data / "steps").mkdir(parents=True)
    (data / "steps" / "wrun_A").symlink_to(outside, target_is_directory=True)
    world = _world(data, monkeypatch)

    with pytest.raises(local_mod.UnsafeEntityIdError):
        await world.steps_get("wrun_A", "step_1")
    assert list(outside.iterdir()) == []


@pytest.mark.skipif(os.name == "nt" or os.geteuid() == 0, reason="needs POSIX permissions")
def test_a_failed_wipe_fails_startup(tmp_path, monkeypatch) -> None:
    events = tmp_path / "events"
    events.mkdir()
    (events / "wrun_old-evnt_1.json").write_text("{}")
    events.chmod(0o500)
    try:
        with pytest.raises(PermissionError):
            _world(tmp_path, monkeypatch)
        assert (events / "wrun_old-evnt_1.json").is_file()
    finally:
        events.chmod(0o700)
