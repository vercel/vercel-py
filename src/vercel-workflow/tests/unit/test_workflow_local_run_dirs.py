"""LocalWorld stores event and step files one directory per run.

Mirrors ``@workflow/world-local``: ``events/<runId>/<runId>-<eventId>.json`` and
``steps/<runId>/<runId>-<stepId>.json``, and a data directory still holding the
old flat files is deleted on startup rather than read, while one that mixes
layouts or holds unknown ``.json`` files is refused and left alone.
"""

from __future__ import annotations

import errno
import json

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


def test_a_failed_wipe_fails_startup(tmp_path, monkeypatch) -> None:
    (tmp_path / "events").mkdir()
    (tmp_path / "events" / "wrun_old-evnt_1.json").write_text("{}")

    def refuse(path, ignore_errors=False, **kwargs):
        if ignore_errors:
            return
        raise PermissionError(13, "Permission denied", str(path))

    monkeypatch.setattr(local_mod.shutil, "rmtree", refuse)

    with pytest.raises(PermissionError):
        _world(tmp_path, monkeypatch)
    assert (tmp_path / "events" / "wrun_old-evnt_1.json").is_file()


def _flat_dir(tmp_path) -> None:
    (tmp_path / "events").mkdir()
    (tmp_path / "events" / "wrun_old-evnt_1.json").write_text("{}")


def test_wiping_an_already_wiped_directory_succeeds(tmp_path) -> None:
    _flat_dir(tmp_path)

    local_mod._wipe_flat_layout(tmp_path)
    local_mod._wipe_flat_layout(tmp_path)
    local_mod._remove_tree(tmp_path)

    assert not tmp_path.exists()


def test_an_entry_removed_by_another_process_mid_wipe_is_not_an_error(
    tmp_path, monkeypatch
) -> None:
    _flat_dir(tmp_path)
    real_unlink = local_mod.os.unlink

    def lose_race(path, *args, **kwargs):
        real_unlink(path, *args, **kwargs)
        raise FileNotFoundError(2, "No such file or directory", str(path))

    monkeypatch.setattr(local_mod.os, "unlink", lose_race)

    local_mod._wipe_flat_layout(tmp_path)

    assert not tmp_path.exists()


def test_a_wipe_retries_when_a_writer_races_the_delete(tmp_path, monkeypatch) -> None:
    _flat_dir(tmp_path)
    real_rmtree = local_mod.shutil.rmtree
    calls = []

    def flaky(path, **kwargs):
        calls.append(path)
        if len(calls) == 1:
            raise OSError(errno.ENOTEMPTY, "Directory not empty", str(path))
        real_rmtree(path, **kwargs)

    monkeypatch.setattr(local_mod.shutil, "rmtree", flaky)

    local_mod._wipe_flat_layout(tmp_path)

    assert len(calls) == 2
    assert not tmp_path.exists()
