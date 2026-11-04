"""Local hook claim writers share directory leases independently of retention."""

from __future__ import annotations

import os

import anyio
import pytest

from tests.payloads import PLAIN_ENCODER
from vercel.workflow._internal import world as w
from vercel.workflow._internal.file_lock import FileLockTimeoutError, file_lock
from vercel.workflow._internal.worlds import local as local_mod

RUN_ID = "wrun_test"
TOKEN = "shared-token"


@pytest.fixture
def world(tmp_path, monkeypatch):
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    return local_mod.LocalWorld()


async def test_stale_token_lock_does_not_block_hook_creation(world) -> None:
    path = world._hook_token_path(TOKEN).with_suffix(".json.lock")
    path.mkdir(parents=True)
    os.utime(path, (0, 0))
    await world.events_create(RUN_ID, w.HookCreatedEventData(token=TOKEN).into_event("hook_1"))
    assert (await world.hooks_get_by_token(TOKEN)).hook_id == "hook_1"
    assert not path.exists()


async def test_token_lock_timeout_is_a_world_error(world, monkeypatch) -> None:
    path = world._hook_token_path(TOKEN).with_suffix(".json.lock")
    path.mkdir(parents=True)
    monkeypatch.setattr(local_mod, "file_lock", lambda path, **_: file_lock(path, timeout=0))
    with pytest.raises(w.WorkflowWorldError, match="Timed out") as raised:
        async with world._hook_token_lock(TOKEN):
            pytest.fail("acquired an already-held token lock")
    assert raised.value.status == 500
    assert isinstance(raised.value.__cause__, FileLockTimeoutError)
    assert path.is_dir()


async def test_lost_token_lock_does_not_remove_successor(world) -> None:
    path = world._hook_token_path(TOKEN).with_suffix(".json.lock")
    with pytest.raises(w.WorkflowWorldError, match="ownership changed"):
        async with world._hook_token_lock(TOKEN):
            path.rmdir()
            path.mkdir()
            os.utime(path, (0, 0))
    assert path.is_dir()


@pytest.mark.parametrize("kind", ["hook_created", "hook_disposed", "run_completed"])
async def test_hook_claim_mutations_wait_for_token_lock(world, kind) -> None:
    await world.events_create(
        RUN_ID,
        w.RunCreatedEventData(
            deployment_id="", workflow_name="test", input=PLAIN_ENCODER.encode([])
        ).into_event(),
    )
    if kind != "hook_created":
        await world.events_create(RUN_ID, w.HookCreatedEventData(token=TOKEN).into_event("hook_1"))
    event: w.Event
    if kind == "hook_created":
        event = w.HookCreatedEventData(token=TOKEN).into_event("hook_1")
    elif kind == "hook_disposed":
        event = w.HookDisposedEvent(correlation_id="hook_1")
    else:
        event = w.RunCompletedEventData(output=PLAIN_ENCODER.encode("done")).into_event()

    started = anyio.Event()
    finished = anyio.Event()

    async def mutate() -> None:
        started.set()
        await world.events_create(RUN_ID, event)
        finished.set()

    path = world._hook_token_path(TOKEN)
    path.parent.mkdir(parents=True, exist_ok=True)
    with anyio.fail_after(5):
        async with anyio.create_task_group() as group:
            async with file_lock(path):
                group.start_soon(mutate)
                await started.wait()
                await anyio.sleep(0.05)
                assert not finished.is_set()
            await finished.wait()
    if kind == "hook_created":
        assert (await world.hooks_get_by_token(TOKEN)).hook_id == "hook_1"
    else:
        with pytest.raises(w.HookNotFoundError):
            await world.hooks_get_by_token(TOKEN)


@pytest.mark.parametrize("successor_run_id", [RUN_ID, "wrun_successor"])
async def test_delayed_old_hook_deletion_preserves_successor_claim(world, successor_run_id) -> None:
    await world.events_create(RUN_ID, w.HookCreatedEventData(token=TOKEN).into_event("hook_old"))
    old_hook = await world.hooks_get_by_token(TOKEN)
    await world.events_create(RUN_ID, w.HookDisposedEvent(correlation_id="hook_old"))
    await world.events_create(
        successor_run_id, w.HookCreatedEventData(token=TOKEN).into_event("hook_successor")
    )
    # A cleanup reader may have captured the old entity before waiting for
    # this token's lock. Its delayed delete must preserve the new claim.
    async with world._hook_token_lock(TOKEN):
        world._delete_hook(old_hook)
    result = await world.events_create(
        "wrun_contender", w.HookCreatedEventData(token=TOKEN).into_event("hook_contender")
    )
    assert isinstance(result.event, w.HookConflictEvent)
    assert result.event.event_data.conflicting_run_id == successor_run_id
    assert (await world.hooks_get_by_token(TOKEN)).hook_id == "hook_successor"


@pytest.mark.parametrize("interruption", ["cancel", "timeout"])
async def test_terminal_cleanup_interruption_does_not_pin_token(world, monkeypatch, interruption):
    await world.events_create(
        RUN_ID,
        w.RunCreatedEventData(
            deployment_id="", workflow_name="test", input=PLAIN_ENCODER.encode([])
        ).into_event(),
    )
    await world.events_create(RUN_ID, w.HookCreatedEventData(token=TOKEN).into_event("hook_old"))
    event = w.RunCompletedEventData(output=PLAIN_ENCODER.encode("done")).into_event()
    async with file_lock(world._hook_token_path(TOKEN)):
        if interruption == "cancel":
            with anyio.move_on_after(0.05) as scope:
                await world.events_create(RUN_ID, event)
            assert scope.cancel_called
        else:
            monkeypatch.setattr(
                local_mod, "file_lock", lambda path, **_: file_lock(path, timeout=0)
            )
            with pytest.raises(w.WorkflowWorldError, match="Timed out"):
                await world.events_create(RUN_ID, event)
    assert world._hook_token_path(TOKEN).exists()
    with pytest.raises(w.EntityConflictError):
        await world.events_create(RUN_ID, event)
    result = await world.events_create(
        "wrun_successor", w.HookCreatedEventData(token=TOKEN).into_event("hook_successor")
    )
    assert isinstance(result.event, w.HookCreatedEvent)
    # Delayed cleanup of the old run must leave the recovered claim intact.
    await world.delete_all_hooks_for_run(RUN_ID)
    assert (await world.hooks_get_by_token(TOKEN)).hook_id == "hook_successor"


async def test_disposal_does_not_delete_hook_published_after_missing_read(world, monkeypatch):
    await world.events_create(RUN_ID, w.HookCreatedEventData(token=TOKEN).into_event("hook_1"))
    hook = await world.hooks_get_by_token(TOKEN)
    path = world.data_dir / "hooks" / "hook_1.json"
    path.unlink()
    read_json = local_mod.read_json

    def publish_after_read(candidate, schema):
        result = read_json(candidate, schema)
        if candidate == path and result is None:
            # A different process publishes between the entry read and the
            # event implementation's existence check, still holding the lock.
            local_mod.write_json(path, hook)
        return result

    async with file_lock(world._hook_token_path(TOKEN)) as owner:
        monkeypatch.setattr(local_mod, "read_json", publish_after_read)
        with pytest.raises(w.HookNotFoundError):
            await world.events_create(RUN_ID, w.HookDisposedEvent(correlation_id="hook_1"))
        owner.check()
        assert world._hook_token_path(TOKEN).exists()
        assert path.exists()


async def test_committed_disposal_with_interrupted_cleanup_releases_token(world):
    await world.events_create(RUN_ID, w.HookCreatedEventData(token=TOKEN).into_event("hook_old"))
    marker = world.data_dir / ".locks" / "hooks" / "hook_old.disposed"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()
    result = await world.events_create(
        "wrun_successor", w.HookCreatedEventData(token=TOKEN).into_event("hook_successor")
    )
    assert isinstance(result.event, w.HookCreatedEvent)
