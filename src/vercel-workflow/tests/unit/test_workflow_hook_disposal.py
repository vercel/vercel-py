"""LocalWorld hook disposal semantics.

Disposing a hook frees its token and tears down the entity. Re-issuing
hook_disposed (or delivering a payload) once the hook is gone raises
HookNotFoundError -- the same typed error the backend's 404 yields, which the
runtime swallows. A concurrent invocation that loses the dispose-lock race gets
EntityConflictError instead of double-deleting and writing a duplicate event.
The durable disposal marker also rejects recreating that hook, while another
hook can claim the released token.
"""

from __future__ import annotations

import pytest

from tests.payloads import PLAIN_ENCODER
from vercel.workflow._internal import world as w
from vercel.workflow._internal.worlds import local as local_mod

RUN_ID = "wrun_test"
TOKEN = "shared-token"


def _world(tmp_path, monkeypatch) -> local_mod.LocalWorld:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    return local_mod.LocalWorld()


async def test_redispose_raises_hook_not_found(tmp_path, monkeypatch) -> None:
    # Once a hook is disposed (and unlinked), re-disposing it finds no hook and
    # raises HookNotFoundError -- the same typed error the backend's 404 yields,
    # which the runtime swallows.
    world = _world(tmp_path, monkeypatch)
    await world.events_create(RUN_ID, w.HookCreatedEventData(token=TOKEN).into_event("hook_1"))
    await world.events_create(RUN_ID, w.HookDisposedEvent(correlation_id="hook_1"))

    try:
        await world.events_create(RUN_ID, w.HookDisposedEvent(correlation_id="hook_1"))
    except w.HookNotFoundError:
        pass
    else:
        raise AssertionError("re-disposing a disposed hook should raise HookNotFoundError")


async def test_hook_received_for_missing_hook_raises_hook_not_found(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    data = w.HookReceivedEventData(payload=PLAIN_ENCODER.encode({}))

    try:
        await world.events_create(RUN_ID, data.into_event("hook_unknown"))
    except w.HookNotFoundError:
        pass
    else:
        raise AssertionError("hook_received for a missing hook should raise HookNotFoundError")


async def test_concurrent_dispose_loser_gets_entity_conflict(tmp_path, monkeypatch) -> None:
    # Cross-process race: the hook is still present but another process already
    # claimed the dispose lock. The loser must get EntityConflictError rather
    # than double-delete and write a duplicate hook_disposed event.
    world = _world(tmp_path, monkeypatch)
    await world.events_create(RUN_ID, w.HookCreatedEventData(token=TOKEN).into_event("hook_1"))
    lock_path = world.data_dir / ".locks" / "hooks" / "hook_1.disposed"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text("")

    try:
        await world.events_create(RUN_ID, w.HookDisposedEvent(correlation_id="hook_1"))
    except w.EntityConflictError:
        pass
    else:
        raise AssertionError("losing the dispose lock should raise EntityConflictError")


@pytest.mark.parametrize("successor_run_id", [None, RUN_ID, "wrun_successor"])
async def test_disposed_hook_cannot_be_recreated(tmp_path, monkeypatch, successor_run_id) -> None:
    world = _world(tmp_path, monkeypatch)
    created = w.HookCreatedEventData(token=TOKEN).into_event("hook_1")
    await world.events_create(RUN_ID, created)
    await world.events_create(RUN_ID, w.HookDisposedEvent(correlation_id="hook_1"))
    if successor_run_id is not None:
        await world.events_create(
            successor_run_id, w.HookCreatedEventData(token=TOKEN).into_event("hook_successor")
        )
    before = (await world.events_list(RUN_ID)).data

    # Another worker has no in-memory knowledge of the first hook's lifetime.
    other_world = _world(tmp_path, monkeypatch)
    with pytest.raises(w.EntityConflictError):
        await other_world.events_create(RUN_ID, created)

    assert (await other_world.events_list(RUN_ID)).data == before
    assert not (other_world.data_dir / "hooks" / "hook_1.json").exists()
    if successor_run_id is None:
        with pytest.raises(w.HookNotFoundError):
            await other_world.hooks_get_by_token(TOKEN)
        await other_world.events_create(
            RUN_ID, w.HookCreatedEventData(token=TOKEN).into_event("hook_successor")
        )
    assert (await other_world.hooks_get_by_token(TOKEN)).hook_id == "hook_successor"
