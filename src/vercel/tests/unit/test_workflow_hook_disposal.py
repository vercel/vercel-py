"""LocalWorld hook disposal semantics.

Disposing a hook frees its token and tears down the entity. Re-issuing
hook_disposed (or delivering a payload) once the hook is gone raises
HookNotFoundError -- the same typed error the backend's 404 yields, which the
runtime swallows. A concurrent invocation that loses the dispose-lock race gets
EntityConflictError instead of double-deleting and writing a duplicate event.
"""

from __future__ import annotations

from vercel._internal.workflow import world as w
from vercel._internal.workflow.worlds import local as local_mod

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
    await world.events_create(RUN_ID, w.HookDisposedEvent(correlationId="hook_1"))

    try:
        await world.events_create(RUN_ID, w.HookDisposedEvent(correlationId="hook_1"))
    except w.HookNotFoundError:
        pass
    else:
        raise AssertionError("re-disposing a disposed hook should raise HookNotFoundError")


async def test_hook_received_for_missing_hook_raises_hook_not_found(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    data = w.HookReceivedEventData(payload=[b"json{}"])

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
        await world.events_create(RUN_ID, w.HookDisposedEvent(correlationId="hook_1"))
    except w.EntityConflictError:
        pass
    else:
        raise AssertionError("losing the dispose lock should raise EntityConflictError")
