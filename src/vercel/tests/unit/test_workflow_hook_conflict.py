"""LocalWorld hook-token conflict semantics.

A hook's token is claimed exclusively the first time its ``hook_created`` event
is issued. A second issue can mean two different things, and the world must tell
them apart:

- the *same* hook (same correlation id) re-claiming its token -- a replay
  re-issue or an overlapping/retried invocation of the same run. This is
  idempotent, so the world raises ``EntityConflictError`` (which the runtime
  swallows), mirroring the backend's hookId-keyed idempotency.
- a *different* hook claiming a token already in use -- a genuine cross-workflow
  conflict, surfaced as a ``HookConflictEvent``.
"""

from __future__ import annotations

from vercel._internal.workflow import world as w
from vercel._internal.workflow.worlds import local as local_mod

RUN_ID = "wrun_test"
TOKEN = "shared-token"


def _world(tmp_path, monkeypatch) -> local_mod.LocalWorld:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    return local_mod.LocalWorld()


async def test_same_hook_reclaim_raises_entity_conflict(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    event = w.HookCreatedEventData(token=TOKEN).into_event("hook_1")

    await world.events_create(RUN_ID, event)

    try:
        await world.events_create(RUN_ID, event)
    except w.EntityConflictError:
        pass
    else:
        raise AssertionError("re-claiming the same hook's token should raise EntityConflictError")


async def test_different_hook_same_token_conflicts(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)

    await world.events_create(RUN_ID, w.HookCreatedEventData(token=TOKEN).into_event("hook_1"))
    result = await world.events_create(
        RUN_ID, w.HookCreatedEventData(token=TOKEN).into_event("hook_2")
    )

    assert isinstance(result.event, w.HookConflictEvent)
    assert result.event.event_data.token == TOKEN
