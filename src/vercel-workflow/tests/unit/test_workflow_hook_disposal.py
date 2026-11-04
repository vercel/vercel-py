"""LocalWorld hook disposal semantics.

Disposing a hook frees its token and tears down the entity. Re-issuing
hook_disposed (or delivering a payload) once the hook is gone raises
HookNotFoundError -- the same typed error the backend's 404 yields, which the
runtime swallows. A concurrent invocation that loses the dispose-lock race gets
EntityConflictError instead of double-deleting and writing a duplicate event.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pydantic
import pytest

from tests.payloads import PLAIN_ENCODER
from vercel._internal.core.polyfills import UTC
from vercel.workflow import Run, get_hook_by_token, start
from vercel.workflow._internal import core, runtime, world as w
from vercel.workflow._internal.worlds import local as local_mod

RUN_ID = "wrun_test"
TOKEN = "shared-token"
RETENTION_NOW = datetime(2026, 9, 22, tzinfo=UTC)

registry = core.Workflows(as_vercel_job=False)


class Claim(core.BaseHook, pydantic.BaseModel):
    pass


@registry.workflow
async def retained_claim(mode: str = "plain", retention: str | None = "1d") -> str:
    hook = Claim.wait(token=TOKEN, experimental_min_retention=retention)
    assert await hook.get_conflict() is None
    if mode.startswith("scoped"):
        async with hook:
            await hook
            if mode == "scoped_failure":
                raise RuntimeError("failed inside scope")
    else:
        await hook
        if mode == "dispose":
            hook.dispose()
    return "done"


class RecordingLocalWorld(local_mod.LocalWorld):
    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
        return "msg_test"


def _world(tmp_path, monkeypatch) -> local_mod.LocalWorld:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    return RecordingLocalWorld()


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
@pytest.mark.parametrize("reload_world", [False, True])
async def test_disposed_hook_cannot_be_recreated(
    tmp_path, monkeypatch, successor_run_id, reload_world
) -> None:
    world = _world(tmp_path, monkeypatch)
    created = w.HookCreatedEventData(token=TOKEN).into_event("hook_1")
    await world.events_create(RUN_ID, created)
    await world.events_create(RUN_ID, w.HookDisposedEvent(correlation_id="hook_1"))
    if successor_run_id is not None:
        await world.events_create(
            successor_run_id, w.HookCreatedEventData(token=TOKEN).into_event("hook_successor")
        )
    before = (await world.events_list(RUN_ID)).data

    if reload_world:
        world = _world(tmp_path, monkeypatch)
    with pytest.raises(w.EntityConflictError):
        await world.events_create(RUN_ID, created)

    assert (await world.events_list(RUN_ID)).data == before
    assert not (world.data_dir / "hooks" / "hook_1.json").exists()
    if successor_run_id is None:
        with pytest.raises(w.HookNotFoundError):
            await world.hooks_get_by_token(TOKEN)
        await world.events_create(
            RUN_ID, w.HookCreatedEventData(token=TOKEN).into_event("hook_successor")
        )
    assert (await world.hooks_get_by_token(TOKEN)).hook_id == "hook_successor"


@pytest.fixture
def retained_world(tmp_path, monkeypatch):
    monkeypatch.setattr(local_mod, "js_now", lambda: RETENTION_NOW)
    world = _world(tmp_path, monkeypatch)
    w.set_world(world)
    yield world
    w.set_world(None)


async def _invoke_retained_claim(run: Run[str]) -> None:
    await runtime.workflow_handler(
        w.WorkflowInvokePayload(run_id=run.run_id).model_dump(),
        attempt=1,
        queue_name=w.get_queue_name(retained_claim.workflow_id),
        message_id="msg_test",
        registry=registry,
    )


async def _complete_retained_claim(run: Run[str]) -> None:
    await Claim().resume(TOKEN)
    await _invoke_retained_claim(run)


@pytest.mark.parametrize("mode", ["dispose", "scoped", "scoped_failure"])
async def test_disposal_at_run_end_overrides_retention(retained_world, mode) -> None:
    run = await start(retained_claim, mode)
    await _invoke_retained_claim(run)
    # Suspension inside async with must not count as leaving the scope.
    assert (await get_hook_by_token(TOKEN)).run_id == run.run_id
    await _complete_retained_claim(run)
    with pytest.raises(w.HookNotFoundError):
        await get_hook_by_token(TOKEN)
    kinds = [e.event_type for e in (await retained_world.events_list(run.run_id)).data]
    terminal = "run_failed" if mode == "scoped_failure" else "run_completed"
    assert kinds.index("hook_disposed") < kinds.index(terminal)
    replacement = await start(retained_claim)
    await _invoke_retained_claim(replacement)
    assert (await get_hook_by_token(TOKEN)).run_id == replacement.run_id


async def test_disposal_of_a_retained_terminal_hook_releases_token(retained_world) -> None:
    run = await start(retained_claim)
    await _invoke_retained_claim(run)
    hook = await get_hook_by_token(TOKEN)
    await _complete_retained_claim(run)
    await retained_world.events_create(run.run_id, w.HookDisposedEvent(correlation_id=hook.hook_id))
    with pytest.raises(w.HookNotFoundError):
        await get_hook_by_token(TOKEN)
    replacement = await start(retained_claim)
    await _invoke_retained_claim(replacement)
    assert (await get_hook_by_token(TOKEN)).run_id == replacement.run_id


async def test_missing_retention_preserves_default_cleanup(retained_world) -> None:
    run = await start(retained_claim, retention=None)
    await _invoke_retained_claim(run)
    event = next(
        e
        for e in (await retained_world.events_list(run.run_id)).data
        if isinstance(e, w.HookCreatedEvent)
    )
    assert "tokenRetentionUntil" not in event.event_data.model_dump()
    await _complete_retained_claim(run)
    with pytest.raises(w.HookNotFoundError):
        await get_hook_by_token(TOKEN)
