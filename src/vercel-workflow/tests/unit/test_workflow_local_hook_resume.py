"""What ``LocalWorld`` does with the resume on a ``hook_received`` write.

Two things. It records the resume id on the row, so a later delivery of the same
resume can see that the event is already there. And it enforces, by itself, that
two writes of one resume end up as one event -- the Vercel world hands that to the
server, but there is no server here.

That second part is the reason any of this exists. A resume can be written twice
on purpose: ``resumeHook()`` writes the event, and whoever consumes its queue
message writes the same event when the first write has not shown up yet. Two
events would hand the payload to the workflow body twice, which is worse than the
stall the second write exists to prevent.

The claim file is byte-shaped like `@workflow/world-local`'s, because the two
implementations share a data directory and have to converge on each other's.
"""

from __future__ import annotations

import hashlib
import json

import pytest

from tests.payloads import PLAIN_ENCODER
from vercel.workflow._internal import world as w
from vercel.workflow._internal.worlds.local import LocalWorld

RUN_ID = "wrun_test"
TOKEN = "tok-abc"
HOOK = "hook_1"
RESUME_ID = "01KZZ42P2MR25DYSW1MTMMACA3"
OTHER_RESUME_ID = "01KZZ42P2MR25DYSW1MTMMACA4"
DIGEST = "66c534074063d7f2dd180074e1f622f2190f830094837ab86d056913eb42560a"
OTHER_DIGEST = "0" * 64


async def _world(tmp_path, monkeypatch) -> LocalWorld:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = LocalWorld()
    await world.events_create(RUN_ID, w.HookCreatedEventData(token=TOKEN).into_event(HOOK))
    return world


def _received(
    payload: object = None,
    *,
    hook: str = HOOK,
    resume_id: str | None = RESUME_ID,
    digest: str = DIGEST,
) -> w.HookReceivedEvent:
    """A `hook_received`, by default carrying the queue input a lazy resume
    arrives with. `resume_id=None` is the plain sequential write, which takes
    none of this file's paths."""
    dehydrated = PLAIN_ENCODER.encode({"n": 1} if payload is None else payload)
    event = w.HookReceivedEventData(payload=dehydrated, token=TOKEN).into_event(hook)
    if resume_id is not None:
        event._queue_input = w.HookResumeInput(
            resume_id=resume_id,
            hook_id=hook,
            token=TOKEN,
            payload=dehydrated,
            payload_digest=digest,
        )
    return event


async def _hook_received_events(world: LocalWorld) -> list[w.Event]:
    page = await world.events_list(RUN_ID)
    return [e for e in page.data if e.event_type == "hook_received"]


def _claim_path(world: LocalWorld, resume_id: str = RESUME_ID):
    key = hashlib.sha256(f"{RUN_ID}\x00{resume_id}".encode()).hexdigest()
    return world.data_dir / "hooks" / "resumes" / f"{key}.json"


def _write_claim(world: LocalWorld, **overrides) -> None:
    """A claim a *other* writer left behind, without its event."""
    claim = {
        "runId": RUN_ID,
        "resumeId": RESUME_ID,
        "hookId": HOOK,
        "eventId": "evnt_00000000000000000000000009",
        "payloadDigest": DIGEST,
    } | overrides
    path = _claim_path(world, claim["resumeId"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(claim), encoding="utf-8")


# ── convergence ────────────────────────────────────────────────────────────


async def test_the_second_writer_of_one_resume_appends_nothing(tmp_path, monkeypatch) -> None:
    """The property the whole claim exists for. Both calls return the same
    event, and the log holds one."""
    world = await _world(tmp_path, monkeypatch)
    first = await world.events_create(RUN_ID, _received())
    second = await world.events_create(RUN_ID, _received())

    assert first.event is not None and second.event is not None
    assert first.event.server_props is not None and second.event.server_props is not None
    assert first.event.server_props.event_id == second.event.server_props.event_id
    assert len(await _hook_received_events(world)) == 1


async def test_distinct_resumes_on_one_hook_each_record_an_event(tmp_path, monkeypatch) -> None:
    """Keyed on `(runId, resumeId)` and not on the hook: an async-iterable hook
    takes many payloads, and collapsing them would drop all but the first."""
    world = await _world(tmp_path, monkeypatch)

    await world.events_create(RUN_ID, _received(1))
    await world.events_create(RUN_ID, _received(2, resume_id=OTHER_RESUME_ID))

    assert len(await _hook_received_events(world)) == 2


async def test_no_claim_is_written_without_a_resume(tmp_path, monkeypatch) -> None:
    """An ordinary sequential `resume_hook()` has no second writer to converge
    with, and must leave the store exactly as it found it."""
    world = await _world(tmp_path, monkeypatch)

    await world.events_create(RUN_ID, _received(resume_id=None))

    assert not (world.data_dir / "hooks" / "resumes").exists()
    assert len(await _hook_received_events(world)) == 1


# ── the two conflicts ──────────────────────────────────────────────────────


async def test_a_resume_id_reused_for_another_hook_is_a_conflict(tmp_path, monkeypatch) -> None:
    """Adopting the first hook's event for a second would attribute the resume to
    the wrong hook. The two writers of one resume always agree on the hook."""
    world = await _world(tmp_path, monkeypatch)
    await world.events_create(RUN_ID, w.HookCreatedEventData(token="other").into_event("hook_2"))
    _write_claim(world, hookId="hook_2")

    with pytest.raises(w.HookResumeConflictError, match="different hook"):
        await world.events_create(RUN_ID, _received())


async def test_a_resume_id_reused_for_another_payload_is_a_conflict(tmp_path, monkeypatch) -> None:
    """A caller bug, not a benign redelivery -- the same rejection the server's
    constraint gives, which is what keys the digest into the claim."""
    world = await _world(tmp_path, monkeypatch)
    _write_claim(world, payloadDigest=OTHER_DIGEST)

    with pytest.raises(w.HookResumeConflictError, match="different payload"):
        await world.events_create(RUN_ID, _received())


# ── a claim whose event is not where it says ───────────────────────────────


async def test_a_claim_whose_event_has_not_landed_is_transient(tmp_path, monkeypatch) -> None:
    """The other writer claimed the resume and has not appended yet. There is
    nothing to converge on *yet*, and this is where we part company with
    `world-local`: it takes over the claimed position, which is safe only while
    both writers share the process its per-hook lock serializes. Ours is another
    process, so taking over appends beside that writer instead of colliding with
    it -- its ids are slot positions, so it bumps rather than losing an exclusive
    create -- and the body gets the payload twice.

    A conflict costs one redelivery, by when its append has landed.
    """
    world = await _world(tmp_path, monkeypatch)
    _write_claim(world)

    with pytest.raises(w.EntityConflictError, match="not observable yet") as excinfo:
        await world.events_create(RUN_ID, _received())

    # Pointedly not the permanent kind: the consumer retries this one, and by the
    # next delivery the other writer's append has landed.
    assert not isinstance(excinfo.value, w.HookResumeConflictError)
    assert await _hook_received_events(world) == []


async def test_a_claim_is_converged_on_by_the_resume_id_not_the_position(
    tmp_path, monkeypatch
) -> None:
    """The claim's eventId is where its writer *meant* to publish. That writer's
    ids are slot positions, so it can bump off its own claim and leave something
    unrelated there; the authority is the `resumeId` persisted on the event."""
    world = await _world(tmp_path, monkeypatch)
    landed = await world.events_create(RUN_ID, _received())
    assert landed.event is not None and landed.event.server_props is not None

    # Repoint the claim at an unrelated event, the way a bumped writer would.
    occupied = "evnt_00000000000000000000000009"
    (world.data_dir / "events" / f"{RUN_ID}-{occupied}.json").write_text(
        json.dumps(
            {
                "eventType": "run_started",
                "runId": RUN_ID,
                "eventId": occupied,
                "createdAt": "2026-08-14T03:08:48.592Z",
                "specVersion": 5,
            }
        ),
        encoding="utf-8",
    )
    _write_claim(world, event_id=occupied)

    again = await world.events_create(RUN_ID, _received())

    assert again.event is not None and again.event.server_props is not None
    assert again.event.server_props.event_id == landed.event.server_props.event_id
    assert len(await _hook_received_events(world)) == 1


async def test_another_payload_at_the_claimed_position_is_not_adopted(
    tmp_path, monkeypatch
) -> None:
    """The dangerous near-miss: a `hook_received` for the same hook, at the id the
    claim names, from a resume that is not this one. It looks right on everything
    except the resume id. Taking it would silently drop this payload -- exactly
    the failure the claim exists to prevent -- so match on the resume id only.
    """
    world = await _world(tmp_path, monkeypatch)
    # A plain sequential resume of the same hook: right hook, no resume id.
    other = await world.events_create(RUN_ID, _received("unrelated", resume_id=None))
    assert other.event is not None and other.event.server_props is not None
    _write_claim(world, event_id=other.event.server_props.event_id)

    with pytest.raises(w.EntityConflictError, match="not observable yet"):
        await world.events_create(RUN_ID, _received())


async def test_convergence_survives_the_hook_being_disposed(tmp_path, monkeypatch) -> None:
    """A redelivered re-ensure arriving after the body disposed the hook. The
    claim proves the resume was accepted while the hook was alive and its event
    is in the log, so this must return that event rather than the not-found the
    disposal would earn it -- the consumer reads not-found as "nothing left to
    resume" and acks a message that may hold the only copy of the payload."""
    world = await _world(tmp_path, monkeypatch)
    first = await world.events_create(RUN_ID, _received())
    await world.events_create(RUN_ID, w.HookDisposedEvent(correlation_id=HOOK))

    again = await world.events_create(RUN_ID, _received())

    assert first.event is not None and again.event is not None
    assert first.event.server_props is not None and again.event.server_props is not None
    assert again.event.server_props.event_id == first.event.server_props.event_id
    # A resume with no claim still gets the disposal's answer.
    with pytest.raises(w.HookNotFoundError):
        await world.events_create(RUN_ID, _received(resume_id=OTHER_RESUME_ID))


# ── the file, which TypeScript also reads ──────────────────────────────────


async def test_the_claim_is_shaped_the_way_world_local_writes_one(tmp_path, monkeypatch) -> None:
    """Same path derivation and same keys: a claim this SDK writes has to be the
    claim the TypeScript writer of the other half converges on -- the two share
    a data directory."""
    world = await _world(tmp_path, monkeypatch)

    result = await world.events_create(RUN_ID, _received())

    assert result.event is not None and result.event.server_props is not None
    assert json.loads(_claim_path(world).read_text()) == {
        "runId": RUN_ID,
        "resumeId": RESUME_ID,
        "hookId": HOOK,
        "eventId": result.event.server_props.event_id,
        "payloadDigest": DIGEST,
    }


async def test_the_row_records_which_resume_it_came_from(tmp_path, monkeypatch) -> None:
    """This is what a later delivery of the same resume looks for before deciding
    it has an event to write."""
    world = await _world(tmp_path, monkeypatch)

    await world.events_create(RUN_ID, _received())

    (received,) = await _hook_received_events(world)
    assert received.server_props is not None
    assert received.server_props.resume_id == RESUME_ID


async def test_a_row_written_without_a_resume_has_no_id(tmp_path, monkeypatch) -> None:
    """A plain `resume_hook()` passes no resume, and its row has to stay what it
    was, or a later delivery would read a resume into it."""
    world = await _world(tmp_path, monkeypatch)

    await world.events_create(RUN_ID, _received(resume_id=None))

    (received,) = await _hook_received_events(world)
    assert received.server_props is not None
    assert received.server_props.resume_id is None
