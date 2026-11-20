"""Full replay reads retain bounded-page compatibility and incremental cursors."""

import json
from datetime import UTC, datetime, timedelta

import pydantic
import pytest

from tests.payloads import PLAIN_ENCODER
from vercel.workflow._internal import runtime, world as w
from vercel.workflow._internal.worlds.local import LocalWorld


@pytest.fixture
def world(tmp_path, monkeypatch):
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    instance = LocalWorld()
    w.set_world(instance)
    yield instance
    w.set_world(None)


async def _populate(world):
    created = await world.events_create(
        None,
        w.RunCreatedEvent(
            event_data=w.RunCreatedEventData(
                deployment_id="dpl_test", workflow_name="test", input=PLAIN_ENCODER.encode([])
            )
        ),
    )
    run_id = created.run.run_id
    await world.events_create(run_id, w.RunStartedEvent())
    for i in range(3):
        await world.events_create(run_id, w.WaitCompletedEvent(correlation_id=f"wait_{i}"))
    return run_id


# @lat: [[vercel-workflow#Worlds#Event pagination]]
@pytest.mark.parametrize("order", ["asc", "desc"])
async def test_pages_join_to_the_full_log_and_retain_final_cursor(world, order):
    run_id = await _populate(world)
    full = await world.events_list(run_id, pagination=w.PaginationOptions(sort_order=order))
    found = []
    cursor = None
    while True:
        page = await world.events_list(
            run_id, pagination=w.PaginationOptions(limit=2, cursor=cursor, sort_order=order)
        )
        found.extend(page.data)
        cursor = page.cursor
        if not page.has_more:
            break
    assert found == full.data
    assert cursor == full.cursor
    assert cursor is not None
    empty = await world.events_list(
        run_id, pagination=w.PaginationOptions(cursor=cursor, sort_order=order)
    )
    assert empty.data == []
    assert not empty.has_more


async def test_full_read_continues_at_the_configured_ceiling(world, monkeypatch):
    run_id = await _populate(world)
    monkeypatch.setenv("WORKFLOW_MAX_EVENTS", "2")
    first = await world.events_list(run_id)
    assert len(first.data) == 2 and first.has_more
    loaded = await runtime.get_all_workflow_run_events(run_id)
    assert len(loaded.events) == 5
    assert loaded.cursor is not None
    await world.events_create(run_id, w.WaitCompletedEvent(correlation_id="new"))
    delta = await runtime.get_all_workflow_run_events(run_id, after_cursor=loaded.cursor)
    assert [e.correlation_id for e in delta.events] == ["new"]
    explicit = await world.events_list(run_id, pagination=w.PaginationOptions(limit=10))
    assert len(explicit.data) == 6 and not explicit.has_more


async def test_slot_ids_order_events_even_when_seal_time_is_later(world):
    now = datetime(2026, 1, 1, tzinfo=UTC)
    for slot in [3, 1, 2]:
        event = w.EventAdaptor.from_wire(
            {
                "eventType": "noop",
                "specVersion": 7,
                "runId": "wrun_slots",
                "eventId": f"evnt_{slot:026d}",
                "createdAt": now + timedelta(seconds=4 - slot),
            }
        )
        path = world.data_dir / "events" / "wrun_slots" / f"wrun_slots-evnt_{slot:026d}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                event.model_dump()
                | {
                    "runId": "wrun_slots",
                    "eventId": f"evnt_{slot:026d}",
                    "createdAt": (now + timedelta(seconds=4 - slot)).isoformat(),
                }
            )
        )
    first = await world.events_list("wrun_slots", pagination=w.PaginationOptions(limit=2))
    assert [e.server_props.event_id for e in first.data] == [f"evnt_{i:026d}" for i in [1, 2]]
    suffix = await world.events_list(
        "wrun_slots", pagination=w.PaginationOptions(cursor=first.cursor)
    )
    assert [e.server_props.event_id for e in suffix.data] == [f"evnt_{3:026d}"]


@pytest.mark.parametrize("raw", ["", "0", "-1", "1.5", "garbage"])
def test_invalid_event_ceiling_uses_default(monkeypatch, raw):
    monkeypatch.setenv("WORKFLOW_MAX_EVENTS", raw)
    assert w.get_max_events_per_run() == 25_000


@pytest.mark.parametrize("limit", [0, -1])
def test_explicit_limit_must_be_positive(limit):
    with pytest.raises(pydantic.ValidationError):
        w.PaginationOptions(limit=limit)


@pytest.mark.parametrize("cursors", [[None], ["same", "same"], ["a", "b", "a"]])
async def test_loader_rejects_missing_and_cyclic_continuations(world, monkeypatch, cursors):
    calls = 0

    async def pages(*args, **kwargs):
        nonlocal calls
        cursor = cursors[calls]
        calls += 1
        return w.PaginatedResult(data=[], cursor=cursor, has_more=True)

    monkeypatch.setattr(world, "events_list", pages)
    with pytest.raises(w.WorkflowWorldError, match="cursor"):
        await runtime.get_all_workflow_run_events("wrun_test")
    assert calls == len(cursors)
