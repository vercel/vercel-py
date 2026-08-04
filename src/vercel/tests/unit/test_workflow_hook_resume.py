"""Resuming a hook from outside the workflow.

``BaseHook.resume()`` dehydrates the hook instance into the ``hook_received``
event, and the orchestrator hydrates it back into the hook class when the run
replays. The two halves are only correct together, so both are exercised here:
a payload is written through the real ``LocalWorld`` and then reconstructed the
way ``resume()`` in the runtime does it.
"""

from __future__ import annotations

import dataclasses
import decimal
from datetime import datetime, timezone

import pydantic
import pytest

from vercel._internal.workflow import runtime, serialization as ser, world as w
from vercel._internal.workflow.worlds.local import LocalWorld
from vercel.workflow import BaseHook

TOKEN = "tok-abc"


class Approval(BaseHook, pydantic.BaseModel):
    approved: bool
    reviewer: str
    at: datetime


@dataclasses.dataclass
class Signoff(BaseHook):
    approved: bool
    reviewer: str


class Refund(BaseHook, pydantic.BaseModel):
    amount: decimal.Decimal


class _Book:
    """A class no one has registered for the wire."""


class Ledger(BaseHook, pydantic.BaseModel):
    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    book: _Book


class _RecordingLocalWorld(LocalWorld):
    """Real LocalWorld for storage; only the outbound (networked) queue is stubbed."""

    def __init__(self, data_dir) -> None:
        super().__init__()
        self.data_dir = data_dir
        self.queued: list[tuple[str, w.QueuePayload]] = []

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs) -> str:
        self.queued.append((queue_name, message))
        return "msg_test"


async def _run_with_hook(world: _RecordingLocalWorld) -> str:
    result = await world.events_create(
        None,
        w.RunCreatedEventData(
            deploymentId="dpl_1", workflowName="test-wf", input=ser.dehydrate([])
        ).into_event(),
    )
    assert result.run is not None
    run_id = result.run.run_id
    await world.events_create(run_id, w.RunStartedEvent())
    await world.events_create(run_id, w.HookCreatedEventData(token=TOKEN).into_event("hook_0"))
    return run_id


def _received_payload(events: list[w.Event]) -> bytes:
    (event,) = [e for e in events if isinstance(e, w.HookReceivedEvent)]
    return event.event_data.payload


async def test_pydantic_hook_round_trips_through_resume(tmp_path, monkeypatch) -> None:
    world = _RecordingLocalWorld(tmp_path)
    monkeypatch.setattr(w, "the_world", world)
    run_id = await _run_with_hook(world)
    at = datetime(2026, 7, 30, 17, 6, 33, tzinfo=timezone.utc)

    await Approval(approved=True, reviewer="ada", at=at).resume(TOKEN)

    payload = _received_payload((await world.events_list(run_id)).data)
    # `mode="python"`: the datetime crosses as a devalue `Date`, not a string.
    assert ser.hydrate(payload, what="the payload") == {
        "approved": True,
        "reviewer": "ada",
        "at": at,
    }
    # ...and the orchestrator rebuilds the model the workflow awaits.
    hook = runtime.Hook(correlation_id="hook_0", token=TOKEN, hook_cls=Approval)
    hook.futures.append(future := _future())
    hook.set_result(ser.hydrate(payload, what="the payload"))
    assert future.result() == Approval(approved=True, reviewer="ada", at=at)


async def test_dataclass_hook_round_trips_through_resume(tmp_path, monkeypatch) -> None:
    world = _RecordingLocalWorld(tmp_path)
    monkeypatch.setattr(w, "the_world", world)
    run_id = await _run_with_hook(world)

    await Signoff(approved=False, reviewer="grace").resume(TOKEN)

    payload = _received_payload((await world.events_list(run_id)).data)
    hook = runtime.Hook(correlation_id="hook_0", token=TOKEN, hook_cls=Signoff)
    hook.futures.append(future := _future())
    hook.set_result(ser.hydrate(payload, what="the payload"))
    assert future.result() == Signoff(approved=False, reviewer="grace")


async def test_json_mode_is_the_way_back_to_json_shaped_values(tmp_path, monkeypatch) -> None:
    # The escape hatch for a field devalue has no wire type for: pydantic
    # flattens it first, exactly as `model_dump_json` used to.
    world = _RecordingLocalWorld(tmp_path)
    monkeypatch.setattr(w, "the_world", world)
    run_id = await _run_with_hook(world)
    at = datetime(2026, 7, 30, 17, 6, 33, tzinfo=timezone.utc)

    await Approval(approved=True, reviewer="ada", at=at).resume(TOKEN, mode="json")

    payload = _received_payload((await world.events_list(run_id)).data)
    assert ser.hydrate(payload, what="the payload")["at"] == "2026-07-30T17:06:33Z"


async def test_a_registered_stdlib_field_survives_python_mode(tmp_path, monkeypatch) -> None:
    # `Decimal` survives `model_dump(mode="python")` as a `Decimal`, which
    # devalue only carries because of the built-in `Instance` registration.
    world = _RecordingLocalWorld(tmp_path)
    monkeypatch.setattr(w, "the_world", world)
    run_id = await _run_with_hook(world)

    await Refund(amount=decimal.Decimal("1.50")).resume(TOKEN)

    payload = _received_payload((await world.events_list(run_id)).data)
    assert ser.hydrate(payload, what="the payload") == {"amount": decimal.Decimal("1.50")}


async def test_an_uncarryable_field_names_the_class_to_register(tmp_path, monkeypatch) -> None:
    # A class nothing has registered. The field it sits in is named too, since
    # a hook can have several and the codec-level message alone locates none.
    world = _RecordingLocalWorld(tmp_path)
    monkeypatch.setattr(w, "the_world", world)
    await _run_with_hook(world)

    with pytest.raises(
        ser.SerializationError, match=r"at \.book.*Register _Book with @serializable"
    ):
        await Ledger(book=_Book()).resume(TOKEN)


def _future():
    import asyncio

    return asyncio.get_event_loop().create_future()
