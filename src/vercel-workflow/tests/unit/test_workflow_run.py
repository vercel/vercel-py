"""Public Run handle contracts."""

from __future__ import annotations

import decimal
from typing import Any
from unittest.mock import AsyncMock, Mock

import pydantic
import pytest

from tests.payloads import PLAIN_ENCODER
from vercel.workflow import Run, start
from vercel.workflow._internal import core, world as w
from vercel.workflow._internal.worlds.local import LocalWorld


def _registry() -> core.Workflows:
    return core.Workflows(as_vercel_job=False)


class Order(pydantic.BaseModel):
    sku: str
    quantity: int
    total: decimal.Decimal


async def test_return_value_validates_against_the_workflow_return(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    registry = _registry()

    @registry.workflow
    async def checkout() -> Order:
        return Order(sku="abc", quantity=1, total=decimal.Decimal("1.00"))

    class _World(LocalWorld):
        async def queue(self, queue_name: str, message: w.QueuePayload, **kw: Any) -> str:
            return "msg_1"

    world = _World()
    w.set_world(world)
    try:
        run = await start(checkout)
        await world.events_create(
            run.run_id,
            w.RunCompletedEventData(
                output=PLAIN_ENCODER.encode(
                    {"sku": "abc", "quantity": 1, "total": decimal.Decimal("1.00")}
                )
            ).into_event(),
        )
        result = await run.return_value()
    finally:
        w.set_world(None)

    assert result == Order(sku="abc", quantity=1, total=decimal.Decimal("1.00"))


async def test_a_run_built_without_a_workflow_reads_the_raw_output(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """`Run(run_id)` has no signature to validate against, and says so by not.

    A run id picked up from a webhook is the case; `start()` is the one that
    knows.
    """
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    registry = _registry()

    @registry.workflow
    async def checkout() -> Order:
        return Order(sku="abc", quantity=1, total=decimal.Decimal("1.00"))

    class _World(LocalWorld):
        async def queue(self, queue_name: str, message: w.QueuePayload, **kw: Any) -> str:
            return "msg_1"

    world = _World()
    w.set_world(world)
    try:
        started = await start(checkout)
        await world.events_create(
            started.run_id,
            w.RunCompletedEventData(
                output=PLAIN_ENCODER.encode(
                    {"sku": "abc", "quantity": 1, "total": decimal.Decimal("1.00")}
                )
            ).into_event(),
        )
        result = await Run[Any](started.run_id).return_value()
    finally:
        w.set_world(None)

    assert result == {"sku": "abc", "quantity": 1, "total": decimal.Decimal("1.00")}


@pytest.mark.parametrize("reason", [None, "", "superseded by newer run", "x" * 512, "😀" * 256])
async def test_terminate_writes_run_cancelled_event(monkeypatch, reason) -> None:
    world = Mock(spec=w.World)
    monkeypatch.setattr(w, "get_world", lambda: world)
    run = Run[None]("wrun_test")
    # The handle keeps the World it was constructed with.
    monkeypatch.setattr(w, "get_world", lambda: Mock(spec=w.World))

    await run.terminate(reason=reason)

    world.events_create.assert_awaited_once()
    run_id, event = world.events_create.call_args.args
    expected = {"eventType": "run_cancelled", "specVersion": w.SPEC_VERSION_CURRENT}
    if reason is not None:
        expected["eventData"] = {"cancelReason": reason}
    assert run_id == "wrun_test"
    assert event.model_dump() == expected
    assert w.EventAdaptor.from_wire(expected).model_dump() == expected
    assert event.payloads() == ()  # The reason is plaintext metadata.
    assert len(world.mock_calls) == 1


@pytest.mark.parametrize("reason", ["x" * 513, "😀" * 257, "😀" * 256 + "x"])
async def test_terminate_rejects_oversized_reason(monkeypatch, reason) -> None:
    world = Mock(spec=w.World)
    monkeypatch.setattr(w, "get_world", lambda: world)
    with pytest.raises(pydantic.ValidationError, match="512 UTF-16 code units"):
        await Run("wrun_test").terminate(reason=reason)
    world.events_create.assert_not_called()
    with pytest.raises(pydantic.ValidationError, match="512 UTF-16 code units"):
        w.EventAdaptor.from_wire(
            {"eventType": "run_cancelled", "eventData": {"cancelReason": reason}}
        )


async def test_terminate_propagates_world_error(monkeypatch) -> None:
    world = Mock(spec=w.World)
    error = w.EntityConflictError("Run already completed")
    world.events_create = AsyncMock(side_effect=error)
    monkeypatch.setattr(w, "get_world", lambda: world)
    with pytest.raises(w.EntityConflictError) as caught:
        await Run("wrun_test").terminate()
    assert caught.value is error


@pytest.mark.parametrize("running", [False, True])
async def test_terminate_persists_status_reason_and_removes_hooks(tmp_path, monkeypatch, running):
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = LocalWorld()
    monkeypatch.setattr(w, "get_world", lambda: world)
    created = await world.events_create(
        None,
        w.RunCreatedEventData(
            deployment_id="dpl_test",
            workflow_name="workflow//tests.terminate",
            input=PLAIN_ENCODER.encode([]),
        ).into_event(),
    )
    assert created.run is not None
    run = Run[None](created.run.run_id)
    if running:
        await world.events_create(run.run_id, w.RunStartedEvent())
        await world.events_create(
            run.run_id, w.HookCreatedEventData(token="cancel-hook").into_event("hook_test")
        )

    await run.terminate(reason="operator terminated")

    assert await run.status() == "cancelled"
    stored = await world.runs_get(run.run_id)
    assert stored.completed_at is not None
    events = await world.events_list(run.run_id)
    cancelled = [event for event in events.data if isinstance(event, w.RunCancelledEvent)]
    assert len(cancelled) == 1
    assert cancelled[0].event_data is not None
    assert cancelled[0].event_data.cancel_reason == "operator terminated"
    if running:
        with pytest.raises(w.HookNotFoundError):
            await world.hooks_get_by_token("cancel-hook")
    with pytest.raises(RuntimeError, match="workflow cancelled"):
        await run.return_value()

    await run.terminate()
    assert await run.status() == "cancelled"
