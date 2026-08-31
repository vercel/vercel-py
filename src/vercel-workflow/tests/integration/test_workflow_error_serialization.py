"""Step errors crossing the event log back into a workflow."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any

import pytest

from vercel.queue.testing import clear_subscriptions
from vercel.workflow import FatalError
from vercel.workflow._internal import core, runtime, world as w
from vercel.workflow._internal.worlds import local as local_mod

RUN_DEADLINE_SECONDS = 30

registry = core.Workflows(as_vercel_job=False)


@registry.step
async def fail_with_cause() -> None:
    try:
        raise TypeError("invalid charge")
    except TypeError as cause:
        raise FatalError("payment failed") from cause


@registry.workflow
async def catch_step_error() -> dict[str, Any]:
    try:
        await fail_with_cause()
    except Exception as error:
        cause = error.__cause__
        return {
            "type": type(error).__name__,
            "message": str(error),
            "fatal": isinstance(error, FatalError),
            "causeType": type(cause).__name__ if cause is not None else None,
            "causeMessage": str(cause) if cause is not None else None,
        }
    return {"type": None}


@pytest.fixture(autouse=True)
def _reset_world():
    clear_subscriptions()
    yield
    w.set_world(None)
    clear_subscriptions()


@contextlib.asynccontextmanager
async def running_world(tmp_path, monkeypatch) -> AsyncIterator[local_mod.LocalWorld]:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = local_mod.LocalWorld()
    w.set_world(world)
    runtime.workflow_entrypoint(registry)
    await world._get_queue_client()
    try:
        yield world
    finally:
        await world.aclose()


async def test_workflow_catches_the_error_raised_by_a_step(tmp_path, monkeypatch) -> None:
    async with running_world(tmp_path, monkeypatch) as world:
        run = await runtime.start(catch_step_error)
        result = await asyncio.wait_for(run.return_value(), RUN_DEADLINE_SECONDS)

        assert result == {
            "type": "FatalError",
            "message": "payment failed",
            "fatal": True,
            "causeType": "TypeError",
            "causeMessage": "invalid charge",
        }

        events = (await world.events_list(run.run_id)).data
        assert sum(event.event_type == "step_failed" for event in events) == 1
