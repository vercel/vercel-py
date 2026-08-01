"""Workflows and steps are called with keyword arguments only.

A call is recorded as the single object `@workflow/core` records for a
one-argument call, so there is nowhere for a positional argument to go. That
is enforced where it is cheapest to notice -- at registration, so a codebase
reports every offending definition at import rather than one per run.
"""

from __future__ import annotations

import pytest

from vercel._internal.workflow import core, runtime, serialization as ser, world as w
from vercel._internal.workflow.worlds.local import LocalWorld


def _registry() -> core.Workflows:
    return core.Workflows(as_vercel_job=False)


def test_a_positional_step_parameter_is_rejected_at_registration() -> None:
    registry = _registry()

    with pytest.raises(TypeError, match=r"step .*charge takes positional parameter\(s\) amount"):

        @registry.step
        async def charge(amount: int) -> int:
            return amount


def test_a_positional_workflow_parameter_is_rejected_at_registration() -> None:
    registry = _registry()

    with pytest.raises(TypeError, match=r"workflow .*checkout takes positional"):

        @registry.workflow
        async def checkout(amount: int) -> int:
            return amount


def test_the_error_shows_how_to_fix_the_signature() -> None:
    registry = _registry()

    with pytest.raises(TypeError, match=r"async def charge\(\*, amount: \.\.\.\)"):

        @registry.step
        async def charge(amount: int, tier: str) -> int:
            return amount


def test_var_positional_is_rejected_too() -> None:
    registry = _registry()

    with pytest.raises(TypeError, match="args"):

        @registry.step
        async def charge(*args: int) -> int:
            return len(args)


def test_keyword_only_and_var_keyword_are_accepted() -> None:
    registry = _registry()

    @registry.step
    async def charge(*, amount: int) -> int:
        return amount

    @registry.step
    async def audit(**fields: str) -> int:
        return len(fields)

    @registry.workflow
    async def nothing() -> None: ...

    assert charge.name.endswith("charge")


async def test_an_untyped_positional_call_is_refused_at_the_call_site() -> None:
    # A type checker rejects this already -- `P.args` is empty for a
    # keyword-only function -- so this is the backstop for untyped callers.
    registry = _registry()

    @registry.step
    async def charge(*, amount: int) -> int:
        return amount

    with pytest.raises(TypeError, match="keyword arguments only"):
        await charge(21)  # type: ignore[misc]


async def test_start_refuses_an_untyped_positional_call(monkeypatch) -> None:
    registry = _registry()

    @registry.workflow
    async def checkout(*, amount: int) -> int:
        return amount

    with pytest.raises(TypeError, match="keyword arguments only"):
        await runtime.start(checkout, 21)  # type: ignore[misc]


async def test_start_records_the_object_argument_ts_would_have(tmp_path, monkeypatch) -> None:
    """The whole point: the bytes `start(wf, {amount: 21})` writes in TS."""
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    registry = _registry()

    @registry.workflow
    async def checkout(*, amount: int) -> int:
        return amount

    queued: list[str] = []

    class _World(LocalWorld):
        async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs) -> str:
            queued.append(queue_name)
            return "msg_1"

    world = _World()
    w.set_world(world)
    try:
        run = await runtime.start(checkout, amount=21)
        stored = await world.runs_get(run.run_id)
    finally:
        w.set_world(None)

    assert stored.input == ser.dehydrate([{"amount": 21}])
    assert ser.hydrate(stored.input, what="the input") == [{"amount": 21}]
