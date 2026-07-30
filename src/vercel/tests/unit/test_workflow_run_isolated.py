"""Isolated-loop execution for workflow bodies.

``run_workflow`` runs the (async but IO-free) workflow body in a fresh event
loop so it can inspect the loop's ``_ready`` queue. ``_run_isolated`` hides the
caller's running loop *and* its current task before doing so, then restores both
afterward -- restoring the current task is what makes this work on Python 3.14,
which errors if a new loop is entered while the outer task is still current.
"""

import asyncio

import pytest

from vercel._internal.workflow import runtime


async def test_run_isolated_returns_result() -> None:
    async def body() -> int:
        return 41 + 1

    assert runtime._run_isolated(body()) == 42


async def test_run_isolated_runs_in_fresh_loop_and_restores_caller() -> None:
    outer_loop = asyncio.get_running_loop()
    outer_task = asyncio.current_task()
    inner_loop: dict[str, asyncio.AbstractEventLoop] = {}

    async def body() -> None:
        inner_loop["loop"] = asyncio.get_running_loop()

    runtime._run_isolated(body())

    assert inner_loop["loop"] is not outer_loop
    assert asyncio.get_running_loop() is outer_loop
    assert asyncio.current_task() is outer_task
    # The caller's loop is still usable after the isolated run.
    await asyncio.sleep(0)


async def test_run_isolated_propagates_exceptions_and_restores_caller() -> None:
    outer_loop = asyncio.get_running_loop()
    outer_task = asyncio.current_task()

    class Boom(Exception):
        pass

    async def body() -> None:
        raise Boom

    with pytest.raises(Boom):
        runtime._run_isolated(body())

    assert asyncio.get_running_loop() is outer_loop
    assert asyncio.current_task() is outer_task
    await asyncio.sleep(0)
