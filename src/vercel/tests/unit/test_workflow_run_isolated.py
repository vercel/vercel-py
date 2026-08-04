"""Isolated-loop execution for workflow bodies.

``run_workflow`` runs the (async but IO-free) workflow body in a fresh event
loop. ``_run_isolated`` hides the caller's running loop *and* its current task
before doing so, then restores both afterward -- restoring the current task is
what makes this work on Python 3.14, which errors if a new loop is entered while
the outer task is still current.
"""

import asyncio

import pytest

from vercel._internal.workflow import loop, runtime


def _loop_factory() -> asyncio.AbstractEventLoop:
    return loop.WorkflowLoop()


def test_run_in_loop_cleans_up_tasks() -> None:
    background_cancelled = False

    async def background() -> None:
        nonlocal background_cancelled
        try:
            await asyncio.Event().wait()
        finally:
            background_cancelled = True

    async def body() -> None:
        asyncio.create_task(background())
        await asyncio.sleep(0)

    runtime._run_in_loop(body(), loop_factory=_loop_factory)

    assert background_cancelled


async def test_run_isolated_returns_result() -> None:
    async def body() -> int:
        return 41 + 1

    assert runtime._run_isolated(body(), loop_factory=_loop_factory) == 42


async def test_run_isolated_runs_in_fresh_loop_and_restores_caller() -> None:
    outer_loop = asyncio.get_running_loop()
    outer_task = asyncio.current_task()
    inner_loop: dict[str, asyncio.AbstractEventLoop] = {}

    async def body() -> None:
        inner_loop["loop"] = asyncio.get_running_loop()

    runtime._run_isolated(body(), loop_factory=_loop_factory)

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
        runtime._run_isolated(body(), loop_factory=_loop_factory)

    assert asyncio.get_running_loop() is outer_loop
    assert asyncio.current_task() is outer_task
    await asyncio.sleep(0)
