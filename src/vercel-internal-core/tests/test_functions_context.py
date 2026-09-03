from __future__ import annotations

from collections.abc import Awaitable

import anyio
import pytest

from vercel.functions.context import (
    WaitUntil,
    get_wait_until,
    set_wait_until,
)


@pytest.fixture(autouse=True)
def clear_wait_until() -> None:
    set_wait_until(None)


def _callback(_: Awaitable[object]) -> None: ...


def test_wait_until_defaults_to_none() -> None:
    assert get_wait_until() is None


def test_set_and_clear_wait_until() -> None:
    set_wait_until(_callback)
    assert get_wait_until() is _callback

    set_wait_until(None)
    assert get_wait_until() is None


@pytest.mark.anyio
async def test_concurrent_contexts_are_isolated() -> None:
    first = _callback

    def second(_: Awaitable[object]) -> None: ...

    first_ready = anyio.Event()
    second_ready = anyio.Event()

    async def observe(
        callback: WaitUntil,
        ready: anyio.Event,
        peer_ready: anyio.Event,
    ) -> None:
        set_wait_until(callback)
        ready.set()
        await peer_ready.wait()
        assert get_wait_until() is callback

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(observe, first, first_ready, second_ready)
        task_group.start_soon(observe, second, second_ready, first_ready)

    assert get_wait_until() is None
