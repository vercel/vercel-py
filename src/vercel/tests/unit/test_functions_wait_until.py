from __future__ import annotations

import asyncio
from typing import Any

import pytest

from vercel.cache.context import set_context
from vercel.functions import wait_until


def sync_work() -> str:
    return "done"


@pytest.fixture(autouse=True)
def clear_wait_until_context() -> Any:
    set_context(wait_until=None)
    yield
    set_context(wait_until=None)


def test_wait_until_registers_awaitable_with_context() -> None:
    registered: list[Any] = []

    async def work() -> None:
        return

    coroutine = work()
    set_context(wait_until=registered.append)
    wait_until(coroutine)

    assert registered == [coroutine]
    coroutine.close()


def test_wait_until_is_a_noop_without_context() -> None:
    async def work() -> None:
        return

    wait_until(work())


@pytest.mark.parametrize("value", [None, 1, False, "work", object()])
def test_wait_until_rejects_non_awaitables(value: object) -> None:
    with pytest.raises(TypeError, match="awaitable"):
        wait_until(value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [sync_work, lambda: None])
def test_wait_until_rejects_uncalled_functions_with_a_hint(value: object) -> None:
    with pytest.raises(TypeError, match=r"asyncio\.to_thread"):
        wait_until(value)  # type: ignore[arg-type]


def test_wait_until_runs_synchronous_work_through_to_thread() -> None:
    registered: list[Any] = []

    set_context(wait_until=registered.append)
    wait_until(asyncio.to_thread(sync_work))

    assert len(registered) == 1
    assert asyncio.run(_await(registered[0])) == "done"


async def _await(awaitable: Any) -> Any:
    return await awaitable
