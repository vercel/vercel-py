from __future__ import annotations

from typing import Any

import pytest

from vercel.cache.context import set_context
from vercel.functions import wait_until


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
