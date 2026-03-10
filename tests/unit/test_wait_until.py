from __future__ import annotations

import pytest

from vercel.wait_until import callback_context, wait_until


def test_returns_none_when_no_callback_is_set() -> None:
    def my_task() -> None:
        pass

    wait_until(my_task)


def test_delegates_awaitable_to_callback() -> None:
    received: list[object] = []

    def fake_callback(work: object) -> None:
        received.append(work)

    async def my_coro() -> None:
        pass

    coro = my_coro()
    with callback_context(fake_callback):
        wait_until(coro)

    coro.close()
    assert len(received) == 1


def test_delegates_callable_to_callback() -> None:
    received: list[object] = []

    def fake_callback(work: object) -> None:
        received.append(work)

    def my_task() -> None:
        pass

    with callback_context(fake_callback):
        wait_until(my_task)

    assert received == [my_task]


def test_rejects_non_work_values() -> None:
    with pytest.raises(TypeError, match="awaitable or zero-argument callable"):
        wait_until(42)  # type: ignore[arg-type]


def test_callback_context_resets_after_exit() -> None:
    received: list[object] = []

    def fake_callback(work: object) -> None:
        received.append(work)

    def task_a() -> None:
        pass

    def task_b() -> None:
        pass

    with callback_context(fake_callback):
        wait_until(task_a)

    assert len(received) == 1

    # After exiting the context, callback should be None again.
    wait_until(task_b)
    assert len(received) == 1


def test_importable_from_vercel_functions() -> None:
    from vercel.functions import wait_until as fn_wait_until

    assert fn_wait_until is wait_until
