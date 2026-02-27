import pytest

from vercel._internal.iter_coroutine import iter_coroutine


class _SuspendingAwaitable:
    def __await__(self):
        yield None
        return None


def test_iter_coroutine_returns_result_and_closes_coroutine() -> None:
    closed = False

    async def coro() -> str:
        nonlocal closed
        try:
            return "ok"
        finally:
            closed = True

    assert iter_coroutine(coro()) == "ok"
    assert closed


def test_iter_coroutine_raises_on_suspending_coroutine_and_closes_coroutine() -> None:
    closed = False

    async def coro() -> None:
        nonlocal closed
        try:
            await _SuspendingAwaitable()
        finally:
            closed = True

    with pytest.raises(RuntimeError, match="did not stop after one iteration"):
        iter_coroutine(coro())

    assert closed
