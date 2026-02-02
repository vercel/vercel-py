"""iter_coroutine - Run simple coroutines synchronously."""

from __future__ import annotations

import typing

_T = typing.TypeVar("_T")


def iter_coroutine(coro: typing.Coroutine[None, None, _T]) -> _T:
    """
    Execute a coroutine that completes in a single iteration.

    This function runs a coroutine synchronously by sending None to it once.
    It only works for coroutines that don't actually suspend (i.e., they
    complete immediately without yielding to an event loop).

    Args:
        coro: A coroutine that completes without suspending.

    Returns:
        The return value of the coroutine.

    Raises:
        RuntimeError: If the coroutine doesn't complete in one iteration.
    """
    try:
        coro.send(None)
    except StopIteration as ex:
        return ex.value  # type: ignore [no-any-return]
    else:
        raise RuntimeError(f"coroutine {coro!r} did not stop after one iteration!")
    finally:
        coro.close()


__all__ = ["iter_coroutine"]
