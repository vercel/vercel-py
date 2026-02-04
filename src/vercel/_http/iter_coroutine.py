"""iter_coroutine - Run simple coroutines synchronously."""

from __future__ import annotations

import typing

_T = typing.TypeVar("_T")


def iter_coroutine(coro: typing.Coroutine[None, None, _T]) -> _T:
    """Execute a non-suspending coroutine synchronously."""
    try:
        coro.send(None)
    except StopIteration as ex:
        return ex.value  # type: ignore [no-any-return]
    else:
        raise RuntimeError(f"coroutine {coro!r} did not stop after one iteration!")
    finally:
        coro.close()


__all__ = ["iter_coroutine"]
