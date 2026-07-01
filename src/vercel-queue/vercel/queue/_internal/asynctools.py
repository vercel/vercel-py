from __future__ import annotations

from typing import Any, TypeGuard, TypeVar

from collections.abc import AsyncIterator, Awaitable, Coroutine, Iterable, Iterator

T = TypeVar("T")


def _is_coroutine(value: Awaitable[T]) -> TypeGuard[Coroutine[Any, Any, T]]:
    return isinstance(value, Coroutine)


def iter_coroutine(coro: Coroutine[Any, Any, T]) -> T:
    """Execute a non-suspending coroutine synchronously."""
    try:
        coro.send(None)
    except StopIteration as exc:
        return exc.value
    else:
        raise RuntimeError(f"coroutine {coro!r} did not stop after one iteration!")
    finally:
        coro.close()


def iter_async_iterator(iterator: AsyncIterator[T]) -> Iterator[T]:
    """Iterate over a non-suspending async iterator synchronously."""
    while True:
        try:
            next_item = anext(iterator)
            if not _is_coroutine(next_item):
                raise TypeError("async iterator __anext__() must return a coroutine")
            yield iter_coroutine(next_item)
        except StopAsyncIteration:
            return


async def iter_bytes_async(payload: Iterable[bytes]) -> AsyncIterator[bytes]:
    for chunk in payload:
        yield chunk
