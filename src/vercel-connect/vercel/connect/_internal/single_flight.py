"""Per-key de-duplication of concurrent cold token fetches.

Sync and async genuinely differ here: threads must block on a mutex, while tasks
must await a future, because blocking the event loop thread while the holder
awaits the network would deadlock. Shared orchestration stays in the service and
the differing primitive is injected.
"""

import asyncio
import threading
from collections.abc import Awaitable, Callable
from typing import Protocol

from vercel.connect._internal.cache import TokenCacheKey
from vercel.connect._internal.state import ConnectTokenState

Loader = Callable[[], Awaitable[ConnectTokenState]]
Reader = Callable[[], ConnectTokenState | None]


class SingleFlight(Protocol):
    """Collapses concurrent loads of one cache key into a single request."""

    async def run(
        self,
        key: TokenCacheKey,
        *,
        read: Reader,
        load: Loader,
    ) -> ConnectTokenState: ...

    def __len__(self) -> int:
        """Number of keys currently being resolved. Intended for tests."""
        ...


class SyncSingleFlight:
    """Thread-based de-duplication.

    Never suspends, so it is safe under `iter_coroutine`.
    """

    def __init__(self) -> None:
        self._guard = threading.Lock()
        # Reference-counted so the table cannot grow without bound: cache keys
        # embed a digest of the platform identity token, and those rotate, so a
        # long-lived process would otherwise accumulate one lock per token
        # generation per request shape.
        self._locks: dict[TokenCacheKey, tuple[threading.Lock, list[int]]] = {}

    def _acquire_slot(self, key: TokenCacheKey) -> tuple[threading.Lock, list[int]]:
        with self._guard:
            slot = self._locks.get(key)
            if slot is None:
                slot = (threading.Lock(), [0])
                self._locks[key] = slot
            slot[1][0] += 1
            return slot

    def _release_slot(self, key: TokenCacheKey, waiters: list[int]) -> None:
        with self._guard:
            waiters[0] -= 1
            if waiters[0] <= 0:
                self._locks.pop(key, None)

    async def run(
        self,
        key: TokenCacheKey,
        *,
        read: Reader,
        load: Loader,
    ) -> ConnectTokenState:
        lock, waiters = self._acquire_slot(key)
        try:
            with lock:
                # The holder may have populated the cache while this thread waited.
                cached = read()
                if cached is not None:
                    return cached
                return await load()
        finally:
            self._release_slot(key, waiters)

    def __len__(self) -> int:
        """Number of keys currently being resolved. Intended for tests."""
        with self._guard:
            return len(self._locks)


class _HolderCancelled(Exception):
    """Internal signal that the caller running a fetch went away."""


class AsyncSingleFlight:
    """Task-based de-duplication over a per-key future.

    The caller that runs the fetch holds no privilege over the rest. A server
    cancels request tasks on client disconnect and on deadline, and the callers
    coalesced onto one fetch are unrelated to each other, so a cancellation must
    reach only the caller that was cancelled. When that caller was the one
    fetching, a waiter takes over.
    """

    def __init__(self) -> None:
        self._pending: dict[TokenCacheKey, asyncio.Future[ConnectTokenState]] = {}

    async def run(
        self,
        key: TokenCacheKey,
        *,
        read: Reader,
        load: Loader,
    ) -> ConnectTokenState:
        while True:
            pending = self._pending.get(key)
            if pending is None:
                return await self._fetch(key, load)
            try:
                return await asyncio.shield(pending)
            except _HolderCancelled:
                # The fetch may still have completed and cached before its caller
                # was cancelled.
                cached = read()
                if cached is not None:
                    return cached

    async def _fetch(self, key: TokenCacheKey, load: Loader) -> ConnectTokenState:
        future: asyncio.Future[ConnectTokenState] = asyncio.get_running_loop().create_future()
        self._pending[key] = future
        try:
            state = await load()
        except BaseException as exc:
            # Cancellation is reported to waiters as a handover, not as a failure:
            # they did not ask to be cancelled, and one of them can fetch instead.
            outcome = _HolderCancelled() if isinstance(exc, asyncio.CancelledError) else exc
            future.set_exception(outcome)
            # Nobody may be awaiting this future; retrieving the exception keeps
            # asyncio from reporting it as never-retrieved.
            future.exception()
            raise
        else:
            future.set_result(state)
            return state
        finally:
            self._pending.pop(key, None)

    def __len__(self) -> int:
        """Number of keys currently being resolved. Intended for tests."""
        return len(self._pending)


__all__ = ["AsyncSingleFlight", "SingleFlight", "SyncSingleFlight"]
