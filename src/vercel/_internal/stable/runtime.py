"""Shared lazy transport runtime for the stable client surface."""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass, field

from vercel._internal.http import (
    AsyncTransport,
    SyncTransport,
    create_base_async_client,
    create_base_client,
)
from vercel.stable.errors import TransportClosedError


@dataclass(frozen=True, slots=True)
class _Uninitialized:
    pass


@dataclass(frozen=True, slots=True)
class _Closed:
    pass


@dataclass(frozen=True, slots=True)
class _SyncInitialized:
    transport: SyncTransport


@dataclass(frozen=True, slots=True)
class _AsyncInitialized:
    transport: AsyncTransport


@dataclass(slots=True)
class SyncRuntime:
    _state: _Uninitialized | _SyncInitialized | _Closed = _Uninitialized()
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def ensure_connected(self, *, timeout: float | None = None) -> None:
        with self._lock:
            match self._state:
                case _Closed():
                    raise TransportClosedError(
                        "This Vercel client lineage has already been closed."
                    )
                case _Uninitialized():
                    self._state = _SyncInitialized(
                        transport=SyncTransport(create_base_client(timeout=timeout))
                    )
                case _SyncInitialized():
                    return

    async def get_transport(self, *, timeout: float | None = None) -> SyncTransport:
        # async def so the async-shaped backends can ``await`` it through
        # iter_coroutine.  Must never truly suspend.
        self.ensure_connected(timeout=timeout)
        match self._state:
            case _SyncInitialized(transport=transport):
                return transport
            case _Closed():
                raise TransportClosedError("This Vercel client lineage has already been closed.")
            case _Uninitialized():
                raise AssertionError("sync runtime should be initialized after ensure_connected()")

    def close(self) -> None:
        transport: SyncTransport | None = None
        with self._lock:
            match self._state:
                case _SyncInitialized(transport=existing_transport):
                    transport = existing_transport
                case _Uninitialized() | _Closed():
                    pass
            self._state = _Closed()

        if transport is not None:
            transport.close()


@dataclass(slots=True)
class AsyncRuntime:
    _state: _Uninitialized | _AsyncInitialized | _Closed = _Uninitialized()
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def ensure_connected(self, *, timeout: float | None = None) -> None:
        async with self._lock:
            match self._state:
                case _Closed():
                    raise TransportClosedError(
                        "This Vercel client lineage has already been closed."
                    )
                case _Uninitialized():
                    self._state = _AsyncInitialized(
                        transport=AsyncTransport(create_base_async_client(timeout=timeout))
                    )
                case _AsyncInitialized():
                    return

    async def get_transport(self, *, timeout: float | None = None) -> AsyncTransport:
        await self.ensure_connected(timeout=timeout)
        match self._state:
            case _AsyncInitialized(transport=transport):
                return transport
            case _Closed():
                raise TransportClosedError("This Vercel client lineage has already been closed.")
            case _Uninitialized():
                raise AssertionError("async runtime should be initialized after ensure_connected()")

    async def aclose(self) -> None:
        transport: AsyncTransport | None = None
        async with self._lock:
            match self._state:
                case _AsyncInitialized(transport=existing_transport):
                    transport = existing_transport
                case _Uninitialized() | _Closed():
                    pass
            self._state = _Closed()

        if transport is not None:
            await transport.aclose()


__all__ = ["AsyncRuntime", "SyncRuntime"]
