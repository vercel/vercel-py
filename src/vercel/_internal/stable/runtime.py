"""Shared lazy transport runtime for the stable client surface."""

from __future__ import annotations

from dataclasses import dataclass

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

    @property
    def is_initialized(self) -> bool:
        return isinstance(self._state, _SyncInitialized)

    async def ensure_connected(self, *, timeout: float | None = None) -> None:
        match self._state:
            case _Closed():
                _raise_closed_lineage()
            case _Uninitialized():
                self._state = _SyncInitialized(
                    transport=SyncTransport(create_base_client(timeout=timeout))
                )
            case _SyncInitialized():
                return

    async def get_transport(self, *, timeout: float | None = None) -> SyncTransport:
        await self.ensure_connected(timeout=timeout)
        match self._state:
            case _SyncInitialized(transport=transport):
                return transport
            case _Closed():
                raise TransportClosedError("This Vercel client lineage has already been closed.")
            case _Uninitialized():
                raise AssertionError("sync runtime should be initialized after ensure_connected()")

    def close(self) -> None:
        match self._state:
            case _SyncInitialized(transport=transport):
                transport.close()
            case _Uninitialized() | _Closed():
                pass

        self._state = _Closed()


@dataclass(slots=True)
class AsyncRuntime:
    _state: _Uninitialized | _AsyncInitialized | _Closed = _Uninitialized()

    @property
    def is_initialized(self) -> bool:
        return isinstance(self._state, _AsyncInitialized)

    async def ensure_connected(self, *, timeout: float | None = None) -> None:
        match self._state:
            case _Closed():
                _raise_closed_lineage()
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
        match self._state:
            case _AsyncInitialized(transport=transport):
                await transport.aclose()
            case _Uninitialized() | _Closed():
                pass

        self._state = _Closed()


__all__ = ["AsyncRuntime", "SyncRuntime"]
